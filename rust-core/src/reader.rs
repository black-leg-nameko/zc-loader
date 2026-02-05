use crate::format::{ShardHeader, ShardMetadata};
use crate::mmap::{MmapError, MmapManager};
use std::io::Cursor;
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ReaderError {
    #[error("Mmap error: {0}")]
    Mmap(#[from] MmapError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid format: {0}")]
    InvalidFormat(String),
    #[error("Sample index out of bounds: {0}")]
    IndexOutOfBounds(usize),
}

/// シャードファイルを読み込むリーダー
pub struct ShardReader {
    mmap: MmapManager,
    header: ShardHeader,
    metadata: ShardMetadata,
    data_start: usize,
}

impl ShardReader {
    /// シャードファイルを開く
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, ReaderError> {
        let mmap = MmapManager::new(path)?;
        let data = mmap.as_slice();

        // ヘッダーを読み込む
        let mut cursor = Cursor::new(&data[..ShardHeader::SIZE]);
        let header = ShardHeader::read(&mut cursor)?;
        header.validate().map_err(|e| ReaderError::InvalidFormat(e))?;

        // メタデータを読み込む
        let metadata_start = header.metadata_offset as usize;
        let metadata_end = header.data_offset as usize;
        if metadata_end > data.len() || metadata_start >= metadata_end {
            return Err(ReaderError::InvalidFormat(
                "Invalid metadata offset".to_string(),
            ));
        }

        let mut cursor = Cursor::new(&data[metadata_start..metadata_end]);
        let metadata = ShardMetadata::read(&mut cursor)?;

        // データセクションの開始位置
        let data_start = header.data_offset as usize;
        if data_start > data.len() {
            return Err(ReaderError::InvalidFormat("Invalid data offset".to_string()));
        }

        Ok(Self {
            mmap,
            header,
            metadata,
            data_start,
        })
    }

    /// サンプル数を取得
    pub fn num_samples(&self) -> usize {
        self.metadata.num_samples as usize
    }

    /// 指定されたインデックスのサンプルを取得（ゼロコピー）
    pub fn get_sample(&self, index: usize) -> Result<&[u8], ReaderError> {
        if index >= self.metadata.samples.len() {
            return Err(ReaderError::IndexOutOfBounds(index));
        }

        let sample_meta = &self.metadata.samples[index];
        let offset = self.data_start + sample_meta.offset as usize;
        let size = sample_meta.size as usize;

        self.mmap
            .get_range(offset, size)
            .map_err(|e| ReaderError::Mmap(e))
    }

    /// 複数のサンプルを一度に取得
    pub fn get_batch(&self, indices: &[usize]) -> Result<Vec<&[u8]>, ReaderError> {
        indices.iter().map(|&idx| self.get_sample(idx)).collect()
    }

    /// ファイルパスを取得
    pub fn path(&self) -> &Path {
        self.mmap.path()
    }

    /// ヘッダーを取得
    pub fn header(&self) -> &ShardHeader {
        &self.header
    }

    /// メタデータを取得
    pub fn metadata(&self) -> &ShardMetadata {
        &self.metadata
    }
}

/// 複数のシャードを管理するリーダー
pub struct MultiShardReader {
    readers: Vec<ShardReader>,
    global_index: Vec<(usize, usize)>, // (shard_index, sample_index_in_shard)
}

impl MultiShardReader {
    /// 複数のシャードファイルからリーダーを作成
    pub fn new<P: AsRef<Path>>(paths: &[P]) -> Result<Self, ReaderError> {
        let mut readers = Vec::new();
        let mut global_index = Vec::new();

        for path in paths {
            let reader = ShardReader::new(path)?;
            let num_samples = reader.num_samples();
            let shard_index = readers.len();
            for sample_idx in 0..num_samples {
                global_index.push((shard_index, sample_idx));
            }
            readers.push(reader);
        }

        Ok(Self {
            readers,
            global_index,
        })
    }

    /// グローバルインデックスからサンプルを取得
    pub fn get_sample(&self, global_index: usize) -> Result<&[u8], ReaderError> {
        let (shard_idx, sample_idx) = self
            .global_index
            .get(global_index)
            .ok_or_else(|| ReaderError::IndexOutOfBounds(global_index))?;
        self.readers[*shard_idx].get_sample(*sample_idx)
    }

    /// バッチでサンプルを取得
    pub fn get_batch(&self, indices: &[usize]) -> Result<Vec<&[u8]>, ReaderError> {
        indices.iter().map(|&idx| self.get_sample(idx)).collect()
    }

    /// 総サンプル数を取得
    pub fn total_samples(&self) -> usize {
        self.global_index.len()
    }

    /// シャード数を取得
    pub fn num_shards(&self) -> usize {
        self.readers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{ShardHeader, ShardMetadata, SampleMetadata};
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_shard(data: &[&[u8]]) -> NamedTempFile {
        use crate::format::SampleMetadata;
        let mut file = NamedTempFile::new().unwrap();
        let mut buf = Vec::new();

        // ヘッダー用のスペースを確保（後で更新）
        let header_offset = 0;
        buf.resize(ShardHeader::SIZE, 0);

        // メタデータを準備
        let mut samples = Vec::new();
        let mut current_offset = 0u64;
        for sample_data in data {
            samples.push(SampleMetadata {
                offset: current_offset,
                size: sample_data.len() as u64,
            });
            current_offset += sample_data.len() as u64;
        }

        let metadata = ShardMetadata {
            num_samples: samples.len() as u64,
            samples,
        };

        // メタデータを書き込む
        let metadata_start = buf.len();
        metadata.write(&mut buf).unwrap();
        let metadata_end = buf.len();
        let metadata_offset = metadata_start as u64;

        // データセクションの開始位置
        let data_offset = metadata_end as u64;

        // ヘッダーを書き込む
        let header = ShardHeader::new(metadata_offset, data_offset);
        let mut header_buf = Vec::new();
        header.write(&mut header_buf).unwrap();
        buf[header_offset..header_offset + ShardHeader::SIZE]
            .copy_from_slice(&header_buf);

        // データを書き込む
        for sample_data in data {
            buf.extend_from_slice(sample_data);
        }

        file.write_all(&buf).unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_shard_reader() {
        let file = create_test_shard(&[b"sample1", b"sample2", b"sample3"]);
        let reader = ShardReader::new(file.path()).unwrap();

        assert_eq!(reader.num_samples(), 3);
        assert_eq!(reader.get_sample(0).unwrap(), b"sample1");
        assert_eq!(reader.get_sample(1).unwrap(), b"sample2");
        assert_eq!(reader.get_sample(2).unwrap(), b"sample3");
    }

    #[test]
    fn test_multi_shard_reader() {
        let file1 = create_test_shard(&[b"shard1_sample1", b"shard1_sample2"]);
        let file2 = create_test_shard(&[b"shard2_sample1"]);

        let reader = MultiShardReader::new(&[file1.path(), file2.path()]).unwrap();
        assert_eq!(reader.total_samples(), 3);
        assert_eq!(reader.get_sample(0).unwrap(), b"shard1_sample1");
        assert_eq!(reader.get_sample(1).unwrap(), b"shard1_sample2");
        assert_eq!(reader.get_sample(2).unwrap(), b"shard2_sample1");
    }
}