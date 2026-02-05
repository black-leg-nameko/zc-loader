pub mod buffer;
pub mod format;
pub mod mmap;
pub mod prefetch;
pub mod reader;

use reader::{MultiShardReader, ReaderError};
use prefetch::{create_prefetcher, Prefetcher, PrefetchError};
use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataLoaderError {
    #[error("Reader error: {0}")]
    Reader(#[from] ReaderError),
    #[error("Prefetch error: {0}")]
    Prefetch(#[from] PrefetchError),
}

/// ゼロコピーデータローダー（メインAPI）
pub struct DataLoader {
    reader: MultiShardReader,
    prefetcher: Box<dyn Prefetcher>,
    shard_paths: Vec<PathBuf>,
    current_shard_index: usize,
}

impl DataLoader {
    /// 新しいデータローダーを作成
    pub fn new<P: AsRef<std::path::Path>>(shard_paths: &[P]) -> Result<Self, DataLoaderError> {
        let paths: Vec<PathBuf> = shard_paths.iter().map(|p| p.as_ref().to_path_buf()).collect();
        let reader = MultiShardReader::new(&paths)?;
        let prefetcher = create_prefetcher(32)?; // デフォルトのキュー深度

        Ok(Self {
            reader,
            prefetcher,
            shard_paths: paths,
            current_shard_index: 0,
        })
    }

    /// 指定されたインデックスのサンプルを取得（ゼロコピー）
    pub fn get_sample(&self, index: usize) -> Result<&[u8], DataLoaderError> {
        self.reader.get_sample(index).map_err(DataLoaderError::Reader)
    }

    /// 複数のサンプルを一度に取得
    pub fn get_batch(&self, indices: &[usize]) -> Result<Vec<&[u8]>, DataLoaderError> {
        self.reader.get_batch(indices).map_err(DataLoaderError::Reader)
    }

    /// 次のN個のシャードをプリフェッチ
    pub fn prefetch_next(&mut self, count: usize) -> Result<(), DataLoaderError> {
        let num_shards = self.reader.num_shards();
        if self.current_shard_index >= num_shards {
            return Ok(()); // すべてのシャードを読み込み済み
        }

        let end_index = (self.current_shard_index + count).min(num_shards);
        let paths_to_prefetch: Vec<PathBuf> = self
            .shard_paths
            [self.current_shard_index..end_index]
            .iter()
            .cloned()
            .collect();

        self.prefetcher
            .prefetch_files(&paths_to_prefetch)
            .map_err(DataLoaderError::Prefetch)?;

        self.current_shard_index = end_index;
        Ok(())
    }

    /// プリフェッチの完了を待つ
    pub fn wait_prefetch(&mut self) -> Result<(), DataLoaderError> {
        self.prefetcher.wait().map_err(DataLoaderError::Prefetch)
    }

    /// 総サンプル数を取得
    pub fn total_samples(&self) -> usize {
        self.reader.total_samples()
    }

    /// シャード数を取得
    pub fn num_shards(&self) -> usize {
        self.reader.num_shards()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{ShardHeader, ShardMetadata, SampleMetadata};
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_shard(data: &[&[u8]]) -> NamedTempFile {
        use format::SampleMetadata;
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
    fn test_data_loader() {
        let file1 = create_test_shard(&[b"sample1", b"sample2"]);
        let file2 = create_test_shard(&[b"sample3"]);

        let loader = DataLoader::new(&[file1.path(), file2.path()]).unwrap();
        assert_eq!(loader.total_samples(), 3);
        assert_eq!(loader.get_sample(0).unwrap(), b"sample1");
        assert_eq!(loader.get_sample(2).unwrap(), b"sample3");
    }
}