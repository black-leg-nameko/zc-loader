use memmap2::{Mmap, MmapOptions};
use std::fs::File;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MmapError {
    #[error("Failed to open file: {0}")]
    OpenFile(#[from] std::io::Error),
    #[error("Failed to create memory map: {0}")]
    MapError(String),
}

/// メモリマップされたファイルを管理する
pub struct MmapManager {
    #[allow(dead_code)] // ファイルを開いたまま保持するため
    file: File,
    mmap: Mmap,
    path: PathBuf,
}

impl MmapManager {
    /// ファイルを開いてメモリマップを作成
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, MmapError> {
        let path_buf = path.as_ref().to_path_buf();
        let file = File::open(&path_buf)?;
        let mmap = unsafe {
            MmapOptions::new()
                .map(&file)
                .map_err(|e| MmapError::MapError(e.to_string()))?
        };
        Ok(Self {
            file,
            mmap,
            path: path_buf,
        })
    }

    /// メモリマップされたデータへのスライス参照を取得
    pub fn as_slice(&self) -> &[u8] {
        &self.mmap
    }

    /// 指定された範囲のスライスを取得
    pub fn get_range(&self, offset: usize, len: usize) -> Result<&[u8], MmapError> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| MmapError::MapError("Offset overflow".to_string()))?;
        if end > self.mmap.len() {
            return Err(MmapError::MapError(format!(
                "Range out of bounds: offset={}, len={}, file_size={}",
                offset,
                len,
                self.mmap.len()
            )));
        }
        Ok(&self.mmap[offset..end])
    }

    /// ファイルパスを取得
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// ファイルサイズを取得
    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    /// ファイルが空かどうか
    pub fn is_empty(&self) -> bool {
        self.mmap.is_empty()
    }
}

/// 複数のメモリマップを管理する
pub struct MmapPool {
    maps: Vec<MmapManager>,
}

impl MmapPool {
    pub fn new() -> Self {
        Self { maps: Vec::new() }
    }

    /// ファイルを追加してメモリマップを作成
    pub fn add<P: AsRef<Path>>(&mut self, path: P) -> Result<usize, MmapError> {
        let index = self.maps.len();
        let manager = MmapManager::new(path)?;
        self.maps.push(manager);
        Ok(index)
    }

    /// 指定されたインデックスのメモリマップを取得
    pub fn get(&self, index: usize) -> Option<&MmapManager> {
        self.maps.get(index)
    }

    /// すべてのメモリマップを取得
    pub fn all(&self) -> &[MmapManager] {
        &self.maps
    }

    /// メモリマップの数を取得
    pub fn len(&self) -> usize {
        self.maps.len()
    }

    /// 空かどうか
    pub fn is_empty(&self) -> bool {
        self.maps.is_empty()
    }

    /// 総メモリ使用量を取得（バイト）
    pub fn total_size(&self) -> usize {
        self.maps.iter().map(|m| m.len()).sum()
    }
}

impl Default for MmapPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_mmap_manager() {
        let mut file = NamedTempFile::new().unwrap();
        let data = b"Hello, World!";
        file.write_all(data).unwrap();
        file.flush().unwrap();

        let manager = MmapManager::new(file.path()).unwrap();
        assert_eq!(manager.len(), data.len());
        assert_eq!(manager.as_slice(), data);
    }

    #[test]
    fn test_mmap_range() {
        let mut file = NamedTempFile::new().unwrap();
        let data = b"Hello, World!";
        file.write_all(data).unwrap();
        file.flush().unwrap();

        let manager = MmapManager::new(file.path()).unwrap();
        let slice = manager.get_range(0, 5).unwrap();
        assert_eq!(slice, b"Hello");
    }

    #[test]
    fn test_mmap_pool() {
        let mut file1 = NamedTempFile::new().unwrap();
        file1.write_all(b"File 1").unwrap();
        file1.flush().unwrap();

        let mut file2 = NamedTempFile::new().unwrap();
        file2.write_all(b"File 2").unwrap();
        file2.flush().unwrap();

        let mut pool = MmapPool::new();
        pool.add(file1.path()).unwrap();
        pool.add(file2.path()).unwrap();

        assert_eq!(pool.len(), 2);
        assert_eq!(pool.get(0).unwrap().as_slice(), b"File 1");
        assert_eq!(pool.get(1).unwrap().as_slice(), b"File 2");
    }
}