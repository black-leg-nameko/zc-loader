use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

/// マジックナンバー: "ZCLD" (Zero Copy Loader)
pub const MAGIC: u32 = 0x5A434C44;

/// フォーマットバージョン
pub const FORMAT_VERSION: u16 = 1;

/// シャードファイルのヘッダー
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardHeader {
    pub magic: u32,
    pub version: u16,
    pub metadata_offset: u64,
    pub data_offset: u64,
}

impl ShardHeader {
    pub const SIZE: usize = 4 + 2 + 8 + 8; // 22 bytes

    pub fn new(metadata_offset: u64, data_offset: u64) -> Self {
        Self {
            magic: MAGIC,
            version: FORMAT_VERSION,
            metadata_offset,
            data_offset,
        }
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u32::<LittleEndian>(self.magic)?;
        writer.write_u16::<LittleEndian>(self.version)?;
        writer.write_u64::<LittleEndian>(self.metadata_offset)?;
        writer.write_u64::<LittleEndian>(self.data_offset)?;
        Ok(())
    }

    pub fn read<R: Read>(reader: &mut R) -> io::Result<Self> {
        let magic = reader.read_u32::<LittleEndian>()?;
        if magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid magic number: 0x{:08X}", magic),
            ));
        }
        let version = reader.read_u16::<LittleEndian>()?;
        if version != FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unsupported format version: {}", version),
            ));
        }
        let metadata_offset = reader.read_u64::<LittleEndian>()?;
        let data_offset = reader.read_u64::<LittleEndian>()?;
        Ok(Self {
            magic,
            version,
            metadata_offset,
            data_offset,
        })
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.magic != MAGIC {
            return Err(format!("Invalid magic number: 0x{:08X}", self.magic));
        }
        if self.version != FORMAT_VERSION {
            return Err(format!("Unsupported version: {}", self.version));
        }
        if self.metadata_offset >= self.data_offset {
            return Err("Invalid offset order".to_string());
        }
        Ok(())
    }
}

/// サンプルのメタデータ（インデックス内のエントリ）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SampleMetadata {
    pub offset: u64,  // データセクション内のオフセット
    pub size: u64,    // サンプルのサイズ（バイト）
}

/// シャードのメタデータ
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMetadata {
    pub num_samples: u64,
    pub samples: Vec<SampleMetadata>,
}

impl ShardMetadata {
    pub fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let json = serde_json::to_vec(self).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Serialization error: {}", e))
        })?;
        writer.write_u64::<LittleEndian>(json.len() as u64)?;
        writer.write_all(&json)?;
        Ok(())
    }

    pub fn read<R: Read>(reader: &mut R) -> io::Result<Self> {
        let json_len = reader.read_u64::<LittleEndian>()?;
        let mut json_buf = vec![0u8; json_len as usize];
        reader.read_exact(&mut json_buf)?;
        serde_json::from_slice(&json_buf).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Deserialization error: {}", e))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_header_roundtrip() {
        let header = ShardHeader::new(100, 200);
        let mut buf = Vec::new();
        header.write(&mut buf).unwrap();
        assert_eq!(buf.len(), ShardHeader::SIZE);

        let mut cursor = Cursor::new(&buf);
        let read_header = ShardHeader::read(&mut cursor).unwrap();
        assert_eq!(header.magic, read_header.magic);
        assert_eq!(header.version, read_header.version);
        assert_eq!(header.metadata_offset, read_header.metadata_offset);
        assert_eq!(header.data_offset, read_header.data_offset);
    }

    #[test]
    fn test_metadata_roundtrip() {
        let metadata = ShardMetadata {
            num_samples: 3,
            samples: vec![
                SampleMetadata { offset: 0, size: 100 },
                SampleMetadata { offset: 100, size: 200 },
                SampleMetadata { offset: 300, size: 150 },
            ],
        };

        let mut buf = Vec::new();
        metadata.write(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let read_metadata = ShardMetadata::read(&mut cursor).unwrap();
        assert_eq!(metadata.num_samples, read_metadata.num_samples);
        assert_eq!(metadata.samples.len(), read_metadata.samples.len());
    }
}