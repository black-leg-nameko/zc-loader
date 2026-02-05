use std::marker::PhantomData;
use std::slice;

/// ゼロコピーバッファ：mmapされたメモリ領域への型安全なアクセス
pub struct ZeroCopyBuffer<'a> {
    data: &'a [u8],
}

impl<'a> ZeroCopyBuffer<'a> {
    /// スライスからバッファを作成
    pub fn from_slice(data: &'a [u8]) -> Self {
        Self { data }
    }

    /// 生のバイトスライスを取得
    pub fn as_bytes(&self) -> &'a [u8] {
        self.data
    }

    /// u8のスライスとして取得
    pub fn as_u8(&self) -> &'a [u8] {
        self.data
    }

    /// u16のスライスとして取得（リトルエンディアン）
    pub fn as_u16(&self) -> Result<&'a [u16], BufferError> {
        if self.data.len() % 2 != 0 {
            return Err(BufferError::InvalidAlignment);
        }
        Ok(unsafe { slice::from_raw_parts(self.data.as_ptr() as *const u16, self.data.len() / 2) })
    }

    /// u32のスライスとして取得（リトルエンディアン）
    pub fn as_u32(&self) -> Result<&'a [u32], BufferError> {
        if self.data.len() % 4 != 0 {
            return Err(BufferError::InvalidAlignment);
        }
        Ok(unsafe { slice::from_raw_parts(self.data.as_ptr() as *const u32, self.data.len() / 4) })
    }

    /// u64のスライスとして取得（リトルエンディアン）
    pub fn as_u64(&self) -> Result<&'a [u64], BufferError> {
        if self.data.len() % 8 != 0 {
            return Err(BufferError::InvalidAlignment);
        }
        Ok(unsafe { slice::from_raw_parts(self.data.as_ptr() as *const u64, self.data.len() / 8) })
    }

    /// f32のスライスとして取得
    pub fn as_f32(&self) -> Result<&'a [f32], BufferError> {
        if self.data.len() % 4 != 0 {
            return Err(BufferError::InvalidAlignment);
        }
        Ok(unsafe { slice::from_raw_parts(self.data.as_ptr() as *const f32, self.data.len() / 4) })
    }

    /// f64のスライスとして取得
    pub fn as_f64(&self) -> Result<&'a [f64], BufferError> {
        if self.data.len() % 8 != 0 {
            return Err(BufferError::InvalidAlignment);
        }
        Ok(unsafe { slice::from_raw_parts(self.data.as_ptr() as *const f64, self.data.len() / 8) })
    }

    /// バッファのサイズを取得
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// バッファが空かどうか
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

/// バッファエラー
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferError {
    InvalidAlignment,
}

impl std::fmt::Display for BufferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BufferError::InvalidAlignment => {
                write!(f, "Buffer is not properly aligned for the requested type")
            }
        }
    }
}

impl std::error::Error for BufferError {}

/// 型安全なビューを提供するジェネリックバッファ
pub struct TypedBuffer<'a, T> {
    data: &'a [T],
    _phantom: PhantomData<&'a T>,
}

impl<'a, T> TypedBuffer<'a, T> {
    /// スライスから作成
    pub fn from_slice(data: &'a [T]) -> Self {
        Self {
            data,
            _phantom: PhantomData,
        }
    }

    /// データスライスを取得
    pub fn as_slice(&self) -> &'a [T] {
        self.data
    }

    /// 要素数を取得
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// 空かどうか
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// インデックスで要素を取得
    pub fn get(&self, index: usize) -> Option<&'a T> {
        self.data.get(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_copy_buffer_u8() {
        let data = vec![1u8, 2, 3, 4, 5];
        let buffer = ZeroCopyBuffer::from_slice(&data);
        assert_eq!(buffer.as_u8(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_zero_copy_buffer_u16() {
        let data = vec![0x01u8, 0x00, 0x02, 0x00, 0x03, 0x00]; // [1, 2, 3] in little-endian
        let buffer = ZeroCopyBuffer::from_slice(&data);
        let u16_slice = buffer.as_u16().unwrap();
        assert_eq!(u16_slice, &[1, 2, 3]);
    }

    #[test]
    fn test_zero_copy_buffer_f32() {
        let data = vec![0u8; 16]; // 4 f32s
        let buffer = ZeroCopyBuffer::from_slice(&data);
        let f32_slice = buffer.as_f32().unwrap();
        assert_eq!(f32_slice.len(), 4);
    }

    #[test]
    fn test_alignment_error() {
        let data = vec![1u8, 2, 3]; // 3 bytes - not aligned for u16
        let buffer = ZeroCopyBuffer::from_slice(&data);
        assert_eq!(buffer.as_u16(), Err(BufferError::InvalidAlignment));
    }
}