use crate::error::{Error, Result};
use super::types::{Fixed32, MapPosition, Direction, Color, SignalType};

/// Binary reader for Factorio protocol data
pub struct BinaryReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> BinaryReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    pub fn remaining_slice(&self) -> &[u8] {
        &self.data[self.pos..]
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn set_position(&mut self, pos: usize) {
        self.pos = pos;
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    pub fn peek(&self) -> Option<u8> {
        self.data.get(self.pos).copied()
    }

    pub fn skip(&mut self, n: usize) -> Result<()> {
        if self.remaining() < n {
            return Err(Error::UnexpectedEof);
        }
        self.pos += n;
        Ok(())
    }

    pub fn read_bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(Error::UnexpectedEof);
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    pub fn read_u8(&mut self) -> Result<u8> {
        if self.remaining() < 1 {
            return Err(Error::UnexpectedEof);
        }
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }

    pub fn read_i8(&mut self) -> Result<i8> {
        Ok(self.read_u8()? as i8)
    }

    pub fn read_bool(&mut self) -> Result<bool> {
        Ok(self.read_u8()? != 0)
    }

    pub fn read_u16_le(&mut self) -> Result<u16> {
        let bytes = self.read_bytes(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    pub fn read_u16_be(&mut self) -> Result<u16> {
        let bytes = self.read_bytes(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    pub fn read_i16_le(&mut self) -> Result<i16> {
        Ok(self.read_u16_le()? as i16)
    }

    pub fn read_u32_le(&mut self) -> Result<u32> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn read_i32_le(&mut self) -> Result<i32> {
        Ok(self.read_u32_le()? as i32)
    }

    pub fn read_u64_le(&mut self) -> Result<u64> {
        let bytes = self.read_bytes(8)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub fn read_f32_le(&mut self) -> Result<f32> {
        let bytes = self.read_bytes(4)?;
        Ok(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn read_f64_le(&mut self) -> Result<f64> {
        let bytes = self.read_bytes(8)?;
        Ok(f64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    /// Read a variable-length unsigned integer (Factorio's "optUint" format)
    /// First byte indicates size: 0xFF means next 4 bytes are u32, else it's the value
    pub fn read_opt_u32(&mut self) -> Result<u32> {
        self.read_opt(Self::read_u32_le)
    }

    /// Read a variable-length unsigned 16-bit (similar to opt_u32)
    pub fn read_opt_u16(&mut self) -> Result<u16> {
        self.read_opt(Self::read_u16_le)
    }

    fn read_opt<T: From<u8>>(&mut self, read_full: fn(&mut Self) -> Result<T>) -> Result<T> {
        let first = self.read_u8()?;
        if first == 0xFF {
            read_full(self)
        } else {
            Ok(T::from(first))
        }
    }

    /// Read a Factorio string (length-prefixed with VarInt/opt_u32)
    pub fn read_string(&mut self) -> Result<String> {
        let len = self.read_opt_u32()? as usize;
        if len > 1024 * 1024 {
            return Err(Error::StringTooLong { len, max: 1024 * 1024 });
        }
        let bytes = self.read_bytes(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| Error::InvalidPacket("invalid UTF-8 string".into()))
    }

    /// Read a string with known length (length already parsed separately)
    pub fn read_string_with_len(&mut self, len: usize) -> Result<String> {
        if len > 1024 * 1024 {
            return Err(Error::StringTooLong { len, max: 1024 * 1024 });
        }
        let bytes = self.read_bytes(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| Error::InvalidPacket("invalid UTF-8 string".into()))
    }

    /// Read a SimpleString (length-prefixed with VarShort/opt_u16)
    /// Used in connection handshake messages
    pub fn read_simple_string(&mut self) -> Result<String> {
        let len = self.read_opt_u16()? as usize;
        if len > 65535 {
            return Err(Error::StringTooLong { len, max: 65535 });
        }
        let bytes = self.read_bytes(len)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| Error::InvalidPacket("invalid UTF-8 string".into()))
    }

    /// Read a Factorio string with empty check
    pub fn read_string_opt(&mut self) -> Result<Option<String>> {
        let is_empty = self.read_bool()?;
        if is_empty {
            Ok(None)
        } else {
            Ok(Some(self.read_string()?))
        }
    }

    /// Read a fixed-point value
    pub fn read_fixed32(&mut self) -> Result<Fixed32> {
        Ok(Fixed32(self.read_i32_le()?))
    }

    /// Read a map position (two fixed32 values)
    pub fn read_map_position(&mut self) -> Result<MapPosition> {
        Ok(MapPosition {
            x: self.read_fixed32()?,
            y: self.read_fixed32()?,
        })
    }

    /// Read a direction (8-way)
    pub fn read_direction(&mut self) -> Result<Direction> {
        let v = self.read_u8()?;
        Direction::from_u8(v)
            .ok_or_else(|| Error::InvalidPacket(format!("invalid direction: {v}")))
    }

    /// Read a color (RGBA)
    pub fn read_color(&mut self) -> Result<Color> {
        Ok(Color {
            r: self.read_u8()?,
            g: self.read_u8()?,
            b: self.read_u8()?,
            a: self.read_u8()?,
        })
    }

    /// Read a signal type
    pub fn read_signal_type(&mut self) -> Result<SignalType> {
        let v = self.read_u8()?;
        SignalType::from_u8(v)
            .ok_or_else(|| Error::InvalidPacket(format!("invalid signal type: {v}")))
    }

    /// Read remaining bytes
    pub fn read_remaining(&mut self) -> &'a [u8] {
        let slice = &self.data[self.pos..];
        self.pos = self.data.len();
        slice
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_primitives() {
        let data = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07];
        let mut reader = BinaryReader::new(&data);

        assert_eq!(reader.read_u8().unwrap(), 0x01);
        assert_eq!(reader.read_u16_le().unwrap(), 0x0302);
        assert_eq!(reader.read_u32_le().unwrap(), 0x07060504);
        assert!(reader.is_empty());
    }

    #[test]
    fn test_read_opt_u32() {
        // Small value
        let data = [0x42];
        let mut reader = BinaryReader::new(&data);
        assert_eq!(reader.read_opt_u32().unwrap(), 0x42);

        // Large value
        let data = [0xFF, 0x01, 0x02, 0x03, 0x04];
        let mut reader = BinaryReader::new(&data);
        assert_eq!(reader.read_opt_u32().unwrap(), 0x04030201);
    }

    #[test]
    fn test_read_string() {
        let data = [0x05, b'h', b'e', b'l', b'l', b'o'];
        let mut reader = BinaryReader::new(&data);
        assert_eq!(reader.read_string().unwrap(), "hello");
    }

    #[test]
    fn test_read_fixed32() {
        // 1.5 tiles = 384 units
        let data = [0x80, 0x01, 0x00, 0x00];
        let mut reader = BinaryReader::new(&data);
        let fixed = reader.read_fixed32().unwrap();
        assert_eq!(fixed.0, 384);
        assert!((fixed.to_tiles() - 1.5).abs() < 0.001);
    }
}
