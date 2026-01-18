use super::types::{Fixed32, MapPosition, Direction, Color, SignalType};

/// Binary writer for Factorio protocol data
pub struct BinaryWriter {
    data: Vec<u8>,
}

impl BinaryWriter {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self { data: Vec::with_capacity(capacity) }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.data
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.data.extend_from_slice(bytes);
    }

    pub fn write_u8(&mut self, v: u8) {
        self.data.push(v);
    }

    pub fn write_i8(&mut self, v: i8) {
        self.write_u8(v as u8);
    }

    pub fn write_bool(&mut self, v: bool) {
        self.write_u8(if v { 1 } else { 0 });
    }

    pub fn write_u16_le(&mut self, v: u16) {
        self.data.extend_from_slice(&v.to_le_bytes());
    }

    pub fn write_u16_be(&mut self, v: u16) {
        self.data.extend_from_slice(&v.to_be_bytes());
    }

    pub fn write_i16_le(&mut self, v: i16) {
        self.write_u16_le(v as u16);
    }

    pub fn write_u32_le(&mut self, v: u32) {
        self.data.extend_from_slice(&v.to_le_bytes());
    }

    pub fn write_i32_le(&mut self, v: i32) {
        self.write_u32_le(v as u32);
    }

    pub fn write_u64_le(&mut self, v: u64) {
        self.data.extend_from_slice(&v.to_le_bytes());
    }

    pub fn write_f32_le(&mut self, v: f32) {
        self.data.extend_from_slice(&v.to_le_bytes());
    }

    pub fn write_f64_le(&mut self, v: f64) {
        self.data.extend_from_slice(&v.to_le_bytes());
    }

    /// Write a variable-length unsigned integer (Factorio's "optUint" format)
    pub fn write_opt_u32(&mut self, v: u32) {
        self.write_opt(v as u64, |s, v| s.write_u32_le(v as u32));
    }

    /// Write a variable-length unsigned 16-bit
    pub fn write_opt_u16(&mut self, v: u16) {
        self.write_opt(v as u64, |s, v| s.write_u16_le(v as u16));
    }

    fn write_opt(&mut self, v: u64, write_full: fn(&mut Self, u64)) {
        if v < 0xFF {
            self.write_u8(v as u8);
        } else {
            self.write_u8(0xFF);
            write_full(self, v);
        }
    }

    /// Write a Factorio string (length-prefixed with VarInt/opt_u32)
    pub fn write_string(&mut self, s: &str) {
        self.write_opt_u32(s.len() as u32);
        self.write_bytes(s.as_bytes());
    }

    /// Write a SimpleString (length-prefixed with VarShort/opt_u16)
    /// Used in connection handshake messages
    pub fn write_simple_string(&mut self, s: &str) {
        self.write_opt_u16(s.len() as u16);
        self.write_bytes(s.as_bytes());
    }

    /// Write an optional Factorio string
    pub fn write_string_opt(&mut self, s: Option<&str>) {
        match s {
            None => self.write_bool(true), // is_empty = true
            Some(s) => {
                self.write_bool(false); // is_empty = false
                self.write_string(s);
            }
        }
    }

    /// Write a fixed-point value
    pub fn write_fixed32(&mut self, v: Fixed32) {
        self.write_i32_le(v.0);
    }

    /// Write a map position
    pub fn write_map_position(&mut self, pos: MapPosition) {
        self.write_fixed32(pos.x);
        self.write_fixed32(pos.y);
    }

    /// Write a direction
    pub fn write_direction(&mut self, dir: Direction) {
        self.write_u8(dir as u8);
    }

    /// Write a color
    pub fn write_color(&mut self, color: Color) {
        self.write_u8(color.r);
        self.write_u8(color.g);
        self.write_u8(color.b);
        self.write_u8(color.a);
    }

    /// Write a signal type
    pub fn write_signal_type(&mut self, sig_type: SignalType) {
        self.write_u8(sig_type as u8);
    }
}

impl Default for BinaryWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl From<BinaryWriter> for Vec<u8> {
    fn from(writer: BinaryWriter) -> Self {
        writer.into_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::reader::BinaryReader;

    #[test]
    fn test_roundtrip_primitives() {
        let mut writer = BinaryWriter::new();
        writer.write_u8(0x42);
        writer.write_u16_le(0x1234);
        writer.write_u32_le(0xDEADBEEF);

        let data = writer.into_vec();
        let mut reader = BinaryReader::new(&data);

        assert_eq!(reader.read_u8().unwrap(), 0x42);
        assert_eq!(reader.read_u16_le().unwrap(), 0x1234);
        assert_eq!(reader.read_u32_le().unwrap(), 0xDEADBEEF);
    }

    #[test]
    fn test_roundtrip_opt_u32() {
        let values = [0u32, 1, 127, 254, 255, 256, 65535, 0xDEADBEEF];

        for &v in &values {
            let mut writer = BinaryWriter::new();
            writer.write_opt_u32(v);

            let data = writer.into_vec();
            let mut reader = BinaryReader::new(&data);

            assert_eq!(reader.read_opt_u32().unwrap(), v);
        }
    }

    #[test]
    fn test_roundtrip_string() {
        let mut writer = BinaryWriter::new();
        writer.write_string("hello world");

        let data = writer.into_vec();
        let mut reader = BinaryReader::new(&data);

        assert_eq!(reader.read_string().unwrap(), "hello world");
    }

    #[test]
    fn test_roundtrip_map_position() {
        let pos = MapPosition::from_tiles(123.456, -789.012);

        let mut writer = BinaryWriter::new();
        writer.write_map_position(pos);

        let data = writer.into_vec();
        let mut reader = BinaryReader::new(&data);

        let read_pos = reader.read_map_position().unwrap();
        assert_eq!(pos, read_pos);
    }
}
