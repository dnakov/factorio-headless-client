use super::BinaryReader;
use crate::error::Result;

#[derive(Debug, Clone)]
pub struct MapEntity {
    pub name: String,
    pub x: f64,
    pub y: f64,
    pub direction: u8,
}

#[derive(Debug, Clone)]
pub struct MapTile {
    pub name: String,
    pub x: i32,
    pub y: i32,
    pub procedural: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MapVersion {
    pub major: u16,
    pub minor: u16,
    pub patch: u16,
    pub build: u16,
    pub quality_version: u8,
}

impl MapVersion {
    pub fn read(reader: &mut BinaryReader) -> Result<Self> {
        Ok(Self {
            major: reader.read_u16_le()?,
            minor: reader.read_u16_le()?,
            patch: reader.read_u16_le()?,
            build: reader.read_u16_le()?,
            quality_version: reader.read_u8()?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct SurfaceData {
    pub name: String,
    pub index: u16,
    pub chunks: Vec<ChunkData>,
}

#[derive(Debug, Clone)]
pub struct ChunkData {
    pub position: (i32, i32),
    pub entities: Vec<EntityData>,
    pub tiles: Vec<TileData>,
    pub decoratives: Vec<DecorativeData>,
}

#[derive(Debug, Clone)]
pub struct EntityData {
    pub prototype_id: u16,
    pub position: (f64, f64),
    pub flags: u16,
    pub bbox_min: (f64, f64),
    pub bbox_max: (f64, f64),
}

#[derive(Debug, Clone)]
pub struct TileData {
    pub prototype_id: u16,
    pub x: u8,
    pub y: u8,
    pub variation: u8,
}

#[derive(Debug, Clone)]
pub struct DecorativeData {
    pub prototype_id: u16,
    pub x: u8,
    pub y: u8,
    pub variation: u8,
}
