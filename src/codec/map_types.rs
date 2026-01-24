use super::BinaryReader;
use crate::error::Result;
use crate::lua::prototype::Prototypes;

#[derive(Debug, Clone)]
pub struct MapEntity {
    pub name: String,
    pub x: f64,
    pub y: f64,
    pub direction: u8,
    pub col_x1: f64,
    pub col_y1: f64,
    pub col_x2: f64,
    pub col_y2: f64,
    pub collides_player: bool,
}

impl MapEntity {
    pub fn tile_width(&self) -> f64 {
        (self.col_x2 - self.col_x1).ceil()
    }

    pub fn tile_height(&self) -> f64 {
        (self.col_y2 - self.col_y1).ceil()
    }
}

pub fn check_player_collision(entities: &[MapEntity], px: f64, py: f64) -> bool {
    let p_half = 0.2;
    entities.iter().any(|e| {
        e.collides_player
            && px - p_half < e.x + e.col_x2
            && px + p_half > e.x + e.col_x1
            && py - p_half < e.y + e.col_y2
            && py + p_half > e.y + e.col_y1
    })
}

pub fn entity_collision_box(name: &str) -> ([f64; 4], bool) {
    if let Some(proto) = Prototypes::global().and_then(|p| p.entity(name)) {
        return (proto.collision_box, proto.collides_player);
    }
    // Fallback for entities not loaded from Lua prototypes
    let half = match name {
        n if n.contains("crash-site-spaceship-wreck-big") => 1.2,
        n if n.contains("crash-site-spaceship-wreck-medium") => 0.9,
        "crash-site-spaceship" => 2.2,
        n if n.contains("spawner") || n.contains("worm") => 1.2,
        n if n.contains("locomotive") || n.contains("cargo-wagon")
            || n.contains("fluid-wagon") || n.contains("artillery-wagon") => 2.7,
        "huge-rock" | "big-sand-rock" | "big-rock" => 0.9,
        _ => 0.4,
    };
    ([-half, -half, half, half], true)
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
