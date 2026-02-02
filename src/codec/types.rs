/// Fixed-point position (256 units per tile)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Fixed32(pub i32);

impl Fixed32 {
    pub const UNITS_PER_TILE: f64 = 256.0;

    pub fn from_tiles(tiles: f64) -> Self {
        Self((tiles * Self::UNITS_PER_TILE) as i32)
    }

    pub fn to_tiles(self) -> f64 {
        self.0 as f64 / Self::UNITS_PER_TILE
    }

    pub fn raw(self) -> i32 {
        self.0
    }
}

impl From<i32> for Fixed32 {
    fn from(v: i32) -> Self {
        Self(v)
    }
}

impl From<f64> for Fixed32 {
    fn from(v: f64) -> Self {
        Self::from_tiles(v)
    }
}

/// Map position in fixed-point coordinates
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct MapPosition {
    pub x: Fixed32,
    pub y: Fixed32,
}

impl MapPosition {
    pub fn new(x: impl Into<Fixed32>, y: impl Into<Fixed32>) -> Self {
        Self {
            x: x.into(),
            y: y.into(),
        }
    }

    pub fn from_tiles(x: f64, y: f64) -> Self {
        Self {
            x: Fixed32::from_tiles(x),
            y: Fixed32::from_tiles(y),
        }
    }

    pub fn to_tiles(self) -> (f64, f64) {
        (self.x.to_tiles(), self.y.to_tiles())
    }

    pub fn distance_to(self, other: Self) -> f64 {
        let dx = self.x.to_tiles() - other.x.to_tiles();
        let dy = self.y.to_tiles() - other.y.to_tiles();
        (dx * dx + dy * dy).sqrt()
    }
}

/// 8-way direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum Direction {
    #[default]
    North = 0,
    NorthEast = 1,
    East = 2,
    SouthEast = 3,
    South = 4,
    SouthWest = 5,
    West = 6,
    NorthWest = 7,
}

impl Direction {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::North),
            1 => Some(Self::NorthEast),
            2 => Some(Self::East),
            3 => Some(Self::SouthEast),
            4 => Some(Self::South),
            5 => Some(Self::SouthWest),
            6 => Some(Self::West),
            7 => Some(Self::NorthWest),
            _ => None,
        }
    }

    pub fn to_vector(self) -> (f64, f64) {
        // Use the f32 constant to ensure length^2 <= 1.0 for diagonals.
        const S: f64 = std::f32::consts::FRAC_1_SQRT_2 as f64;
        match self {
            Self::North => (0.0, -1.0),
            Self::NorthEast => (S, -S),
            Self::East => (1.0, 0.0),
            Self::SouthEast => (S, S),
            Self::South => (0.0, 1.0),
            Self::SouthWest => (-S, S),
            Self::West => (-1.0, 0.0),
            Self::NorthWest => (-S, -S),
        }
    }

    pub fn opposite(self) -> Self {
        match self {
            Self::North => Self::South,
            Self::NorthEast => Self::SouthWest,
            Self::East => Self::West,
            Self::SouthEast => Self::NorthWest,
            Self::South => Self::North,
            Self::SouthWest => Self::NorthEast,
            Self::West => Self::East,
            Self::NorthWest => Self::SouthEast,
        }
    }

    pub fn rotate_cw(self) -> Self {
        Self::from_u8((self as u8 + 1) % 8).unwrap()
    }

    pub fn rotate_ccw(self) -> Self {
        Self::from_u8((self as u8 + 7) % 8).unwrap()
    }
}

/// Tile position (integer coordinates)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct TilePosition {
    pub x: i32,
    pub y: i32,
}

impl TilePosition {
    pub fn new(x: i32, y: i32) -> Self {
        Self { x, y }
    }

    pub fn to_map_position(self) -> MapPosition {
        MapPosition::from_tiles(self.x as f64, self.y as f64)
    }
}

impl From<MapPosition> for TilePosition {
    fn from(pos: MapPosition) -> Self {
        let (x, y) = pos.to_tiles();
        Self::new(x.floor() as i32, y.floor() as i32)
    }
}

/// Chunk position (32x32 tile chunks)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ChunkPosition {
    pub x: i32,
    pub y: i32,
}

impl ChunkPosition {
    pub const CHUNK_SIZE: i32 = 32;

    pub fn new(x: i32, y: i32) -> Self {
        Self { x, y }
    }

    pub fn from_tile(tile: TilePosition) -> Self {
        Self {
            x: tile.x.div_euclid(Self::CHUNK_SIZE),
            y: tile.y.div_euclid(Self::CHUNK_SIZE),
        }
    }

    pub fn from_map_position(pos: MapPosition) -> Self {
        Self::from_tile(TilePosition::from(pos))
    }
}

/// Bounding box in map coordinates
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct BoundingBox {
    pub left_top: MapPosition,
    pub right_bottom: MapPosition,
}

impl BoundingBox {
    pub fn new(left_top: MapPosition, right_bottom: MapPosition) -> Self {
        Self {
            left_top,
            right_bottom,
        }
    }

    pub fn from_tiles(x1: f64, y1: f64, x2: f64, y2: f64) -> Self {
        Self {
            left_top: MapPosition::from_tiles(x1, y1),
            right_bottom: MapPosition::from_tiles(x2, y2),
        }
    }

    pub fn contains(&self, pos: MapPosition) -> bool {
        pos.x.0 >= self.left_top.x.0
            && pos.x.0 <= self.right_bottom.x.0
            && pos.y.0 >= self.left_top.y.0
            && pos.y.0 <= self.right_bottom.y.0
    }
}

/// Color (RGBA, 0-255)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Color {
    pub r: u8,
    pub g: u8,
    pub b: u8,
    pub a: u8,
}

impl Color {
    pub const fn new(r: u8, g: u8, b: u8, a: u8) -> Self {
        Self { r, g, b, a }
    }

    pub const fn rgb(r: u8, g: u8, b: u8) -> Self {
        Self::new(r, g, b, 255)
    }
}

/// Signal ID for circuit network
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SignalId {
    pub signal_type: SignalType,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SignalType {
    Item = 0,
    Fluid = 1,
    Virtual = 2,
}

impl SignalType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Item),
            1 => Some(Self::Fluid),
            2 => Some(Self::Virtual),
            _ => None,
        }
    }
}
