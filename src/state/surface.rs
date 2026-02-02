use std::collections::HashMap;
use crate::codec::{ChunkPosition, TilePosition, MapPosition};
use crate::state::entity::{Entity, EntityId};

/// Surface identifier
pub type SurfaceId = u32;

/// A game surface (e.g., nauvis, space platforms)
#[derive(Debug, Clone)]
pub struct Surface {
    pub id: SurfaceId,
    pub name: String,
    pub chunks: HashMap<ChunkPosition, Chunk>,
    pub entities: HashMap<EntityId, Entity>,
}

impl Surface {
    pub fn new(id: SurfaceId, name: String) -> Self {
        Self {
            id,
            name,
            chunks: HashMap::new(),
            entities: HashMap::new(),
        }
    }

    pub fn get_chunk(&self, pos: ChunkPosition) -> Option<&Chunk> {
        self.chunks.get(&pos)
    }

    pub fn get_chunk_mut(&mut self, pos: ChunkPosition) -> Option<&mut Chunk> {
        self.chunks.get_mut(&pos)
    }

    pub fn get_or_create_chunk(&mut self, pos: ChunkPosition) -> &mut Chunk {
        self.chunks.entry(pos).or_insert_with(|| Chunk::new(pos))
    }

    pub fn get_tile(&self, pos: TilePosition) -> Option<&Tile> {
        let chunk_pos = ChunkPosition::from_tile(pos);
        let local_x = pos.x.rem_euclid(32) as usize;
        let local_y = pos.y.rem_euclid(32) as usize;
        self.chunks.get(&chunk_pos)
            .and_then(|c| c.tiles.get(local_y * 32 + local_x))
    }

    pub fn add_entity(&mut self, entity: Entity) {
        let chunk_pos = ChunkPosition::from_map_position(entity.position);
        self.get_or_create_chunk(chunk_pos);
        self.entities.insert(entity.id, entity);
    }

    pub fn get_entity(&self, id: EntityId) -> Option<&Entity> {
        self.entities.get(&id)
    }

    pub fn get_entity_mut(&mut self, id: EntityId) -> Option<&mut Entity> {
        self.entities.get_mut(&id)
    }

    pub fn remove_entity(&mut self, id: EntityId) -> Option<Entity> {
        self.entities.remove(&id)
    }

    /// Find entities within a bounding box
    pub fn find_entities_in_area(&self, left_top: MapPosition, right_bottom: MapPosition) -> Vec<&Entity> {
        self.entities.values()
            .filter(|e| {
                e.position.x.0 >= left_top.x.0 &&
                e.position.x.0 <= right_bottom.x.0 &&
                e.position.y.0 >= left_top.y.0 &&
                e.position.y.0 <= right_bottom.y.0
            })
            .collect()
    }

    /// Find entities of a specific type
    pub fn find_entities_by_name(&self, name: &str) -> Vec<&Entity> {
        self.entities.values()
            .filter(|e| e.name == name)
            .collect()
    }

    /// Find nearest entity to a position
    pub fn find_nearest_entity(&self, pos: MapPosition, filter: impl Fn(&Entity) -> bool) -> Option<&Entity> {
        self.entities.values()
            .filter(|e| filter(e))
            .min_by_key(|e| {
                let dx = e.position.x.0 - pos.x.0;
                let dy = e.position.y.0 - pos.y.0;
                dx * dx + dy * dy
            })
    }
}

/// A chunk of the map (32x32 tiles)
#[derive(Debug, Clone)]
pub struct Chunk {
    pub position: ChunkPosition,
    pub tiles: Vec<Tile>,
    pub generated: bool,
    pub charted: bool,
}

impl Chunk {
    pub fn new(position: ChunkPosition) -> Self {
        Self {
            position,
            tiles: vec![Tile::default(); 32 * 32],
            generated: false,
            charted: false,
        }
    }

    pub fn get_tile(&self, local_x: u8, local_y: u8) -> &Tile {
        &self.tiles[(local_y as usize) * 32 + (local_x as usize)]
    }

    pub fn set_tile(&mut self, local_x: u8, local_y: u8, tile: Tile) {
        self.tiles[(local_y as usize) * 32 + (local_x as usize)] = tile;
    }
}

/// A single tile
#[derive(Debug, Clone, Default)]
pub struct Tile {
    pub name: String,
    pub collides_with_player: bool,
    pub is_water: bool,
    pub walking_speed_modifier: f64,
}

impl Tile {
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        let mut tile = Self {
            name,
            collides_with_player: false,
            is_water: false,
            walking_speed_modifier: 1.0,
        };

        // Use Lua prototypes if available to set accurate collision + walking speed.
        if let Some(proto) = crate::lua::prototype::Prototypes::global()
            .and_then(|p| p.tile(&tile.name))
        {
            tile.walking_speed_modifier = proto.walking_speed_modifier;
            if let Some(mask) = &proto.collision_mask {
                tile.collides_with_player = mask.iter().any(|l| l == "player" || l == "out_of_map");
                tile.is_water = mask.iter().any(|l| l == "water_tile");
            }
        }

        // Fallback heuristics if prototypes are missing.
        if !tile.collides_with_player {
            tile.is_water = tile.name.contains("water") || tile.name.contains("deepwater");
            tile.collides_with_player = tile.is_water || tile.name.contains("out-of-map");
        }

        tile
    }
}

/// Common tile names
pub mod tiles {
    pub const GRASS: &str = "grass-1";
    pub const DIRT: &str = "dirt-1";
    pub const SAND: &str = "sand-1";
    pub const WATER: &str = "water";
    pub const DEEP_WATER: &str = "deepwater";
    pub const STONE_PATH: &str = "stone-path";
    pub const CONCRETE: &str = "concrete";
    pub const REFINED_CONCRETE: &str = "refined-concrete";
    pub const HAZARD_CONCRETE: &str = "hazard-concrete-left";
    pub const LANDFILL: &str = "landfill";
    pub const OUT_OF_MAP: &str = "out-of-map";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_surface_creation() {
        let surface = Surface::new(1, "nauvis".into());
        assert_eq!(surface.id, 1);
        assert_eq!(surface.name, "nauvis");
    }

    #[test]
    fn test_chunk_position() {
        let tile_pos = TilePosition { x: 47, y: -10 };
        let chunk_pos = ChunkPosition::from_tile(tile_pos);
        assert_eq!(chunk_pos.x, 1);
        assert_eq!(chunk_pos.y, -1);
    }

    #[test]
    fn test_tile() {
        let water = Tile::new("water");
        assert!(water.is_water);
        assert!(water.collides_with_player);

        let grass = Tile::new("grass-1");
        assert!(!grass.is_water);
        assert!(!grass.collides_with_player);
    }
}
