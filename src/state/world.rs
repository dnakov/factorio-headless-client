use std::collections::HashMap;
use crate::codec::MapPosition;
use crate::state::entity::{Entity, EntityId};
use crate::state::player::{Player, PlayerId};
use crate::state::recipe::RecipeDatabase;
use crate::state::surface::{Surface, SurfaceId};

/// The complete game world state
#[derive(Debug, Clone)]
pub struct GameWorld {
    /// Current game tick
    pub tick: u32,

    /// Game seed
    pub seed: u32,

    /// All surfaces (nauvis, space, etc.)
    pub surfaces: HashMap<SurfaceId, Surface>,

    /// All players
    pub players: HashMap<PlayerId, Player>,

    /// Research
    pub research: ResearchState,

    /// Force data
    pub forces: HashMap<String, ForceData>,

    /// Recipe database (populated from map data)
    pub recipes: RecipeDatabase,

    /// Global entity ID counter
    next_entity_id: EntityId,
}

impl GameWorld {
    pub fn new() -> Self {
        let mut world = Self {
            tick: 0,
            seed: 0,
            surfaces: HashMap::new(),
            players: HashMap::new(),
            research: ResearchState::new(),
            forces: HashMap::new(),
            recipes: RecipeDatabase::new(),
            next_entity_id: 1,
        };

        // Create default surface "nauvis"
        world.surfaces.insert(1, Surface::new(1, "nauvis".into()));

        // Create default force "player"
        world.forces.insert("player".into(), ForceData::new("player"));

        world
    }

    /// Get the default surface (nauvis)
    pub fn nauvis(&self) -> Option<&Surface> {
        self.surfaces.get(&1)
    }

    pub fn nauvis_mut(&mut self) -> Option<&mut Surface> {
        self.surfaces.get_mut(&1)
    }

    /// Get a surface by ID
    pub fn get_surface(&self, id: SurfaceId) -> Option<&Surface> {
        self.surfaces.get(&id)
    }

    pub fn get_surface_mut(&mut self, id: SurfaceId) -> Option<&mut Surface> {
        self.surfaces.get_mut(&id)
    }

    /// Get a surface by name
    pub fn get_surface_by_name(&self, name: &str) -> Option<&Surface> {
        self.surfaces.values().find(|s| s.name == name)
    }

    /// Get a player by ID
    pub fn get_player(&self, id: PlayerId) -> Option<&Player> {
        self.players.get(&id)
    }

    pub fn get_player_mut(&mut self, id: PlayerId) -> Option<&mut Player> {
        self.players.get_mut(&id)
    }

    /// Get player by name
    pub fn get_player_by_name(&self, name: &str) -> Option<&Player> {
        self.players.values().find(|p| p.name == name)
    }

    /// Add a new player
    pub fn add_player(&mut self, id: PlayerId, name: String) -> &mut Player {
        self.players.entry(id).or_insert_with(|| Player::new(id, name))
    }

    /// Generate a new unique entity ID
    pub fn next_entity_id(&mut self) -> EntityId {
        let id = self.next_entity_id;
        self.next_entity_id += 1;
        id
    }

    /// Find an entity across all surfaces
    pub fn find_entity(&self, id: EntityId) -> Option<(&Surface, &Entity)> {
        for surface in self.surfaces.values() {
            if let Some(entity) = surface.get_entity(id) {
                return Some((surface, entity));
            }
        }
        None
    }

    /// Find entities near a position on a surface
    pub fn find_entities_near(
        &self,
        surface_id: SurfaceId,
        position: MapPosition,
        radius: i32,
    ) -> Vec<&Entity> {
        let surface = match self.surfaces.get(&surface_id) {
            Some(s) => s,
            None => return Vec::new(),
        };

        let radius_fixed = radius * 256; // Convert to fixed point
        let left = position.x.0 - radius_fixed;
        let right = position.x.0 + radius_fixed;
        let top = position.y.0 - radius_fixed;
        let bottom = position.y.0 + radius_fixed;

        surface.entities.values()
            .filter(|e| {
                e.position.x.0 >= left &&
                e.position.x.0 <= right &&
                e.position.y.0 >= top &&
                e.position.y.0 <= bottom
            })
            .collect()
    }

    /// Update tick
    pub fn advance_tick(&mut self) {
        self.tick += 1;
    }
}

impl Default for GameWorld {
    fn default() -> Self {
        Self::new()
    }
}

/// Research state
#[derive(Debug, Clone, Default)]
pub struct ResearchState {
    /// Currently researching technology
    pub current_research: Option<String>,

    /// Research progress (0.0 to 1.0)
    pub progress: f64,

    /// Completed technologies
    pub researched: Vec<String>,

    /// Queued technologies
    pub queue: Vec<String>,
}

impl ResearchState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_researched(&self, tech: &str) -> bool {
        self.researched.iter().any(|t| t == tech)
    }

    pub fn is_researching(&self, tech: &str) -> bool {
        self.current_research.as_ref().map(|t| t == tech).unwrap_or(false)
    }
}

/// Force data (team data)
#[derive(Debug, Clone)]
pub struct ForceData {
    pub name: String,
    pub friendly_fire: bool,
    pub share_chart: bool,
    pub evolution_factor: f64,
    pub recipes_enabled: Vec<String>,
}

impl ForceData {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            friendly_fire: false,
            share_chart: true,
            evolution_factor: 0.0,
            recipes_enabled: Vec::new(),
        }
    }

    pub fn is_recipe_enabled(&self, recipe: &str) -> bool {
        self.recipes_enabled.iter().any(|r| r == recipe)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_world_creation() {
        let world = GameWorld::new();
        assert_eq!(world.tick, 0);
        assert!(world.nauvis().is_some());
        assert!(world.forces.contains_key("player"));
    }

    #[test]
    fn test_player_management() {
        let mut world = GameWorld::new();
        world.add_player(1, "Player1".into());
        world.add_player(2, "Player2".into());

        assert_eq!(world.players.len(), 2);
        assert!(world.get_player(1).is_some());
        assert_eq!(world.get_player_by_name("Player2").unwrap().id, 2);
    }

    #[test]
    fn test_entity_id_generation() {
        let mut world = GameWorld::new();
        let id1 = world.next_entity_id();
        let id2 = world.next_entity_id();
        let id3 = world.next_entity_id();

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
    }

    #[test]
    fn test_research_state() {
        let mut research = ResearchState::new();
        research.researched.push("automation".into());
        research.current_research = Some("logistics".into());

        assert!(research.is_researched("automation"));
        assert!(!research.is_researched("logistics"));
        assert!(research.is_researching("logistics"));
    }
}
