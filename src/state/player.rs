use crate::codec::{MapPosition, Direction, Color, LogisticFilter};
use crate::state::inventory::{Inventory, ItemStack};

/// Player identifier
pub type PlayerId = u16;

/// Player state
#[derive(Debug, Clone)]
pub struct Player {
    pub id: PlayerId,
    pub name: String,
    pub connected: bool,

    // Position and movement
    pub position: MapPosition,
    pub direction: Direction,
    pub walking: bool,
    pub walking_direction: Direction,
    pub riding_vehicle: Option<u32>,

    // State
    pub health: f32,
    pub max_health: f32,
    pub crafting_queue_size: u32,

    // Controller
    pub controller_type: ControllerType,
    pub character_id: Option<u32>,

    // Cursor
    pub cursor_stack: Option<ItemStack>,
    pub cursor_ghost: Option<String>,

    // GUI
    pub opened_entity_id: Option<u32>,
    pub opened_gui_type: Option<GuiType>,

    // Selection
    pub selected_entity_id: Option<u32>,
    pub selected_tile_position: Option<MapPosition>,

    // Appearance
    pub color: Color,

    // Inventories (loaded separately)
    pub main_inventory: Option<Inventory>,
    pub quickbar: Option<Inventory>,
    pub trash_inventory: Option<Inventory>,
    pub armor_inventory: Option<Inventory>,
    pub guns_inventory: Option<Inventory>,
    pub ammo_inventory: Option<Inventory>,

    // Logistics
    pub logistic_requests: Vec<LogisticFilter>,
}

impl Player {
    pub fn new(id: PlayerId, name: String) -> Self {
        Self {
            id,
            name,
            connected: true,
            position: MapPosition::default(),
            direction: Direction::North,
            walking: false,
            walking_direction: Direction::North,
            riding_vehicle: None,
            health: 250.0,
            max_health: 250.0,
            crafting_queue_size: 0,
            controller_type: ControllerType::Character,
            character_id: None,
            cursor_stack: None,
            cursor_ghost: None,
            opened_entity_id: None,
            opened_gui_type: None,
            selected_entity_id: None,
            selected_tile_position: None,
            color: Color::new(255, 165, 0, 255), // Orange default
            main_inventory: None,
            quickbar: None,
            trash_inventory: None,
            armor_inventory: None,
            guns_inventory: None,
            ammo_inventory: None,
            logistic_requests: Vec::new(),
        }
    }

    pub fn is_alive(&self) -> bool {
        self.health > 0.0
    }

    pub fn is_in_vehicle(&self) -> bool {
        self.riding_vehicle.is_some()
    }

    pub fn has_item(&self, item_name: &str) -> bool {
        self.count_item(item_name) > 0
    }

    pub fn count_item(&self, item_name: &str) -> u32 {
        let mut count = 0;
        if let Some(ref inv) = self.main_inventory {
            count += inv.count_item(item_name);
        }
        if let Some(ref inv) = self.quickbar {
            count += inv.count_item(item_name);
        }
        count
    }

    pub fn clear_cursor(&mut self) {
        self.cursor_stack = None;
        self.cursor_ghost = None;
    }
}

/// Controller type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerType {
    Character,
    Ghost,
    God,
    Editor,
    Spectator,
}

/// GUI type that can be opened
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuiType {
    None,
    Entity,
    Character,
    Technology,
    Production,
    Logistics,
    BlueprintLibrary,
    Trains,
    Achievements,
    Permissions,
    Tutorials,
    TipsAndTricks,
    BonusInfo,
    TrainStation,
}

/// Crafting queue item
#[derive(Debug, Clone)]
pub struct CraftingQueueItem {
    pub recipe: String,
    pub count: u32,
    pub prerequisite: Option<usize>,
}

/// Research progress
#[derive(Debug, Clone)]
pub struct ResearchProgress {
    pub technology: String,
    pub progress: f64,
    pub research_unit_count: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_player_creation() {
        let player = Player::new(1, "TestPlayer".into());
        assert_eq!(player.id, 1);
        assert_eq!(player.name, "TestPlayer");
        assert!(player.connected);
        assert!(player.is_alive());
    }

    #[test]
    fn test_player_inventory_count() {
        let mut player = Player::new(1, "Test".into());
        let mut inv = Inventory::new(10);
        inv.set(0, Some(ItemStack::new("iron-plate", 50)));
        inv.set(1, Some(ItemStack::new("iron-plate", 30)));
        player.main_inventory = Some(inv);

        assert_eq!(player.count_item("iron-plate"), 80);
        assert!(player.has_item("iron-plate"));
        assert!(!player.has_item("copper-plate"));
    }
}
