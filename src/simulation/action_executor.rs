use crate::codec::{InputAction, Direction, MapPosition};
use crate::error::Result;
use crate::state::{GameWorld, Inventory, ItemStack};

/// Convert direction vector (x, y) to Direction enum
fn direction_from_vector(x: f64, y: f64) -> Direction {
    let angle = y.atan2(x);
    let octant = ((angle + std::f64::consts::PI) / (std::f64::consts::PI / 4.0)).round() as i32 % 8;
    match octant {
        0 => Direction::West,
        1 => Direction::NorthWest,
        2 => Direction::North,
        3 => Direction::NorthEast,
        4 => Direction::East,
        5 => Direction::SouthEast,
        6 => Direction::South,
        7 => Direction::SouthWest,
        _ => Direction::North,
    }
}

/// Executes input actions against the game world
pub struct ActionExecutor {
    /// Pending crafts that will complete (tick, player_index, recipe_name, count)
    pending_crafts: Vec<(u32, u16, String, u32)>,
}

impl ActionExecutor {
    pub fn new() -> Self {
        Self {
            pending_crafts: Vec::new(),
        }
    }

    /// Process pending crafts that have completed
    pub fn tick(&mut self, world: &mut GameWorld) {
        let current_tick = world.tick;
        let mut completed = Vec::new();

        // Find completed crafts
        for (i, (finish_tick, player_index, recipe_name, count)) in self.pending_crafts.iter().enumerate() {
            if current_tick >= *finish_tick {
                completed.push((i, *player_index, recipe_name.clone(), *count));
            }
        }

        // Process completed crafts (reverse order to not mess up indices)
        for (idx, player_index, recipe_name, count) in completed.into_iter().rev() {
            self.pending_crafts.remove(idx);

            // Add products to player inventory
            if let Some(recipe) = world.recipes.get_by_name(&recipe_name) {
                if let Some(player) = world.players.get_mut(&player_index) {
                    // Ensure player has main inventory
                    if player.main_inventory.is_none() {
                        player.main_inventory = Some(Inventory::new(80));
                    }

                    if let Some(ref mut inv) = player.main_inventory {
                        for product in &recipe.products {
                            let stack = ItemStack::new(&product.name, product.amount * count);
                            inv.insert(stack);
                        }
                    }

                    // Decrement crafting queue
                    player.crafting_queue_size = player.crafting_queue_size.saturating_sub(count);
                }
            }
        }
    }

    /// Execute an input action for a player
    pub fn execute(&mut self, world: &mut GameWorld, player_index: u16, action: InputAction) -> Result<()> {
        match action {
            InputAction::Nothing => Ok(()),

            InputAction::StartWalking { direction_x, direction_y } => {
                self.execute_start_walking(world, player_index, direction_x, direction_y)
            }

            InputAction::StopWalking => {
                self.execute_stop_walking(world, player_index)
            }

            InputAction::BeginMining { position, .. } => {
                self.execute_begin_mining(world, player_index, position)
            }

            InputAction::StopMining => {
                self.execute_stop_mining(world, player_index)
            }

            InputAction::Build { position, direction, .. } => {
                self.execute_build(world, player_index, position, direction)
            }

            InputAction::RotateEntity { position, reverse } => {
                self.execute_rotate(world, position, reverse)
            }

            InputAction::Craft { recipe_id, count } => {
                self.execute_craft(world, player_index, recipe_id, count)
            }

            InputAction::WriteToConsole { message } => {
                self.execute_chat(world, player_index, message)
            }

            InputAction::ChangeRidingState { acceleration: _, direction: _ } => {
                // Update player riding state - implementation depends on having vehicle state
                Ok(())
            }

            InputAction::ChangeShootingState { state: _, position: _ } => {
                // Update player shooting state
                Ok(())
            }

            InputAction::ClearCursor => {
                if let Some(player) = world.players.get_mut(&player_index) {
                    player.clear_cursor();
                }
                Ok(())
            }

            InputAction::OpenGui { entity_id } => {
                if let Some(player) = world.players.get_mut(&player_index) {
                    player.opened_entity_id = Some(entity_id);
                }
                Ok(())
            }

            InputAction::CloseGui => {
                if let Some(player) = world.players.get_mut(&player_index) {
                    player.opened_entity_id = None;
                    player.opened_gui_type = None;
                }
                Ok(())
            }

            InputAction::SelectedEntityChanged { entity_id } => {
                if let Some(player) = world.players.get_mut(&player_index) {
                    player.selected_entity_id = entity_id;
                }
                Ok(())
            }

            InputAction::SelectedEntityCleared => {
                if let Some(player) = world.players.get_mut(&player_index) {
                    player.selected_entity_id = None;
                }
                Ok(())
            }

            // For unhandled actions, just succeed silently
            // Full implementation would handle all 210+ action types
            _ => Ok(()),
        }
    }

    fn execute_start_walking(&mut self, world: &mut GameWorld, player_index: u16, direction_x: f64, direction_y: f64) -> Result<()> {
        // Convert direction vector to Direction enum
        let direction = direction_from_vector(direction_x, direction_y);
        if let Some(player) = world.players.get_mut(&player_index) {
            player.walking = true;
            player.walking_direction = direction;
            player.direction = direction;
        }
        Ok(())
    }

    fn execute_stop_walking(&mut self, world: &mut GameWorld, player_index: u16) -> Result<()> {
        if let Some(player) = world.players.get_mut(&player_index) {
            player.walking = false;
        }
        Ok(())
    }

    fn execute_begin_mining(&mut self, world: &mut GameWorld, player_index: u16, position: MapPosition) -> Result<()> {
        // In a full implementation, this would:
        // 1. Find the entity at the position
        // 2. Start a mining operation
        // 3. Track mining progress
        if let Some(player) = world.players.get_mut(&player_index) {
            // Update player's selected position for mining
            player.selected_tile_position = Some(position);
        }
        Ok(())
    }

    fn execute_stop_mining(&mut self, world: &mut GameWorld, player_index: u16) -> Result<()> {
        if let Some(player) = world.players.get_mut(&player_index) {
            player.selected_tile_position = None;
        }
        Ok(())
    }

    fn execute_build(&mut self, _world: &mut GameWorld, _player_index: u16, _position: MapPosition, _direction: Direction) -> Result<()> {
        // In a full implementation, this would:
        // 1. Check player has item in cursor
        // 2. Validate placement
        // 3. Create entity
        // 4. Deduct item from player
        Ok(())
    }

    fn execute_rotate(&mut self, _world: &mut GameWorld, _position: MapPosition, _reverse: bool) -> Result<()> {
        // Find entity at position and rotate it
        // This requires spatial lookup which is expensive
        // In a full implementation, we'd have spatial indices
        Ok(())
    }

    fn execute_craft(&mut self, world: &mut GameWorld, player_index: u16, recipe_id: u16, count: u32) -> Result<()> {
        // Look up recipe by ID
        let recipe = match world.recipes.get(recipe_id) {
            Some(r) => r.clone(),
            None => {
                // Unknown recipe - just track the queue
                if let Some(player) = world.players.get_mut(&player_index) {
                    player.crafting_queue_size += count;
                }
                return Ok(());
            }
        };

        // Get player's inventory
        let player = match world.players.get_mut(&player_index) {
            Some(p) => p,
            None => return Ok(()),
        };

        // Ensure player has main inventory
        if player.main_inventory.is_none() {
            player.main_inventory = Some(Inventory::new(80));
        }

        let inv = player.main_inventory.as_mut().unwrap();

        // Check if player has required ingredients
        let available = inv.contents();
        if !recipe.can_craft(&available, count) {
            // Not enough ingredients - the server will reject this anyway
            // but we track it for the queue display
            player.crafting_queue_size += count;
            return Ok(());
        }

        // Deduct ingredients
        for ingredient in &recipe.ingredients {
            inv.remove(&ingredient.name, ingredient.amount * count);
        }

        // Add to crafting queue
        player.crafting_queue_size += count;

        // Schedule completion (crafting time in ticks, 60 ticks per second)
        let crafting_ticks = (recipe.crafting_time * 60.0) as u32;
        let finish_tick = world.tick + crafting_ticks;
        self.pending_crafts.push((finish_tick, player_index, recipe.name.clone(), count));

        Ok(())
    }

    fn execute_chat(&mut self, _world: &mut GameWorld, _player_index: u16, _message: String) -> Result<()> {
        // Chat messages don't affect game state directly
        // In a full implementation, could emit events
        Ok(())
    }
}

impl Default for ActionExecutor {
    fn default() -> Self {
        Self::new()
    }
}

/// Initialize player inventory with freeplay starting items
pub fn init_freeplay_inventory(player: &mut crate::state::Player) {
    let mut inv = Inventory::new(80);

    // Freeplay scenario starting items
    inv.insert(ItemStack::new("burner-mining-drill", 1));
    inv.insert(ItemStack::new("stone-furnace", 1));
    inv.insert(ItemStack::new("iron-plate", 8));
    inv.insert(ItemStack::new("pistol", 1));
    inv.insert(ItemStack::new("firearm-magazine", 10));

    player.main_inventory = Some(inv);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_start_walking() {
        let mut executor = ActionExecutor::new();
        let mut world = GameWorld::new();
        world.add_player(1, "Test".into());

        // North = (0, -1) in Factorio coordinates
        let action = InputAction::StartWalking { direction_x: 0.0, direction_y: -1.0 };
        executor.execute(&mut world, 1, action).unwrap();

        let player = world.get_player(1).unwrap();
        assert!(player.walking);
        assert_eq!(player.walking_direction, Direction::North);
    }

    #[test]
    fn test_stop_walking() {
        let mut executor = ActionExecutor::new();
        let mut world = GameWorld::new();
        world.add_player(1, "Test".into());

        // Start walking East = (1, 0)
        executor.execute(&mut world, 1, InputAction::StartWalking { direction_x: 1.0, direction_y: 0.0 }).unwrap();

        // Stop walking
        executor.execute(&mut world, 1, InputAction::StopWalking).unwrap();

        let player = world.get_player(1).unwrap();
        assert!(!player.walking);
    }

    #[test]
    fn test_clear_cursor() {
        let mut executor = ActionExecutor::new();
        let mut world = GameWorld::new();
        world.add_player(1, "Test".into());

        executor.execute(&mut world, 1, InputAction::ClearCursor).unwrap();
        // Just verify it doesn't crash
    }
}
