use crate::codec::{InputAction, MapPosition, TilePosition};
use crate::error::Result;
use crate::state::GameWorld;
use crate::simulation::action_executor::ActionExecutor;
use crate::codec::map_types::entity_collision_box;

/// Data for a single input action (player action within a tick)
#[derive(Debug, Clone)]
pub struct TickAction {
    pub player_index: u16,
    pub action: InputAction,
}

/// A tick closure containing input actions for a specific tick
#[derive(Debug, Clone)]
pub struct TickClosureData {
    pub update_tick: u32,
    pub input_actions: Vec<TickAction>,
}

/// Result of executing a tick
#[derive(Debug, Clone)]
pub struct TickResult {
    pub tick: u32,
    pub actions_executed: usize,
    pub checksum: u32,
}

/// Tick executor - advances game state by processing tick closures
pub struct TickExecutor {
    action_executor: ActionExecutor,
}

impl TickExecutor {
    pub fn new() -> Self {
        Self {
            action_executor: ActionExecutor::new(),
        }
    }

    /// Execute a single tick closure
    pub fn execute_tick(&mut self, world: &mut GameWorld, closure: &TickClosureData) -> Result<TickResult> {
        let mut actions_executed = 0;
        let prev_tick = world.tick;
        let delta_ticks = closure.update_tick.saturating_sub(prev_tick);

        // Advance movement before applying actions for this tick
        if delta_ticks > 0 {
            self.advance_player_movement(world, delta_ticks);
        }

        // Process all input actions in this closure
        for action_data in &closure.input_actions {
            self.action_executor
                .execute(world, action_data.player_index, action_data.action.clone())?;
            actions_executed += 1;
        }

        // Advance world tick
        world.tick = closure.update_tick;
        self.action_executor.tick(world);

        // Calculate checksum
        let checksum = crate::simulation::checksum::ChecksumCalculator::calculate_world_checksum(world);

        Ok(TickResult {
            tick: closure.update_tick,
            actions_executed,
            checksum,
        })
    }

    /// Execute multiple tick closures
    pub fn execute_ticks(&mut self, world: &mut GameWorld, closures: &[TickClosureData]) -> Result<Vec<TickResult>> {
        let mut results = Vec::with_capacity(closures.len());

        for closure in closures {
            results.push(self.execute_tick(world, closure)?);
        }

        Ok(results)
    }

    fn advance_player_movement(&self, world: &mut GameWorld, ticks: u32) {
        if ticks == 0 {
            return;
        }
        let player_ids: Vec<u16> = world.players.keys().cloned().collect();
        for _ in 0..ticks {
            for player_id in &player_ids {
                let (walking, direction, position) = {
                    let player = match world.players.get(player_id) {
                        Some(p) => p,
                        None => continue,
                    };
                    (player.walking, player.walking_direction, player.position)
                };
                if !walking {
                    continue;
                }
                let speed_mod = self.tile_speed_modifier(world, position);
                let speed = world.character_speed * speed_mod;
                if speed <= 0.0 {
                    continue;
                }
                let (vx, vy) = direction.to_vector();
                let step_x = (speed * vx * 256.0).trunc() as i32;
                let step_y = (speed * vy * 256.0).trunc() as i32;
                if step_x == 0 && step_y == 0 {
                    continue;
                }

                let mut next_pos = position;
                if step_x != 0 {
                    let cand = MapPosition::new(position.x.0 + step_x, position.y.0);
                    if !self.collides(world, cand) {
                        next_pos = cand;
                    }
                }
                if step_y != 0 {
                    let cand = MapPosition::new(next_pos.x.0, position.y.0 + step_y);
                    if !self.collides(world, cand) {
                        next_pos = cand;
                    }
                }

                if let Some(player) = world.players.get_mut(player_id) {
                    player.position = next_pos;
                }
            }
        }
    }

    fn tile_speed_modifier(&self, world: &GameWorld, pos: MapPosition) -> f64 {
        let tile_pos = TilePosition::from(pos);
        let surface = match world.nauvis() {
            Some(s) => s,
            None => return 1.0,
        };
        surface
            .get_tile(tile_pos)
            .map(|t| t.walking_speed_modifier)
            .unwrap_or(1.0)
    }

    fn collides(&self, world: &GameWorld, pos: MapPosition) -> bool {
        let surface = match world.nauvis() {
            Some(s) => s,
            None => return false,
        };
        let (px, py) = pos.to_tiles();
        let cbox = world.character_collision_box;
        let min_x = px + cbox[0];
        let min_y = py + cbox[1];
        let max_x = px + cbox[2];
        let max_y = py + cbox[3];

        let tile_min_x = min_x.floor() as i32;
        let tile_max_x = max_x.floor() as i32;
        let tile_min_y = min_y.floor() as i32;
        let tile_max_y = max_y.floor() as i32;
        for ty in tile_min_y..=tile_max_y {
            for tx in tile_min_x..=tile_max_x {
                if let Some(tile) = surface.get_tile(TilePosition::new(tx, ty)) {
                    if tile.collides_with_player {
                        return true;
                    }
                }
            }
        }

        for entity in surface.entities.values() {
            if entity.name == "character" {
                continue;
            }
            let (ebox, collides_player) = entity_collision_box(&entity.name);
            if !collides_player {
                continue;
            }
            let (ex, ey) = entity.position.to_tiles();
            let e_min_x = ex + ebox[0];
            let e_min_y = ey + ebox[1];
            let e_max_x = ex + ebox[2];
            let e_max_y = ey + ebox[3];
            let overlap = min_x < e_max_x
                && max_x > e_min_x
                && min_y < e_max_y
                && max_y > e_min_y;
            if overlap {
                return true;
            }
        }

        false
    }
}

impl Default for TickExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tick_executor_creation() {
        let _executor = TickExecutor::new();
    }

    #[test]
    fn test_empty_tick_closure() {
        let mut executor = TickExecutor::new();
        let mut world = GameWorld::new();

        let closure = TickClosureData {
            update_tick: 100,
            input_actions: Vec::new(),
        };

        let result = executor.execute_tick(&mut world, &closure).unwrap();
        assert_eq!(result.tick, 100);
        assert_eq!(result.actions_executed, 0);
        assert_eq!(world.tick, 100);
    }
}
