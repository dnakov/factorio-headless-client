use crate::codec::InputAction;
use crate::error::Result;
use crate::state::GameWorld;
use crate::simulation::action_executor::ActionExecutor;

/// Data for a single input action (player action within a tick)
#[derive(Debug, Clone)]
pub struct InputActionData {
    pub player_index: u16,
    pub action_type: u8,
    pub data: Vec<u8>,
}

/// A tick closure containing input actions for a specific tick
#[derive(Debug, Clone)]
pub struct TickClosureData {
    pub update_tick: u32,
    pub input_actions: Vec<InputActionData>,
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

        // Process all input actions in this closure
        for action_data in &closure.input_actions {
            // Parse the raw action data into an InputAction
            if let Ok(action) = self.parse_action(action_data) {
                self.action_executor.execute(world, action_data.player_index, action)?;
                actions_executed += 1;
            }
        }

        // Advance world tick
        world.tick = closure.update_tick;

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

    fn parse_action(&self, action_data: &InputActionData) -> Result<InputAction> {
        // The action type is already in action_data.action_type
        // We need to prepend it to make a complete action
        let mut full_data = vec![action_data.action_type];
        full_data.extend_from_slice(&action_data.data);

        let mut reader = crate::codec::BinaryReader::new(&full_data);
        InputAction::read(&mut reader)
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
