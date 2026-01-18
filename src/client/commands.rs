use crate::codec::{
    InputAction, Direction, MapPosition,
    ShootingState, RidingAcceleration, RidingDirection,
};

/// Convert Direction enum to (x, y) unit vector
fn direction_to_vector(dir: Direction) -> (f64, f64) {
    match dir {
        Direction::North => (0.0, -1.0),
        Direction::NorthEast => (0.707, -0.707),
        Direction::East => (1.0, 0.0),
        Direction::SouthEast => (0.707, 0.707),
        Direction::South => (0.0, 1.0),
        Direction::SouthWest => (-0.707, 0.707),
        Direction::West => (-1.0, 0.0),
        Direction::NorthWest => (-0.707, -0.707),
    }
}

/// Builder for common player actions
pub struct ActionBuilder;

impl ActionBuilder {
    /// Start walking in a direction
    pub fn walk(direction: Direction) -> InputAction {
        let (direction_x, direction_y) = direction_to_vector(direction);
        InputAction::StartWalking { direction_x, direction_y }
    }

    /// Stop walking
    pub fn stop() -> InputAction {
        InputAction::StopWalking
    }

    /// Start mining at a position
    pub fn mine(position: MapPosition) -> InputAction {
        InputAction::BeginMining {
            position,
            notify_server: true,
        }
    }

    /// Mine terrain at a position
    pub fn mine_terrain(position: MapPosition) -> InputAction {
        InputAction::BeginMiningTerrain { position }
    }

    /// Stop mining
    pub fn stop_mining() -> InputAction {
        InputAction::StopMining
    }

    /// Build something at a position
    pub fn build(position: MapPosition, direction: Direction) -> InputAction {
        InputAction::Build {
            position,
            direction,
            shift_build: false,
            skip_fog_of_war: false,
        }
    }

    /// Build with shift (for underground belts, etc.)
    pub fn shift_build(position: MapPosition, direction: Direction) -> InputAction {
        InputAction::Build {
            position,
            direction,
            shift_build: true,
            skip_fog_of_war: false,
        }
    }

    /// Rotate an entity
    pub fn rotate(position: MapPosition) -> InputAction {
        InputAction::RotateEntity {
            position,
            reverse: false,
        }
    }

    /// Reverse rotate an entity
    pub fn reverse_rotate(position: MapPosition) -> InputAction {
        InputAction::RotateEntity {
            position,
            reverse: true,
        }
    }

    /// Start crafting a recipe
    pub fn craft(recipe_id: u16, count: u32) -> InputAction {
        InputAction::Craft { recipe_id, count }
    }

    /// Send a chat message
    pub fn chat(message: impl Into<String>) -> InputAction {
        InputAction::WriteToConsole {
            message: message.into(),
        }
    }

    /// Run a console command
    pub fn command(command: impl Into<String>) -> InputAction {
        InputAction::ServerCommand {
            command: command.into(),
        }
    }

    /// Open an entity's GUI
    pub fn open_gui(entity_id: u32) -> InputAction {
        InputAction::OpenGui { entity_id }
    }

    /// Close any open GUI
    pub fn close_gui() -> InputAction {
        InputAction::CloseGui
    }

    /// Clear the cursor
    pub fn clear_cursor() -> InputAction {
        InputAction::ClearCursor
    }

    /// Start research
    pub fn research(technology_id: u16) -> InputAction {
        InputAction::StartResearch { technology_id }
    }

    /// Cancel research
    pub fn cancel_research() -> InputAction {
        InputAction::CancelResearch
    }

    /// Change shooting state
    pub fn shoot(position: MapPosition) -> InputAction {
        InputAction::ChangeShootingState {
            state: ShootingState::ShootingSelected,
            position,
        }
    }

    /// Stop shooting
    pub fn stop_shooting() -> InputAction {
        InputAction::ChangeShootingState {
            state: ShootingState::NotShooting,
            position: MapPosition::default(),
        }
    }

    /// Drive vehicle forward
    pub fn drive_forward() -> InputAction {
        InputAction::ChangeRidingState {
            acceleration: RidingAcceleration::Accelerating,
            direction: RidingDirection::Straight,
        }
    }

    /// Brake vehicle
    pub fn brake() -> InputAction {
        InputAction::ChangeRidingState {
            acceleration: RidingAcceleration::Braking,
            direction: RidingDirection::Straight,
        }
    }

    /// Reverse vehicle
    pub fn reverse() -> InputAction {
        InputAction::ChangeRidingState {
            acceleration: RidingAcceleration::Reversing,
            direction: RidingDirection::Straight,
        }
    }

    /// Turn vehicle left
    pub fn turn_left() -> InputAction {
        InputAction::ChangeRidingState {
            acceleration: RidingAcceleration::Nothing,
            direction: RidingDirection::Left,
        }
    }

    /// Turn vehicle right
    pub fn turn_right() -> InputAction {
        InputAction::ChangeRidingState {
            acceleration: RidingAcceleration::Nothing,
            direction: RidingDirection::Right,
        }
    }

    /// Toggle driving mode
    pub fn toggle_driving() -> InputAction {
        InputAction::ToggleDriving
    }

    /// Drop item at position
    pub fn drop_item(position: MapPosition) -> InputAction {
        InputAction::DropItem { position }
    }

    /// Use item (capsules, etc.) at position
    pub fn use_item(position: MapPosition) -> InputAction {
        InputAction::UseItem { position }
    }

    /// Launch rocket
    pub fn launch_rocket() -> InputAction {
        InputAction::LaunchRocket
    }

    /// Toggle personal roboport
    pub fn toggle_roboport() -> InputAction {
        InputAction::TogglePersonalRoboport
    }

    /// Toggle personal logistics
    pub fn toggle_logistics() -> InputAction {
        InputAction::TogglePersonalLogisticRequests
    }

    /// Open character inventory
    pub fn open_character() -> InputAction {
        InputAction::OpenCharacterGui
    }

    /// Open production statistics
    pub fn open_production() -> InputAction {
        InputAction::OpenProductionGui
    }

    /// Open trains GUI
    pub fn open_trains() -> InputAction {
        InputAction::OpenTrainsGui
    }

    /// Set quickbar slot
    pub fn set_quickbar(page: u8, slot: u8, item: impl Into<String>) -> InputAction {
        InputAction::QuickBarSetSlot {
            page,
            slot,
            item_name: item.into(),
        }
    }

    /// Pick from quickbar slot
    pub fn pick_quickbar(page: u8, slot: u8) -> InputAction {
        InputAction::QuickBarPickSlot { page, slot }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_action_builder() {
        let walk = ActionBuilder::walk(Direction::North);
        match walk {
            InputAction::StartWalking { direction_x, direction_y } => {
                // North = (0, -1) in Factorio coordinates
                assert!((direction_x - 0.0).abs() < f64::EPSILON);
                assert!((direction_y - (-1.0)).abs() < f64::EPSILON);
            }
            _ => panic!("Wrong action type"),
        }

        let stop = ActionBuilder::stop();
        assert!(matches!(stop, InputAction::StopWalking));

        let chat = ActionBuilder::chat("Hello!");
        match chat {
            InputAction::WriteToConsole { message } => {
                assert_eq!(message, "Hello!");
            }
            _ => panic!("Wrong action type"),
        }
    }
}
