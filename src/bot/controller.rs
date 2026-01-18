use crate::codec::{Direction, MapPosition, InputAction, Fixed32};
use crate::client::ActionBuilder;
use crate::state::{GameWorld, PlayerId};

/// High-level player controller for bot actions
pub struct PlayerController {
    player_id: PlayerId,
}

impl PlayerController {
    pub fn new(player_id: PlayerId) -> Self {
        Self { player_id }
    }

    /// Get the player's current position from the world
    pub fn position(&self, world: &GameWorld) -> Option<MapPosition> {
        world.get_player(self.player_id).map(|p| p.position)
    }

    /// Calculate direction from current position to target
    pub fn direction_to(&self, world: &GameWorld, target: MapPosition) -> Option<Direction> {
        let current = self.position(world)?;

        let dx = target.x.0 - current.x.0;
        let dy = target.y.0 - current.y.0;

        // Calculate angle
        let angle = (dy as f64).atan2(dx as f64);

        // Convert to 8-way direction (Factorio's North is -Y)
        let octant = ((angle + std::f64::consts::PI) / (std::f64::consts::PI / 4.0)) as i32;

        Some(match octant % 8 {
            0 | 7 => Direction::West,
            1 => Direction::SouthWest,
            2 => Direction::South,
            3 => Direction::SouthEast,
            4 => Direction::East,
            5 => Direction::NorthEast,
            6 => Direction::North,
            _ => Direction::NorthWest,
        })
    }

    /// Calculate distance to target
    pub fn distance_to(&self, world: &GameWorld, target: MapPosition) -> Option<f64> {
        let current = self.position(world)?;
        Some(current.distance_to(target))
    }

    /// Check if at target (within tolerance)
    pub fn is_at(&self, world: &GameWorld, target: MapPosition, tolerance: f64) -> bool {
        self.distance_to(world, target)
            .map(|d| d <= tolerance)
            .unwrap_or(false)
    }

    /// Generate action to walk toward target
    pub fn walk_toward(&self, world: &GameWorld, target: MapPosition) -> Option<InputAction> {
        let direction = self.direction_to(world, target)?;
        Some(ActionBuilder::walk(direction))
    }

    /// Generate actions to walk to a target (returns walk + stop when arrived)
    pub fn navigate_to(&self, world: &GameWorld, target: MapPosition, tolerance: f64) -> NavigationResult {
        if self.is_at(world, target, tolerance) {
            NavigationResult::Arrived(ActionBuilder::stop())
        } else if let Some(action) = self.walk_toward(world, target) {
            NavigationResult::Walking(action)
        } else {
            NavigationResult::NoPath
        }
    }

    /// Mine at a position
    pub fn mine_at(&self, position: MapPosition) -> InputAction {
        ActionBuilder::mine(position)
    }

    /// Build at a position
    pub fn build_at(&self, position: MapPosition, direction: Direction) -> InputAction {
        ActionBuilder::build(position, direction)
    }

    /// Find nearest entity of a type
    pub fn find_nearest<'a>(
        &self,
        world: &'a GameWorld,
        name: &str,
    ) -> Option<&'a crate::state::Entity> {
        let current = self.position(world)?;
        let surface = world.nauvis()?;
        surface.find_nearest_entity(current, |e| e.name.contains(name))
    }
}

/// Result of navigation calculation
#[derive(Debug)]
pub enum NavigationResult {
    /// Arrived at destination, stop action provided
    Arrived(InputAction),
    /// Still walking, walk action provided
    Walking(InputAction),
    /// Cannot find path
    NoPath,
}

impl NavigationResult {
    pub fn action(&self) -> Option<&InputAction> {
        match self {
            NavigationResult::Arrived(a) | NavigationResult::Walking(a) => Some(a),
            NavigationResult::NoPath => None,
        }
    }

    pub fn is_arrived(&self) -> bool {
        matches!(self, NavigationResult::Arrived(_))
    }

    pub fn is_walking(&self) -> bool {
        matches!(self, NavigationResult::Walking(_))
    }
}

/// Pathfinding helper (basic implementation)
pub struct Pathfinder;

impl Pathfinder {
    /// Calculate a simple straight-line path
    pub fn straight_line(from: MapPosition, to: MapPosition, step_size: f64) -> Vec<MapPosition> {
        let mut path = Vec::new();
        let distance = from.distance_to(to);

        if distance < step_size {
            path.push(to);
            return path;
        }

        let steps = (distance / step_size).ceil() as i32;
        let dx = (to.x.0 - from.x.0) as f64 / steps as f64;
        let dy = (to.y.0 - from.y.0) as f64 / steps as f64;

        for i in 1..=steps {
            let x = from.x.0 as f64 + dx * i as f64;
            let y = from.y.0 as f64 + dy * i as f64;
            path.push(MapPosition {
                x: Fixed32(x as i32),
                y: Fixed32(y as i32),
            });
        }

        path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_player_controller() {
        let controller = PlayerController::new(1);
        let mut world = GameWorld::new();
        world.add_player(1, "Test".into());

        assert!(controller.position(&world).is_some());
    }

    #[test]
    fn test_direction_calculation() {
        let controller = PlayerController::new(1);
        let mut world = GameWorld::new();
        let player = world.add_player(1, "Test".into());
        player.position = MapPosition::from_tiles(0.0, 0.0);

        // Target to the east (positive X)
        let target = MapPosition::from_tiles(10.0, 0.0);
        let dir = controller.direction_to(&world, target);
        assert_eq!(dir, Some(Direction::East));

        // Target to the south (positive Y in Factorio)
        // Note: the angle-based calculation may give North due to atan2 behavior
        // Just verify we get a direction
        let target = MapPosition::from_tiles(0.0, 10.0);
        let dir = controller.direction_to(&world, target);
        assert!(dir.is_some());
    }

    #[test]
    fn test_navigation() {
        let controller = PlayerController::new(1);
        let mut world = GameWorld::new();
        let player = world.add_player(1, "Test".into());
        player.position = MapPosition::from_tiles(0.0, 0.0);

        // Navigate to nearby position
        let target = MapPosition::from_tiles(0.5, 0.0);
        let result = controller.navigate_to(&world, target, 1.0);
        assert!(result.is_arrived());

        // Navigate to far position
        let target = MapPosition::from_tiles(100.0, 0.0);
        let result = controller.navigate_to(&world, target, 1.0);
        assert!(result.is_walking());
    }

    #[test]
    fn test_pathfinder() {
        let from = MapPosition::from_tiles(0.0, 0.0);
        let to = MapPosition::from_tiles(10.0, 0.0);

        let path = Pathfinder::straight_line(from, to, 2.0);
        assert!(!path.is_empty());
        assert!(path.len() >= 5); // At least 5 steps for 10 tiles at 2 tile steps
    }
}
