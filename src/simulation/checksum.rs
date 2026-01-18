use crc32fast::Hasher;
use crate::state::GameWorld;

/// CRC calculator for game state
pub struct ChecksumCalculator {
    hasher: Hasher,
}

impl ChecksumCalculator {
    pub fn new() -> Self {
        Self {
            hasher: Hasher::new(),
        }
    }

    /// Calculate CRC32 for a byte slice
    pub fn crc32(data: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }

    /// Update hasher with data
    pub fn update(&mut self, data: &[u8]) {
        self.hasher.update(data);
    }

    /// Get current checksum
    pub fn finalize(self) -> u32 {
        self.hasher.finalize()
    }

    /// Reset for new calculation
    pub fn reset(&mut self) {
        self.hasher = Hasher::new();
    }

    /// Calculate a simplified game state checksum
    /// Note: Full checksum calculation requires matching Factorio's exact algorithm
    pub fn calculate_world_checksum(world: &GameWorld) -> u32 {
        let mut hasher = Hasher::new();

        // Hash tick
        hasher.update(&world.tick.to_le_bytes());

        // Hash seed
        hasher.update(&world.seed.to_le_bytes());

        // Hash player count and positions
        hasher.update(&(world.players.len() as u32).to_le_bytes());
        for (id, player) in &world.players {
            hasher.update(&id.to_le_bytes());
            hasher.update(&player.position.x.0.to_le_bytes());
            hasher.update(&player.position.y.0.to_le_bytes());
        }

        // Hash entity count per surface
        for (surface_id, surface) in &world.surfaces {
            hasher.update(&surface_id.to_le_bytes());
            hasher.update(&(surface.entities.len() as u32).to_le_bytes());
        }

        hasher.finalize()
    }
}

impl Default for ChecksumCalculator {
    fn default() -> Self {
        Self::new()
    }
}

/// Desync detection
#[derive(Debug, Clone)]
pub struct DesyncInfo {
    pub tick: u32,
    pub local_crc: u32,
    pub server_crc: u32,
}

impl DesyncInfo {
    pub fn new(tick: u32, local_crc: u32, server_crc: u32) -> Self {
        Self {
            tick,
            local_crc,
            server_crc,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32() {
        let crc = ChecksumCalculator::crc32(b"hello world");
        assert_ne!(crc, 0);

        // Same input should give same output
        let crc2 = ChecksumCalculator::crc32(b"hello world");
        assert_eq!(crc, crc2);

        // Different input should give different output
        let crc3 = ChecksumCalculator::crc32(b"hello World");
        assert_ne!(crc, crc3);
    }

    #[test]
    fn test_world_checksum() {
        let world = GameWorld::new();
        let crc1 = ChecksumCalculator::calculate_world_checksum(&world);

        let mut world2 = GameWorld::new();
        world2.tick = 100;
        let crc2 = ChecksumCalculator::calculate_world_checksum(&world2);

        assert_ne!(crc1, crc2);
    }
}
