//! Register-based noise cache for batch terrain generation
//!
//! Mirrors Factorio's NoiseCache architecture: a set of registers that hold
//! computed values for an entire 32x32 chunk (1024 positions).

use std::collections::HashMap;

/// Size of a chunk in tiles
pub const CHUNK_SIZE: usize = 32;
/// Number of tiles per chunk
pub const TILES_PER_CHUNK: usize = CHUNK_SIZE * CHUNK_SIZE;

/// Register-based cache for noise computations.
///
/// Each register holds 1024 f32 values (one per tile in a chunk).
/// Operations read from input registers and write to output registers.
pub struct NoiseCache {
    /// Map seed for noise generation
    pub seed: u32,
    /// Named registers holding computed values
    registers: HashMap<&'static str, Vec<f32>>,
}

impl NoiseCache {
    pub fn new(seed: u32) -> Self {
        let mut cache = Self {
            seed,
            registers: HashMap::with_capacity(32),
        };

        // Pre-allocate common registers
        cache.ensure_register("x");
        cache.ensure_register("y");
        cache.ensure_register("distance");
        cache.ensure_register("moisture");
        cache.ensure_register("aux");

        cache
    }

    /// Initialize X/Y registers for a chunk
    pub fn init_chunk(&mut self, chunk_x: i32, chunk_y: i32) {
        let base_x = (chunk_x * CHUNK_SIZE as i32) as f32;
        let base_y = (chunk_y * CHUNK_SIZE as i32) as f32;

        // Initialize x register
        self.ensure_register("x");
        for dy in 0..CHUNK_SIZE {
            for dx in 0..CHUNK_SIZE {
                let idx = dy * CHUNK_SIZE + dx;
                self.registers.get_mut("x").unwrap()[idx] = base_x + dx as f32;
            }
        }

        // Initialize y register
        self.ensure_register("y");
        for dy in 0..CHUNK_SIZE {
            for dx in 0..CHUNK_SIZE {
                let idx = dy * CHUNK_SIZE + dx;
                self.registers.get_mut("y").unwrap()[idx] = base_y + dy as f32;
            }
        }

        // Compute distance from origin (simplified - real Factorio uses starting_positions)
        self.ensure_register("distance");
        let x_vals: Vec<f32> = self.registers.get("x").unwrap().clone();
        let y_vals: Vec<f32> = self.registers.get("y").unwrap().clone();
        let dist_reg = self.registers.get_mut("distance").unwrap();
        for i in 0..TILES_PER_CHUNK {
            let x = x_vals[i];
            let y = y_vals[i];
            dist_reg[i] = (x * x + y * y).sqrt();
        }
    }

    /// Get or create a register by name
    pub fn ensure_register(&mut self, name: &'static str) -> &mut Vec<f32> {
        self.registers.entry(name).or_insert_with(|| vec![0.0; TILES_PER_CHUNK])
    }

    /// Get a register for reading
    pub fn get(&self, name: &str) -> Option<&[f32]> {
        self.registers.get(name).map(|v| v.as_slice())
    }

    /// Get a register for writing
    pub fn get_mut(&mut self, name: &'static str) -> &mut [f32] {
        self.ensure_register(name)
    }

    /// Copy register values
    pub fn copy_register(&mut self, from: &'static str, to: &'static str) {
        if let Some(src) = self.registers.get(from) {
            let src_copy: Vec<f32> = src.clone();
            let dst = self.ensure_register(to);
            dst.copy_from_slice(&src_copy);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_init() {
        let mut cache = NoiseCache::new(12345);
        cache.init_chunk(0, 0);

        let x = cache.get("x").unwrap();
        let y = cache.get("y").unwrap();

        // First tile should be (0, 0)
        assert_eq!(x[0], 0.0);
        assert_eq!(y[0], 0.0);

        // Last tile should be (31, 31)
        assert_eq!(x[TILES_PER_CHUNK - 1], 31.0);
        assert_eq!(y[TILES_PER_CHUNK - 1], 31.0);

        // Tile at (5, 3) = index 3*32+5 = 101
        assert_eq!(x[101], 5.0);
        assert_eq!(y[101], 3.0);
    }

    #[test]
    fn test_chunk_offset() {
        let mut cache = NoiseCache::new(12345);
        cache.init_chunk(1, -1);

        let x = cache.get("x").unwrap();
        let y = cache.get("y").unwrap();

        // Chunk (1, -1) starts at world position (32, -32)
        assert_eq!(x[0], 32.0);
        assert_eq!(y[0], -32.0);
    }
}
