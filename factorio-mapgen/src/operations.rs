//! Noise operations that process entire chunks at once
//!
//! Each operation reads from input registers and writes to output registers,
//! processing all 1024 tiles in a single call.

use crate::cache::{NoiseCache, TILES_PER_CHUNK};

/// Trait for noise operations
pub trait NoiseOp {
    /// Execute the operation on all tiles in the cache
    fn execute(&self, cache: &mut NoiseCache);

    /// Name of the output register this operation writes to
    fn output_register(&self) -> &'static str;
}

// ============================================================================
// Noise generation (Perlin-style)
// ============================================================================

/// 256 gradient vectors - initialized lazily
fn get_gradient(idx: usize) -> (f32, f32) {
    // Generate 256 evenly distributed unit vectors
    let angle = (idx as f32) * std::f32::consts::TAU / 256.0;
    (angle.cos(), angle.sin())
}

/// Factorio's xorshift PRNG - combines scalar and SIMD-like operations
/// From RE at 0x1015d63fc
fn factorio_xorshift(state: &mut u32, simd_state: &mut [u32; 2]) -> u32 {
    // Scalar part
    let x = *state;
    let tmp = x ^ (x << 13);
    let shifted = x << 12;
    let scalar = (shifted & 0xFFFFE000) | ((tmp >> 19) & 0x1FFF);
    *state = scalar;

    // SIMD-like part (two lanes)
    let s = *simd_state;
    let lane0 = (((s[0] << 3) ^ s[0]) >> 11) | ((s[0] << 17) & 0xffe00000);
    let lane1 = (((s[1] << 2) ^ s[1]) >> 25) | ((s[1] << 4) & 0xffffff80);

    simd_state[0] = lane0;
    simd_state[1] = lane1;

    // Combine all three
    lane0 ^ lane1 ^ scalar
}

/// Shuffle permutation table using Factorio's Fisher-Yates
fn shuffle_permutation(seed: u32, perm: &mut [u8; 256]) {
    let mut scalar = seed.max(0x155);
    let mut simd = [scalar, scalar];

    for i in (1..=255).rev() {
        let rand = factorio_xorshift(&mut scalar, &mut simd);
        let j = (rand as usize) % (i + 1);
        perm.swap(i, j);
    }
}

pub struct PerlinNoise {
    perm1: [u8; 256],
    perm2: [u8; 256],
    gradients: [(f32, f32); 256],
    seed_byte: u8,
}

impl PerlinNoise {
    const GRADIENT_MAGNITUDE: f64 = 4.2;

    pub fn new(seed0: u32, seed1: i64) -> Self {
        // Factorio seed combination: combined = seed0 + ((seed1 >> 8) * 7)
        let combined_seed = seed0.wrapping_add(((seed1 >> 8) as u32).wrapping_mul(7));
        let seed_byte_idx = (seed1 & 0xFF) as usize;

        let mut perm1 = [0u8; 256];
        let mut perm2 = [0u8; 256];
        for i in 0..256 {
            perm1[i] = i as u8;
            perm2[i] = i as u8;
        }

        // Generate gradients matching Factorio's defaultNoise template:
        // gradient[i] = (cos(θ), sin(θ)) * 4.2 where θ = 2π*i/256
        // Factorio truncates angle to f32 precision before computing cos/sin
        let mut gradients = [(0.0f32, 0.0f32); 256];
        for i in 0..256 {
            let angle_raw = (i as f64) * std::f64::consts::TAU / 256.0;
            let angle = (angle_raw as f32) as f64; // float precision loss (matches binary)
            let gx = (angle.cos() * Self::GRADIENT_MAGNITUDE) as f32;
            let gy = (angle.sin() * Self::GRADIENT_MAGNITUDE) as f32;
            gradients[i] = (gx, gy);
        }

        // PRNG state (continues across all 4 shuffles)
        let init = combined_seed.max(0x155);
        let mut scalar = init;
        let mut simd = [init, init];

        // SHUFFLE A: shuffle a COPY of perm1 for seed_byte extraction only
        let mut perm1_copy = perm1;
        for i in (1..=255).rev() {
            let rand = factorio_xorshift(&mut scalar, &mut simd);
            perm1_copy.swap(i, (rand as usize) % (i + 1));
        }
        let seed_byte = perm1_copy[seed_byte_idx];

        // SHUFFLE B: shuffle the actual perm1 (different from A, PRNG continues)
        for i in (1..=255).rev() {
            let rand = factorio_xorshift(&mut scalar, &mut simd);
            perm1.swap(i, (rand as usize) % (i + 1));
        }

        // SHUFFLE C: shuffle perm2
        for i in (1..=255).rev() {
            let rand = factorio_xorshift(&mut scalar, &mut simd);
            perm2.swap(i, (rand as usize) % (i + 1));
        }

        // SHUFFLE D: shuffle gradients
        for i in (1..=255).rev() {
            let rand = factorio_xorshift(&mut scalar, &mut simd);
            gradients.swap(i, (rand as usize) % (i + 1));
        }

        Self { perm1, perm2, gradients, seed_byte }
    }

    #[inline]
    fn hash(&self, xi: i32, yi: i32) -> usize {
        (self.perm1[(yi & 0xFF) as usize] ^ self.seed_byte ^ self.perm2[(xi & 0xFF) as usize]) as usize
    }

    /// Factorio noise uses radial falloff: (1 - min(dist², 1))³
    pub fn noise(&self, x: f32, y: f32) -> f32 {
        let x0 = x.floor() as i32;
        let y0 = y.floor() as i32;
        let fx = x - x0 as f32;
        let fy = y - y0 as f32;

        let mut result = 0.0f32;

        for dy in 0..2i32 {
            for dx in 0..2i32 {
                let idx = self.hash(x0 + dx, y0 + dy);
                let (grad_x, grad_y) = self.gradients[idx];
                let fx_off = fx - dx as f32;
                let fy_off = fy - dy as f32;
                let dist_sq = fx_off * fx_off + fy_off * fy_off;
                let w = (1.0 - dist_sq.min(1.0)).powi(3);
                result += (grad_x * fx_off + grad_y * fy_off) * w;
            }
        }

        result
    }
}

// ============================================================================
// Basis Noise Operation
// ============================================================================

/// Single-octave Perlin noise
pub struct BasisNoiseOp {
    pub output: &'static str,
    pub seed1: i64,
    pub input_scale: f32,
    pub output_scale: f32,
    pub offset_x: f32,
}

impl NoiseOp for BasisNoiseOp {
    fn execute(&self, cache: &mut NoiseCache) {
        let noise = PerlinNoise::new(cache.seed, self.seed1);
        let scale = self.input_scale;
        let out_scale = self.output_scale;
        let offset = self.offset_x;

        // Read x/y registers
        let x_vals: Vec<f32> = cache.get("x").unwrap().to_vec();
        let y_vals: Vec<f32> = cache.get("y").unwrap().to_vec();
        let out = cache.get_mut(self.output);

        for i in 0..TILES_PER_CHUNK {
            let x = (x_vals[i] + offset) * scale;
            let y = y_vals[i] * scale;
            out[i] = noise.noise(x, y) * out_scale;
        }
    }

    fn output_register(&self) -> &'static str {
        self.output
    }
}

// ============================================================================
// Multi-octave Noise Operation
// ============================================================================

/// Multi-octave fractal noise
pub struct MultioctaveNoiseOp {
    pub output: &'static str,
    pub seed1: i64,
    pub octaves: u32,
    pub persistence: f32,
    pub input_scale: f32,
    pub output_scale: f32,
    pub offset_x: f32,
}

impl NoiseOp for MultioctaveNoiseOp {
    fn execute(&self, cache: &mut NoiseCache) {
        let noise = PerlinNoise::new(cache.seed, self.seed1);

        let x_vals: Vec<f32> = cache.get("x").unwrap().to_vec();
        let y_vals: Vec<f32> = cache.get("y").unwrap().to_vec();
        let out = cache.get_mut(self.output);

        for i in 0..TILES_PER_CHUNK {
            let base_x = x_vals[i] + self.offset_x;
            let base_y = y_vals[i];

            let mut total = 0.0f32;
            let mut amplitude = 1.0f32;
            let mut frequency = self.input_scale;
            let mut max_amplitude = 0.0f32;

            for _ in 0..self.octaves {
                total += noise.noise(base_x * frequency, base_y * frequency) * amplitude;
                max_amplitude += amplitude;
                amplitude *= self.persistence;
                frequency *= 2.0;
            }

            out[i] = (total / max_amplitude) * self.output_scale;
        }
    }

    fn output_register(&self) -> &'static str {
        self.output
    }
}

// ============================================================================
// Quick Multi-octave Noise (variable multipliers)
// ============================================================================

/// Quick multi-octave noise with configurable scale multipliers
pub struct QuickMultioctaveNoiseOp {
    pub output: &'static str,
    pub seed1: i64,
    pub octaves: u32,
    pub input_scale: f32,
    pub output_scale: f32,
    pub offset_x: f32,
    pub octave_output_scale_multiplier: f32,
    pub octave_input_scale_multiplier: f32,
}

impl NoiseOp for QuickMultioctaveNoiseOp {
    fn execute(&self, cache: &mut NoiseCache) {
        let noise = PerlinNoise::new(cache.seed, self.seed1);

        let x_vals: Vec<f32> = cache.get("x").unwrap().to_vec();
        let y_vals: Vec<f32> = cache.get("y").unwrap().to_vec();
        let out = cache.get_mut(self.output);

        for i in 0..TILES_PER_CHUNK {
            let base_x = x_vals[i] + self.offset_x;
            let base_y = y_vals[i];

            let mut total = 0.0f32;
            let mut amplitude = 1.0f32;
            let mut frequency = self.input_scale;
            let mut max_amplitude = 0.0f32;

            for _ in 0..self.octaves {
                total += noise.noise(base_x * frequency, base_y * frequency) * amplitude;
                max_amplitude += amplitude;
                amplitude *= self.octave_output_scale_multiplier;
                frequency *= self.octave_input_scale_multiplier;
            }

            out[i] = (total / max_amplitude) * self.output_scale;
        }
    }

    fn output_register(&self) -> &'static str {
        self.output
    }
}

// ============================================================================
// Arithmetic Operations
// ============================================================================

/// Add two registers: output = a + b
pub struct AddOp {
    pub output: &'static str,
    pub input_a: &'static str,
    pub input_b: &'static str,
}

impl NoiseOp for AddOp {
    fn execute(&self, cache: &mut NoiseCache) {
        let a: Vec<f32> = cache.get(self.input_a).unwrap_or(&[0.0; TILES_PER_CHUNK]).to_vec();
        let b: Vec<f32> = cache.get(self.input_b).unwrap_or(&[0.0; TILES_PER_CHUNK]).to_vec();
        let out = cache.get_mut(self.output);
        for i in 0..TILES_PER_CHUNK {
            out[i] = a[i] + b[i];
        }
    }

    fn output_register(&self) -> &'static str {
        self.output
    }
}

/// Multiply register by constant: output = input * scale
pub struct ScaleOp {
    pub output: &'static str,
    pub input: &'static str,
    pub scale: f32,
}

impl NoiseOp for ScaleOp {
    fn execute(&self, cache: &mut NoiseCache) {
        let inp: Vec<f32> = cache.get(self.input).unwrap_or(&[0.0; TILES_PER_CHUNK]).to_vec();
        let out = cache.get_mut(self.output);
        for i in 0..TILES_PER_CHUNK {
            out[i] = inp[i] * self.scale;
        }
    }

    fn output_register(&self) -> &'static str {
        self.output
    }
}

/// Add constant to register: output = input + offset
pub struct OffsetOp {
    pub output: &'static str,
    pub input: &'static str,
    pub offset: f32,
}

impl NoiseOp for OffsetOp {
    fn execute(&self, cache: &mut NoiseCache) {
        let inp: Vec<f32> = cache.get(self.input).unwrap_or(&[0.0; TILES_PER_CHUNK]).to_vec();
        let out = cache.get_mut(self.output);
        for i in 0..TILES_PER_CHUNK {
            out[i] = inp[i] + self.offset;
        }
    }

    fn output_register(&self) -> &'static str {
        self.output
    }
}

/// Clamp register: output = clamp(input, min, max)
pub struct ClampOp {
    pub output: &'static str,
    pub input: &'static str,
    pub min: f32,
    pub max: f32,
}

impl NoiseOp for ClampOp {
    fn execute(&self, cache: &mut NoiseCache) {
        let inp: Vec<f32> = cache.get(self.input).unwrap_or(&[0.0; TILES_PER_CHUNK]).to_vec();
        let out = cache.get_mut(self.output);
        for i in 0..TILES_PER_CHUNK {
            out[i] = inp[i].clamp(self.min, self.max);
        }
    }

    fn output_register(&self) -> &'static str {
        self.output
    }
}

/// Absolute value: output = abs(input)
pub struct AbsOp {
    pub output: &'static str,
    pub input: &'static str,
}

impl NoiseOp for AbsOp {
    fn execute(&self, cache: &mut NoiseCache) {
        let inp: Vec<f32> = cache.get(self.input).unwrap_or(&[0.0; TILES_PER_CHUNK]).to_vec();
        let out = cache.get_mut(self.output);
        for i in 0..TILES_PER_CHUNK {
            out[i] = inp[i].abs();
        }
    }

    fn output_register(&self) -> &'static str {
        self.output
    }
}

// ============================================================================
// Expression In Range (tile probability calculation)
// ============================================================================

/// Calculate tile probability based on aux/moisture ranges
pub struct ExpressionInRangeOp {
    pub output: &'static str,
    pub aux_from: f32,
    pub moisture_from: f32,
    pub aux_to: f32,
    pub moisture_to: f32,
    pub slope: f32,
    pub output_scale: f32,
}

impl NoiseOp for ExpressionInRangeOp {
    fn execute(&self, cache: &mut NoiseCache) {
        let aux: Vec<f32> = cache.get("aux").unwrap_or(&[0.5; TILES_PER_CHUNK]).to_vec();
        let moisture: Vec<f32> = cache.get("moisture").unwrap_or(&[0.4; TILES_PER_CHUNK]).to_vec();
        let out = cache.get_mut(self.output);

        let mid_aux = (self.aux_from + self.aux_to) * 0.5;
        let half_aux = (self.aux_to - self.aux_from) * 0.5;
        let mid_moisture = (self.moisture_from + self.moisture_to) * 0.5;
        let half_moisture = (self.moisture_to - self.moisture_from) * 0.5;

        for i in 0..TILES_PER_CHUNK {
            let peak_aux = ((half_aux - (aux[i] - mid_aux).abs()) * self.slope).min(self.output_scale);
            let peak_moisture = ((half_moisture - (moisture[i] - mid_moisture).abs()) * self.slope).min(self.output_scale);
            out[i] = peak_aux.min(peak_moisture).min(self.output_scale);
        }
    }

    fn output_register(&self) -> &'static str {
        self.output
    }
}

// ============================================================================
// Noise layer noise (for tile selection randomization)
// ============================================================================

/// Noise layer for tile selection variation
pub struct NoiseLayerOp {
    pub output: &'static str,
    pub layer_seed: i64,
}

impl NoiseOp for NoiseLayerOp {
    fn execute(&self, cache: &mut NoiseCache) {
        let noise = PerlinNoise::new(cache.seed, self.layer_seed);

        let x_vals: Vec<f32> = cache.get("x").unwrap().to_vec();
        let y_vals: Vec<f32> = cache.get("y").unwrap().to_vec();
        let out = cache.get_mut(self.output);

        let input_scale = 1.0 / 6.0;
        let output_scale = 2.0 / 3.0;
        let persistence = 0.7f32;

        for i in 0..TILES_PER_CHUNK {
            let base_x = x_vals[i];
            let base_y = y_vals[i];

            let mut total = 0.0f32;
            let mut amplitude = 1.0f32;
            let mut frequency = input_scale;
            let mut max_amplitude = 0.0f32;

            for _ in 0..4 {
                total += noise.noise(base_x * frequency, base_y * frequency) * amplitude;
                max_amplitude += amplitude;
                amplitude *= persistence;
                frequency *= 2.0;
            }

            out[i] = (total / max_amplitude) * output_scale;
        }
    }

    fn output_register(&self) -> &'static str {
        self.output
    }
}
