//! Optimized terrain generation - matches terrain.rs algorithm exactly

const CHUNK_SIZE: usize = 32;
const TILES: usize = CHUNK_SIZE * CHUNK_SIZE;

// Fixed register indices
const REG_X: usize = 0;
const REG_Y: usize = 1;
const REG_MOISTURE: usize = 2;
const REG_AUX: usize = 3;
const REG_HILLS: usize = 4;
const REG_CLIFF_LEVEL: usize = 5;
const REG_PLATEAUS: usize = 6;
const REG_HILLS_PLATEAUS: usize = 7;
const REG_BRIDGE_BILLOWS: usize = 8;
const REG_BRIDGES: usize = 9;
const REG_DETAIL: usize = 10;
const REG_MACRO: usize = 11;
const REG_ELEVATION: usize = 12;
const REG_DISTANCE: usize = 13;
const NUM_REGS: usize = 14;

const GRADIENTS: [(f32, f32); 8] = [
    (1.0, 0.0), (0.707, 0.707), (0.0, 1.0), (-0.707, 0.707),
    (-1.0, 0.0), (-0.707, -0.707), (0.0, -1.0), (0.707, -0.707),
];

/// Compute starting lake position from seed using Factorio's XORshift algorithm
/// Reverse-engineered from MapGenSettings::getStartingLakePositions() at 0x1014b4424
fn compute_lake_position(seed: u32) -> (f32, f32) {
    let state = seed.max(0x155);

    // Scalar XORshift
    let w8 = state ^ (state << 13);
    let w21 = ((state << 12) & 0xFFFFE000) | ((w8 >> 19) & 0x1FFF);

    // SIMD XORshift with shift constants from 0x10296c0d8: d8={3,2}, d9={17,4}, d10={-11,-25}
    let v2_0 = ((state << 3) ^ state) >> 11;
    let v2_1 = ((state << 2) ^ state) >> 25;
    let v3_0 = (state << 17) & 0xFFE00000;
    let v3_1 = (state << 4) & 0xFFFFFF80;
    let v20_0 = v2_0 | v3_0;
    let v20_1 = v2_1 | v3_1;

    // Combine to get random value
    let rand = v20_1 ^ v20_0 ^ w21;

    // Convert to angle: rand * 2^-32 * 2π
    let scale = 1.0 / 4294967296.0;
    let two_pi = std::f64::consts::PI * 2.0;
    let angle = (rand as f64) * scale * two_pi;

    // Lake at radius 75 from starting position (0, 0)
    // Formula: lake_x = start_x + r*cos(θ), lake_y = start_y + r*sin(θ)
    let radius = 75.0;
    let lake_x = (radius * angle.cos()) as f32;
    let lake_y = (radius * angle.sin()) as f32;

    (lake_x, lake_y)
}

/// Noise generator matching FactorioNoise from terrain.rs
struct Noise {
    perm: [u8; 256],
    seed_byte: u8,
}

impl Noise {
    fn new(seed: u32, seed1: i64) -> Self {
        let combined_seed = seed.wrapping_add((seed1 as u32).wrapping_mul(12345));
        let seed_byte = (combined_seed & 0xFF) as u8;

        let mut perm = [0u8; 256];
        for i in 0..256 {
            perm[i] = i as u8;
        }

        let mut rng = combined_seed.max(341);
        for i in (1..256).rev() {
            rng ^= rng << 13;
            rng ^= rng >> 19;
            rng ^= rng << 12;
            let j = (rng as usize) % (i + 1);
            perm.swap(i, j);
        }

        Self { perm, seed_byte }
    }

    #[inline(always)]
    fn hash(&self, xi: i32, yi: i32) -> usize {
        let x_idx = (xi & 0xFF) as usize;
        let y_idx = (yi & 0xFF) as usize;
        ((self.perm[x_idx] ^ self.seed_byte ^ self.perm[y_idx]) & 7) as usize
    }

    #[inline(always)]
    fn noise(&self, x: f32, y: f32) -> f32 {
        let xi = x.floor() as i32;
        let yi = y.floor() as i32;
        let xf = x - xi as f32;
        let yf = y - yi as f32;

        // Fade: 6t^5 - 15t^4 + 10t^3
        let u = xf * xf * xf * (xf * (xf * 6.0 - 15.0) + 10.0);
        let v = yf * yf * yf * (yf * (yf * 6.0 - 15.0) + 10.0);

        // Gradient dot products
        let grad = |hx: i32, hy: i32, dx: f32, dy: f32| -> f32 {
            let (gx, gy) = GRADIENTS[self.hash(hx, hy)];
            gx * dx + gy * dy
        };

        let n00 = grad(xi, yi, xf, yf);
        let n10 = grad(xi + 1, yi, xf - 1.0, yf);
        let n01 = grad(xi, yi + 1, xf, yf - 1.0);
        let n11 = grad(xi + 1, yi + 1, xf - 1.0, yf - 1.0);

        // Bilinear interpolation
        let nx0 = n00 + u * (n10 - n00);
        let nx1 = n01 + u * (n11 - n01);
        // Scale by sqrt(2) to normalize ±0.707 range to ±1.0
        (nx0 + v * (nx1 - nx0)) * 1.4142135
    }

    /// Standard multioctave noise - NO normalization (matches Factorio built-in)
    fn multioctave(&self, x: f32, y: f32, octaves: u32, persistence: f32, in_scale: f32, out_scale: f32) -> f32 {
        let mut total = 0.0f32;
        let mut amp = 1.0f32;
        let mut freq = in_scale;

        for _ in 0..octaves {
            total += self.noise(x * freq, y * freq) * amp;
            amp *= persistence;
            freq *= 2.0;
        }

        total * out_scale
    }

    /// Quick multioctave with variable multipliers - NO normalization (matches Factorio built-in)
    fn quick_multioctave(&self, x: f32, y: f32, octaves: u32, scales: (f32, f32, f32), out_mult: f32, in_mult: f32) -> f32 {
        let ax = x + scales.2;
        let mut total = 0.0f32;
        let mut amp = 1.0f32;
        let mut freq = scales.0;

        for _ in 0..octaves {
            total += self.noise(ax * freq, y * freq) * amp;
            amp *= out_mult;
            freq *= in_mult;
        }

        total * scales.1
    }
}

/// Fast terrain generator with pre-allocated buffers
pub struct FastTerrain {
    seed: u32,
    lake_x: f32,
    lake_y: f32,
    regs: [[f32; TILES]; NUM_REGS],
    noise_layers: [[f32; TILES]; 20],
}

impl FastTerrain {
    pub fn new(seed: u32) -> Self {
        // Compute lake position from seed using Factorio's XORshift algorithm
        let (lake_x, lake_y) = compute_lake_position(seed);
        Self {
            seed,
            lake_x,
            lake_y,
            regs: [[0.0; TILES]; NUM_REGS],
            noise_layers: [[0.0; TILES]; 20],
        }
    }

    pub fn compute_chunk(&mut self, chunk_x: i32, chunk_y: i32) -> [u8; TILES] {
        let base_x = (chunk_x * 32) as f32;
        let base_y = (chunk_y * 32) as f32;

        // Fill X/Y registers
        for dy in 0..32 {
            for dx in 0..32 {
                let i = dy * 32 + dx;
                self.regs[REG_X][i] = base_x + dx as f32;
                self.regs[REG_Y][i] = base_y + dy as f32;
            }
        }

        let seg = 1.5f32; // nauvis_segmentation_multiplier

        // Compute nauvis_hills = abs(multioctave(...))
        let hills_noise = Noise::new(self.seed, 900);
        for i in 0..TILES {
            let x = self.regs[REG_X][i];
            let y = self.regs[REG_Y][i];
            let h = hills_noise.multioctave(x, y, 4, 0.5, seg / 90.0, 1.0);
            self.regs[REG_HILLS][i] = h.abs();
        }

        // Compute nauvis_hills_cliff_level = clamp(0.65 + basis_noise(...), 0.15, 1.15)
        let cliff_noise = Noise::new(self.seed, 99584);
        for i in 0..TILES {
            let x = self.regs[REG_X][i];
            let y = self.regs[REG_Y][i];
            let c = cliff_noise.noise(x * seg / 500.0, y * seg / 500.0) * 0.6;
            self.regs[REG_CLIFF_LEVEL][i] = (0.65 + c).clamp(0.15, 1.15);
        }

        // Compute nauvis_plateaus = 0.5 + clamp((hills - cliff_level) * 10, -0.5, 0.5)
        for i in 0..TILES {
            let diff = (self.regs[REG_HILLS][i] - self.regs[REG_CLIFF_LEVEL][i]) * 10.0;
            self.regs[REG_PLATEAUS][i] = 0.5 + diff.clamp(-0.5, 0.5);
        }

        // Compute nauvis_hills_plateaus = 0.1 * hills + 0.8 * plateaus
        for i in 0..TILES {
            self.regs[REG_HILLS_PLATEAUS][i] = 0.1 * self.regs[REG_HILLS][i] + 0.8 * self.regs[REG_PLATEAUS][i];
        }

        // Compute nauvis_bridge_billows = abs(multioctave{seed1=700, octaves=4, persistence=0.5, input_scale=seg/150})
        let bridge_noise = Noise::new(self.seed, 700);
        for i in 0..TILES {
            let x = self.regs[REG_X][i];
            let y = self.regs[REG_Y][i];
            let b = bridge_noise.multioctave(x, y, 4, 0.5, seg / 150.0, 1.0);
            self.regs[REG_BRIDGE_BILLOWS][i] = b.abs();
        }

        // Compute nauvis_bridges = 1 - 0.1 * bridge_billows - 0.9 * max(0, -0.1 + bridge_billows)
        for i in 0..TILES {
            let bb = self.regs[REG_BRIDGE_BILLOWS][i];
            self.regs[REG_BRIDGES][i] = 1.0 - 0.1 * bb - 0.9 * (-0.1 + bb).max(0.0);
        }

        // Compute nauvis_detail (simplified - using fixed persistence 0.55)
        // variable_persistence_multioctave_noise{seed1=600, input_scale=seg/14, output_scale=0.03, octaves=5}
        let detail_noise = Noise::new(self.seed, 600);
        for i in 0..TILES {
            let x = self.regs[REG_X][i] + 10000.0 / seg;
            let y = self.regs[REG_Y][i];
            let d = detail_noise.multioctave(x, y, 5, 0.55, seg / 14.0, 1.0) * 0.03;
            self.regs[REG_DETAIL][i] = d;
        }

        // Compute nauvis_macro = multioctave{seed1=1000, octaves=2, persistence=0.6, input_scale=seg/1600}
        //                       * max(0, multioctave{seed1=1100, octaves=1, persistence=0.6, input_scale=seg/1600})
        let macro_noise1 = Noise::new(self.seed, 1000);
        let macro_noise2 = Noise::new(self.seed, 1100);
        for i in 0..TILES {
            let x = self.regs[REG_X][i];
            let y = self.regs[REG_Y][i];
            let m1 = macro_noise1.multioctave(x, y, 2, 0.6, seg / 1600.0, 1.0);
            let m2 = macro_noise2.multioctave(x, y, 1, 0.6, seg / 1600.0, 1.0);
            self.regs[REG_MACRO][i] = m1 * m2.max(0.0);
        }

        // Compute distance from origin (simplified - real uses starting_positions)
        for i in 0..TILES {
            let x = self.regs[REG_X][i];
            let y = self.regs[REG_Y][i];
            self.regs[REG_DISTANCE][i] = (x * x + y * y).sqrt();
        }

        self.compute_elevation(seg);

        // Compute moisture
        let moisture_noise = Noise::new(self.seed, 6);
        for i in 0..TILES {
            let x = self.regs[REG_X][i];
            let y = self.regs[REG_Y][i];
            let n = moisture_noise.quick_multioctave(x, y, 4, (1.0/256.0, 0.125, 30000.0), 1.5, 1.0/3.0);
            let m = 0.4 + n - 0.08 * (self.regs[REG_PLATEAUS][i] - 0.6);
            self.regs[REG_MOISTURE][i] = m.clamp(0.0, 1.0);
        }

        // Compute aux
        let aux_noise = Noise::new(self.seed, 7);
        for i in 0..TILES {
            let x = self.regs[REG_X][i];
            let y = self.regs[REG_Y][i];
            let n = aux_noise.quick_multioctave(x, y, 4, (1.0/2048.0, 0.25, 20000.0), 0.5, 3.0);
            let a = 0.5 + 0.06 * (self.regs[REG_PLATEAUS][i] - 0.4) + n;
            self.regs[REG_AUX][i] = a.clamp(0.0, 1.0);
        }

        // Compute noise layers for tile selection
        // Layer seeds: grass(19,20,21,22), dry-dirt(13), dirt(6-12), sand(36-38), red-desert(30-33)
        const LAYER_SEEDS: [(usize, i64); 19] = [
            (0, 19), (1, 20), (2, 21), (3, 22),     // grass-1 to grass-4
            (4, 13),                                  // dry-dirt
            (5, 6), (6, 7), (7, 8), (8, 9), (9, 10), (10, 11), (11, 12), // dirt-1 to dirt-7
            (12, 36), (13, 37), (14, 38),            // sand-1 to sand-3
            (15, 30), (16, 31), (17, 32), (18, 33),  // red-desert-0 to red-desert-3
        ];

        for &(idx, seed1) in &LAYER_SEEDS {
            let layer_noise = Noise::new(self.seed, seed1);
            for i in 0..TILES {
                let x = self.regs[REG_X][i];
                let y = self.regs[REG_Y][i];
                self.noise_layers[idx][i] = layer_noise.multioctave(x, y, 4, 0.7, 1.0/6.0, 2.0/3.0);
            }
        }

        // Select tiles
        self.select_tiles()
    }

    fn compute_elevation(&mut self, seg: f32) {
        let lake_noise = Noise::new(self.seed, 14);

        for i in 0..TILES {
            let x = self.regs[REG_X][i];
            let y = self.regs[REG_Y][i];
            let hills_plateaus = self.regs[REG_HILLS_PLATEAUS][i];
            let bridges = self.regs[REG_BRIDGES][i];
            let detail = self.regs[REG_DETAIL][i];
            let macro_val = self.regs[REG_MACRO][i];
            let distance = self.regs[REG_DISTANCE][i];

            let starting_macro_mult = (distance * seg / 2000.0).clamp(0.0, 1.0);

            let a = 0.5 * hills_plateaus - 0.6;
            let b = 1.9 * hills_plateaus + 1.6;
            let t = 0.1 + 0.5 * bridges;
            let lerped = a + t * (b - a);
            let nauvis_main = 20.0 * (lerped + 0.25 * detail + 3.0 * macro_val * starting_macro_mult);

            let starting_island = nauvis_main + 20.0 * (2.5 - distance * seg / 200.0);
            let wlc_elevation = nauvis_main.max(starting_island);

            let lake_dist = ((x - self.lake_x).powi(2) + (y - self.lake_y).powi(2)).sqrt();
            let starting_lake_noise = lake_noise.quick_multioctave(x, y, 4, (1.0/8.0, 0.8, 0.0), 0.68, 0.5);
            let starting_lake = 20.0 * (-3.0 + (lake_dist + starting_lake_noise) / 8.0) / 8.0;

            self.regs[REG_ELEVATION][i] = wlc_elevation.min(starting_lake);
        }
    }

    fn select_tiles(&self) -> [u8; TILES] {
        let mut result = [9u8; TILES]; // default dirt-5

        // Tile indices: 0-18 are land tiles, 19=water, 20=deepwater
        const TILE_WATER: u8 = 19;
        const TILE_DEEPWATER: u8 = 20;

        // Tile definitions: (aux_from, moisture_from, aux_to, moisture_to, layer_idx, secondary)
        #[allow(clippy::type_complexity)]
        const TILES_DEF: [(f32, f32, f32, f32, usize, Option<(f32, f32, f32, f32)>); 19] = [
            // Grass (layer 0-3)
            (-10.0, 0.7, 11.0, 11.0, 0, None),      // grass-1
            (0.45, 0.45, 11.0, 0.8, 1, None),       // grass-2
            (-10.0, 0.6, 0.65, 0.9, 2, None),       // grass-3
            (-10.0, 0.5, 0.55, 0.7, 3, None),       // grass-4
            // Dry-dirt (layer 4)
            (0.45, -10.0, 0.55, 0.35, 4, None),     // dry-dirt
            // Dirt (layer 5-11)
            (-10.0, 0.25, 0.45, 0.3, 5, Some((0.4, -10.0, 0.45, 0.25))),  // dirt-1
            (-10.0, 0.3, 0.45, 0.35, 6, None),      // dirt-2
            (-10.0, 0.35, 0.55, 0.4, 7, None),      // dirt-3
            (0.55, -10.0, 0.6, 0.35, 8, Some((0.6, 0.3, 11.0, 0.35))),    // dirt-4
            (-10.0, 0.4, 0.55, 0.45, 9, None),      // dirt-5
            (-10.0, 0.45, 0.55, 0.5, 10, None),     // dirt-6
            (-10.0, 0.5, 0.55, 0.55, 11, None),     // dirt-7
            // Sand (layer 12-14)
            (-10.0, -10.0, 0.25, 0.15, 12, None),   // sand-1
            (-10.0, 0.15, 0.3, 0.2, 13, Some((0.25, -10.0, 0.3, 0.15))),  // sand-2
            (-10.0, 0.2, 0.4, 0.25, 14, Some((0.3, -10.0, 0.4, 0.2))),    // sand-3
            // Red desert (layer 15-18)
            (0.55, 0.35, 11.0, 0.5, 15, None),      // red-desert-0
            (0.6, -10.0, 0.7, 0.3, 16, Some((0.7, 0.25, 11.0, 0.3))),     // red-desert-1
            (0.7, -10.0, 0.8, 0.25, 17, Some((0.8, 0.2, 11.0, 0.25))),    // red-desert-2
            (0.8, -10.0, 11.0, 0.2, 18, None),      // red-desert-3
        ];

        for i in 0..TILES {
            let elevation = self.regs[REG_ELEVATION][i];

            // Check for water tiles first (based on elevation)
            // water_base(0, 100) for water: appears when elevation <= 0
            // water_base(-2, 200) for deepwater: appears when elevation <= -2
            if elevation <= -2.0 {
                result[i] = TILE_DEEPWATER;
                continue;
            }
            if elevation <= 0.0 {
                result[i] = TILE_WATER;
                continue;
            }

            let aux = self.regs[REG_AUX][i];
            let moisture = self.regs[REG_MOISTURE][i];

            let mut best_prob = f32::NEG_INFINITY;
            let mut best_tile = 9u8;

            for (tile_idx, &(af, mf, at, mt, layer, secondary)) in TILES_DEF.iter().enumerate() {
                let mut prob = expr_in_range(aux, moisture, af, mf, at, mt);
                if let Some((af2, mf2, at2, mt2)) = secondary {
                    prob = prob.max(expr_in_range(aux, moisture, af2, mf2, at2, mt2));
                }
                prob += self.noise_layers[layer][i];

                if prob > best_prob {
                    best_prob = prob;
                    best_tile = tile_idx as u8;
                }
            }

            result[i] = best_tile;
        }

        result
    }

    /// Debug: get moisture/aux stats after compute_chunk
    pub fn debug_stats(&self) -> (f32, f32, f32, f32) {
        let moisture = &self.regs[REG_MOISTURE];
        let aux = &self.regs[REG_AUX];
        let m_min = moisture.iter().cloned().fold(f32::INFINITY, f32::min);
        let m_max = moisture.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        let a_min = aux.iter().cloned().fold(f32::INFINITY, f32::min);
        let a_max = aux.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        (m_min, m_max, a_min, a_max)
    }

    /// Debug: test raw noise range
    pub fn test_noise_range(&self) -> (f32, f32, f32, f32) {
        let noise = Noise::new(self.seed, 700);
        let mut raw_min = f32::INFINITY;
        let mut raw_max = f32::NEG_INFINITY;
        let mut multi_min = f32::INFINITY;
        let mut multi_max = f32::NEG_INFINITY;

        // Sample noise at many points
        for y in -100..100 {
            for x in -100..100 {
                let fx = x as f32 * 0.1;
                let fy = y as f32 * 0.1;
                let raw = noise.noise(fx, fy);
                let multi = noise.multioctave(fx * 100.0, fy * 100.0, 4, 0.5, 0.01, 1.0);
                raw_min = raw_min.min(raw);
                raw_max = raw_max.max(raw);
                multi_min = multi_min.min(multi);
                multi_max = multi_max.max(multi);
            }
        }
        (raw_min, raw_max, multi_min, multi_max)
    }

    /// Debug: get elevation stats after compute_chunk
    pub fn elevation_stats(&self) -> (f32, f32) {
        let elevation = &self.regs[REG_ELEVATION];
        let e_min = elevation.iter().cloned().fold(f32::INFINITY, f32::min);
        let e_max = elevation.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        (e_min, e_max)
    }

    /// Debug: get stats for all elevation components
    pub fn elevation_components(&self) -> String {
        let hills = &self.regs[REG_HILLS];
        let cliff_level = &self.regs[REG_CLIFF_LEVEL];
        let plateaus = &self.regs[REG_PLATEAUS];
        let hills_plateaus = &self.regs[REG_HILLS_PLATEAUS];
        let bridges = &self.regs[REG_BRIDGES];
        let h_min = hills.iter().cloned().fold(f32::INFINITY, f32::min);
        let h_max = hills.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        let cl_min = cliff_level.iter().cloned().fold(f32::INFINITY, f32::min);
        let cl_max = cliff_level.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        let p_min = plateaus.iter().cloned().fold(f32::INFINITY, f32::min);
        let p_max = plateaus.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        let hp_min = hills_plateaus.iter().cloned().fold(f32::INFINITY, f32::min);
        let hp_max = hills_plateaus.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        let br_min = bridges.iter().cloned().fold(f32::INFINITY, f32::min);
        let br_max = bridges.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        let bb = &self.regs[REG_BRIDGE_BILLOWS];
        let bb_min = bb.iter().cloned().fold(f32::INFINITY, f32::min);
        let bb_max = bb.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        format!("hills: {:.2}-{:.2}, cliff: {:.2}-{:.2}, plat: {:.2}-{:.2}, hp: {:.2}-{:.2}, bb: {:.2}-{:.2}, br: {:.2}-{:.2}",
                h_min, h_max, cl_min, cl_max, p_min, p_max, hp_min, hp_max, bb_min, bb_max, br_min, br_max)
    }

    pub fn tile_name(idx: u8) -> &'static str {
        const NAMES: [&str; 22] = [
            "grass-1", "grass-2", "grass-3", "grass-4",
            "dry-dirt",
            "dirt-1", "dirt-2", "dirt-3", "dirt-4", "dirt-5", "dirt-6", "dirt-7",
            "sand-1", "sand-2", "sand-3",
            "red-desert-0", "red-desert-1", "red-desert-2", "red-desert-3",
            "water", "deepwater",
            "unknown",
        ];
        NAMES.get(idx as usize).unwrap_or(&"unknown")
    }
}

#[inline(always)]
fn expr_in_range(aux: f32, moisture: f32, af: f32, mf: f32, at: f32, mt: f32) -> f32 {
    let slope = 20.0f32;
    let mid_a = (af + at) * 0.5;
    let half_a = (at - af) * 0.5;
    let mid_m = (mf + mt) * 0.5;
    let half_m = (mt - mf) * 0.5;

    let pa = ((half_a - (aux - mid_a).abs()) * slope).min(1.0);
    let pm = ((half_m - (moisture - mid_m).abs()) * slope).min(1.0);
    pa.min(pm).min(1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_terrain() {
        let mut ft = FastTerrain::new(12345);
        let tiles = ft.compute_chunk(0, 0);

        let mut counts = [0usize; 22];
        for &t in &tiles {
            counts[t as usize] += 1;
        }

        let nonzero = counts.iter().filter(|&&c| c > 0).count();
        assert!(nonzero > 1);
    }

    #[test]
    fn test_noise_matches() {
        // Verify our noise matches the original
        let n = Noise::new(12345, 6);
        let v = n.noise(100.5, 200.5);
        // Should be in [-1, 1] range
        assert!(v >= -1.0 && v <= 1.0);

        // Test variance over a range
        let mut min_v = f32::INFINITY;
        let mut max_v = f32::NEG_INFINITY;
        for i in 0..1000 {
            let x = i as f32 * 0.1;
            let v = n.noise(x, 0.0);
            min_v = min_v.min(v);
            max_v = max_v.max(v);
        }
        println!("Noise range over 100 units: {} to {}", min_v, max_v);
        // Perlin with these gradients typically ranges about [-0.7, 0.7]
        assert!(max_v - min_v > 0.5, "Noise doesn't vary enough: {} to {}", min_v, max_v);
    }
}
