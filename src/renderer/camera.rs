pub struct Camera2D {
    pub x: f64,
    pub y: f64,
    pub zoom: f64, // tiles visible vertically
    pub target_x: f64,
    pub target_y: f64,
    pub target_zoom: f64,
    pub aspect: f32,
}

impl Camera2D {
    pub fn new() -> Self {
        Self {
            x: 0.0,
            y: 0.0,
            zoom: 64.0,
            target_x: 0.0,
            target_y: 0.0,
            target_zoom: 64.0,
            aspect: 16.0 / 9.0,
        }
    }

    pub fn update(&mut self, lerp_factor: f64) {
        self.x += (self.target_x - self.x) * lerp_factor;
        self.y += (self.target_y - self.y) * lerp_factor;
        self.zoom += (self.target_zoom - self.zoom) * lerp_factor;
        self.zoom = self.zoom.clamp(4.0, 256.0);
        self.target_zoom = self.target_zoom.clamp(4.0, 256.0);
    }

    pub fn view_proj(&self) -> [[f32; 4]; 4] {
        let half_h = (self.zoom / 2.0) as f32;
        let half_w = half_h * self.aspect;
        let cx = self.x as f32;
        let cy = self.y as f32;

        let l = cx - half_w;
        let r = cx + half_w;
        let b = cy + half_h; // +Y is down in Factorio
        let t = cy - half_h;

        ortho(l, r, b, t)
    }

    pub fn visible_bounds(&self) -> (f64, f64, f64, f64) {
        let half_h = self.zoom / 2.0;
        let half_w = half_h * self.aspect as f64;
        (self.x - half_w, self.y - half_h, self.x + half_w, self.y + half_h)
    }

    pub fn pan(&mut self, dx: f64, dy: f64) {
        self.target_x += dx * self.zoom * 0.02;
        self.target_y += dy * self.zoom * 0.02;
    }

    pub fn zoom_by(&mut self, factor: f64) {
        self.target_zoom *= factor;
    }
}

fn ortho(l: f32, r: f32, b: f32, t: f32) -> [[f32; 4]; 4] {
    [
        [2.0 / (r - l), 0.0, 0.0, 0.0],
        [0.0, 2.0 / (t - b), 0.0, 0.0],
        [0.0, 0.0, 1.0, 0.0],
        [-(r + l) / (r - l), -(t + b) / (t - b), 0.0, 1.0],
    ]
}
