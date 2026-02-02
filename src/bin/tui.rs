//! Factorio Client TUI - Interactive game client
//!
//! Run with: cargo run --bin factorio-tui --features tui -- [server:port] [username]
//! Offline: cargo run --bin factorio-tui --features tui -- --map captured_map.zip
//! Procedural only: cargo run --bin factorio-tui --features tui -- --seed 12345 --size 1000
//!
//! Controls:
//!   WASD/Arrows - Move player / Pan map (offline)
//!   IJKL - Move cursor
//!   +/= - Zoom in
//!   -/_ - Zoom out
//!   M - Mine at cursor position
//!   B - Build at cursor position
//!   R - Rotate entity at cursor
//!   P - Toggle parsed map display
//!   Space - Stop action
//!   Tab - Toggle cursor/movement mode
//!   Enter - Connect (when disconnected)
//!   C - Chat mode
//!   H - Help
//!   Q - Quit

#[cfg(not(feature = "tui"))]
fn main() {
    eprintln!("TUI feature not enabled. Run with:");
    eprintln!("  cargo run --bin factorio-tui --features tui -- [server:port] [username]");
}

#[cfg(feature = "tui")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tui_main::run()
}

#[cfg(feature = "tui")]
mod tui_main {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::io::stdout;
    use std::fs::OpenOptions;
    use std::io::Write as IoWrite;
    #[cfg(feature = "gpu")]
    use std::io::Write;
    use std::net::SocketAddr;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};

    use ratatui_image::{picker::Picker, protocol::StatefulProtocol, StatefulImage, Resize};

    use crossterm::{
        event::{self, Event, KeyCode, KeyEventKind},
        terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
        ExecutableCommand,
    };
    use ratatui::{
        prelude::*,
        widgets::{Block, Borders, Paragraph},
    };

    use factorio_client::codec::{MapEntity, MapTile, parse_map_data_with_progress, parse_map_resources, check_player_collision, ParseProgress};
    use factorio_client::noise::terrain::TerrainGenerator;
    use factorio_client::protocol::{Connection, PlayerState};

    #[cfg(feature = "gpu")]
    use image::ImageEncoder;
    #[cfg(feature = "gpu")]
    use factorio_client::renderer::{
        atlas::TextureAtlas,
        camera::Camera2D,
        gpu::GpuState,
        sprites::{SpriteInstance, SpriteRenderer},
        tilemap::{TileInstance, TilemapRenderer, tile_color as gpu_tile_color},
    };

    const ZOOM_LEVELS: [f64; 11] = [0.03125, 0.0625, 0.125, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0];
    const DEFAULT_ZOOM_IDX: usize = 5; // 1.0x
    const Y_STRETCH: f64 = 1.1; // compensate for terminal cells being taller than wide
    const FACTORIO_DATA_PATH: &str = "/Applications/factorio.app/Contents/data";

    struct EntityIcons {
        picker: Picker,
        icon_paths: HashMap<String, PathBuf>,
        images: HashMap<String, image::DynamicImage>,
        protocols: HashMap<String, StatefulProtocol>,
        last_zoom: f64,
        no_icon: HashMap<String, ()>,
    }

    impl EntityIcons {
        fn new(picker: Picker) -> Self {
            Self {
                picker,
                icon_paths: HashMap::new(),
                images: HashMap::new(),
                protocols: HashMap::new(),
                last_zoom: 0.0,
                no_icon: HashMap::new(),
            }
        }

        /// Scan entity Lua files to build name -> icon path mapping
        fn load_icon_paths(&mut self, factorio_path: &Path) {
            let proto_dir = factorio_path.join("base/prototypes");
            let lua_files = [
                "entity/entities.lua", "entity/transport-belts.lua", "entity/enemies.lua",
                "entity/trees.lua", "entity/turrets.lua", "entity/trains.lua",
                "entity/resources.lua", "entity/mining-drill.lua", "entity/flying-robots.lua",
                "decorative/decoratives.lua",
            ];

            for filename in &lua_files {
                let path = proto_dir.join(filename);
                if let Ok(content) = std::fs::read_to_string(&path) {
                    self.parse_lua_icons(&content, factorio_path);
                }
            }
        }

        fn parse_lua_icons(&mut self, content: &str, factorio_path: &Path) {
            let mut current_name: Option<String> = None;

            for line in content.lines() {
                let trimmed = line.trim();

                if let Some(name) = extract_lua_string(trimmed, "name") {
                    current_name = Some(name);
                }

                if let Some(icon_path) = extract_lua_string(trimmed, "icon") {
                    if let Some(ref name) = current_name {
                        if let Some(resolved) = resolve_factorio_path(&icon_path, factorio_path) {
                            if resolved.exists() {
                                self.icon_paths.insert(name.clone(), resolved);
                            }
                        }
                    }
                }

                if trimmed == "}," || trimmed == "}" {
                    current_name = None;
                }
            }
        }

        fn get_image(&mut self, entity_name: &str) -> Option<&image::DynamicImage> {
            if self.no_icon.contains_key(entity_name) {
                return None;
            }
            if !self.images.contains_key(entity_name) {
                let path = self.icon_paths.get(entity_name).cloned().or_else(|| {
                    let fallback = Path::new(FACTORIO_DATA_PATH)
                        .join("base/graphics/icons")
                        .join(format!("{}.png", entity_name));
                    if fallback.exists() { Some(fallback) } else { None }
                });
                let path = match path {
                    Some(p) => p,
                    None => {
                        self.no_icon.insert(entity_name.to_string(), ());
                        return None;
                    }
                };
                match image::open(&path) {
                    Ok(mut img) => {
                        // Icons are mipmap strips (64+32+16+8 = 120 wide, 64 tall)
                        // Crop to just the first 64x64 icon
                        let icon = if img.width() > 64 && img.height() >= 64 {
                            img.crop(0, 0, 64, 64)
                        } else {
                            img
                        };
                        self.images.insert(entity_name.to_string(), icon);
                    }
                    Err(_) => {
                        self.no_icon.insert(entity_name.to_string(), ());
                        return None;
                    }
                };
            }
            self.images.get(entity_name)
        }
    }

    fn extract_lua_string(line: &str, field: &str) -> Option<String> {
        let pattern = format!("{} = \"", field);
        let pos = line.find(&pattern)?;
        if pos > 0 && (line.as_bytes()[pos - 1].is_ascii_alphanumeric() || line.as_bytes()[pos - 1] == b'_') {
            return None;
        }
        let start = pos + pattern.len();
        let rest = &line[start..];
        let end = rest.find('"')?;
        Some(rest[..end].to_string())
    }

    fn resolve_factorio_path(lua_path: &str, factorio_path: &Path) -> Option<PathBuf> {
        if let Some(rest) = lua_path.strip_prefix("__base__/") {
            Some(factorio_path.join("base").join(rest))
        } else if let Some(rest) = lua_path.strip_prefix("__core__/") {
            Some(factorio_path.join("core").join(rest))
        } else {
            None
        }
    }

    #[cfg(feature = "gpu")]
    struct GpuMapRenderer {
        gpu: GpuState,
        camera: Camera2D,
        atlas: TextureAtlas,
        tilemap: TilemapRenderer,
        sprites: SpriteRenderer,
        pixels: Vec<u8>,
        png_buf: Vec<u8>,
        pending_area: Option<Rect>,
        last_cam_x: f64,
        last_cam_y: f64,
        last_cam_zoom: f64,
        last_parsed: bool,
        last_w: u32,
        last_h: u32,
    }

    #[cfg(feature = "gpu")]
    impl GpuMapRenderer {
        fn new() -> Self {
            let gpu = GpuState::new(800, 600);
            let factorio_path = Path::new(FACTORIO_DATA_PATH);
            let atlas = TextureAtlas::new(&gpu.device, &gpu.queue, factorio_path);
            let tilemap = TilemapRenderer::new(&gpu.device, gpu.format, &gpu.camera_bind_group_layout);
            let sprites = SpriteRenderer::new(&gpu.device, gpu.format, &gpu.camera_bind_group_layout, &atlas.bind_group_layout);
            Self {
                gpu, camera: Camera2D::new(), atlas, tilemap, sprites,
                pixels: Vec::new(),
                png_buf: Vec::new(),
                pending_area: None,
                last_cam_x: f64::NAN,
                last_cam_y: f64::NAN,
                last_cam_zoom: f64::NAN,
                last_parsed: false,
                last_w: 800,
                last_h: 600,
            }
        }

        fn render_frame(&mut self, app: &App, render_w: u32, render_h: u32, tiles_visible: f64) {
            if render_w != self.last_w || render_h != self.last_h {
                self.gpu.resize(render_w, render_h);
            }
            self.camera.target_x = app.player_x;
            self.camera.target_y = app.player_y;
            self.camera.target_zoom = tiles_visible;
            self.camera.aspect = render_w as f32 / render_h as f32;
            self.camera.update(0.15);
            self.gpu.upload_camera(&self.camera.view_proj());

            let (min_x, min_y, max_x, max_y) = self.camera.visible_bounds();
            let tx0 = min_x.floor() as i32 - 1;
            let ty0 = min_y.floor() as i32 - 1;
            let tx1 = max_x.ceil() as i32 + 1;
            let ty1 = max_y.ceil() as i32 + 1;

            let mut tile_instances = Vec::with_capacity(((tx1 - tx0) * (ty1 - ty0)) as usize);
            for ty in ty0..ty1 {
                for tx in tx0..tx1 {
                    let name: &str = if app.show_parsed_map {
                        app.tile_index.get(&(tx, ty))
                            .and_then(|&idx| app.tiles.get(idx))
                            .filter(|t| !t.procedural)
                            .map(|t| t.name.as_str())
                            .unwrap_or_else(|| app.procedural_tile_at(tx, ty))
                    } else {
                        app.procedural_tile_at(tx, ty)
                    };
                    tile_instances.push(TileInstance {
                        pos: [tx as f32, ty as f32],
                        color: gpu_tile_color(name),
                    });
                }
            }
            self.tilemap.upload(&self.gpu.device, &self.gpu.queue, &tile_instances);

            let mut sprite_instances: Vec<SpriteInstance> = Vec::new();
            // Player marker
            if let Some(uv) = self.atlas.get_uv("character") {
                sprite_instances.push(SpriteInstance {
                    pos: [app.player_x as f32, app.player_y as f32],
                    size: [1.0, 1.0],
                    uv_min: [uv[0], uv[1]],
                    uv_max: [uv[2], uv[3]],
                    rotation: 0.0,
                    _pad: 0.0,
                });
            }
            if app.show_parsed_map {
                for ent in &app.entities {
                    if ent.x < min_x - 2.0 || ent.x > max_x + 2.0
                        || ent.y < min_y - 2.0 || ent.y > max_y + 2.0 { continue; }
                    let uv = match self.atlas.get_uv_or_fallback(&ent.name) {
                        Some(uv) => uv,
                        None => continue,
                    };
                    sprite_instances.push(SpriteInstance {
                        pos: [ent.x as f32, ent.y as f32],
                        size: [ent.tile_width().max(1.0) as f32, ent.tile_height().max(1.0) as f32],
                        uv_min: [uv[0], uv[1]],
                        uv_max: [uv[2], uv[3]],
                        rotation: ent.direction as f32 * std::f32::consts::FRAC_PI_4,
                        _pad: 0.0,
                    });
                }
            }
            self.sprites.upload(&self.gpu.device, &self.gpu.queue, &sprite_instances);

            let mut encoder = self.gpu.device.create_command_encoder(
                &wgpu::CommandEncoderDescriptor { label: None });
            {
                let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                    label: None,
                    color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                        view: self.gpu.render_view(),
                        resolve_target: None,
                        ops: wgpu::Operations {
                            load: wgpu::LoadOp::Clear(wgpu::Color {
                                r: 0.02, g: 0.02, b: 0.04, a: 1.0,
                            }),
                            store: wgpu::StoreOp::Store,
                        },
                    })],
                    depth_stencil_attachment: None,
                    ..Default::default()
                });
                self.tilemap.draw(&mut pass, &self.gpu.camera_bind_group);
                self.sprites.draw(&mut pass, &self.gpu.camera_bind_group, &self.atlas.bind_group);
            }
            self.gpu.queue.submit(std::iter::once(encoder.finish()));
            self.gpu.readback(&mut self.pixels);

            self.png_buf.clear();
            image::codecs::png::PngEncoder::new_with_quality(
                &mut self.png_buf,
                image::codecs::png::CompressionType::Fast,
                image::codecs::png::FilterType::Sub,
            ).write_image(&self.pixels, render_w, render_h, image::ExtendedColorType::Rgba8)
                .ok();
            self.last_cam_x = app.player_x;
            self.last_cam_y = app.player_y;
            self.last_cam_zoom = self.camera.target_zoom;
            self.last_parsed = app.show_parsed_map;
            self.last_w = render_w;
            self.last_h = render_h;
        }
    }

    struct App {
        server_addr: SocketAddr,
        username: String,
        connection: Option<Connection>,

        state: AppState,

        // Player position
        player_x: f64,
        player_y: f64,

        // Cursor offset from player (for targeting)
        cursor_dx: i32,
        cursor_dy: i32,
        cursor_mode: bool,

        // Zoom
        zoom_idx: usize,

        // Server state
        server_tick: u32,
        player_index: Option<u16>,
        server_name: Option<String>,
        packets_received: u64,

        // Actions
        mining: bool,
        walking: bool,
        walk_direction: u8,

        // Map data
        entities: Vec<MapEntity>,
        tiles: Vec<MapTile>,
        tile_index: HashMap<(i32, i32), usize>,
        entity_chunk_index: HashMap<(i32, i32), Vec<usize>>,
        map_parse_handle: Option<std::thread::JoinHandle<()>>,
        map_parse_rx: Option<std::sync::mpsc::Receiver<Result<ParsedMapBundle, String>>>,
        resource_parse_rx: Option<std::sync::mpsc::Receiver<Vec<MapEntity>>>,
        map_parse_progress: Option<std::sync::Arc<ParseProgress>>,
        map_parse_started_at: Option<Instant>,
        resource_parse_started_at: Option<Instant>,
        map_size: usize,
        map_seed: u32,
        map_bounds: Option<f64>, // Half-size: map extends from -bounds to +bounds
        map_controls: HashMap<String, f32>,
        terrain_generator: RefCell<Option<TerrainGenerator>>,
        terrain_cache: RefCell<HashMap<(i32, i32), [u8; 1024]>>,

        // Other players
        other_players: Vec<PlayerState>,

        // UI
        log: Vec<(String, Color)>,
        log_file: Option<std::fs::File>,
        status: String,
        chat_input: String,
        chat_mode: bool,
        show_help: bool,
        show_parsed_map: bool,

        #[cfg(feature = "gpu")]
        gpu_renderer: RefCell<Option<GpuMapRenderer>>,
    }

    #[derive(Debug, Clone, Copy, PartialEq)]
    enum AppState {
        Disconnected,
        Connecting,
        DownloadingMap,
        Connected,
        OfflineMap, // Viewing a saved map file
        Error,
    }

    impl App {
        fn new(server_addr: SocketAddr, username: String) -> Self {
            Self {
                server_addr,
                username: username.clone(),
                connection: None,
                state: AppState::Disconnected,
                player_x: 0.0,
                player_y: 0.0,
                cursor_dx: 0,
                cursor_dy: 0,
                cursor_mode: false,
                zoom_idx: DEFAULT_ZOOM_IDX,
                server_tick: 0,
                player_index: None,
                server_name: None,
                packets_received: 0,
                mining: false,
                walking: false,
                walk_direction: 0,
                entities: Vec::new(),
                tiles: Vec::new(),
                tile_index: HashMap::new(),
                entity_chunk_index: HashMap::new(),
                map_parse_handle: None,
                map_parse_rx: None,
                resource_parse_rx: None,
                map_parse_progress: None,
                map_parse_started_at: None,
                resource_parse_started_at: None,
                map_size: 0,
                map_seed: 0,
                map_bounds: None,
                map_controls: HashMap::new(),
                terrain_generator: RefCell::new(None),
                terrain_cache: RefCell::new(HashMap::new()),
                other_players: Vec::new(),
                log: vec![
                    (format!("Factorio TUI Client"), Color::Cyan),
                    (format!("Player: {}", username), Color::White),
                    ("Press Enter to connect".into(), Color::Yellow),
                ],
                log_file: None,
                status: "Ready".into(),
                chat_input: String::new(),
                chat_mode: false,
                show_help: false,
                show_parsed_map: false,
                #[cfg(feature = "gpu")]
                gpu_renderer: RefCell::new(None),
            }
        }

        fn zoom(&self) -> f64 {
            ZOOM_LEVELS[self.zoom_idx]
        }

        fn log(&mut self, msg: impl Into<String>, color: Color) {
            let msg = msg.into();
            if let Some(file) = self.log_file.as_mut() {
                let _ = writeln!(file, "{}", msg);
                let _ = file.flush();
            }
            self.log.push((msg, color));
            if self.log.len() > 100 {
                self.log.remove(0);
            }
        }

        fn clear_parse_progress_if_done(&mut self) {
            if self.map_parse_rx.is_none() && self.resource_parse_rx.is_none() {
                self.map_parse_progress = None;
                self.map_parse_started_at = None;
                self.resource_parse_started_at = None;
            }
        }

        fn map_parse_status_line(&self) -> Option<String> {
            let parsing_map = self.map_parse_rx.is_some();
            let parsing_resources = self.resource_parse_rx.is_some();
            if !parsing_map && !parsing_resources {
                return None;
            }

            let spinner = ['|', '/', '-', '\\'];
            let start = if parsing_map {
                self.map_parse_started_at
            } else {
                self.resource_parse_started_at
            };
            let spin_idx = start
                .map(|t| ((t.elapsed().as_millis() / 200) as usize) % spinner.len())
                .unwrap_or(0);

            let mut line = if parsing_map {
                if let Some(progress) = self.map_parse_progress.as_ref() {
                    let entities_total = progress.entities_total();
                    let resources_total = progress.resources_total();
                    let tiles_total = progress.tiles_total();
                    let total_total = entities_total + resources_total + tiles_total;
                    let entities_done = progress.entities_done();
                    let resources_done = progress.resources_done();
                    let tiles_done = progress.tiles_done();
                    let total_done = entities_done + resources_done + tiles_done;
                    let pct = if total_total > 0 {
                        (total_done as f64 / total_total as f64) * 100.0
                    } else {
                        0.0
                    };
                    let bar_width = 20usize;
                    let filled = if total_total > 0 {
                        ((total_done as f64 / total_total as f64) * bar_width as f64).round() as usize
                    } else {
                        0
                    };
                    let mut bar = String::with_capacity(bar_width);
                    for i in 0..bar_width {
                        bar.push(if i < filled { '#' } else { '-' });
                    }
                    format!(
                        "Parsing map {:.1}% [{}] stage={} ({}/{})",
                        pct,
                        bar,
                        progress.stage().as_str(),
                        total_done,
                        total_total
                    )
                } else {
                    "Parsing map...".to_string()
                }
            } else {
                "Parsing resources...".to_string()
            };

            if let Some(start) = start {
                let elapsed = start.elapsed().as_secs();
                line.push_str(&format!(" elapsed={}s", elapsed));
            }
            line.push(' ');
            line.push(spinner[spin_idx]);
            Some(line)
        }

        fn cursor_world_pos(&self) -> (f64, f64) {
            let z = self.zoom();
            (self.player_x + self.cursor_dx as f64 / z,
             self.player_y + self.cursor_dy as f64 * Y_STRETCH / z)
        }

        fn entity_at_cursor(&self) -> Option<&MapEntity> {
            let (cx, cy) = self.cursor_world_pos();
            let tile_x = cx.floor() as i32;
            let tile_y = cy.floor() as i32;
            let chunk_x = tile_x.div_euclid(32);
            let chunk_y = tile_y.div_euclid(32);
            self.entity_chunk_index
                .get(&(chunk_x, chunk_y))
                .and_then(|indices| {
                    indices.iter().find_map(|idx| {
                        self.entities.get(*idx).and_then(|e| {
                            let half_w = e.tile_width() / 2.0;
                            let half_h = e.tile_height() / 2.0;
                            if cx >= e.x - half_w && cx < e.x + half_w
                                && cy >= e.y - half_h && cy < e.y + half_h
                            {
                                Some(e)
                            } else {
                                None
                            }
                        })
                    })
                })
        }

        fn nearby_entity_count(&self) -> usize {
            let radius = 50.0;
            self.entities.iter().filter(|e| {
                (e.x - self.player_x).abs() < radius && (e.y - self.player_y).abs() < radius
            }).count()
        }

        /// Get procedural terrain tile name at world position
        fn procedural_tile_at(&self, tile_x: i32, tile_y: i32) -> &'static str {
            let chunk_x = tile_x.div_euclid(32);
            let chunk_y = tile_y.div_euclid(32);
            let local_x = tile_x.rem_euclid(32) as usize;
            let local_y = tile_y.rem_euclid(32) as usize;

            // Ensure terrain generator is initialized
            {
                let mut gen = self.terrain_generator.borrow_mut();
                if gen.is_none() {
                    *gen = TerrainGenerator::new_with_controls(self.map_seed, &self.map_controls).ok();
                }
            }

            let mut cache = self.terrain_cache.borrow_mut();
            let chunk = cache.entry((chunk_x, chunk_y)).or_insert_with(|| {
                let gen = self.terrain_generator.borrow();
                if let Some(ref generator) = *gen {
                    generator.compute_chunk(chunk_x, chunk_y)
                } else {
                    [0u8; 1024] // Fallback to water
                }
            });
            let tile_idx = chunk[local_y * 32 + local_x];

            // Get tile name from generator
            let gen = self.terrain_generator.borrow();
            if let Some(ref generator) = *gen {
                // We need to return a static str, so use match for common tiles
                let name = generator.tile_name(tile_idx);
                match name {
                    "water" => "water",
                    "deepwater" => "deepwater",
                    n if n.starts_with("grass") => {
                        if n.contains("-1") { "grass-1" }
                        else if n.contains("-2") { "grass-2" }
                        else if n.contains("-3") { "grass-3" }
                        else if n.contains("-4") { "grass-4" }
                        else { "grass" }
                    }
                    n if n.starts_with("dirt") => {
                        if n.contains("-1") { "dirt-1" }
                        else if n.contains("-2") { "dirt-2" }
                        else if n.contains("-3") { "dirt-3" }
                        else if n.contains("-4") { "dirt-4" }
                        else if n.contains("-5") { "dirt-5" }
                        else if n.contains("-6") { "dirt-6" }
                        else if n.contains("-7") { "dirt-7" }
                        else { "dirt" }
                    }
                    n if n.starts_with("dry-dirt") => "dry-dirt",
                    n if n.starts_with("sand") => {
                        if n.contains("-1") { "sand-1" }
                        else if n.contains("-2") { "sand-2" }
                        else if n.contains("-3") { "sand-3" }
                        else { "sand" }
                    }
                    n if n.starts_with("red-desert") => "red-desert",
                    n if n.contains("landfill") => "landfill",
                    n if n.contains("concrete") => {
                        if n.contains("refined") { "refined-concrete" }
                        else { "concrete" }
                    }
                    n if n.contains("stone-path") => "stone-path",
                    "out-of-map" => "out-of-map",
                    _ => "unknown",
                }
            } else {
                "water" // Fallback
            }
        }
    }

    pub fn run() -> Result<(), Box<dyn std::error::Error>> {
        let args: Vec<String> = std::env::args().collect();

        // Check for --map flag for offline mode
        let map_file = args.iter().position(|a| a == "--map")
            .and_then(|i| args.get(i + 1).map(PathBuf::from));

        // Check for --seed flag to override or specify seed
        let seed_override = args.iter().position(|a| a == "--seed")
            .and_then(|i| args.get(i + 1))
            .and_then(|s| s.parse::<u32>().ok());

        // Check for --size flag to set map bounds
        let size_arg = args.iter().position(|a| a == "--size")
            .and_then(|i| args.get(i + 1))
            .and_then(|s| s.parse::<f64>().ok());

        let (server_addr, username, offline_map) = if map_file.is_some() || seed_override.is_some() {
            // Offline mode - load map from file or just use seed
            let default_addr: SocketAddr = "127.0.0.1:34197".parse().unwrap();
            (default_addr, "Offline".to_string(), map_file)
        } else {
            // Online mode - connect to server
            let server_addr: SocketAddr = args.get(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(|| "127.0.0.1:34197".parse().unwrap());

            let username = args.get(2)
                .cloned()
                .unwrap_or_else(|| format!("Bot{}", std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() % 1000));

            (server_addr, username, None)
        };

        let rt = tokio::runtime::Runtime::new()?;

        // Query terminal for graphics protocol support + font size (before raw mode)
        let disable_images = std::env::var("TUI_NO_IMAGES").is_ok()
            || std::env::var("TMUX").is_ok()
            || std::env::var("TERM").ok().map(|t| t.starts_with("tmux")).unwrap_or(false);
        let picker = if disable_images {
            None
        } else {
            Picker::from_query_stdio().ok()
        };
        let entity_icons: RefCell<Option<EntityIcons>> = RefCell::new(picker.map(|p| {
            let mut icons = EntityIcons::new(p);
            let factorio_path = Path::new(FACTORIO_DATA_PATH);
            if factorio_path.exists() {
                icons.load_icon_paths(factorio_path);
            }
            icons
        }));

        #[cfg(feature = "gpu")]
        let gpu_init = Some(GpuMapRenderer::new());

        enable_raw_mode()?;
        stdout().execute(EnterAlternateScreen)?;
        let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;

        let mut app = App::new(server_addr, username);
        if let Ok(file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open("tui.log")
        {
            app.log_file = Some(file);
            app.log("Log file: tui.log", Color::Gray);
        }
        if disable_images {
            app.log("Image rendering disabled (tmux/TUI_NO_IMAGES)", Color::Gray);
        }
        if std::env::var("FACTORIO_AUTO_CONNECT").is_ok() {
            app.log("Auto-connect enabled", Color::Yellow);
            app.state = AppState::Connecting;
        }
        #[cfg(feature = "gpu")]
        { *app.gpu_renderer.borrow_mut() = gpu_init; }

        // Load offline map if specified
        if let Some(ref path) = offline_map {
            match std::fs::read(path) {
                Ok(data) => {
                    app.map_size = data.len();
                    app.state = AppState::OfflineMap;
                    app.show_parsed_map = false;
                    app.log.clear();
                    app.log("Parsing offline map...", Color::Yellow);
                    app.status = "Offline Map Viewer".into();

                    let (tx, rx) = std::sync::mpsc::channel();
                    let (rtx, rrx) = std::sync::mpsc::channel();
                    app.map_parse_rx = Some(rx);
                    app.resource_parse_rx = Some(rrx);
                    let progress = std::sync::Arc::new(ParseProgress::new());
                    app.map_parse_progress = Some(progress.clone());
                    app.map_parse_started_at = Some(Instant::now());
                    app.resource_parse_started_at = Some(Instant::now());

                    let map_bytes = std::sync::Arc::new(data);
                    let map_bytes_for_parse = std::sync::Arc::clone(&map_bytes);
                    let map_bytes_for_resources = std::sync::Arc::clone(&map_bytes);
                    app.map_parse_handle = Some(std::thread::spawn(move || {
                        let prev_skip_resources = std::env::var("FACTORIO_SKIP_RESOURCE_PARSE").ok();
                        std::env::set_var("FACTORIO_SKIP_RESOURCE_PARSE", "1");
                        std::env::set_var("FACTORIO_SKIP_PROCEDURAL_TILES", "1");
                        let result = parse_map_data_with_progress(&map_bytes_for_parse, Some(progress))
                            .map(|map| {
                                let tile_index = build_tile_index(&map.tiles);
                                let entity_chunk_index = build_entity_chunk_index(&map.entities);
                                ParsedMapBundle {
                                    entities: map.entities,
                                    tiles: map.tiles,
                                    tile_index,
                                    entity_chunk_index,
                                    seed: map.seed,
                                    spawn: map.player_spawn,
                                    map_width: map.map_width,
                                    map_height: map.map_height,
                                    map_controls: map.map_controls,
                                }
                            })
                            .map_err(|e| e.to_string());
                        std::env::remove_var("FACTORIO_SKIP_PROCEDURAL_TILES");
                        if let Some(prev) = prev_skip_resources {
                            std::env::set_var("FACTORIO_SKIP_RESOURCE_PARSE", prev);
                        } else {
                            std::env::remove_var("FACTORIO_SKIP_RESOURCE_PARSE");
                        }
                        let _ = tx.send(result);

                        std::thread::spawn(move || {
                            match parse_map_resources(&map_bytes_for_resources) {
                                Ok(resources) => {
                                    let _ = rtx.send(resources);
                                }
                                Err(e) => {
                                    eprintln!("[ERROR] Resource parse failed: {}", e);
                                }
                            }
                        });
                    }));
                }
                Err(e) => {
                    app.log(format!("Failed to load map: {}", e), Color::Red);
                    app.state = AppState::Error;
                }
            }
        } else if let Some(seed) = seed_override {
            // Seed-only mode - just procedural terrain
            app.map_seed = seed;
            app.map_bounds = size_arg.map(|s| s / 2.0);
            app.state = AppState::OfflineMap;
            app.log.clear();
            app.log(format!("Procedural terrain with seed: {}", seed), Color::Green);
            if let Some(size) = size_arg {
                app.log(format!("Map size: {}x{}", size, size), Color::Gray);
            }
            app.log("Use WASD to pan, +/- to zoom", Color::Yellow);
            app.status = "Procedural Terrain".into();
        }

        // Apply size override if specified
        if size_arg.is_some() {
            app.map_bounds = size_arg.map(|s| s / 2.0);
        }

        let tick_rate = Duration::from_millis(16); // ~60 FPS, needed for connection
        let mut last_tick = Instant::now();

        loop {
            terminal.draw(|frame| render(frame, &app, &entity_icons))?;

            #[cfg(feature = "gpu")]
            {
                let gpu_ref = app.gpu_renderer.borrow();
                if let Some(ref renderer) = *gpu_ref {
                    if let Some(area) = renderer.pending_area {
                        if !renderer.png_buf.is_empty() {
                            let mut out = stdout();
                            write!(out, "\x1b[{};{}H", area.y + 1, area.x + 1)?;
                            let encoded = gpu_base64(&renderer.png_buf);
                            let chunks: Vec<&[u8]> = encoded.as_bytes().chunks(4096).collect();
                            for (i, chunk) in chunks.iter().enumerate() {
                                let more = if i < chunks.len() - 1 { 1 } else { 0 };
                                if i == 0 {
                                    write!(out, "\x1b_Ga=T,f=100,s={},v={},c={},r={},m={};",
                                        renderer.last_w, renderer.last_h,
                                        area.width, area.height, more)?;
                                } else {
                                    write!(out, "\x1b_Gm={};", more)?;
                                }
                                out.write_all(chunk)?;
                                write!(out, "\x1b\\")?;
                            }
                            out.flush()?;
                        }
                    }
                }
            }

            let timeout = tick_rate.saturating_sub(last_tick.elapsed());
            if event::poll(timeout)? {
                if let Event::Key(key) = event::read()? {
                    if key.kind == KeyEventKind::Press {
                        if key.code == KeyCode::Char('q') && !app.chat_mode {
                            break;
                        }
                        handle_input(&mut app, key.code, &rt);
                    }
                }
            }

            if last_tick.elapsed() >= tick_rate {
                update(&mut app, &rt);
                last_tick = Instant::now();
            }
        }

        disable_raw_mode()?;
        stdout().execute(LeaveAlternateScreen)?;
        Ok(())
    }

    fn load_offline_map(path: &PathBuf) -> Result<(Vec<MapEntity>, Vec<MapTile>, usize, u32), Box<dyn std::error::Error>> {
        let data = std::fs::read(path)?;
        let map_size = data.len();
        let map_data = parse_map_data_with_progress(&data, None)?;
        Ok((map_data.entities, map_data.tiles, map_size, map_data.seed))
    }

    fn build_entity_chunk_index(entities: &[MapEntity]) -> HashMap<(i32, i32), Vec<usize>> {
        let mut index: HashMap<(i32, i32), Vec<usize>> = HashMap::new();
        for (idx, entity) in entities.iter().enumerate() {
            let half_w = entity.tile_width() / 2.0;
            let half_h = entity.tile_height() / 2.0;
            let min_x = (entity.x - half_w).floor() as i32;
            let max_x = (entity.x + half_w).ceil() as i32;
            let min_y = (entity.y - half_h).floor() as i32;
            let max_y = (entity.y + half_h).ceil() as i32;

            let min_chunk_x = min_x.div_euclid(32);
            let max_chunk_x = max_x.div_euclid(32);
            let min_chunk_y = min_y.div_euclid(32);
            let max_chunk_y = max_y.div_euclid(32);

            for cy in min_chunk_y..=max_chunk_y {
                for cx in min_chunk_x..=max_chunk_x {
                    index.entry((cx, cy)).or_default().push(idx);
                }
            }
        }
        index
    }

    fn build_tile_index(tiles: &[MapTile]) -> HashMap<(i32, i32), usize> {
        let mut index = HashMap::with_capacity(tiles.len());
        for (idx, tile) in tiles.iter().enumerate() {
            index.insert((tile.x, tile.y), idx);
        }
        index
    }

    struct ParsedMapBundle {
        entities: Vec<MapEntity>,
        tiles: Vec<MapTile>,
        tile_index: HashMap<(i32, i32), usize>,
        entity_chunk_index: HashMap<(i32, i32), Vec<usize>>,
        seed: u32,
        spawn: (f64, f64),
        map_width: u32,
        map_height: u32,
        map_controls: HashMap<String, f32>,
    }

    fn update(app: &mut App, rt: &tokio::runtime::Runtime) {
        match app.state {
            AppState::Connecting => {
                match rt.block_on(Connection::new(app.server_addr, app.username.clone())) {
                    Ok(mut conn) => {
                        app.log("Handshaking...", Color::Gray);
                        match rt.block_on(conn.connect()) {
                            Ok(()) => {
                                app.player_index = conn.player_index();
                                app.server_name = conn.server_name().map(|s| s.to_string());
                                app.entities.clear();
                                app.tiles.clear();
                                app.tile_index.clear();
                                app.entity_chunk_index.clear();
                                app.map_seed = 0;
                                app.map_bounds = None;
                                app.map_controls.clear();
                                app.show_parsed_map = false;
                                app.resource_parse_rx = None;
                                app.map_parse_progress = None;
                                app.map_parse_started_at = None;
                                app.resource_parse_started_at = None;
                                *app.terrain_generator.borrow_mut() = None;
                                app.terrain_cache.borrow_mut().clear();
                                app.log(format!("Connected as player #{}", app.player_index.unwrap_or(0)), Color::Green);
                                if let Some(ref name) = app.server_name {
                                    app.log(format!("Server: {}", name), Color::Cyan);
                                }
                                app.connection = Some(conn);
                                app.state = AppState::DownloadingMap;
                            }
                            Err(e) => {
                                app.log(format!("Failed: {}", e), Color::Red);
                                app.state = AppState::Error;
                            }
                        }
                    }
                    Err(e) => {
                        app.log(format!("Connection error: {}", e), Color::Red);
                        app.state = AppState::Error;
                    }
                }
            }

            AppState::DownloadingMap => {
                app.log("Downloading map...", Color::Yellow);
                if let Some(mut conn) = app.connection.take() {
                    std::env::set_var("FACTORIO_SKIP_MAP_PARSE", "1");
                    let download_result = rt.block_on(conn.download_map());
                    std::env::remove_var("FACTORIO_SKIP_MAP_PARSE");
                    match download_result {
                        Ok(size) => {
                            let map_bytes = conn.map_data().to_vec();
                            let (x, y) = conn.player_position();
                            app.map_size = size;
                            app.player_x = x;
                            app.player_y = y;
                            app.log("Parsing map in background...", Color::Yellow);
                            let (tx, rx) = std::sync::mpsc::channel();
                            let (rtx, rrx) = std::sync::mpsc::channel();
                            app.map_parse_rx = Some(rx);
                            app.resource_parse_rx = Some(rrx);
                            let progress = std::sync::Arc::new(ParseProgress::new());
                            app.map_parse_progress = Some(progress.clone());
                            app.map_parse_started_at = Some(Instant::now());
                            app.resource_parse_started_at = Some(Instant::now());
                            let map_bytes = std::sync::Arc::new(map_bytes);
                            let map_bytes_for_parse = std::sync::Arc::clone(&map_bytes);
                            let map_bytes_for_resources = std::sync::Arc::clone(&map_bytes);
                            app.map_parse_handle = Some(std::thread::spawn(move || {
                                let prev_skip_resources = std::env::var("FACTORIO_SKIP_RESOURCE_PARSE").ok();
                                std::env::set_var("FACTORIO_SKIP_RESOURCE_PARSE", "1");
                                std::env::set_var("FACTORIO_SKIP_PROCEDURAL_TILES", "1");
                                let result = parse_map_data_with_progress(&map_bytes_for_parse, Some(progress))
                                    .map(|map| {
                                        eprintln!(
                                            "[DEBUG] TUI map parse: {} entities, {} tiles (building indexes)",
                                            map.entities.len(),
                                            map.tiles.len()
                                        );
                                        let tile_index = build_tile_index(&map.tiles);
                                        let entity_chunk_index = build_entity_chunk_index(&map.entities);
                                        eprintln!("[DEBUG] TUI map parse: index build complete");
                                        ParsedMapBundle {
                                            entities: map.entities,
                                            tiles: map.tiles,
                                            tile_index,
                                            entity_chunk_index,
                                            seed: map.seed,
                                            spawn: map.player_spawn,
                                            map_width: map.map_width,
                                            map_height: map.map_height,
                                            map_controls: map.map_controls,
                                        }
                                    })
                                    .map_err(|e| e.to_string());
                                std::env::remove_var("FACTORIO_SKIP_PROCEDURAL_TILES");
                                if let Some(prev) = prev_skip_resources {
                                    std::env::set_var("FACTORIO_SKIP_RESOURCE_PARSE", prev);
                                } else {
                                    std::env::remove_var("FACTORIO_SKIP_RESOURCE_PARSE");
                                }
                                let _ = tx.send(result);

                                std::thread::spawn(move || {
                                    match parse_map_resources(&map_bytes_for_resources) {
                                        Ok(resources) => {
                                            let _ = rtx.send(resources);
                                        }
                                        Err(e) => {
                                            eprintln!("[ERROR] Resource parse failed: {}", e);
                                        }
                                    }
                                });
                            }));
                            app.log(
                                format!("Map: {} KB, {} entities, {} tiles",
                                    size / 1024, app.entities.len(), app.tiles.len()),
                                Color::Green,
                            );
                            app.state = AppState::Connected;
                            app.status = "Connected".into();
                        }
                        Err(e) => {
                            app.log(format!("Map download failed: {}", e), Color::Red);
                            app.state = AppState::Error;
                        }
                    }
                    app.connection = Some(conn);
                }
            }

            AppState::Connected => {
                if let Some(rx) = app.map_parse_rx.as_ref() {
                    match rx.try_recv() {
                        Ok(Ok(parsed)) => {
                            app.entities = parsed.entities;
                            app.tiles = parsed.tiles;
                            app.tile_index = parsed.tile_index;
                            app.entity_chunk_index = parsed.entity_chunk_index;
                            app.map_seed = parsed.seed;
                            app.map_controls = parsed.map_controls;
                            *app.terrain_generator.borrow_mut() = None;
                            app.terrain_cache.borrow_mut().clear();
                            if parsed.map_width > 0 && parsed.map_height > 0 {
                                let half_w = (parsed.map_width / 2) as f64;
                                let half_h = (parsed.map_height / 2) as f64;
                                app.map_bounds = Some(half_w.max(half_h));
                            }
                            if app.player_x == 0.0 && app.player_y == 0.0 {
                                app.player_x = parsed.spawn.0;
                                app.player_y = parsed.spawn.1;
                            }
                            app.log(
                                format!(
                                    "Parsed map: {} entities, {} tiles (seed={}, w={}, h={})",
                                    app.entities.len(),
                                    app.tiles.len(),
                                    app.map_seed,
                                    parsed.map_width,
                                    parsed.map_height
                                ),
                                Color::Green,
                            );
                            app.show_parsed_map = true;
                            app.status = "Map parsed".into();
                            app.map_parse_rx = None;
                            app.map_parse_handle = None;
                        }
                        Ok(Err(err)) => {
                            app.log(format!("Map parse failed: {}", err), Color::Red);
                            app.map_parse_rx = None;
                            app.map_parse_handle = None;
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty) => {}
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            app.log("Map parse thread disconnected".to_string(), Color::Red);
                            app.map_parse_rx = None;
                            app.map_parse_handle = None;
                        }
                    }
                }
                if let Some(rx) = app.resource_parse_rx.as_ref() {
                    match rx.try_recv() {
                        Ok(resources) => {
                            let before = app.entities.len();
                            app.entities.extend(resources);
                            app.entity_chunk_index = build_entity_chunk_index(&app.entities);
                            let added = app.entities.len().saturating_sub(before);
                            app.log(format!("Resources parsed: {} new entities", added), Color::Green);
                            app.resource_parse_rx = None;
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty) => {}
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            app.resource_parse_rx = None;
                        }
                    }
                }
                app.clear_parse_progress_if_done();
                let mut poll_error: Option<String> = None;
                if let Some(ref mut conn) = app.connection {
                    for _ in 0..5 {
                        match rt.block_on(conn.poll()) {
                            Ok(Some(_)) => app.packets_received += 1,
                            Ok(None) => {}
                            Err(e) => {
                                poll_error = Some(e.to_string());
                                break;
                            }
                        }
                    }
                    conn.update_position();
                    conn.update_other_players();
                    app.server_tick = conn.server_tick();
                    let (x, y) = conn.player_position();
                    app.player_x = x;
                    app.player_y = y;
                    // Update other players list
                    app.other_players = conn.other_players()
                        .values()
                        .filter(|p| p.connected)
                        .cloned()
                        .collect();
                }
                if let Some(err) = poll_error {
                    app.log(format!("Connection error: {}", err), Color::Red);
                    app.state = AppState::Error;
                }
            }

            AppState::OfflineMap => {
                if let Some(rx) = app.map_parse_rx.as_ref() {
                    match rx.try_recv() {
                        Ok(Ok(parsed)) => {
                            app.entities = parsed.entities;
                            app.tiles = parsed.tiles;
                            app.tile_index = parsed.tile_index;
                            app.entity_chunk_index = parsed.entity_chunk_index;
                            app.map_seed = parsed.seed;
                            app.map_controls = parsed.map_controls;
                            *app.terrain_generator.borrow_mut() = None;
                            app.terrain_cache.borrow_mut().clear();
                            if parsed.map_width > 0 && parsed.map_height > 0 {
                                let half_w = (parsed.map_width / 2) as f64;
                                let half_h = (parsed.map_height / 2) as f64;
                                app.map_bounds = Some(half_w.max(half_h));
                            }
                            if app.player_x == 0.0 && app.player_y == 0.0 {
                                app.player_x = parsed.spawn.0;
                                app.player_y = parsed.spawn.1;
                            }
                            app.log(
                                format!(
                                    "Parsed map: {} entities, {} tiles (seed={}, w={}, h={})",
                                    app.entities.len(),
                                    app.tiles.len(),
                                    app.map_seed,
                                    parsed.map_width,
                                    parsed.map_height
                                ),
                                Color::Green,
                            );
                            app.show_parsed_map = true;
                            app.status = "Offline Map Viewer".into();
                            app.map_parse_rx = None;
                            app.map_parse_handle = None;
                        }
                        Ok(Err(err)) => {
                            app.log(format!("Map parse failed: {}", err), Color::Red);
                            app.map_parse_rx = None;
                            app.map_parse_handle = None;
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty) => {}
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            app.log("Map parse thread disconnected".to_string(), Color::Red);
                            app.map_parse_rx = None;
                            app.map_parse_handle = None;
                        }
                    }
                }
                if let Some(rx) = app.resource_parse_rx.as_ref() {
                    match rx.try_recv() {
                        Ok(resources) => {
                            let before = app.entities.len();
                            app.entities.extend(resources);
                            app.entity_chunk_index = build_entity_chunk_index(&app.entities);
                            let added = app.entities.len().saturating_sub(before);
                            app.log(format!("Resources parsed: {} new entities", added), Color::Green);
                            app.resource_parse_rx = None;
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty) => {}
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            app.resource_parse_rx = None;
                        }
                    }
                }
                app.clear_parse_progress_if_done();
            }

            _ => {}
        }
    }

    fn move_player_or_pan(
        app: &mut App,
        rt: &tokio::runtime::Runtime,
        dx: i32,
        dy: i32,
        direction: u8,
    ) {
        if app.cursor_mode {
            app.cursor_dx += dx;
            app.cursor_dy += dy;
            return;
        }

        if app.state == AppState::OfflineMap {
            let step = 1.0 / app.zoom();
            let new_x = app.player_x + dx as f64 * step;
            let new_y = app.player_y + dy as f64 * step;
            if !check_player_collision(&app.entities, new_x, new_y) {
                app.player_x = new_x;
                app.player_y = new_y;
            }
            if let Some(bounds) = app.map_bounds {
                app.player_x = app.player_x.clamp(-bounds, bounds);
                app.player_y = app.player_y.clamp(-bounds, bounds);
            }
            return;
        }

        let result = if let Some(conn) = app.connection.as_mut() {
            if !conn.is_in_game() {
                Err("Not in game".to_string())
            } else {
                rt.block_on(conn.actions().send_walk(direction))
                    .map_err(|e| e.to_string())
            }
        } else {
            Err("Not connected".to_string())
        };

        match result {
            Ok(()) => {
                app.walking = true;
                app.walk_direction = direction;
            }
            Err(e) => app.log(format!("Walk failed: {}", e), Color::Red),
        }
    }

    fn move_cursor(app: &mut App, dx: i32, dy: i32) {
        app.cursor_dx += dx;
        app.cursor_dy += dy;
    }

    fn handle_input(app: &mut App, key: KeyCode, rt: &tokio::runtime::Runtime) {
        if app.show_help {
            app.show_help = false;
            return;
        }

        if app.chat_mode {
            match key {
                KeyCode::Enter => {
                    let message = app.chat_input.trim().to_string();
                    app.chat_input.clear();
                    app.chat_mode = false;
                    if message.is_empty() {
                        return;
                    }
                    let result = if let Some(conn) = app.connection.as_mut() {
                        rt.block_on(conn.actions().send_chat(&message))
                            .map_err(|e| e.to_string())
                    } else {
                        Err("Not connected".to_string())
                    };
                    match result {
                        Ok(()) => app.log(format!("Chat: {}", message), Color::Green),
                        Err(e) => app.log(format!("Chat failed: {}", e), Color::Red),
                    }
                }
                KeyCode::Esc => {
                    app.chat_input.clear();
                    app.chat_mode = false;
                }
                KeyCode::Backspace => {
                    app.chat_input.pop();
                }
                KeyCode::Char(c) => {
                    if !c.is_ascii_control() {
                        app.chat_input.push(c);
                    }
                }
                _ => {}
            }
            return;
        }

        match key {
            KeyCode::Enter => {
                if matches!(app.state, AppState::Disconnected | AppState::Error) {
                    app.log("Connecting...", Color::Yellow);
                    app.state = AppState::Connecting;
                }
            }
            KeyCode::Tab => {
                app.cursor_mode = !app.cursor_mode;
            }
            KeyCode::Up => move_player_or_pan(app, rt, 0, -1, 0),
            KeyCode::Down => move_player_or_pan(app, rt, 0, 1, 4),
            KeyCode::Left => move_player_or_pan(app, rt, -1, 0, 6),
            KeyCode::Right => move_player_or_pan(app, rt, 1, 0, 2),
            KeyCode::Char(c) => {
                let c = c.to_ascii_lowercase();
                match c {
                    'h' => app.show_help = true,
                    'c' => {
                        app.chat_mode = true;
                        app.chat_input.clear();
                    }
                    '0' => {
                        app.cursor_dx = 0;
                        app.cursor_dy = 0;
                    }
                    '1' => {
                        app.zoom_idx = DEFAULT_ZOOM_IDX;
                    }
                    '+' | '=' => {
                        if app.zoom_idx + 1 < ZOOM_LEVELS.len() {
                            app.zoom_idx += 1;
                        }
                    }
                    '-' | '_' => {
                        if app.zoom_idx > 0 {
                            app.zoom_idx -= 1;
                        }
                    }
                    ' ' => {
                        app.mining = false;
                        app.walking = false;
                        let mut stop_walk_err = None;
                        let mut stop_mine_err = None;
                        if let Some(conn) = app.connection.as_mut() {
                            if conn.is_in_game() {
                                if let Err(e) = rt.block_on(conn.actions().send_stop_walk()) {
                                    stop_walk_err = Some(e.to_string());
                                }
                                if let Err(e) = rt.block_on(conn.actions().send_stop_mine()) {
                                    stop_mine_err = Some(e.to_string());
                                }
                            }
                        }
                        if let Some(e) = stop_walk_err {
                            app.log(format!("Stop walk failed: {}", e), Color::Red);
                        }
                        if let Some(e) = stop_mine_err {
                            app.log(format!("Stop mining failed: {}", e), Color::Red);
                        }
                    }
                    'm' => {
                        let (x, y) = app.cursor_world_pos();
                        let result = if let Some(conn) = app.connection.as_mut() {
                            rt.block_on(conn.actions().send_mine(x, y))
                                .map_err(|e| e.to_string())
                        } else {
                            Err("Not connected".to_string())
                        };
                        match result {
                            Ok(()) => {
                                app.mining = true;
                                app.log(format!("Mining at ({:.1}, {:.1})", x, y), Color::Yellow);
                            }
                            Err(e) => app.log(format!("Mine failed: {}", e), Color::Red),
                        }
                    }
                    'b' => {
                        let (x, y) = app.cursor_world_pos();
                        let result = if let Some(conn) = app.connection.as_mut() {
                            rt.block_on(conn.actions().send_build(x, y, 0))
                                .map_err(|e| e.to_string())
                        } else {
                            Err("Not connected".to_string())
                        };
                        if let Err(e) = result {
                            app.log(format!("Build failed: {}", e), Color::Red);
                        }
                    }
                    'r' => {
                        let (x, y) = app.cursor_world_pos();
                        let result = if let Some(conn) = app.connection.as_mut() {
                            rt.block_on(conn.actions().send_rotate(x, y, false))
                                .map_err(|e| e.to_string())
                        } else {
                            Err("Not connected".to_string())
                        };
                        if let Err(e) = result {
                            app.log(format!("Rotate failed: {}", e), Color::Red);
                        }
                    }
                    'w' => move_player_or_pan(app, rt, 0, -1, 0),
                    's' => move_player_or_pan(app, rt, 0, 1, 4),
                    'a' => move_player_or_pan(app, rt, -1, 0, 6),
                    'd' => move_player_or_pan(app, rt, 1, 0, 2),
                    'i' => move_cursor(app, 0, -1),
                    'k' => move_cursor(app, 0, 1),
                    'j' => move_cursor(app, -1, 0),
                    'l' => move_cursor(app, 1, 0),
                    'p' => {
                        app.show_parsed_map = !app.show_parsed_map;
                        app.log(
                            format!("Parsed map: {}", if app.show_parsed_map { "ON" } else { "OFF" }),
                            Color::Yellow,
                        );
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    fn render(frame: &mut Frame, app: &App, entity_icons: &RefCell<Option<EntityIcons>>) {
        // Help overlay
        if app.show_help {
            render_help(frame);
            return;
        }

        let area = frame.area();

        // Layout: Header (3) | Main | Footer (1)
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(10),
                Constraint::Length(1),
            ])
            .split(area);

        render_header(frame, app, chunks[0]);
        render_main(frame, app, chunks[1], entity_icons);
        render_footer(frame, app, chunks[2]);
    }

    fn render_header(frame: &mut Frame, app: &App, area: Rect) {
        let (icon, icon_color) = match app.state {
            AppState::Disconnected => ("OFFLINE", Color::DarkGray),
            AppState::Connecting => ("CONNECTING", Color::Yellow),
            AppState::DownloadingMap => ("DOWNLOADING", Color::Yellow),
            AppState::Connected => ("ONLINE", Color::Green),
            AppState::OfflineMap => ("MAP VIEW", Color::Cyan),
            AppState::Error => ("ERROR", Color::Red),
        };

        let header_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(14),
                Constraint::Min(20),
                Constraint::Length(30),
            ])
            .split(area);

        // Status box
        let status_block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(icon_color))
            .title(" Status ");
        let status_text = Paragraph::new(icon)
            .style(Style::default().fg(icon_color).add_modifier(Modifier::BOLD))
            .alignment(Alignment::Center)
            .block(status_block);
        frame.render_widget(status_text, header_chunks[0]);

        // Info panel
        let mode_str = if app.cursor_mode { "CURSOR" } else { "MOVE" };
        let walk_str = if app.walking {
            match app.walk_direction {
                0 => "N", 1 => "NE", 2 => "E", 3 => "SE",
                4 => "S", 5 => "SW", 6 => "W", 7 => "NW", _ => "?"
            }
        } else { "-" };

        let info = vec![
            Line::from(vec![
                Span::styled(format!("{} ", app.username), Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
                Span::styled(format!("@ {}", app.server_addr), Style::default().fg(Color::DarkGray)),
            ]),
            Line::from(vec![
                Span::raw("Pos: "),
                Span::styled(format!("({:.1}, {:.1})", app.player_x, app.player_y), Style::default().fg(Color::Yellow)),
                Span::raw("  Mode: "),
                Span::styled(mode_str, Style::default().fg(if app.cursor_mode { Color::Magenta } else { Color::Green })),
                Span::raw("  Walk: "),
                Span::styled(walk_str, Style::default().fg(if app.walking { Color::Green } else { Color::DarkGray })),
                Span::raw("  Map: "),
                Span::styled(if app.show_parsed_map { "ON" } else { "OFF" }, Style::default().fg(if app.show_parsed_map { Color::Green } else { Color::DarkGray })),
            ]),
        ];
        let info_block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray))
            .title(format!(" {} ", app.server_name.as_deref().unwrap_or("Factorio")));
        frame.render_widget(Paragraph::new(info).block(info_block), header_chunks[1]);

        // Stats panel
        let players_str = if app.other_players.is_empty() {
            "0".to_string()
        } else {
            format!("{}", app.other_players.len())
        };
        let stats = vec![
            Line::from(vec![
                Span::raw("Tick: "),
                Span::styled(format!("{}", app.server_tick), Style::default().fg(Color::Cyan)),
                Span::raw("  Zoom: "),
                Span::styled(format!("{:.0}%", app.zoom() * 100.0), Style::default().fg(Color::Yellow)),
            ]),
            Line::from(vec![
                Span::raw("Players: "),
                Span::styled(players_str, Style::default().fg(Color::Magenta)),
                Span::raw("  Rx: "),
                Span::styled(format!("{}", app.packets_received), Style::default().fg(Color::DarkGray)),
            ]),
        ];
        let stats_block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray))
            .title(" Stats ");
        frame.render_widget(Paragraph::new(stats).block(stats_block), header_chunks[2]);
    }

    fn render_main(frame: &mut Frame, app: &App, area: Rect, entity_icons: &RefCell<Option<EntityIcons>>) {
        let main_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Min(40), Constraint::Length(28)])
            .split(area);

        // Map
        render_map(frame, app, main_chunks[0], entity_icons);

        // Side panel
        render_sidebar(frame, app, main_chunks[1]);
    }

    fn render_map(frame: &mut Frame, app: &App, area: Rect, entity_icons: &RefCell<Option<EntityIcons>>) {
        // Draw border
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray))
            .title(" Map ");
        let inner_area = block.inner(area);
        frame.render_widget(block, area);

        #[cfg(feature = "gpu")]
        {
            let mut gpu_ref = app.gpu_renderer.borrow_mut();
            if let Some(renderer) = gpu_ref.as_mut() {
                // Cells are ~2:1 (height:width in pixels), so pixel aspect of display area:
                let pixel_aspect = inner_area.width as f32 / (inner_area.height.max(1) as f32 * 2.0);
                let (render_w, render_h) = if pixel_aspect > 1.0 {
                    (800, (800.0 / pixel_aspect) as u32)
                } else {
                    ((800.0 * pixel_aspect) as u32, 800)
                };
                if render_w >= 8 && render_h >= 8 {
                    let tiles_visible = inner_area.height as f64 * Y_STRETCH / app.zoom();
                    let animating = (renderer.camera.x - renderer.camera.target_x).abs() > 0.001
                        || (renderer.camera.y - renderer.camera.target_y).abs() > 0.001
                        || (renderer.camera.zoom - renderer.camera.target_zoom).abs() > 0.01;
                    let dirty = animating
                        || renderer.last_cam_x != app.player_x
                        || renderer.last_cam_y != app.player_y
                        || renderer.last_cam_zoom != tiles_visible
                        || renderer.last_parsed != app.show_parsed_map
                        || renderer.last_w != render_w
                        || renderer.last_h != render_h;
                    if dirty {
                        renderer.render_frame(app, render_w, render_h, tiles_visible);
                    }
                    renderer.pending_area = Some(inner_area);
                    return;
                }
            }
        }

        let width = inner_area.width as i32;
        let height = inner_area.height as i32;
        let zoom = app.zoom();
        let base_x = inner_area.x;
        let base_y = inner_area.y;

        // Use 2-char cells for consistent emoji rendering
        let cell_width = 2;
        let cols = width / cell_width;
        let y_stretch = Y_STRETCH;
        let cell_half_x = 0.5 / zoom;
        let cell_half_y = 0.5 * y_stretch / zoom;

        let buf = frame.buffer_mut();

        for row in 0..height {
            for col in 0..cols {
                let screen_x = base_x + (col * cell_width) as u16;
                let screen_y = base_y + row as u16;

                // Map screen position to world position
                let offset_x = (col - cols / 2) as f64 / zoom;
                let offset_y = (row - height / 2) as f64 * y_stretch / zoom;
                let world_x = app.player_x + offset_x;
                let world_y = app.player_y + offset_y;
                let tile_x = world_x.floor() as i32;
                let tile_y = world_y.floor() as i32;

                let is_player = col == cols / 2 && row == height / 2;
                let is_cursor = col - cols / 2 == app.cursor_dx && row - height / 2 == app.cursor_dy;

                // Check if another player is at this position
                let other_player = app.other_players.iter().find(|p| {
                    let dx = ((p.x - world_x) * zoom).round() as i32;
                    let dy = ((p.y - world_y) * zoom).round() as i32;
                    dx == 0 && dy == 0
                });

                // Find entity at this position (only if showing parsed map)
                let entity = if app.show_parsed_map {
                    let chunk_x = tile_x.div_euclid(32);
                    let chunk_y = tile_y.div_euclid(32);
                    let cell_min_x = world_x - cell_half_x;
                    let cell_max_x = world_x + cell_half_x;
                    let cell_min_y = world_y - cell_half_y;
                    let cell_max_y = world_y + cell_half_y;
                    app.entity_chunk_index
                        .get(&(chunk_x, chunk_y))
                        .and_then(|indices| {
                            indices.iter().find_map(|idx| {
                                app.entities.get(*idx).and_then(|e| {
                                    let half_w = e.tile_width() / 2.0;
                                    let half_h = e.tile_height() / 2.0;
                                    if cell_min_x < e.x + half_w && cell_max_x > e.x - half_w
                                        && cell_min_y < e.y + half_h && cell_max_y > e.y - half_h
                                    {
                                        Some(e)
                                    } else {
                                        None
                                    }
                                })
                            })
                        })
                } else {
                    None
                };

                // Check if position is within map bounds
                let in_bounds = app.map_bounds.map_or(true, |b| {
                    world_x.abs() <= b && world_y.abs() <= b
                });

                // Get parsed tile (only used when show_parsed_map is on)
                let parsed_tile = if app.show_parsed_map && in_bounds {
                    app.tile_index
                        .get(&(tile_x, tile_y))
                        .and_then(|idx| app.tiles.get(*idx))
                        .filter(|t| !t.procedural)
                } else {
                    None
                };

                let hide_map = matches!(app.state, AppState::Disconnected | AppState::Connecting | AppState::DownloadingMap)
                    && app.map_seed == 0
                    && !app.show_parsed_map;

                // Compute procedural terrain as base layer (only if in bounds)
                let procedural_tile_name = if in_bounds && !hide_map && (!app.show_parsed_map || app.tiles.is_empty()) {
                    Some(app.procedural_tile_at(tile_x, tile_y))
                } else {
                    None
                };

                let (icon, style) = if is_player {
                    ("😀", Style::default().fg(Color::White).bg(Color::Blue))
                } else if let Some(player) = other_player {
                    // Show other player with different icon
                    let color = if player.walking { Color::LightGreen } else { Color::Magenta };
                    ("👤", Style::default().fg(color))
                } else if is_cursor {
                    ("░░", Style::default().fg(Color::Yellow))
                } else if !in_bounds || hide_map {
                    // Out of bounds - show void
                    ("  ", Style::default().bg(Color::Black))
                } else if let Some(ent) = entity {
                    // Always draw a fallback icon in the text layer so entities remain visible
                    let (icon, color) = entity_icon(&ent.name);
                    (icon, Style::default().fg(color))
                } else if let Some(tile) = parsed_tile {
                    // Parsed tile overlays procedural terrain
                    let color = tile_color(&tile.name);
                    ("  ", Style::default().bg(color))
                } else if let Some(name) = procedural_tile_name {
                    // Procedural terrain as base layer
                    let color = tile_color(name);
                    ("  ", Style::default().bg(color))
                } else {
                    // Fallback (shouldn't happen)
                    ("  ", Style::default())
                };

                buf.set_string(screen_x, screen_y, icon, style);
            }
        }

        // Render entity images using kitty/sixel protocol
        if app.show_parsed_map {
            let mut icons_ref = entity_icons.borrow_mut();
            if let Some(ref mut icons) = *icons_ref {
                // Clear protocol cache when zoom changes
                if icons.last_zoom != zoom {
                    icons.protocols.clear();
                    icons.last_zoom = zoom;
                }

                let mut to_render: Vec<(Rect, String)> = Vec::new();
                let view_tiles_x = cols as f64 / zoom;
                let view_tiles_y = height as f64 * Y_STRETCH / zoom;
                let min_tile_x = (app.player_x - view_tiles_x / 2.0).floor() as i32;
                let max_tile_x = (app.player_x + view_tiles_x / 2.0).ceil() as i32;
                let min_tile_y = (app.player_y - view_tiles_y / 2.0).floor() as i32;
                let max_tile_y = (app.player_y + view_tiles_y / 2.0).ceil() as i32;
                let min_chunk_x = min_tile_x.div_euclid(32);
                let max_chunk_x = max_tile_x.div_euclid(32);
                let min_chunk_y = min_tile_y.div_euclid(32);
                let max_chunk_y = max_tile_y.div_euclid(32);

                let mut seen = std::collections::HashSet::new();
                for cy in min_chunk_y..=max_chunk_y {
                    for cx in min_chunk_x..=max_chunk_x {
                        if let Some(indices) = app.entity_chunk_index.get(&(cx, cy)) {
                            for idx in indices {
                                if !seen.insert(*idx) {
                                    continue;
                                }
                                let entity = match app.entities.get(*idx) {
                                    Some(e) => e,
                                    None => continue,
                                };
                                let dx = entity.x - app.player_x;
                                let dy = entity.y - app.player_y;
                                let screen_col = (dx * zoom).round() as i32 + cols / 2;
                                let screen_row = (dy * zoom / Y_STRETCH).round() as i32 + height / 2;
                                let ew = (entity.tile_width() * zoom).round() as i32;
                                let eh = (entity.tile_height() * zoom / Y_STRETCH).round() as i32;
                                if ew < 1 || eh < 1 { continue; }
                                let x0 = screen_col - ew / 2;
                                let y0 = screen_row - eh / 2;
                                if x0 < 0 || y0 < 0 || x0 + ew > cols || y0 + eh > height { continue; }
                                let rect = Rect::new(
                                    base_x + (x0 * cell_width) as u16,
                                    base_y + y0 as u16,
                                    (ew * cell_width) as u16,
                                    eh as u16,
                                );
                                to_render.push((rect, entity.name.clone()));
                            }
                        }
                    }
                }

                // Ensure images and protocols exist for visible entity types
                for (_, name) in &to_render {
                    icons.get_image(name);
                }
                let needs_proto: Vec<String> = to_render.iter()
                    .map(|(_, name)| name.clone())
                    .filter(|name| !icons.protocols.contains_key(name) && icons.images.contains_key(name))
                    .collect();
                for name in needs_proto {
                    let img = icons.images.get(&name).unwrap().clone();
                    let proto = icons.picker.new_resize_protocol(img);
                    icons.protocols.insert(name, proto);
                }

                // Render each entity using cached protocol
                for (rect, name) in &to_render {
                    if let Some(proto) = icons.protocols.get_mut(name) {
                        let widget = StatefulImage::default().resize(Resize::Scale(None));
                        frame.render_stateful_widget(widget, *rect, proto);
                    }
                }
            }
        }
    }

    fn render_sidebar(frame: &mut Frame, app: &App, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(7),  // Cursor + entity info
                Constraint::Length(8),  // Entity counts
                Constraint::Min(5),    // Log
            ])
            .split(area);

        // Cursor info + entity under cursor
        let (cx, cy) = app.cursor_world_pos();
        let cursor_entity = app.entity_at_cursor();

        let mut cursor_info = vec![
            Line::from(vec![
                Span::raw("World: "),
                Span::styled(format!("({:.1}, {:.1})", cx, cy), Style::default().fg(Color::Cyan)),
            ]),
            Line::from(vec![
                Span::raw("Offset: "),
                Span::styled(format!("({}, {})", app.cursor_dx, app.cursor_dy), Style::default().fg(Color::Yellow)),
            ]),
        ];
        if let Some(ent) = cursor_entity {
            let (icon, color) = entity_icon(&ent.name);
            cursor_info.push(Line::from(vec![
                Span::styled(icon, Style::default().fg(color)),
                Span::styled(&ent.name, Style::default().fg(color)),
            ]));
        }

        let cursor_block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(if app.cursor_mode { Color::Magenta } else { Color::DarkGray }))
            .title(" Cursor ");
        frame.render_widget(Paragraph::new(cursor_info).block(cursor_block), chunks[0]);

        // Entity/resource counts
        let total = app.entities.len();
        let trees = app.entities.iter().filter(|e| e.name.starts_with("tree-") || e.name.starts_with("dead-")).count();
        let rocks = app.entities.iter().filter(|e| e.name.contains("rock") && !e.name.contains("rocket")).count();
        let iron = app.entities.iter().filter(|e| e.name == "iron-ore").count();
        let copper = app.entities.iter().filter(|e| e.name == "copper-ore").count();
        let coal = app.entities.iter().filter(|e| e.name == "coal").count();
        let stone = app.entities.iter().filter(|e| e.name == "stone").count();

        let entity_lines = vec![
            Line::from(vec![
                Span::raw("Total: "),
                Span::styled(format!("{}", total), Style::default().fg(Color::White)),
            ]),
            Line::from(vec![
                Span::styled("Fe:", Style::default().fg(Color::LightBlue)),
                Span::raw(format!("{} ", iron)),
                Span::styled("Cu:", Style::default().fg(Color::Rgb(255, 140, 0))),
                Span::raw(format!("{}", copper)),
            ]),
            Line::from(vec![
                Span::styled("C:", Style::default().fg(Color::DarkGray)),
                Span::raw(format!("{} ", coal)),
                Span::styled("St:", Style::default().fg(Color::Gray)),
                Span::raw(format!("{}", stone)),
            ]),
            Line::from(vec![
                Span::styled("Trees:", Style::default().fg(Color::Green)),
                Span::raw(format!("{} ", trees)),
                Span::styled("Rock:", Style::default().fg(Color::Gray)),
                Span::raw(format!("{}", rocks)),
            ]),
        ];

        let entity_block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray))
            .title(" Entities ");
        frame.render_widget(Paragraph::new(entity_lines).block(entity_block), chunks[1]);

        // Log
        let log_height = chunks[2].height.saturating_sub(2) as usize;
        let log_lines: Vec<Line> = app.log.iter()
            .rev()
            .take(log_height)
            .map(|(msg, color)| Line::from(Span::styled(msg.clone(), Style::default().fg(*color))))
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();

        let log_block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray))
            .title(" Log ");
        frame.render_widget(Paragraph::new(log_lines).block(log_block), chunks[2]);
    }

    fn render_footer(frame: &mut Frame, app: &App, area: Rect) {
        let parse_line = app.map_parse_status_line();
        let text = if let Some(line) = parse_line {
            format!(" {} ", line)
        } else if app.chat_mode {
            format!(" Chat: {}_ ", app.chat_input)
        } else if app.mining {
            " MINING... [M] to stop ".into()
        } else {
            " WASD:move  IJKL:cursor  +/-:zoom  M:mine  B:build  R:rotate  P:toggle-map  C:chat  H:help  Q:quit ".into()
        };

        let style = if parse_line.is_some() {
            Style::default().fg(Color::Yellow)
        } else if app.chat_mode {
            Style::default().fg(Color::Black).bg(Color::Yellow)
        } else if app.mining {
            Style::default().fg(Color::Black).bg(Color::Yellow)
        } else {
            Style::default().fg(Color::DarkGray)
        };

        frame.render_widget(Paragraph::new(text).style(style), area);
    }

    fn render_help(frame: &mut Frame) {
        let area = frame.area();
        let popup_width = 50;
        let popup_height = 19;
        let popup_x = (area.width.saturating_sub(popup_width)) / 2;
        let popup_y = (area.height.saturating_sub(popup_height)) / 2;
        let popup_area = Rect::new(popup_x, popup_y, popup_width, popup_height);

        // Clear background
        frame.render_widget(
            Block::default().style(Style::default().bg(Color::Black)),
            area
        );

        let help_text = vec![
            Line::from(Span::styled("CONTROLS", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))),
            Line::from(""),
            Line::from(vec![Span::styled("WASD / Arrows", Style::default().fg(Color::Yellow)), Span::raw("  Move player")]),
            Line::from(vec![Span::styled("IJKL", Style::default().fg(Color::Yellow)), Span::raw("          Move cursor")]),
            Line::from(vec![Span::styled("+ / -", Style::default().fg(Color::Yellow)), Span::raw("         Zoom in/out")]),
            Line::from(vec![Span::styled("Tab", Style::default().fg(Color::Yellow)), Span::raw("           Toggle cursor mode")]),
            Line::from(vec![Span::styled("M", Style::default().fg(Color::Yellow)), Span::raw("             Mine at cursor")]),
            Line::from(vec![Span::styled("B", Style::default().fg(Color::Yellow)), Span::raw("             Build at cursor")]),
            Line::from(vec![Span::styled("R", Style::default().fg(Color::Yellow)), Span::raw("             Rotate at cursor")]),
            Line::from(vec![Span::styled("Space", Style::default().fg(Color::Yellow)), Span::raw("         Stop all actions")]),
            Line::from(vec![Span::styled("C", Style::default().fg(Color::Yellow)), Span::raw("             Chat mode")]),
            Line::from(vec![Span::styled("P", Style::default().fg(Color::Yellow)), Span::raw("             Toggle parsed map")]),
            Line::from(vec![Span::styled("0", Style::default().fg(Color::Yellow)), Span::raw("             Reset cursor")]),
            Line::from(vec![Span::styled("1", Style::default().fg(Color::Yellow)), Span::raw("             Reset zoom")]),
            Line::from(vec![Span::styled("Q", Style::default().fg(Color::Yellow)), Span::raw("             Quit")]),
            Line::from(""),
            Line::from(Span::styled("Press any key to close", Style::default().fg(Color::DarkGray))),
        ];

        let help_block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan))
            .title(" Help ");
        frame.render_widget(Paragraph::new(help_text).block(help_block), popup_area);
    }

    fn entity_icon(name: &str) -> (&'static str, Color) {
        match name {
            // Resources
            n if n.contains("iron-ore") => ("⬤ ", Color::LightBlue),
            n if n.contains("copper-ore") => ("⬤ ", Color::Rgb(255, 140, 0)),
            n if n.contains("coal") => ("⬤ ", Color::DarkGray),
            n if n.contains("stone") && !n.contains("furnace") => ("⬤ ", Color::Gray),
            n if n.contains("uranium") => ("☢ ", Color::LightGreen),
            n if n.contains("ore") => ("⬤ ", Color::White),

            // Belts (must be before generic patterns)
            "transport-belt" => ("══", Color::Yellow),
            "fast-transport-belt" => ("══", Color::Red),
            "express-transport-belt" => ("══", Color::LightBlue),
            n if n.contains("turbo") && n.contains("belt") => ("══", Color::Magenta),
            n if n.contains("underground") => ("⊏⊐", Color::Yellow),
            n if n.contains("splitter") => ("⋔⋔", Color::Yellow),
            n if n.contains("belt") => ("══", Color::Yellow),
            n if n.contains("loader") => ("⊐⊏", Color::Yellow),

            // Inserters
            "burner-inserter" => ("→ ", Color::Rgb(139, 90, 43)),
            "inserter" => ("→ ", Color::Yellow),
            "fast-inserter" => ("⇒ ", Color::LightBlue),
            "bulk-inserter" | "stack-inserter" => ("⇛ ", Color::LightGreen),
            "long-handed-inserter" => ("⟹ ", Color::Red),
            n if n.contains("inserter") => ("→ ", Color::Yellow),

            // Mining
            "burner-mining-drill" => ("⛏ ", Color::Rgb(139, 90, 43)),
            "electric-mining-drill" => ("⛏ ", Color::LightBlue),
            n if n.contains("drill") => ("⛏ ", Color::Yellow),

            // Furnaces
            "stone-furnace" => ("🔥", Color::Gray),
            "steel-furnace" => ("🔥", Color::White),
            "electric-furnace" => ("⚡ ", Color::LightBlue),
            n if n.contains("furnace") => ("🔥", Color::Yellow),

            // Assemblers
            n if n.contains("assembling") => ("⚙ ", Color::LightBlue),

            // Storage (before cargo-wagon to not match "chest")
            "wooden-chest" => ("📦", Color::Rgb(139, 90, 43)),
            "iron-chest" => ("📦", Color::White),
            "steel-chest" => ("📦", Color::LightBlue),
            n if n.contains("chest") => ("📦", Color::Yellow),

            // Trains (must be before generic "car" pattern!)
            n if n.contains("locomotive") => ("🚂", Color::LightBlue),
            n if n.contains("cargo-wagon") => ("🚃", Color::Yellow),
            n if n.contains("fluid-wagon") => ("🚃", Color::LightBlue),
            n if n.contains("artillery-wagon") => ("🚃", Color::Red),
            n if n.contains("rail") && !n.contains("signal") => ("╪╪", Color::Gray),
            n if n.contains("rail-chain-signal") => ("🚦", Color::Yellow),
            n if n.contains("rail-signal") => ("🚦", Color::Green),
            n if n.contains("train-stop") => ("🚏", Color::Yellow),

            // Power poles
            "small-electric-pole" => ("│ ", Color::Rgb(139, 90, 43)),
            "medium-electric-pole" => ("┃ ", Color::LightBlue),
            "big-electric-pole" => ("╽ ", Color::LightBlue),
            n if n.contains("substation") => ("╋ ", Color::LightBlue),
            n if n.contains("pole") => ("│ ", Color::LightBlue),

            // Power generation
            "boiler" => ("♨ ", Color::Gray),
            "steam-engine" => ("♨ ", Color::LightBlue),
            n if n.contains("solar") => ("☀ ", Color::LightBlue),
            n if n.contains("accumulator") => ("🔋", Color::LightGreen),
            n if n.contains("nuclear") => ("☢ ", Color::LightGreen),

            // Pipes & fluids
            n if n.contains("storage-tank") => ("◯ ", Color::Gray),
            n if n.contains("pipe") => ("──", Color::Gray),
            n if n.contains("pump") && !n.contains("pumpjack") => ("⊳ ", Color::LightBlue),
            n if n.contains("pumpjack") => ("⛽", Color::Gray),
            n if n.contains("offshore") => ("🌊", Color::LightBlue),

            // Military
            n if n.contains("gun-turret") => ("⊕ ", Color::Yellow),
            n if n.contains("laser-turret") => ("⊕ ", Color::Red),
            n if n.contains("flamethrower-turret") => ("⊕ ", Color::Rgb(255, 100, 0)),
            n if n.contains("artillery-turret") => ("⊕ ", Color::White),
            n if n.contains("turret") => ("⊕ ", Color::Red),
            n if n.contains("land-mine") => ("💣", Color::Yellow),
            n if n.contains("wall") => ("▓▓", Color::Gray),
            n if n.contains("gate") => ("▒▒", Color::Gray),

            // Labs & science
            "lab" => ("🔬", Color::Magenta),
            "radar" => ("◎ ", Color::LightGreen),
            n if n.contains("roboport") => ("🤖", Color::LightBlue),
            n if n.contains("beacon") => ("◉ ", Color::Magenta),

            // Rockets & space
            n if n.contains("rocket-silo") => ("🚀", Color::White),
            n if n.contains("cargo-landing-pad") => ("🛬", Color::White),
            n if n.contains("satellite") => ("🛰 ", Color::White),

            // Vehicles (specific patterns, "car" == exactly car entity)
            "car" => ("🚗", Color::Red),
            "tank" => ("🚙", Color::Green),
            n if n.contains("spidertron") => ("🕷 ", Color::Yellow),

            // Robots
            n if n.contains("logistic-robot") => ("🤖", Color::Yellow),
            n if n.contains("construction-robot") => ("🤖", Color::LightBlue),

            // Chemistry / Oil
            n if n.contains("refinery") => ("🏭", Color::Gray),
            n if n.contains("chemical-plant") => ("⚗ ", Color::LightBlue),
            n if n.contains("centrifuge") => ("☢ ", Color::LightGreen),

            // Nature
            n if n.contains("tree") => ("🌲", Color::Green),
            n if n.contains("rock") => ("🪨", Color::Gray),
            n if n.contains("fish") => ("🐟", Color::LightBlue),

            // Misc buildings
            n if n.contains("constant-combinator") => ("CC", Color::Green),
            n if n.contains("arithmetic-combinator") => ("AC", Color::Red),
            n if n.contains("decider-combinator") => ("DC", Color::Yellow),
            n if n.contains("power-switch") => ("⏻ ", Color::LightBlue),
            n if n.contains("lamp") => ("💡", Color::Yellow),
            n if n.contains("speaker") => ("🔊", Color::Yellow),

            // Default - show ? with name hint
            _ => ("? ", Color::DarkGray),
        }
    }

    #[cfg(feature = "gpu")]
    fn gpu_base64(data: &[u8]) -> String {
        const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = Vec::with_capacity((data.len() + 2) / 3 * 4);
        for chunk in data.chunks(3) {
            let b0 = chunk[0] as u32;
            let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
            let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
            let n = (b0 << 16) | (b1 << 8) | b2;
            out.push(TABLE[((n >> 18) & 63) as usize]);
            out.push(TABLE[((n >> 12) & 63) as usize]);
            out.push(if chunk.len() > 1 { TABLE[((n >> 6) & 63) as usize] } else { b'=' });
            out.push(if chunk.len() > 2 { TABLE[(n & 63) as usize] } else { b'=' });
        }
        unsafe { String::from_utf8_unchecked(out) }
    }

    fn tile_color(name: &str) -> Color {
        match name {
            n if n.contains("deepwater") => Color::Rgb(20, 50, 100),
            n if n.contains("water") => Color::Rgb(40, 80, 140),
            n if n.contains("grass-1") => Color::Rgb(60, 100, 40),
            n if n.contains("grass-2") => Color::Rgb(70, 110, 45),
            n if n.contains("grass-3") => Color::Rgb(80, 120, 50),
            n if n.contains("grass-4") => Color::Rgb(90, 130, 55),
            n if n.contains("grass") => Color::Rgb(70, 110, 45),
            n if n.contains("dry-dirt") => Color::Rgb(140, 110, 70),
            n if n.contains("dirt-1") => Color::Rgb(100, 70, 40),
            n if n.contains("dirt-2") => Color::Rgb(110, 75, 45),
            n if n.contains("dirt-3") => Color::Rgb(115, 80, 50),
            n if n.contains("dirt-4") => Color::Rgb(120, 85, 55),
            n if n.contains("dirt-5") => Color::Rgb(125, 90, 55),
            n if n.contains("dirt-6") => Color::Rgb(130, 95, 60),
            n if n.contains("dirt-7") => Color::Rgb(135, 100, 65),
            n if n.contains("dirt") => Color::Rgb(110, 80, 50),
            n if n.contains("red-desert") => Color::Rgb(150, 90, 60),
            n if n.contains("sand-1") => Color::Rgb(180, 160, 100),
            n if n.contains("sand-2") => Color::Rgb(190, 170, 110),
            n if n.contains("sand-3") => Color::Rgb(200, 180, 120),
            n if n.contains("sand") => Color::Rgb(190, 170, 110),
            n if n.contains("stone-path") => Color::Rgb(100, 100, 100),
            n if n.contains("concrete") => Color::Rgb(120, 120, 120),
            n if n.contains("refined-concrete") => Color::Rgb(140, 140, 140),
            n if n.contains("landfill") => Color::Rgb(90, 85, 70),
            n if n.contains("unexplored") => Color::Rgb(50, 50, 70), // Visible fog of war
            n if n.contains("out-of-map") || n.contains("empty-space") => Color::Rgb(10, 10, 15),
            _ => Color::Rgb(60, 60, 60),
        }
    }
}
