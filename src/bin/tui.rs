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
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    use crossterm::{
        event::{self, Event, KeyCode, KeyEventKind},
        terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
        ExecutableCommand,
    };
    use ratatui::{
        prelude::*,
        widgets::{Block, Borders, Paragraph},
    };

    use factorio_client::codec::{MapEntity, MapTile, parse_map_data};
    use factorio_client::noise::terrain::TerrainGenerator;
    use factorio_client::protocol::{Connection, PlayerState};

    const ZOOM_LEVELS: [f64; 11] = [0.03125, 0.0625, 0.125, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0];
    const DEFAULT_ZOOM_IDX: usize = 5; // 1.0x

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
        map_size: usize,
        map_seed: u32,
        map_bounds: Option<f64>, // Half-size: map extends from -bounds to +bounds
        terrain_generator: RefCell<Option<TerrainGenerator>>,
        terrain_cache: RefCell<HashMap<(i32, i32), [u8; 1024]>>,

        // Other players
        other_players: Vec<PlayerState>,

        // UI
        log: Vec<(String, Color)>,
        status: String,
        chat_input: String,
        chat_mode: bool,
        show_help: bool,
        show_parsed_map: bool,
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
                map_size: 0,
                map_seed: 0,
                map_bounds: None,
                terrain_generator: RefCell::new(None),
                terrain_cache: RefCell::new(HashMap::new()),
                other_players: Vec::new(),
                log: vec![
                    (format!("Factorio TUI Client"), Color::Cyan),
                    (format!("Player: {}", username), Color::White),
                    ("Press Enter to connect".into(), Color::Yellow),
                ],
                status: "Ready".into(),
                chat_input: String::new(),
                chat_mode: false,
                show_help: false,
                show_parsed_map: true,
            }
        }

        fn zoom(&self) -> f64 {
            ZOOM_LEVELS[self.zoom_idx]
        }

        fn log(&mut self, msg: impl Into<String>, color: Color) {
            self.log.push((msg.into(), color));
            if self.log.len() > 100 {
                self.log.remove(0);
            }
        }

        fn cursor_world_pos(&self) -> (f64, f64) {
            let z = self.zoom();
            (self.player_x + self.cursor_dx as f64 / z,
             self.player_y + self.cursor_dy as f64 / z)
        }

        fn entity_at_cursor(&self) -> Option<&MapEntity> {
            let (cx, cy) = self.cursor_world_pos();
            let radius = 0.5 / self.zoom().max(1.0);
            self.entities.iter().find(|e| {
                (e.x - cx).abs() < radius && (e.y - cy).abs() < radius
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
                    *gen = TerrainGenerator::new(self.map_seed).ok();
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

        enable_raw_mode()?;
        stdout().execute(EnterAlternateScreen)?;
        let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;

        let mut app = App::new(server_addr, username);

        // Load offline map if specified
        if let Some(ref path) = offline_map {
            match load_offline_map(path) {
                Ok((entities, tiles, map_size, seed)) => {
                    app.entities = entities;
                    app.tiles = tiles;
                    app.tile_index = build_tile_index(&app.tiles);
                    app.map_size = map_size;
                    app.map_seed = seed_override.unwrap_or(seed);

                    if size_arg.is_none() {
                        let max_extent = app.tiles.iter()
                            .map(|t| t.x.abs().max(t.y.abs()))
                            .max()
                            .unwrap_or(100) as f64;
                        app.map_bounds = Some(max_extent + 10.0);
                    }

                    app.state = AppState::OfflineMap;
                    app.log.clear();
                    app.log(format!("Loaded map: {} entities, {} tiles", app.entities.len(), app.tiles.len()), Color::Green);
                    if seed_override.is_some() {
                        app.log(format!("Seed override: {}", app.map_seed), Color::Yellow);
                    } else {
                        app.log(format!("Map seed: {}", app.map_seed), Color::Gray);
                    }
                    if let Some(bounds) = app.map_bounds {
                        app.log(format!("Map bounds: ±{:.0} tiles", bounds), Color::Gray);
                    }
                    app.log("Use WASD to pan, +/- to zoom, P to toggle parsed map", Color::Yellow);
                    app.status = "Offline Map Viewer".into();
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
            terminal.draw(|frame| render(frame, &app))?;

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
        let map_data = parse_map_data(&data)?;
        Ok((map_data.entities, map_data.tiles, map_size, map_data.seed))
    }

    fn build_tile_index(tiles: &[MapTile]) -> HashMap<(i32, i32), usize> {
        let mut index = HashMap::with_capacity(tiles.len());
        for (idx, tile) in tiles.iter().enumerate() {
            index.insert((tile.x, tile.y), idx);
        }
        index
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
                if let Some(ref mut conn) = app.connection {
                    match rt.block_on(conn.download_map()) {
                        Ok(size) => {
                            app.map_size = size;
                            app.entities = conn.entities().to_vec();
                            let (x, y) = conn.player_position();
                            app.player_x = x;
                            app.player_y = y;
                            app.log(format!("Map loaded: {} KB, {} entities",
                                size / 1024, app.entities.len()), Color::Green);
                            app.state = AppState::Connected;
                            app.status = "Connected".into();
                        }
                        Err(e) => {
                            app.log(format!("Map download failed: {}", e), Color::Red);
                            app.state = AppState::Error;
                        }
                    }
                }
            }

            AppState::Connected => {
                if let Some(ref mut conn) = app.connection {
                    for _ in 0..5 {
                        if let Ok(Some(_)) = rt.block_on(conn.poll()) {
                            app.packets_received += 1;
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
            app.player_x += dx as f64 * step;
            app.player_y += dy as f64 * step;
            // Clamp to map bounds if set
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
                rt.block_on(conn.send_walk(direction))
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
                        rt.block_on(conn.send_chat(&message))
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
                                if let Err(e) = rt.block_on(conn.send_stop_walk()) {
                                    stop_walk_err = Some(e.to_string());
                                }
                                if let Err(e) = rt.block_on(conn.send_stop_mine()) {
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
                            rt.block_on(conn.send_mine(x, y))
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
                            rt.block_on(conn.send_build(x, y, 0))
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
                            rt.block_on(conn.send_rotate(x, y, false))
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

    fn render(frame: &mut Frame, app: &App) {
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
        render_main(frame, app, chunks[1]);
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

    fn render_main(frame: &mut Frame, app: &App, area: Rect) {
        let main_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Min(40), Constraint::Length(28)])
            .split(area);

        // Map
        render_map(frame, app, main_chunks[0]);

        // Side panel
        render_sidebar(frame, app, main_chunks[1]);
    }

    fn render_map(frame: &mut Frame, app: &App, area: Rect) {
        // Draw border
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray))
            .title(" Map ");
        let inner_area = block.inner(area);
        frame.render_widget(block, area);

        let width = inner_area.width as i32;
        let height = inner_area.height as i32;
        let zoom = app.zoom();
        let base_x = inner_area.x;
        let base_y = inner_area.y;

        // Use 2-char cells for consistent emoji rendering
        let cell_width = 2;
        let cols = width / cell_width;

        let buf = frame.buffer_mut();

        for row in 0..height {
            for col in 0..cols {
                let screen_x = base_x + (col * cell_width) as u16;
                let screen_y = base_y + row as u16;

                // Map screen position to world position
                let offset_x = (col - cols / 2) as f64 / zoom;
                let offset_y = (row - height / 2) as f64 / zoom;
                let world_x = app.player_x + offset_x;
                let world_y = app.player_y + offset_y;

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
                    app.entities.iter().find(|e| {
                        let dx = ((e.x - world_x) * zoom).round() as i32;
                        let dy = ((e.y - world_y) * zoom).round() as i32;
                        dx == 0 && dy == 0
                    })
                } else {
                    None
                };

                let tile_x = world_x.floor() as i32;
                let tile_y = world_y.floor() as i32;

                // Check if position is within map bounds
                let in_bounds = app.map_bounds.map_or(true, |b| {
                    world_x.abs() <= b && world_y.abs() <= b
                });

                // Get parsed tile (only used when show_parsed_map is on)
                let parsed_tile = if app.show_parsed_map && in_bounds {
                    app.tile_index
                        .get(&(tile_x, tile_y))
                        .and_then(|idx| app.tiles.get(*idx))
                        .filter(|t| !t.procedural) // Only non-procedural (parsed) tiles
                } else {
                    None
                };

                // Compute procedural terrain as base layer (only if in bounds)
                let procedural_tile_name = if in_bounds {
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
                } else if !in_bounds {
                    // Out of bounds - show void
                    ("  ", Style::default().bg(Color::Black))
                } else if let Some(ent) = entity {
                    // Show entity (only when show_parsed_map is on)
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
    }

    fn render_sidebar(frame: &mut Frame, app: &App, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(5),  // Cursor info
                Constraint::Min(5),     // Log
            ])
            .split(area);

        // Cursor info
        let (cx, cy) = app.cursor_world_pos();

        let cursor_info = vec![
            Line::from(vec![
                Span::raw("World: "),
                Span::styled(format!("({:.1}, {:.1})", cx, cy), Style::default().fg(Color::Cyan)),
            ]),
            Line::from(vec![
                Span::raw("Offset: "),
                Span::styled(format!("({}, {})", app.cursor_dx, app.cursor_dy), Style::default().fg(Color::Yellow)),
            ]),
        ];

        let cursor_block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(if app.cursor_mode { Color::Magenta } else { Color::DarkGray }))
            .title(" Cursor ");
        frame.render_widget(Paragraph::new(cursor_info).block(cursor_block), chunks[0]);

        // Log
        let log_height = chunks[1].height.saturating_sub(2) as usize;
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
        frame.render_widget(Paragraph::new(log_lines).block(log_block), chunks[1]);
    }

    fn render_footer(frame: &mut Frame, app: &App, area: Rect) {
        let text = if app.chat_mode {
            format!(" Chat: {}_ ", app.chat_input)
        } else if app.mining {
            " MINING... [M] to stop ".into()
        } else {
            " WASD:move  IJKL:cursor  +/-:zoom  M:mine  B:build  R:rotate  P:toggle-map  C:chat  H:help  Q:quit ".into()
        };

        let style = if app.chat_mode {
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
