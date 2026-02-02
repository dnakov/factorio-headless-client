use std::{collections::HashMap, io::{stdout, Write}, path::PathBuf, time::Instant};
use image::ImageEncoder;

use crossterm::{
    cursor, event::{self, Event, KeyCode, KeyEventKind},
    style::{self, Color, SetBackgroundColor, SetForegroundColor},
    terminal::{self, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand, QueueableCommand,
};

use factorio_client::{
    codec::{MapEntity, MapTile, parse_map_data},
    noise::terrain::TerrainGenerator,
    renderer::{
        atlas::TextureAtlas, camera::Camera2D, gpu::GpuState,
        sprites::{SpriteInstance, SpriteRenderer},
        tilemap::{TileInstance, TilemapRenderer, tile_color},
    },
};

const FACTORIO_DATA_PATH: &str = "/Applications/factorio.app/Contents/data";

struct GameState {
    entities: Vec<MapEntity>,
    tiles: Vec<MapTile>,
    tile_index: HashMap<(i32, i32), usize>,
    terrain_generator: Option<TerrainGenerator>,
    terrain_cache: HashMap<(i32, i32), [u8; 1024]>,
}

impl GameState {
    fn tile_name_at(&mut self, tx: i32, ty: i32) -> &'static str {
        if let Some(&idx) = self.tile_index.get(&(tx, ty)) {
            return tile_name_static(&self.tiles[idx].name);
        }
        let chunk_x = tx.div_euclid(32);
        let chunk_y = ty.div_euclid(32);
        let local_x = tx.rem_euclid(32) as usize;
        let local_y = ty.rem_euclid(32) as usize;

        let gen = match self.terrain_generator.as_ref() {
            Some(g) => g,
            None => return "out-of-map",
        };

        let chunk = self.terrain_cache.entry((chunk_x, chunk_y)).or_insert_with(|| {
            gen.compute_chunk(chunk_x, chunk_y)
        });
        let tile_idx = chunk[local_y * 32 + local_x];
        tile_name_from_gen(gen, tile_idx)
    }
}

fn tile_name_static(name: &str) -> &'static str {
    match name {
        n if n.contains("deepwater") => "deepwater",
        n if n.contains("water") => "water",
        n if n.contains("grass-1") => "grass-1",
        n if n.contains("grass-2") => "grass-2",
        n if n.contains("grass-3") => "grass-3",
        n if n.contains("grass-4") => "grass-4",
        n if n.starts_with("grass") => "grass",
        n if n.contains("dry-dirt") => "dry-dirt",
        n if n.contains("dirt-1") => "dirt-1",
        n if n.contains("dirt-2") => "dirt-2",
        n if n.contains("dirt-3") => "dirt-3",
        n if n.contains("dirt-4") => "dirt-4",
        n if n.contains("dirt-5") => "dirt-5",
        n if n.contains("dirt-6") => "dirt-6",
        n if n.contains("dirt-7") => "dirt-7",
        n if n.starts_with("dirt") => "dirt",
        n if n.contains("red-desert") => "red-desert",
        n if n.contains("sand-1") => "sand-1",
        n if n.contains("sand-2") => "sand-2",
        n if n.contains("sand-3") => "sand-3",
        n if n.starts_with("sand") => "sand",
        n if n.contains("stone-path") => "stone-path",
        n if n.contains("refined-concrete") => "refined-concrete",
        n if n.contains("concrete") => "concrete",
        n if n.contains("landfill") => "landfill",
        n if n.contains("out-of-map") || n.contains("empty-space") => "out-of-map",
        _ => "unknown",
    }
}

fn tile_name_from_gen(gen: &TerrainGenerator, idx: u8) -> &'static str {
    let name = gen.tile_name(idx);
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
        n if n.contains("refined-concrete") => "refined-concrete",
        n if n.contains("concrete") => "concrete",
        n if n.contains("stone-path") => "stone-path",
        "out-of-map" => "out-of-map",
        _ => "unknown",
    }
}

#[derive(Clone, Copy, PartialEq)]
enum DisplayMode {
    Kitty,
    Iterm2,
    HalfBlock,
}

fn detect_display_mode() -> DisplayMode {
    if let Ok(prog) = std::env::var("TERM_PROGRAM") {
        let p = prog.to_lowercase();
        if p.contains("iterm") { return DisplayMode::Iterm2; }
        if p.contains("kitty") || p.contains("wezterm") { return DisplayMode::Kitty; }
    }
    if let Ok(term) = std::env::var("TERM") {
        if term.contains("kitty") { return DisplayMode::Kitty; }
    }
    if std::env::var("KITTY_WINDOW_ID").is_ok() { return DisplayMode::Kitty; }
    if std::env::var("ITERM_SESSION_ID").is_ok() { return DisplayMode::Iterm2; }
    DisplayMode::HalfBlock
}

fn terminal_pixel_size() -> Option<(u32, u32)> {
    #[cfg(unix)]
    {
        let mut ws: libc::winsize = unsafe { std::mem::zeroed() };
        let ret = unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut ws) };
        if ret == 0 && ws.ws_xpixel > 0 && ws.ws_ypixel > 0 {
            return Some((ws.ws_xpixel as u32, ws.ws_ypixel as u32));
        }
    }
    None
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let map_file = args.iter().position(|a| a == "--map")
        .and_then(|i| args.get(i + 1).map(PathBuf::from));
    let seed_override = args.iter().position(|a| a == "--seed")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse::<u32>().ok());

    let force_mode = if args.iter().any(|a| a == "--halfblock") {
        Some(DisplayMode::HalfBlock)
    } else if args.iter().any(|a| a == "--kitty") {
        Some(DisplayMode::Kitty)
    } else if args.iter().any(|a| a == "--iterm2") {
        Some(DisplayMode::Iterm2)
    } else {
        None
    };

    let mut game = GameState {
        entities: Vec::new(),
        tiles: Vec::new(),
        tile_index: HashMap::new(),
        terrain_generator: None,
        terrain_cache: HashMap::new(),
    };

    if let Some(ref path) = map_file {
        eprintln!("Loading map: {}", path.display());
        let data = std::fs::read(path).expect("Failed to read map file");
        let map_data = parse_map_data(&data).expect("Failed to parse map");
        eprintln!("Loaded {} entities, {} tiles", map_data.entities.len(), map_data.tiles.len());
        game.tile_index = build_tile_index(&map_data.tiles);
        game.entities = map_data.entities;
        game.tiles = map_data.tiles;
        let seed = seed_override.unwrap_or(map_data.seed);
        game.terrain_generator = TerrainGenerator::new(seed).ok();
    } else if let Some(seed) = seed_override {
        eprintln!("Procedural terrain with seed: {seed}");
        game.terrain_generator = TerrainGenerator::new(seed).ok();
    } else {
        eprintln!("Usage: factorio-gpu --map <file.zip> | --seed <N>");
        std::process::exit(1);
    }

    let mode = force_mode.unwrap_or_else(detect_display_mode);
    eprintln!("Display: {}", match mode {
        DisplayMode::Kitty => "kitty graphics",
        DisplayMode::Iterm2 => "iterm2 inline images",
        DisplayMode::HalfBlock => "half-block (use --kitty or --iterm2 for better quality)",
    });

    run_terminal(game, mode).unwrap();
}

fn run_terminal(mut game: GameState, mode: DisplayMode) -> Result<(), Box<dyn std::error::Error>> {
    let (term_w, term_h) = terminal::size()?;

    // Determine render resolution
    let (mut render_w, mut render_h) = compute_render_size(mode, term_w as u32, term_h as u32);

    let mut gpu = GpuState::new(render_w, render_h);
    let factorio_path = std::path::Path::new(FACTORIO_DATA_PATH);
    let atlas = TextureAtlas::new(&gpu.device, &gpu.queue, factorio_path);
    let mut tilemap = TilemapRenderer::new(&gpu.device, gpu.format, &gpu.camera_bind_group_layout);
    let mut sprites = SpriteRenderer::new(&gpu.device, gpu.format, &gpu.camera_bind_group_layout, &atlas.bind_group_layout);

    let mut camera = Camera2D::new();
    camera.aspect = render_w as f32 / render_h as f32;

    let mut out = stdout();
    out.execute(EnterAlternateScreen)?;
    terminal::enable_raw_mode()?;
    out.execute(cursor::Hide)?;

    let mut pixels = Vec::new();
    let mut png_buf = Vec::new();
    let mut last_frame = Instant::now();
    let mut term_cols = term_w as u32;
    let mut term_rows = term_h as u32;

    loop {
        while event::poll(std::time::Duration::from_millis(0))? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press { continue; }
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => {
                        if mode == DisplayMode::Kitty {
                            write!(out, "\x1b_Ga=d;\x1b\\")?;
                        }
                        terminal::disable_raw_mode()?;
                        out.execute(cursor::Show)?;
                        out.execute(LeaveAlternateScreen)?;
                        return Ok(());
                    }
                    KeyCode::Char('w') | KeyCode::Up => camera.pan(0.0, -1.0),
                    KeyCode::Char('s') | KeyCode::Down => camera.pan(0.0, 1.0),
                    KeyCode::Char('a') | KeyCode::Left => camera.pan(-1.0, 0.0),
                    KeyCode::Char('d') | KeyCode::Right => camera.pan(1.0, 0.0),
                    KeyCode::Char('+') | KeyCode::Char('=') => camera.zoom_by(1.0 / 1.3),
                    KeyCode::Char('-') | KeyCode::Char('_') => camera.zoom_by(1.3),
                    _ => {}
                }
            }
        }

        // Check for terminal resize
        let (tw, th) = terminal::size()?;
        if tw as u32 != term_cols || th as u32 != term_rows {
            term_cols = tw as u32;
            term_rows = th as u32;
            let (new_rw, new_rh) = compute_render_size(mode, term_cols, term_rows);
            render_w = new_rw;
            render_h = new_rh;
            gpu.resize(render_w, render_h);
            camera.aspect = render_w as f32 / render_h as f32;
        }

        camera.update(0.15);
        gpu.upload_camera(&camera.view_proj());

        // Build tile instances
        let (min_x, min_y, max_x, max_y) = camera.visible_bounds();
        let tx0 = min_x.floor() as i32 - 1;
        let ty0 = min_y.floor() as i32 - 1;
        let tx1 = max_x.ceil() as i32 + 1;
        let ty1 = max_y.ceil() as i32 + 1;

        let mut tile_instances = Vec::with_capacity(((tx1 - tx0) * (ty1 - ty0)) as usize);
        for ty in ty0..ty1 {
            for tx in tx0..tx1 {
                let name = game.tile_name_at(tx, ty);
                tile_instances.push(TileInstance {
                    pos: [tx as f32, ty as f32],
                    color: tile_color(name),
                });
            }
        }
        tilemap.upload(&gpu.device, &gpu.queue, &tile_instances);

        // Build sprite instances
        let mut sprite_instances = Vec::new();
        for ent in &game.entities {
            if ent.x < min_x - 2.0 || ent.x > max_x + 2.0 || ent.y < min_y - 2.0 || ent.y > max_y + 2.0 {
                continue;
            }
            let uv = match atlas.get_uv_or_fallback(&ent.name) {
                Some(uv) => uv,
                None => continue,
            };
            let w = ent.tile_width().max(1.0) as f32;
            let h = ent.tile_height().max(1.0) as f32;
            sprite_instances.push(SpriteInstance {
                pos: [ent.x as f32, ent.y as f32],
                size: [w, h],
                uv_min: [uv[0], uv[1]],
                uv_max: [uv[2], uv[3]],
                rotation: ent.direction as f32 * std::f32::consts::FRAC_PI_4,
                _pad: 0.0,
            });
        }
        sprites.upload(&gpu.device, &gpu.queue, &sprite_instances);

        // GPU render pass
        let mut encoder = gpu.device.create_command_encoder(&wgpu::CommandEncoderDescriptor { label: None });
        {
            let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: None,
                color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                    view: gpu.render_view(),
                    resolve_target: None,
                    ops: wgpu::Operations {
                        load: wgpu::LoadOp::Clear(wgpu::Color { r: 0.02, g: 0.02, b: 0.04, a: 1.0 }),
                        store: wgpu::StoreOp::Store,
                    },
                })],
                depth_stencil_attachment: None,
                ..Default::default()
            });
            tilemap.draw(&mut pass, &gpu.camera_bind_group);
            sprites.draw(&mut pass, &gpu.camera_bind_group, &atlas.bind_group);
        }
        gpu.queue.submit(std::iter::once(encoder.finish()));

        // Readback and display
        gpu.readback(&mut pixels);
        match mode {
            DisplayMode::Kitty => {
                let spec = KittyDrawSpec {
                    width: render_w,
                    height: render_h,
                    cols: term_cols,
                    rows: term_rows,
                };
                draw_kitty(&mut out, &mut png_buf, &pixels, spec)?;
            }
            DisplayMode::Iterm2 => {
                draw_iterm2(&mut out, &pixels, render_w, render_h, &mut png_buf)?;
            }
            DisplayMode::HalfBlock => {
                const SSAA: u32 = 8;
                draw_halfblocks_ssaa(&mut out, &pixels, render_w, term_cols, term_rows, SSAA)?;
            }
        }
        out.flush()?;

        // Frame pacing ~30fps
        let elapsed = last_frame.elapsed();
        let target = std::time::Duration::from_millis(33);
        if elapsed < target {
            std::thread::sleep(target - elapsed);
        }
        last_frame = Instant::now();
    }
}

fn compute_render_size(mode: DisplayMode, cols: u32, rows: u32) -> (u32, u32) {
    match mode {
        DisplayMode::Kitty | DisplayMode::Iterm2 => {
            let (pw, ph) = terminal_pixel_size()
                .unwrap_or((cols * 8, rows * 16));
            let max_dim = 800u32;
            if pw > max_dim || ph > max_dim {
                let scale = max_dim as f32 / pw.max(ph) as f32;
                ((pw as f32 * scale) as u32, (ph as f32 * scale) as u32)
            } else {
                (pw, ph)
            }
        }
        DisplayMode::HalfBlock => {
            const SSAA: u32 = 8;
            (cols * SSAA, rows * 2 * SSAA)
        }
    }
}

struct KittyDrawSpec {
    width: u32,
    height: u32,
    cols: u32,
    rows: u32,
}

fn draw_kitty(
    out: &mut impl Write,
    png_buf: &mut Vec<u8>,
    pixels: &[u8],
    spec: KittyDrawSpec,
) -> std::io::Result<()> {
    png_buf.clear();
    image::codecs::png::PngEncoder::new_with_quality(
        &mut *png_buf,
        image::codecs::png::CompressionType::Fast,
        image::codecs::png::FilterType::Sub,
    ).write_image(pixels, spec.width, spec.height, image::ExtendedColorType::Rgba8)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let encoded = base64_encode(png_buf);
    let chunk_size = 4096;

    out.queue(cursor::MoveTo(0, 0))?;

    let chunks: Vec<&[u8]> = encoded.as_bytes().chunks(chunk_size).collect();
    for (i, chunk) in chunks.iter().enumerate() {
        let more = if i < chunks.len() - 1 { 1 } else { 0 };
        if i == 0 {
            write!(out, "\x1b_Ga=T,f=100,s={},v={},c={},r={},m={};",
                spec.width, spec.height, spec.cols, spec.rows, more)?;
        } else {
            write!(out, "\x1b_Gm={};", more)?;
        }
        out.write_all(chunk)?;
        write!(out, "\x1b\\")?;
    }
    Ok(())
}

fn draw_halfblocks_ssaa(
    out: &mut impl Write, pixels: &[u8],
    src_w: u32, dst_w: u32, dst_h: u32, ssaa: u32,
) -> std::io::Result<()> {
    out.queue(cursor::MoveTo(0, 0))?;
    let area = ssaa * ssaa;
    for row in 0..dst_h {
        let top_src_y = row * 2 * ssaa;
        let bot_src_y = top_src_y + ssaa;
        for col in 0..dst_w {
            let src_x = col * ssaa;

            let (mut tr, mut tg, mut tb) = (0u32, 0u32, 0u32);
            for dy in 0..ssaa {
                for dx in 0..ssaa {
                    let i = (((top_src_y + dy) * src_w + src_x + dx) * 4) as usize;
                    tr += pixels[i] as u32;
                    tg += pixels[i + 1] as u32;
                    tb += pixels[i + 2] as u32;
                }
            }

            let (mut br, mut bg, mut bb) = (0u32, 0u32, 0u32);
            for dy in 0..ssaa {
                for dx in 0..ssaa {
                    let i = (((bot_src_y + dy) * src_w + src_x + dx) * 4) as usize;
                    br += pixels[i] as u32;
                    bg += pixels[i + 1] as u32;
                    bb += pixels[i + 2] as u32;
                }
            }

            out.queue(SetForegroundColor(Color::Rgb {
                r: (tr / area) as u8, g: (tg / area) as u8, b: (tb / area) as u8,
            }))?;
            out.queue(SetBackgroundColor(Color::Rgb {
                r: (br / area) as u8, g: (bg / area) as u8, b: (bb / area) as u8,
            }))?;
            out.queue(style::Print("▀"))?;
        }
        if row < dst_h - 1 {
            out.queue(style::Print("\r\n"))?;
        }
    }
    out.queue(style::ResetColor)?;
    Ok(())
}

fn draw_iterm2(
    out: &mut impl Write, pixels: &[u8],
    width: u32, height: u32, png_buf: &mut Vec<u8>,
) -> std::io::Result<()> {
    png_buf.clear();
    image::codecs::png::PngEncoder::new(&mut *png_buf)
        .write_image(pixels, width, height, image::ExtendedColorType::Rgba8)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let encoded = base64_encode(png_buf);
    out.queue(cursor::MoveTo(0, 0))?;
    write!(out, "\x1b]1337;File=inline=1;size={};width=auto;height=auto:{}\x07",
        png_buf.len(), encoded)?;
    Ok(())
}

fn base64_encode(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((n >> 18) & 63) as usize]);
        out.push(TABLE[((n >> 12) & 63) as usize]);
        if chunk.len() > 1 {
            out.push(TABLE[((n >> 6) & 63) as usize]);
        } else {
            out.push(b'=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(n & 63) as usize]);
        } else {
            out.push(b'=');
        }
    }
    unsafe { String::from_utf8_unchecked(out) }
}

fn build_tile_index(tiles: &[MapTile]) -> HashMap<(i32, i32), usize> {
    let mut index = HashMap::with_capacity(tiles.len());
    for (i, tile) in tiles.iter().enumerate() {
        index.insert((tile.x, tile.y), i);
    }
    index
}
