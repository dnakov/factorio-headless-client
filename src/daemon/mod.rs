pub mod protocol;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot};

use crate::protocol::{Connection, ConnectionState};
use crate::bot::TilePathfinder;
use crate::codec::{Direction, MapPosition};
use crate::lua::prototype::Prototypes;

pub use protocol::{Request, Response, CommandResult};

pub fn socket_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".factorio-bot")
        .join("daemon.sock")
}

pub fn pid_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".factorio-bot")
        .join("daemon.pid")
}

pub struct Daemon {
    pub connection: Connection,
}

impl Daemon {
    pub async fn connect(host: &str, port: u16, username: &str) -> crate::error::Result<Self> {
        let addr: SocketAddr = format!("{}:{}", host, port).parse()
            .map_err(|e| crate::error::Error::Io(format!("Invalid address: {}", e)))?;

        let mut connection = Connection::new(addr, username.to_string()).await?;
        connection.connect().await?;
        connection.download_map().await?;

        Ok(Self { connection })
    }

    pub fn player_id(&self) -> Option<u16> {
        self.connection.player_index()
    }
}

enum DaemonCommand {
    Execute(Request, oneshot::Sender<Response>),
}

pub async fn run_daemon(daemon: Daemon, socket_path: PathBuf) -> crate::error::Result<()> {
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| crate::error::Error::Io(e.to_string()))?;
    }

    let _ = std::fs::remove_file(&socket_path);

    let listener = UnixListener::bind(&socket_path)
        .map_err(|e| crate::error::Error::Io(format!("Failed to bind socket: {}", e)))?;

    let (cmd_tx, mut cmd_rx) = mpsc::channel::<DaemonCommand>(100);

    // Spawn connection acceptor
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let cmd_tx = cmd_tx.clone();
                    tokio::spawn(handle_client(stream, cmd_tx));
                }
                Err(e) => eprintln!("Accept error: {}", e),
            }
        }
    });

    // Main loop - exactly like play-game.rs
    let mut connection = daemon.connection;
    let mut path_follower = PathFollower::new();

    loop {
        // Poll 5 times like play-game does
        for _ in 0..5 {
            let _ = connection.poll().await;
        }

        // Check for commands (non-blocking)
        while let Ok(cmd) = cmd_rx.try_recv() {
            match cmd {
                DaemonCommand::Execute(request, response_tx) => {
                    let response = handle_command(&mut connection, &mut path_follower, request).await;
                    let _ = response_tx.send(response);
                }
            }
        }

        if let Err(e) = path_follower.tick(&mut connection).await {
            eprintln!("[daemon] path follower error: {}", e);
            path_follower.clear();
        }

        // Sleep 8ms like play-game
        tokio::time::sleep(Duration::from_millis(8)).await;
    }
}

async fn handle_client(
    stream: UnixStream,
    cmd_tx: mpsc::Sender<DaemonCommand>,
) {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    while reader.read_line(&mut line).await.is_ok() && !line.is_empty() {
        if let Ok(request) = serde_json::from_str::<Request>(&line) {
            let (response_tx, response_rx) = oneshot::channel();

            if cmd_tx.send(DaemonCommand::Execute(request, response_tx)).await.is_ok() {
                if let Ok(response) = response_rx.await {
                    let json = serde_json::to_string(&response).unwrap_or_default();
                    let _ = writer.write_all(json.as_bytes()).await;
                    let _ = writer.write_all(b"\n").await;
                }
            }
        }
        line.clear();
    }
}

async fn handle_command(conn: &mut Connection, path_follower: &mut PathFollower, request: Request) -> Response {
    let result = match request.command.as_str() {
        "status" => {
            let pos = conn.player_position();
            CommandResult::ok(serde_json::json!({
                "connected": conn.state() == ConnectionState::InGame,
                "player_id": conn.player_index(),
                "position": { "x": pos.0, "y": pos.1 },
                "tick": conn.server_tick(),
            }))
        }
        "position" => {
            let pos = conn.player_position();
            CommandResult::ok(serde_json::json!({
                "x": pos.0,
                "y": pos.1
            }))
        }
        "walk" => {
            let dir = request.args.get("direction")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as u8;
            path_follower.clear();
            match conn.send_walk(dir).await {
                Ok(_) => CommandResult::ok_empty(),
                Err(e) => CommandResult::err(e.to_string()),
            }
        }
        "stop" => {
            path_follower.clear();
            match conn.send_stop_walk().await {
                Ok(_) => CommandResult::ok_empty(),
                Err(e) => CommandResult::err(e.to_string()),
            }
        }
        "mine" => {
            let x = request.args.get("x").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let y = request.args.get("y").and_then(|v| v.as_f64()).unwrap_or(0.0);
            match conn.send_mine(x, y).await {
                Ok(_) => CommandResult::ok_empty(),
                Err(e) => CommandResult::err(e.to_string()),
            }
        }
        "stop-mining" => {
            match conn.send_stop_mine().await {
                Ok(_) => CommandResult::ok_empty(),
                Err(e) => CommandResult::err(e.to_string()),
            }
        }
        "chat" => {
            let msg = request.args.get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            match conn.send_chat(msg).await {
                Ok(_) => CommandResult::ok_empty(),
                Err(e) => CommandResult::err(e.to_string()),
            }
        }
        "craft" => {
            let recipe_id = request.args.get("recipe_id")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as u16;
            let count = request.args.get("count")
                .and_then(|v| v.as_u64())
                .unwrap_or(1) as u32;
            match conn.send_craft(recipe_id, count).await {
                Ok(_) => CommandResult::ok(serde_json::json!({
                    "queued": count,
                    "recipe_id": recipe_id
                })),
                Err(e) => CommandResult::err(e.to_string()),
            }
        }
        "move-to" => {
            let x = request.args.get("x").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let y = request.args.get("y").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let tolerance = request
                .args
                .get("tolerance")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.25);
            let max_nodes = request
                .args
                .get("max_nodes")
                .and_then(|v| v.as_u64())
                .unwrap_or(50_000) as usize;

            if Prototypes::global().is_none() {
                if let Some(path) = default_factorio_data_path() {
                    if let Err(e) = Prototypes::init_global(&path) {
                        return Response {
                            id: request.id,
                            success: false,
                            result: None,
                            error: Some(format!("Failed to load Factorio Lua data: {}", e)),
                        };
                    }
                }
            }

            conn.update_position();
            let (px, py) = conn.player_position();
            let start = MapPosition::from_tiles(px, py);
            let goal = MapPosition::from_tiles(x, y);
            let path = {
                let map = match conn.parsed_map.as_ref() {
                    Some(map) => map,
                    None => {
                        return Response {
                            id: request.id,
                            success: false,
                            result: None,
                            error: Some("Map data not available yet".into()),
                        };
                    }
                };
                let pathfinder = TilePathfinder::new(map);
                pathfinder.find_path(start, goal, max_nodes)
            };

            match path {
                Some(path) => {
                    path_follower.set_path(path, tolerance);
                    let _ = path_follower.tick(conn).await;

                    let blocking = request
                        .args
                        .get("blocking")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(true);
                    let timeout = request
                        .args
                        .get("timeout_ms")
                        .and_then(|v| v.as_u64())
                        .map(Duration::from_millis);

                    if blocking {
                        let start = std::time::Instant::now();
                        while path_follower.is_active() {
                            for _ in 0..5 {
                                let _ = conn.poll().await;
                            }
                            if let Err(e) = path_follower.tick(conn).await {
                                path_follower.clear();
                                return Response {
                                    id: request.id,
                                    success: false,
                                    result: None,
                                    error: Some(e.to_string()),
                                };
                            }
                            if let Some(limit) = timeout {
                                if start.elapsed() >= limit {
                                    path_follower.clear();
                                    return Response {
                                        id: request.id,
                                        success: false,
                                        result: None,
                                        error: Some("Move-to timed out".into()),
                                    };
                                }
                            }
                            tokio::time::sleep(Duration::from_millis(8)).await;
                        }
                    }

                    let (px, py) = conn.player_position();
                    CommandResult::ok(serde_json::json!({
                        "target": { "x": x, "y": y },
                        "tolerance": tolerance,
                        "waypoints": path_follower.path_len(),
                        "position": { "x": px, "y": py },
                        "blocking": blocking
                    }))
                }
                None => CommandResult::err("No path found"),
            }
        }
        _ => CommandResult::err(format!("Unknown command: {}", request.command)),
    };

    Response {
        id: request.id,
        success: result.success,
        result: result.data,
        error: result.error,
    }
}

fn default_factorio_data_path() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("FACTORIO_DATA_PATH") {
        let p = PathBuf::from(path);
        if p.exists() {
            return Some(p);
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        let local = cwd.join("lua");
        if local.exists() {
            return Some(local);
        }
    }
    let mac = PathBuf::from("/Applications/factorio.app/Contents/data");
    if mac.exists() {
        return Some(mac);
    }
    None
}

struct PathFollower {
    path: Vec<MapPosition>,
    next_idx: usize,
    tolerance: f64,
    last_direction: Option<u8>,
}

impl PathFollower {
    fn new() -> Self {
        Self {
            path: Vec::new(),
            next_idx: 0,
            tolerance: 0.25,
            last_direction: None,
        }
    }

    fn clear(&mut self) {
        self.path.clear();
        self.next_idx = 0;
        self.last_direction = None;
    }

    fn set_path(&mut self, path: Vec<MapPosition>, tolerance: f64) {
        self.path = path;
        self.next_idx = 0;
        self.tolerance = tolerance;
        self.last_direction = None;
    }

    fn path_len(&self) -> usize {
        self.path.len().saturating_sub(self.next_idx)
    }

    fn is_active(&self) -> bool {
        self.next_idx < self.path.len()
    }

    async fn tick(&mut self, conn: &mut Connection) -> crate::error::Result<()> {
        if !self.is_active() || conn.state() != ConnectionState::InGame {
            return Ok(());
        }

        conn.update_position();
        let (px, py) = conn.player_position();
        let current = MapPosition::from_tiles(px, py);

        while self.next_idx < self.path.len() {
            let target = self.path[self.next_idx];
            if current.distance_to(target) <= self.tolerance {
                self.next_idx += 1;
            } else {
                break;
            }
        }

        if !self.is_active() {
            self.clear();
            return conn.send_stop_walk().await;
        }

        let target = self.path[self.next_idx];
        let dir = direction_to((px, py), target);
        let dir_u8 = dir as u8;
        if self.last_direction != Some(dir_u8) {
            conn.send_walk(dir_u8).await?;
            self.last_direction = Some(dir_u8);
        }
        Ok(())
    }
}

fn direction_to(current: (f64, f64), target: MapPosition) -> Direction {
    let (tx, ty) = target.to_tiles();
    let dx = tx - current.0;
    let dy = ty - current.1;
    let angle = dy.atan2(dx);
    let octant = ((angle + std::f64::consts::PI) / (std::f64::consts::PI / 4.0)) as i32;
    match octant.rem_euclid(8) {
        0 => Direction::West,
        1 => Direction::NorthWest,
        2 => Direction::North,
        3 => Direction::NorthEast,
        4 => Direction::East,
        5 => Direction::SouthEast,
        6 => Direction::South,
        _ => Direction::SouthWest,
    }
}
