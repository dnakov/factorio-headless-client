pub mod protocol;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use std::collections::VecDeque;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot};

use crate::protocol::{Connection, ConnectionState};
use crate::bot::TilePathfinder;
use crate::codec::{
    ClientItemStackLocation, Direction, ItemStackTransferSpecification, LogisticFilter,
    MapPosition, RelativeItemStackLocation, SignalId, parse_map_data_with_progress,
    ParseProgress, ParseStage, map_transfer::MapData,
};
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
        connection.download_map_with_parse(false).await?;

        Ok(Self { connection })
    }

    pub fn player_id(&self) -> Option<u16> {
        self.connection.player_index()
    }
}

enum DaemonCommand {
    Execute(Request, oneshot::Sender<Response>),
}

struct MapParseMessage {
    map: Option<MapData>,
    error: Option<String>,
    cached: bool,
    duration: Duration,
}

struct DaemonState {
    map_parse_started_at: Option<Instant>,
    map_parse_last: Option<Duration>,
    map_parse_cached: Option<bool>,
    map_parse_error: Option<String>,
    map_parse_progress: Option<Arc<ParseProgress>>,
    map_parse_last_report: Option<Instant>,
    map_parse_last_done: usize,
}

impl DaemonState {
    fn new() -> Self {
        Self {
            map_parse_started_at: None,
            map_parse_last: None,
            map_parse_cached: None,
            map_parse_error: None,
            map_parse_progress: None,
            map_parse_last_report: None,
            map_parse_last_done: 0,
        }
    }
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
    let mut daemon_state = DaemonState::new();
    let (map_parse_tx, mut map_parse_rx) = mpsc::unbounded_channel::<MapParseMessage>();
    if connection.parsed_map.is_none() && !connection.map_data().is_empty() {
        daemon_state.map_parse_started_at = Some(Instant::now());
        daemon_state.map_parse_last = None;
        daemon_state.map_parse_cached = None;
        daemon_state.map_parse_error = None;
        daemon_state.map_parse_last_report = None;
        daemon_state.map_parse_last_done = 0;
        let progress = Arc::new(ParseProgress::new());
        daemon_state.map_parse_progress = Some(progress.clone());
        let map_blob = connection.map_data().to_vec();
        eprintln!("[daemon] parsing map in background ({} bytes)", map_blob.len());
        tokio::task::spawn_blocking(move || {
            let start = Instant::now();
            if let Some(map) = load_cached_map(&map_blob) {
                eprintln!("[daemon] map cache hit in {:?}", start.elapsed());
                let _ = map_parse_tx.send(MapParseMessage {
                    map: Some(map),
                    error: None,
                    cached: true,
                    duration: start.elapsed(),
                });
                return;
            }
            let parsed = parse_map_data_with_progress(&map_blob, Some(progress));
            let mut msg = MapParseMessage {
                map: None,
                error: None,
                cached: false,
                duration: start.elapsed(),
            };
            match parsed {
                Ok(map) => {
                    store_cached_map(&map_blob, &map);
                    msg.map = Some(map);
                }
                Err(e) => {
                    msg.error = Some(e.to_string());
                }
            }
            eprintln!("[daemon] map parse finished in {:?}", start.elapsed());
            let _ = map_parse_tx.send(msg);
        });
    }
    let mut path_follower = PathFollower::new();
    let mut action_tracker = ActionTracker::new();
    let mut last_state = connection.state();

    loop {
        // Poll 5 times like play-game does
        for _ in 0..5 {
            let _ = connection.poll().await;
        }

        let state = connection.state();
        if state != last_state {
            if state == ConnectionState::Disconnected {
                let reason = connection
                    .last_disconnect_reason()
                    .unwrap_or("unknown");
                eprintln!("[daemon] connection dropped: {}", reason);
                path_follower.clear();
                action_tracker.clear();
            }
            last_state = state;
        }

        // Check for map parse completion (non-blocking)
        while let Ok(msg) = map_parse_rx.try_recv() {
            daemon_state.map_parse_started_at = None;
            daemon_state.map_parse_last = Some(msg.duration);
            daemon_state.map_parse_cached = Some(msg.cached);
            daemon_state.map_parse_error = msg.error.clone();
            match msg.map {
                Some(map) => {
                    eprintln!(
                        "[daemon] map parse complete: {} entities, {} tiles",
                        map.entities.len(),
                        map.tiles.len()
                    );
                    connection.apply_parsed_map(map);
                }
                None => {
                    let e = msg.error.unwrap_or_else(|| "unknown error".to_string());
                    eprintln!("[daemon] map parse failed: {}", e);
                }
            }
        }

        // Check for commands (non-blocking)
        while let Ok(cmd) = cmd_rx.try_recv() {
            match cmd {
                DaemonCommand::Execute(request, response_tx) => {
                    let response = handle_command(
                        &mut connection,
                        &mut path_follower,
                        &mut action_tracker,
                        &daemon_state,
                        request,
                    ).await;
                    let _ = response_tx.send(response);
                }
            }
        }

        let was_active = path_follower.is_active();
        let mut path_error: Option<String> = None;
        if let Err(e) = path_follower.tick(&mut connection).await {
            let msg = e.to_string();
            if msg.contains("move_to_stuck")
                && try_replan_move(&mut connection, &mut path_follower, &mut action_tracker).await
            {
                path_error = None;
            } else {
                eprintln!("[daemon] path follower error: {}", msg);
                path_follower.clear();
                let result = if msg.contains("move_to_stuck") {
                    "stuck"
                } else {
                    "error"
                };
                action_tracker.complete_if_action("move", result);
                path_error = Some(msg);
            }
        }
        let is_active = path_follower.is_active();
        if was_active && !is_active && path_error.is_none() {
            action_tracker.complete_if_action("move", "arrived");
        }

        if let Some(progress) = daemon_state.map_parse_progress.as_ref() {
            if connection.parsed_map.is_none() {
                let entities_total = progress.entities_total();
                let resources_total = progress.resources_total();
                let tiles_total = progress.tiles_total();
                let total_total = entities_total + resources_total + tiles_total;
                if total_total > 0 {
                    let entities_done = progress.entities_done();
                    let resources_done = progress.resources_done();
                    let tiles_done = progress.tiles_done();
                    let total_done = entities_done + resources_done + tiles_done;
                    let now = Instant::now();
                    let changed = total_done != daemon_state.map_parse_last_done;
                    let min_interval = if changed {
                        Duration::from_secs(1)
                    } else {
                        Duration::from_secs(5)
                    };
                    let should_report = match daemon_state.map_parse_last_report {
                        None => true,
                        Some(last) => now.duration_since(last) >= min_interval,
                    };
                    if should_report {
                        let pct = (total_done as f64 / total_total as f64) * 100.0;
                        let mut line = format!(
                            "[daemon] map parse {:.1}% ({}/{}) stage={}",
                            pct,
                            total_done,
                            total_total,
                            progress.stage().as_str()
                        );
                        if !changed {
                            line.push_str(" (still parsing)");
                        }
                        if progress.stage() == ParseStage::Resources {
                            let cur = progress.resources_current();
                            let len = progress.resources_current_len();
                            if len > 0 {
                                line.push_str(&format!(" resources_chunk={} len={}", cur, len));
                            }
                        }
                        eprintln!("{}", line);
                        daemon_state.map_parse_last_report = Some(now);
                        if changed {
                            daemon_state.map_parse_last_done = total_done;
                        }
                    }
                }
            }
        }

        // Sleep 8ms like play-game
        tokio::time::sleep(Duration::from_millis(8)).await;
    }
}

async fn handle_client(
    stream: UnixStream,
    cmd_tx: mpsc::Sender<DaemonCommand>,
) {
    eprintln!("[socket] client connected");
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    while reader.read_line(&mut line).await.is_ok() && !line.is_empty() {
        let trimmed = line.trim();
        eprintln!("[socket] received line: {:?}", trimmed);
        if let Ok(request) = serde_json::from_str::<Request>(trimmed) {
            eprintln!("[socket] parsed request: {:?}", request.command);
            let (response_tx, response_rx) = oneshot::channel();

            if cmd_tx.send(DaemonCommand::Execute(request, response_tx)).await.is_ok() {
                eprintln!("[socket] sent to channel, waiting for response...");
                if let Ok(response) = response_rx.await {
                    let json = serde_json::to_string(&response).unwrap_or_default();
                    eprintln!("[socket] sending response: {} bytes", json.len());
                    let _ = writer.write_all(json.as_bytes()).await;
                    let _ = writer.write_all(b"\n").await;
                } else {
                    eprintln!("[socket] response_rx failed");
                }
            } else {
                eprintln!("[socket] cmd_tx.send failed");
            }
        } else {
            eprintln!("[socket] JSON parse failed for: {:?}", trimmed);
        }
        line.clear();
    }
    eprintln!("[socket] client disconnected");
}

async fn handle_command(
    conn: &mut Connection,
    path_follower: &mut PathFollower,
    action_tracker: &mut ActionTracker,
    daemon_state: &DaemonState,
    request: Request,
) -> Response {
    let result = match request.command.as_str() {
        "status" => cmd_status(conn, daemon_state),
        "position" => cmd_position(conn),
        "walk" => cmd_walk(conn, path_follower, action_tracker, &request.args).await,
        "stop" => cmd_stop(conn, path_follower, action_tracker).await,
        "mine" => cmd_mine(conn, &request.args).await,
        "stop-mining" => cmd_stop_mine(conn).await,
        "chat" => cmd_chat(conn, &request.args).await,
        "craft" => cmd_craft(conn, &request.args).await,
        "research" => cmd_research(conn, &request.args).await,
        "cancel-craft" => cmd_cancel_craft(conn, &request.args).await,
        "select-entity" => cmd_select_entity(conn, &request.args).await,
        "clear-selection" => cmd_clear_selection(conn).await,
        "clear-cursor" => cmd_clear_cursor(conn).await,
        "drop-item" => cmd_drop_item(conn, &request.args).await,
        "use-item" => cmd_use_item(conn, &request.args).await,
        "set-ghost-cursor" => cmd_set_ghost_cursor(conn, &request.args).await,
        "place-ghost" => cmd_place_ghost(conn, &request.args).await,
        "inspect" => cmd_inspect(conn, &request.args).await,
        "scan-area" => cmd_scan_area(conn, &request.args),
        "find-nearest" => cmd_find_nearest(conn, &request.args),
        "state" => cmd_state(conn, &request.args),
        "recipes" => cmd_recipes(conn),
        "techs" => cmd_techs(conn),
        "alerts" => cmd_alerts(conn, &request.args),
        "spawn" => cmd_spawn(conn).await,
        "get-train" => cmd_get_train(conn, &request.args),
        "set-train-stop" => cmd_set_train_stop(conn, &request.args).await,
        "set-train-schedule" => cmd_set_train_schedule(conn, &request.args).await,
        "train-go" => cmd_train_go(conn, &request.args).await,
        "set-combinator" => cmd_set_combinator(conn, &request.args).await,
        "get-signals" => cmd_get_signals(conn, &request.args),
        "power-status" => cmd_power_status(conn, &request.args),
        "logistics-status" => cmd_logistics_status(conn, &request.args),
        "pollution" => cmd_pollution(conn, &request.args),
        "build" => cmd_build(conn, &request.args).await,
        "place" => cmd_place(conn, &request.args).await,
        "build-blueprint" => cmd_build_blueprint(conn, &request.args).await,
        "rotate" => cmd_rotate(conn, &request.args).await,
        "set-logistics" => cmd_set_logistics(conn, &request.args).await,
        "cursor-transfer" => cmd_cursor_transfer(conn, &request.args, false).await,
        "cursor-split" => cmd_cursor_transfer(conn, &request.args, true).await,
        "stack-transfer" => cmd_stack_transfer(conn, &request.args, false, false).await,
        "inventory-transfer" => cmd_stack_transfer(conn, &request.args, true, false).await,
        "stack-split" => cmd_stack_transfer(conn, &request.args, false, true).await,
        "inventory-split" => cmd_stack_transfer(conn, &request.args, true, true).await,
        "filter" => cmd_filter(conn, &request.args).await,
        "clear-filter" => cmd_clear_filter(conn, &request.args).await,
        "fast-transfer" => cmd_fast_transfer(conn, &request.args, false).await,
        "fast-split" => cmd_fast_transfer(conn, &request.args, true).await,
        "insert" => cmd_insert(conn, &request.args).await,
        "extract" => cmd_extract(conn, &request.args).await,
        "equip" => cmd_equip(conn, &request.args).await,
        "drop" => cmd_drop(conn, &request.args).await,
        "set-recipe" => cmd_set_recipe(conn, &request.args).await,
        "server-command" => cmd_server_command(conn, &request.args).await,
        "eval" => cmd_eval(conn, &request.args).await,
        "reload" => cmd_reload(conn).await,
        "test-reload" => cmd_test_reload(conn).await,
        "pickup" => cmd_pickup(conn, &request.args).await,
        "shoot" => cmd_shoot(conn, &request.args).await,
        "stop-shoot" => cmd_stop_shoot(conn).await,
        "toggle-driving" => cmd_toggle_driving(conn).await,
        "drive" => cmd_drive(conn, &request.args).await,
        "enter-vehicle" => cmd_enter_vehicle(conn, &request.args).await,
        "exit-vehicle" => cmd_exit_vehicle(conn).await,
        "connect-wire" => cmd_connect_wire(conn, &request.args).await,
        "disconnect-wire" => cmd_disconnect_wire(conn, &request.args).await,
        "deconstruct" => cmd_deconstruct(conn, &request.args).await,
        "cancel-deconstruct" => cmd_cancel_deconstruct(conn, &request.args).await,
        "copy-settings" => cmd_copy_settings(conn, &request.args).await,
        "paste-settings" => cmd_paste_settings(conn, &request.args).await,
        "remove-cables" => cmd_remove_cables(conn, &request.args).await,
        "launch-rocket" => cmd_launch_rocket(conn, &request.args).await,
        "move-to" => cmd_move_to(conn, path_follower, action_tracker, &request.args).await,
        "find-path" => cmd_find_path(conn, &request.args).await,
        "action-status" => cmd_action_status(conn, action_tracker, &request.args),
        _ => CommandResult::err(format!("Unknown command: {}", request.command)),
    };

    Response {
        id: request.id,
        success: result.success,
        result: result.data,
        error: result.error,
    }
}

fn cmd_status(conn: &Connection, daemon_state: &DaemonState) -> CommandResult {
    let pos = conn.player_position();
    let map_ready = conn.parsed_map.is_some();
    let map_parsing = daemon_state.map_parse_started_at.is_some();
    let map_parse_ms = daemon_state.map_parse_last.map(|d| d.as_millis() as u64);
    let map_parse_elapsed_ms = daemon_state
        .map_parse_started_at
        .map(|t| t.elapsed().as_millis() as u64);
    let (map_parse_stage, map_parse_progress, map_parse_counts) = if let Some(progress) = daemon_state.map_parse_progress.as_ref() {
        let mut entities_done = progress.entities_done();
        let entities_total = progress.entities_total();
        let mut resources_done = progress.resources_done();
        let resources_total = progress.resources_total();
        let resources_current = progress.resources_current();
        let resources_current_len = progress.resources_current_len();
        let mut tiles_done = progress.tiles_done();
        let tiles_total = progress.tiles_total();
        if map_ready {
            if entities_total > 0 {
                entities_done = entities_total;
            }
            if resources_total > 0 {
                resources_done = resources_total;
            }
            if tiles_total > 0 {
                tiles_done = tiles_total;
            }
        }
        let stage = if !map_ready {
            if entities_total > 0 && entities_done < entities_total {
                ParseStage::Entities
            } else if resources_total > 0 && resources_done < resources_total {
                ParseStage::Resources
            } else if tiles_total > 0 && tiles_done < tiles_total {
                ParseStage::Tiles
            } else {
                progress.stage()
            }
        } else {
            ParseStage::Done
        };
        let total_done = entities_done + resources_done + tiles_done;
        let total_total = entities_total + resources_total + tiles_total;
        let pct = if total_total > 0 {
            Some(total_done as f64 / total_total as f64)
        } else {
            None
        };
        let counts = serde_json::json!({
            "entities": { "done": entities_done, "total": entities_total },
            "resources": { "done": resources_done, "total": resources_total, "current": resources_current, "current_len": resources_current_len },
            "tiles": { "done": tiles_done, "total": tiles_total },
            "done": total_done,
            "total": total_total,
        });
        (Some(stage.as_str()), pct, Some(counts))
    } else {
        (None, None, None)
    };
    let connection_state = format!("{:?}", conn.state());
    CommandResult::ok(serde_json::json!({
        "connected": conn.state() == ConnectionState::InGame,
        "connection_state": connection_state,
        "last_disconnect_reason": conn.last_disconnect_reason(),
        "last_server_heartbeat_ms": conn.last_server_heartbeat_age_ms(),
        "player_id": conn.player_index(),
        "position": { "x": pos.0, "y": pos.1 },
        "tick": conn.server_tick(),
        "tick_state": {
            "start_sending_tick": conn.start_sending_tick(),
            "confirmed_tick": conn.confirmed_tick(),
            "server_tick": conn.server_tick(),
            "server_seq": conn.server_seq(),
            "client_tick": conn.client_tick(),
            "client_seq": conn.client_seq(),
            "client_seq_base": conn.client_seq_base(),
            "peer_constant": conn.peer_constant(),
            "latency": conn.latency_value(),
        },
        "map_ready": map_ready,
        "map_parsing": map_parsing,
        "map_parse_ms": map_parse_ms,
        "map_parse_elapsed_ms": map_parse_elapsed_ms,
        "map_parse_cached": daemon_state.map_parse_cached,
        "map_parse_error": daemon_state.map_parse_error,
        "map_parse_stage": map_parse_stage,
        "map_parse_progress": map_parse_progress,
        "map_parse_counts": map_parse_counts,
    }))
}

fn cmd_action_status(
    conn: &Connection,
    action_tracker: &mut ActionTracker,
    args: &serde_json::Value,
) -> CommandResult {
    let tick = conn.server_tick();
    let filter_id = arg_u64_opt(args, "id");
    let current = action_tracker.current.as_ref();
    let completed_entry = filter_id.and_then(|id| {
        action_tracker
            .completed
            .iter()
            .rev()
            .find(|entry| entry.id == id)
    });
    let (payload, clear_current) = match (filter_id, current, completed_entry) {
        (Some(id), Some(cur), _) if cur.id == id => (
            serde_json::json!({
                "id": cur.id,
                "action": cur.action.clone(),
                "started_at": cur.started_tick,
                "details": cur.details.clone(),
                "completed": cur.completed,
                "result": cur.result.clone(),
                "duration": tick.saturating_sub(cur.started_tick)
            }),
            cur.completed,
        ),
        (Some(_), _, Some(done)) => (
            serde_json::json!({
                "id": done.id,
                "action": done.action.clone(),
                "started_at": done.started_tick,
                "details": done.details.clone(),
                "completed": true,
                "result": done.result.clone(),
                "duration": tick.saturating_sub(done.started_tick)
            }),
            false,
        ),
        (Some(id), _, _) => (
            serde_json::json!({
                "id": id,
                "action": serde_json::Value::Null,
                "completed": true
            }),
            false,
        ),
        (None, None, _) => (
            serde_json::json!({
                "action": serde_json::Value::Null,
                "completed": true
            }),
            false,
        ),
        (None, Some(cur), _) => (
            serde_json::json!({
                "id": cur.id,
                "action": cur.action.clone(),
                "started_at": cur.started_tick,
                "details": cur.details.clone(),
                "completed": cur.completed,
                "result": cur.result.clone(),
                "duration": tick.saturating_sub(cur.started_tick)
            }),
            cur.completed,
        ),
    };

    if clear_current {
        action_tracker.clear();
    }

    CommandResult::ok(payload)
}

fn cmd_position(conn: &mut Connection) -> CommandResult {
    conn.update_position();
    let pos = conn.player_position();
    CommandResult::ok(serde_json::json!({
        "x": pos.0,
        "y": pos.1
    }))
}

async fn cmd_walk(
    conn: &mut Connection,
    path_follower: &mut PathFollower,
    action_tracker: &mut ActionTracker,
    args: &serde_json::Value,
) -> CommandResult {
    let dir = arg_u64(args, "direction", 0) as u8;
    path_follower.clear();
    action_tracker.complete_if_action("move", "interrupted");
    match conn.actions().send_walk(dir).await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_stop(
    conn: &mut Connection,
    path_follower: &mut PathFollower,
    action_tracker: &mut ActionTracker,
) -> CommandResult {
    path_follower.clear();
    action_tracker.complete_if_action("move", "stopped");
    match conn.actions().send_stop_walk().await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_mine(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    match conn.actions().send_mine(x, y).await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_stop_mine(conn: &mut Connection) -> CommandResult {
    match conn.actions().send_stop_mine().await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_chat(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let msg = arg_str(args, "message").unwrap_or("");
    match conn.actions().send_chat(msg).await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_craft(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let count = arg_u64(args, "count", 1) as u32;
    let Some(recipe_id) = lookup_recipe_id(conn, args) else {
        return CommandResult::err("Missing or unknown recipe (use recipe_id or recipe name)");
    };
    match conn.actions().send_craft(recipe_id, count).await {
        Ok(_) => CommandResult::ok(serde_json::json!({
            "queued": count,
            "recipe_id": recipe_id
        })),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_research(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let Some(tech_id) = lookup_tech_id(conn, args) else {
        return CommandResult::err("Missing or unknown technology (use technology_id or name)");
    };
    match conn.actions().send_start_research(tech_id).await {
        Ok(_) => CommandResult::ok(serde_json::json!({
            "technology_id": tech_id
        })),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_cancel_craft(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let index = arg_u64(args, "index", 1) as u16;
    let count = arg_u64(args, "count", 1) as u32;
    match conn.actions().send_cancel_craft(index, count).await {
        Ok(_) => CommandResult::ok(serde_json::json!({
            "index": index,
            "count": count
        })),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_select_entity(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    match conn.actions().send_selected_entity_changed(x, y).await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_clear_selection(conn: &mut Connection) -> CommandResult {
    match conn.actions().send_selected_entity_cleared().await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_clear_cursor(conn: &mut Connection) -> CommandResult {
    match conn.actions().send_clear_cursor().await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_drop_item(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    match conn.actions().send_drop_item(x, y).await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_use_item(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    match conn.actions().send_use_item(x, y).await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_set_ghost_cursor(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let Some(item_id) = lookup_item_id(conn, args) else {
        return CommandResult::err("Missing or unknown item (use item_id or item name)");
    };
    let quality_id = arg_u64_opt(args, "quality_id").map(|v| v as u8);
    match conn.actions().send_set_ghost_cursor(item_id, quality_id).await {
        Ok(_) => CommandResult::ok(serde_json::json!({
            "item_id": item_id,
            "quality_id": quality_id
        })),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_place_ghost(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let direction = arg_u64(args, "direction", 0) as u8;
    let Some(item_id) = lookup_item_id(conn, args) else {
        return CommandResult::err("Missing or unknown item (use item_id or item name)");
    };
    let quality_id = arg_u64_opt(args, "quality_id").map(|v| v as u8);
    conn.actions().send_set_ghost_cursor(item_id, quality_id).await.ok();
    let result = conn.actions().send_build(x, y, direction).await;
    conn.actions().send_clear_cursor().await.ok();
    match result {
        Ok(_) => CommandResult::ok(serde_json::json!({
            "item_id": item_id,
            "position": { "x": x, "y": y },
            "direction": direction
        })),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_inspect(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let radius = arg_f64(args, "radius", 1.0);
    let _ = conn.actions().send_selected_entity_changed(x, y).await;
    if let Some(world) = conn.sim_world() {
        let mut closest = None;
        let mut best_dist = radius * radius;
        if let Some(surface) = world.nauvis() {
            for e in surface.entities.values() {
                let (ex, ey) = e.position.to_tiles();
                let dx = ex - x;
                let dy = ey - y;
                let dist = dx * dx + dy * dy;
                if dist <= best_dist {
                    best_dist = dist;
                    closest = Some(e);
                }
            }
        }
        if let Some(entity) = closest {
            let (ex, ey) = entity.position.to_tiles();
            let inventories: serde_json::Map<String, serde_json::Value> = entity
                .inventories
                .iter()
                .map(|(name, inv)| (name.clone(), serde_json::json!(inv.contents())))
                .collect();
            let mut extra = serde_json::Map::new();
            match &entity.data {
                crate::state::entity::EntityData::Resource(data) => {
                    extra.insert("amount".to_string(), serde_json::json!(data.amount));
                    extra.insert("infinite".to_string(), serde_json::json!(data.infinite));
                }
                crate::state::entity::EntityData::AssemblingMachine(data) => {
                    if let Some(recipe) = &data.recipe {
                        extra.insert("recipe".to_string(), serde_json::json!(recipe));
                    }
                }
                crate::state::entity::EntityData::Furnace(data) => {
                    if let Some(recipe) = &data.smelting_recipe {
                        extra.insert("recipe".to_string(), serde_json::json!(recipe));
                    }
                }
                _ => {}
            }
            if let Some(stack) = &entity.item_stack {
                extra.insert("item_stack".to_string(), serde_json::json!({
                    "name": stack.name,
                    "count": stack.count
                }));
            }
            CommandResult::ok(serde_json::json!({
                "name": entity.name,
                "type": format!("{:?}", entity.entity_type),
                "position": { "x": ex, "y": ey },
                "direction": entity.direction as u8,
                "inventories": inventories,
                "distance": best_dist.sqrt(),
                "data": extra
            }))
        } else {
            CommandResult::err("No entity found in radius")
        }
    } else {
        CommandResult::err("Simulation state not available yet")
    }
}

fn cmd_scan_area(conn: &Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let radius = arg_f64(args, "radius", 10.0);
    let max_entities = arg_u64(args, "max_entities", 200) as usize;
    if let Some(world) = conn.sim_world() {
        let r2 = radius * radius;
        let mut entities = Vec::new();
        let mut resource_patches: std::collections::HashMap<String, (u64, Vec<(f64, f64)>)> =
            std::collections::HashMap::new();
        if let Some(surface) = world.nauvis() {
            for e in surface.entities.values() {
                let (ex, ey) = e.position.to_tiles();
                let dx = ex - x;
                let dy = ey - y;
                if dx * dx + dy * dy <= r2 {
                    entities.push(serde_json::json!({
                        "name": e.name,
                        "x": ex,
                        "y": ey
                    }));
                    if let crate::state::entity::EntityData::Resource(data) = &e.data {
                        let entry = resource_patches
                            .entry(e.name.clone())
                            .or_insert((0u64, Vec::new()));
                        entry.0 += data.amount as u64;
                        if entry.1.len() < 5 {
                            entry.1.push((ex, ey));
                        }
                    }
                    if entities.len() >= max_entities {
                        break;
                    }
                }
            }
        }

        let mut tiles = std::collections::HashMap::<String, usize>::new();
        if let Some(surface) = world.nauvis() {
            for (chunk_pos, chunk) in &surface.chunks {
                let base_x = chunk_pos.x * 32;
                let base_y = chunk_pos.y * 32;
                for ly in 0..32 {
                    for lx in 0..32 {
                        let idx = (ly * 32 + lx) as usize;
                        let tile = &chunk.tiles[idx];
                        if tile.name.is_empty() {
                            continue;
                        }
                        let tx = (base_x + lx as i32) as f64 + 0.5;
                        let ty = (base_y + ly as i32) as f64 + 0.5;
                        let dx = tx - x;
                        let dy = ty - y;
                        if dx * dx + dy * dy <= r2 {
                            *tiles.entry(tile.name.clone()).or_insert(0) += 1;
                        }
                    }
                }
            }
        }

        let tile_list: Vec<_> = tiles
            .into_iter()
            .map(|(name, count)| serde_json::json!({"name": name, "count": count}))
            .collect();
        let resources: Vec<_> = resource_patches
            .into_iter()
            .map(|(name, (amount, positions))| {
                serde_json::json!({
                    "name": name,
                    "amount": amount,
                    "positions": positions
                        .into_iter()
                        .map(|(rx, ry)| serde_json::json!({"x": rx, "y": ry}))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        CommandResult::ok(serde_json::json!({
            "center": { "x": x, "y": y },
            "radius": radius,
            "entities": entities,
            "resources": resources,
            "tiles": tile_list,
        }))
    } else {
        CommandResult::err("Simulation state not available yet")
    }
}

const WATER_TILE_NAMES: [&str; 6] = [
    "water",
    "deepwater",
    "water-green",
    "deepwater-green",
    "water-shallow",
    "water-mud",
];

fn find_nearest_water(map: &crate::codec::map_transfer::MapData, px: f64, py: f64, max_radius: f64) -> Option<(f64, f64, f64)> {
    let r2 = max_radius * max_radius;
    let mut best = None;
    let mut best_dist = r2;
    for t in &map.tiles {
        if !WATER_TILE_NAMES.iter().any(|w| *w == t.name) {
            continue;
        }
        let tx = t.x as f64 + 0.5;
        let ty = t.y as f64 + 0.5;
        let dx = tx - px;
        let dy = ty - py;
        let dist = dx * dx + dy * dy;
        if dist <= best_dist {
            best_dist = dist;
            best = Some((tx, ty));
        }
    }
    best.map(|(x, y)| (x, y, best_dist.sqrt()))
}

fn build_water_set(
    map: &crate::codec::map_transfer::MapData,
    px: f64,
    py: f64,
    max_radius: f64,
) -> std::collections::HashSet<(i32, i32)> {
    let mut water_set = std::collections::HashSet::new();
    let r2 = max_radius * max_radius + 2.0;
    for t in &map.tiles {
        let tx = t.x as f64 + 0.5;
        let ty = t.y as f64 + 0.5;
        let dx = tx - px;
        let dy = ty - py;
        if dx * dx + dy * dy <= r2 {
            if WATER_TILE_NAMES.iter().any(|w| *w == t.name) {
                water_set.insert((t.x, t.y));
            }
        }
    }
    water_set
}

fn can_place_entity(
    map: &crate::codec::map_transfer::MapData,
    water_set: &std::collections::HashSet<(i32, i32)>,
    entity_name: &str,
    x: f64,
    y: f64,
) -> bool {
    let (col, _collides_player) = crate::codec::map_types::entity_collision_box(entity_name);
    let min_x = x + col[0];
    let min_y = y + col[1];
    let max_x = x + col[2];
    let max_y = y + col[3];
    for e in &map.entities {
        if !e.collides_player {
            continue;
        }
        let ex1 = e.x + e.col_x1;
        let ey1 = e.y + e.col_y1;
        let ex2 = e.x + e.col_x2;
        let ey2 = e.y + e.col_y2;
        let overlap = min_x < ex2 && max_x > ex1 && min_y < ey2 && max_y > ey1;
        if overlap {
            return false;
        }
    }
    let tile_x = x.floor() as i32;
    let tile_y = y.floor() as i32;
    if water_set.contains(&(tile_x, tile_y)) {
        return false;
    }
    true
}

fn find_offshore_pump_spot(
    map: &crate::codec::map_transfer::MapData,
    water_set: &std::collections::HashSet<(i32, i32)>,
    px: f64,
    py: f64,
    max_radius: f64,
) -> Option<(f64, f64, &'static str, f64)> {
    let directions = [
        ("north", 0, -1, 0, 1),
        ("south", 0, 1, 0, -1),
        ("east", 1, 0, -1, 0),
        ("west", -1, 0, 1, 0),
    ];
    let mut best = None;
    let mut best_dist = max_radius + 1.0;

    let center_x = px.round() as i32;
    let center_y = py.round() as i32;
    let max_r = max_radius.floor() as i32;
    for radius in (5..=max_r).step_by(5) {
        for x in (center_x - radius)..=(center_x + radius) {
            for y in (center_y - radius)..=(center_y + radius) {
                let fx = x as f64;
                let fy = y as f64;
                if !can_place_entity(map, water_set, "offshore-pump", fx, fy) {
                    continue;
                }
                for (name, dx, dy, idx, idy) in directions {
                    let out_tile = (x + dx, y + dy);
                    let in_tile = (x + idx, y + idy);
                    if !water_set.contains(&out_tile) && water_set.contains(&in_tile) {
                        let dxp = fx - px;
                        let dyp = fy - py;
                        let dist = (dxp * dxp + dyp * dyp).sqrt();
                        if dist < best_dist {
                            best_dist = dist;
                            best = Some((fx, fy, name));
                        }
                    }
                }
            }
        }
        if best.is_some() {
            break;
        }
    }

    best.map(|(x, y, dir)| (x, y, dir, best_dist))
}

fn find_spot_entity(
    map: &crate::codec::map_transfer::MapData,
    water_set: &std::collections::HashSet<(i32, i32)>,
    entity_name: &str,
    px: f64,
    py: f64,
    max_radius: f64,
) -> Option<(f64, f64, f64)> {
    let mut best = None;
    let mut best_dist = max_radius + 1.0;
    let center_x = px.round() as i32;
    let center_y = py.round() as i32;
    let max_r = max_radius.floor() as i32;

    for radius in 1..=max_r {
        for x in (center_x - radius)..=(center_x + radius) {
            for y in (center_y - radius)..=(center_y + radius) {
                let fx = x as f64;
                let fy = y as f64;
                if !can_place_entity(map, water_set, entity_name, fx, fy) {
                    continue;
                }
                let dxp = fx - px;
                let dyp = fy - py;
                let dist = (dxp * dxp + dyp * dyp).sqrt();
                if dist < best_dist {
                    best_dist = dist;
                    best = Some((fx, fy));
                }
            }
        }
        if best.is_some() {
            break;
        }
    }

    best.map(|(x, y)| (x, y, best_dist))
}

fn cmd_find_nearest(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let query = arg_str(args, "query").unwrap_or("");
    let max_radius = arg_f64(args, "max_radius", 100.0);
    conn.update_position();
    let (px, py) = conn.player_position();
    let Some(map) = conn.parsed_map.as_ref() else {
        if !conn.map_data().is_empty() {
            return CommandResult::err("Map is still parsing; retry in a moment or check `factorio-bot status`");
        }
        return CommandResult::err("Map data not available yet");
    };
    if query == "water" {
        if let Some((tx, ty, dist)) = find_nearest_water(map, px, py, max_radius) {
            return CommandResult::ok(serde_json::json!({
                "name": "water",
                "x": tx,
                "y": ty,
                "distance": dist
            }));
        }
        return CommandResult::err("No water found in radius");
    }

    if query == "offshore-pump-spot" || query.starts_with("find-spot-") {
        if Prototypes::global().is_none() {
            if let Some(path) = default_factorio_data_path() {
                let _ = Prototypes::init_global(&path);
            }
        }
        let water_set = build_water_set(map, px, py, max_radius);

        if query == "offshore-pump-spot" {
            if let Some((x, y, dir, dist)) =
                find_offshore_pump_spot(map, &water_set, px, py, max_radius)
            {
                return CommandResult::ok(serde_json::json!({
                    "name": "offshore-pump-spot",
                    "x": x,
                    "y": y,
                    "direction": dir,
                    "distance": dist
                }));
            }
            return CommandResult::err(format!(
                "No valid offshore-pump placement found within radius {}",
                max_radius
            ));
        }

        if let Some(entity_name) = query.strip_prefix("find-spot-") {
            if let Some((x, y, dist)) = find_spot_entity(map, &water_set, entity_name, px, py, max_radius) {
                return CommandResult::ok(serde_json::json!({
                    "name": entity_name,
                    "x": x,
                    "y": y,
                    "distance": dist
                }));
            }
            return CommandResult::err(format!(
                "No valid {} placement found within radius {}",
                entity_name, max_radius
            ));
        }
    }

    let mut best = None;
    let mut best_dist = max_radius * max_radius;
    for e in &map.entities {
        if !query.is_empty() && e.name != query {
            continue;
        }
        let dx = e.x - px;
        let dy = e.y - py;
        let dist = dx * dx + dy * dy;
        if dist <= best_dist {
            best_dist = dist;
            best = Some(e);
        }
    }
    if let Some(entity) = best {
        CommandResult::ok(serde_json::json!({
            "name": entity.name,
            "x": entity.x,
            "y": entity.y,
            "distance": best_dist.sqrt()
        }))
    } else {
        CommandResult::err("Nothing found")
    }
}

fn cmd_state(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let radius = arg_f64(args, "radius", 30.0);
    let max_entities = arg_u64(args, "max_entities", 100) as usize;
    if let Some(world) = conn.sim_world() {
        let tick = world.tick;
        let player_id = conn.player_index().unwrap_or(1);
        let player = world.players.get(&player_id);
        let (px, py) = player
            .map(|p| p.position.to_tiles())
            .unwrap_or(conn.player_position());
        let r2 = radius * radius;
        let mut entities = Vec::new();
        let mut resource_patches: std::collections::HashMap<String, (u64, Vec<(f64, f64)>)> =
            std::collections::HashMap::new();
        if let Some(surface) = world.nauvis() {
            for e in surface.entities.values() {
                let (ex, ey) = e.position.to_tiles();
                let dx = ex - px;
                let dy = ey - py;
                if dx * dx + dy * dy <= r2 {
                    entities.push(serde_json::json!({
                        "name": e.name,
                        "x": ex,
                        "y": ey
                    }));
                    if let crate::state::entity::EntityData::Resource(data) = &e.data {
                        let entry = resource_patches
                            .entry(e.name.clone())
                            .or_insert((0u64, Vec::new()));
                        entry.0 += data.amount as u64;
                        if entry.1.len() < 5 {
                            entry.1.push((ex, ey));
                        }
                    }
                    if entities.len() >= max_entities {
                        break;
                    }
                }
            }
        }
        let inventory = player
            .and_then(|p| p.main_inventory.as_ref())
            .map(|inv| inv.contents())
            .unwrap_or_default();
        let crafting_queue = player.map(|p| p.crafting_queue_size).unwrap_or(0);
        let research = world.research.current_research.clone().map(|name| {
            serde_json::json!({
                "name": name,
                "progress": world.research.progress
            })
        });
        let resources: Vec<_> = resource_patches
            .into_iter()
            .map(|(name, (amount, positions))| {
                serde_json::json!({
                    "name": name,
                    "amount": amount,
                    "positions": positions.into_iter().map(|(x, y)| serde_json::json!({"x": x, "y": y})).collect::<Vec<_>>()
                })
            })
            .collect();
        CommandResult::ok(serde_json::json!({
            "position": { "x": px, "y": py },
            "tick": tick,
            "inventory": inventory,
            "crafting_queue": crafting_queue,
            "research": research,
            "entities": entities,
            "resources": resources
        }))
    } else {
        conn.update_position();
        let (px, py) = conn.player_position();
        let tick = conn.server_tick();
        CommandResult::ok(serde_json::json!({
            "position": { "x": px, "y": py },
            "tick": tick,
            "entities": []
        }))
    }
}

fn cmd_recipes(conn: &Connection) -> CommandResult {
    if Prototypes::global().is_none() {
        if let Some(path) = default_factorio_data_path() {
            let _ = Prototypes::init_global(&path);
        }
    }
    if let Some(proto) = Prototypes::global() {
        let mut list = Vec::new();
        for (name, recipe) in proto.recipes() {
            let ingredients: Vec<_> = recipe.ingredients.iter().map(|i| {
                serde_json::json!({"name": i.name, "amount": i.amount, "type": i.ingredient_type})
            }).collect();
            list.push(serde_json::json!({
                "name": name,
                "ingredients": ingredients
            }));
        }
        CommandResult::ok(serde_json::json!({
            "source": "lua",
            "recipes": list
        }))
    } else if let Some(map) = conn.parsed_map.as_ref() {
        let mut names: Vec<_> = map
            .prototype_mappings
            .tables
            .get("Recipe")
            .map(|t| t.values().cloned().collect())
            .unwrap_or_default();
        names.sort();
        CommandResult::ok(serde_json::json!({
            "source": "map",
            "recipes": names
        }))
    } else {
        CommandResult::err("No recipe data available")
    }
}

fn cmd_techs(conn: &Connection) -> CommandResult {
    if let Some(map) = conn.parsed_map.as_ref() {
        let mut names: Vec<_> = map
            .prototype_mappings
            .tables
            .get("Technology")
            .map(|t| t.values().cloned().collect())
            .unwrap_or_default();
        names.sort();
        CommandResult::ok(serde_json::json!({
            "technologies": names
        }))
    } else {
        CommandResult::err("No technology data available")
    }
}

fn cmd_alerts(conn: &Connection, args: &serde_json::Value) -> CommandResult {
    let radius = arg_f64(args, "radius", 100.0);
    let Some(map) = conn.parsed_map.as_ref() else {
        return CommandResult::err("Map data not available yet");
    };
    let (px, py) = conn.player_position();
    let r2 = radius * radius;
    let mut enemies = 0u32;
    for e in &map.entities {
        if !is_enemy_name(&e.name) {
            continue;
        }
        let dx = e.x - px;
        let dy = e.y - py;
        if dx * dx + dy * dy <= r2 {
            enemies += 1;
        }
    }
    let mut alerts = Vec::new();
    if enemies > 0 {
        alerts.push(serde_json::json!({
            "type": "enemies_nearby",
            "count": enemies
        }));
    }
    CommandResult::ok(serde_json::json!({
        "partial": true,
        "note": "Alerts are limited without live server state (no fuel/health/inventory checks yet).",
        "alerts": alerts
    }))
}

async fn cmd_spawn(conn: &mut Connection) -> CommandResult {
    match conn.actions().send_continue_singleplayer().await {
        Ok(_) => CommandResult::ok(serde_json::json!({"status": "requested"})),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

fn cmd_get_train(conn: &Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    if let Some(world) = conn.sim_world() {
        let mut best = None;
        let mut best_dist = 25.0;
        if let Some(surface) = world.nauvis() {
            for e in surface.entities.values() {
                let (ex, ey) = e.position.to_tiles();
                let dx = ex - x;
                let dy = ey - y;
                let dist = dx * dx + dy * dy;
                if dist <= best_dist {
                    match e.entity_type {
                        crate::state::entity::EntityType::Locomotive
                        | crate::state::entity::EntityType::CargoWagon
                        | crate::state::entity::EntityType::FluidWagon
                        | crate::state::entity::EntityType::ArtilleryWagon => {
                            best_dist = dist;
                            best = Some((e.id, e.name.clone(), ex, ey));
                        }
                        _ => {}
                    }
                }
            }
        }
        if let Some((train_id, name, ex, ey)) = best {
            let state = world.trains.get(&train_id).cloned().unwrap_or_default();
            let schedule: Vec<_> = state
                .schedule
                .iter()
                .map(|s| {
                    let mut obj = serde_json::json!({"station": s.station});
                    if let Some((x, y)) = s.position {
                        if let Some(map) = obj.as_object_mut() {
                            map.insert("x".to_string(), serde_json::json!(x));
                            map.insert("y".to_string(), serde_json::json!(y));
                        }
                    }
                    obj
                })
                .collect();
            let current_target = state
                .schedule
                .get(state.current)
                .and_then(|s| s.position)
                .map(|(x, y)| serde_json::json!({"x": x, "y": y}));
            return CommandResult::ok(serde_json::json!({
                "source": "simulated",
                "train_id": train_id,
                "entity": { "name": name, "x": ex, "y": ey, "distance": best_dist.sqrt() },
                "schedule": schedule,
                "current": state.current,
                "current_target": current_target,
                "manual_mode": state.manual_mode,
                "speed": state.speed
            }));
        }
    }

    let Some(map) = conn.parsed_map.as_ref() else {
        return CommandResult::err("Map data not available yet");
    };
    let mut closest = None;
    let mut best_dist = 5.0f64 * 5.0;
    for e in &map.entities {
        if !is_train_name(&e.name) {
            continue;
        }
        let dx = e.x - x;
        let dy = e.y - y;
        let dist = dx * dx + dy * dy;
        if dist <= best_dist {
            best_dist = dist;
            closest = Some(e);
        }
    }
    let Some(train) = closest else {
        return CommandResult::err("No train entity found near position");
    };
    CommandResult::ok(serde_json::json!({
        "partial": true,
        "note": "Train schedules require live entity IDs; only static map data is available.",
        "entity": {
            "name": train.name,
            "x": train.x,
            "y": train.y,
            "distance": best_dist.sqrt()
        }
    }))
}

async fn cmd_set_train_stop(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let name = arg_str(args, "name").unwrap_or("train-stop");
    if let Some(world) = conn.sim_world_mut() {
        if let Some(surface) = world.nauvis_mut() {
            let mut best = None;
            let mut best_dist = 4.0;
            for e in surface.entities.values() {
                if e.entity_type != crate::state::entity::EntityType::TrainStop {
                    continue;
                }
                let (ex, ey) = e.position.to_tiles();
                let dx = ex - x;
                let dy = ey - y;
                let dist = dx * dx + dy * dy;
                if dist <= best_dist {
                    best_dist = dist;
                    best = Some(e.id);
                }
            }
            if let Some(entity_id) = best {
                if let Some(entity) = surface.get_entity_mut(entity_id) {
                    if let crate::state::entity::EntityData::TrainStop(ref mut data) = entity.data {
                    data.station_name = name.to_string();
                    return CommandResult::ok(serde_json::json!({
                        "name": data.station_name,
                        "position": { "x": x, "y": y }
                    }));
                    }
                }
            }
        }
    }
    CommandResult::err("No train stop found near position")
}

async fn cmd_set_train_schedule(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let stations_val = args.get("stations").and_then(|v| v.as_array()).cloned();
    let mut records: Vec<crate::state::world::TrainScheduleRecord> = Vec::new();
    if let Some(arr) = stations_val {
        for item in arr {
            if let Some(name) = item.as_str() {
                let mut pos = None;
                if let Some((px, py)) = parse_station_position(name) {
                    pos = Some((px, py));
                }
                records.push(crate::state::world::TrainScheduleRecord {
                    station: name.to_string(),
                    position: pos,
                });
                continue;
            }
            if let Some(obj) = item.as_object() {
                let name = obj
                    .get("station")
                    .or_else(|| obj.get("name"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("station");
                let pos = if let (Some(x), Some(y)) = (obj.get("x").and_then(|v| v.as_f64()), obj.get("y").and_then(|v| v.as_f64())) {
                    Some((x, y))
                } else {
                    None
                };
                records.push(crate::state::world::TrainScheduleRecord {
                    station: name.to_string(),
                    position: pos,
                });
            }
        }
    }
    if records.is_empty() {
        if let Some(s) = arg_str(args, "station") {
            records.push(crate::state::world::TrainScheduleRecord {
                station: s.to_string(),
                position: None,
            });
        }
    }
    if records.is_empty() {
        return CommandResult::err("No stations provided");
    }

    if let Some(world) = conn.sim_world_mut() {
        if let Some(surface) = world.nauvis() {
            let mut best = None;
            let mut best_dist = 25.0;
            for e in surface.entities.values() {
                let (ex, ey) = e.position.to_tiles();
                let dx = ex - x;
                let dy = ey - y;
                let dist = dx * dx + dy * dy;
                if dist <= best_dist {
                    match e.entity_type {
                        crate::state::entity::EntityType::Locomotive
                        | crate::state::entity::EntityType::CargoWagon
                        | crate::state::entity::EntityType::FluidWagon
                        | crate::state::entity::EntityType::ArtilleryWagon => {
                            best_dist = dist;
                            best = Some(e.id);
                        }
                        _ => {}
                    }
                }
            }
            if let Some(train_id) = best {
                let stop_positions = surface
                    .entities
                    .values()
                    .filter_map(|e| {
                        if e.entity_type != crate::state::entity::EntityType::TrainStop {
                            return None;
                        }
                        if let crate::state::entity::EntityData::TrainStop(data) = &e.data {
                            Some((data.station_name.clone(), e.position.to_tiles()))
                        } else {
                            None
                        }
                    })
                    .collect::<std::collections::HashMap<_, _>>();
                for record in &mut records {
                    if record.position.is_none() {
                        if let Some((px, py)) = stop_positions.get(&record.station) {
                            record.position = Some((*px, *py));
                        }
                    }
                }
                let state = world.trains.entry(train_id).or_default();
                state.schedule = records.clone();
                state.current = 0;
                state.manual_mode = false;
                return CommandResult::ok(serde_json::json!({
                    "train_id": train_id,
                    "stations": state.schedule.iter().map(|s| s.station.clone()).collect::<Vec<_>>()
                }));
            }
        }
    }
    CommandResult::err("No train entity found near position")
}

async fn cmd_train_go(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let station = arg_str(args, "station");
    let target_x = arg_f64_opt(args, "target_x");
    let target_y = arg_f64_opt(args, "target_y");
    if let Some(world) = conn.sim_world_mut() {
        if let Some(surface) = world.nauvis() {
            let mut best = None;
            let mut best_dist = 25.0;
            for e in surface.entities.values() {
                let (ex, ey) = e.position.to_tiles();
                let dx = ex - x;
                let dy = ey - y;
                let dist = dx * dx + dy * dy;
                if dist <= best_dist {
                    match e.entity_type {
                        crate::state::entity::EntityType::Locomotive
                        | crate::state::entity::EntityType::CargoWagon
                        | crate::state::entity::EntityType::FluidWagon
                        | crate::state::entity::EntityType::ArtilleryWagon => {
                            best_dist = dist;
                            best = Some(e.id);
                        }
                        _ => {}
                    }
                }
            }
            if let Some(train_id) = best {
                let state = world.trains.entry(train_id).or_default();
                if let (Some(tx), Some(ty)) = (target_x, target_y) {
                    state.schedule = vec![crate::state::world::TrainScheduleRecord {
                        station: "target".to_string(),
                        position: Some((tx, ty)),
                    }];
                    state.current = 0;
                    state.manual_mode = false;
                    return CommandResult::ok(serde_json::json!({
                        "train_id": train_id,
                        "target": { "x": tx, "y": ty }
                    }));
                }
                if let Some(target) = station {
                    if let Some(idx) = state.schedule.iter().position(|s| s.station == target) {
                        state.current = idx;
                        state.manual_mode = false;
                        return CommandResult::ok(serde_json::json!({
                            "train_id": train_id,
                            "station": target
                        }));
                    } else {
                        return CommandResult::err("Station not in schedule");
                    }
                } else {
                    state.manual_mode = !state.manual_mode;
                    return CommandResult::ok(serde_json::json!({
                        "train_id": train_id,
                        "manual_mode": state.manual_mode
                    }));
                }
            }
        }
    }
    CommandResult::err("No train entity found near position")
}

async fn cmd_set_combinator(conn: &mut Connection, _args: &serde_json::Value) -> CommandResult {
    let _ = conn;
    CommandResult::err("SetCombinator requires entity IDs from live state; not implemented yet")
}

fn cmd_get_signals(conn: &Connection, _args: &serde_json::Value) -> CommandResult {
    let _ = conn;
    CommandResult::err("GetSignals requires entity IDs from live state; not implemented yet")
}

fn cmd_power_status(conn: &Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let radius = arg_f64(args, "radius", 50.0);
    let r2 = radius * radius;
    if let Some(world) = conn.sim_world() {
        let mut sources = 0u32;
        let mut consumers = 0u32;
        let mut accumulators = 0u32;
        let mut accumulator_energy = 0.0f64;
        if let Some(surface) = world.nauvis() {
            for e in surface.entities.values() {
                let (ex, ey) = e.position.to_tiles();
                let dx = ex - x;
                let dy = ey - y;
                if dx * dx + dy * dy > r2 {
                    continue;
                }
                match e.entity_type {
                    crate::state::entity::EntityType::SolarPanel
                    | crate::state::entity::EntityType::Boiler
                    | crate::state::entity::EntityType::Generator
                    | crate::state::entity::EntityType::Reactor => {
                        sources += 1;
                    }
                    crate::state::entity::EntityType::Accumulator => {
                        accumulators += 1;
                        if let crate::state::entity::EntityData::Accumulator(data) = &e.data {
                            accumulator_energy += data.energy;
                        }
                    }
                    crate::state::entity::EntityType::AssemblingMachine
                    | crate::state::entity::EntityType::Furnace
                    | crate::state::entity::EntityType::MiningDrill
                    | crate::state::entity::EntityType::Lab => {
                        consumers += 1;
                    }
                    _ => {}
                }
            }
        }
        return CommandResult::ok(serde_json::json!({
            "source": "simulated",
            "center": { "x": x, "y": y },
            "radius": radius,
            "sources": sources,
            "consumers": consumers,
            "accumulators": accumulators,
            "accumulator_energy": accumulator_energy
        }));
    }
    CommandResult::err("Simulation state not available yet")
}

fn cmd_logistics_status(conn: &Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let radius = arg_f64(args, "radius", 50.0);
    let r2 = radius * radius;
    if let Some(world) = conn.sim_world() {
        let mut roboports = 0u32;
        let mut logistics_chests = 0u32;
        let mut available_logistic_robots = 0u32;
        let mut available_construction_robots = 0u32;
        if let Some(surface) = world.nauvis() {
            for e in surface.entities.values() {
                let (ex, ey) = e.position.to_tiles();
                let dx = ex - x;
                let dy = ey - y;
                if dx * dx + dy * dy > r2 {
                    continue;
                }
                match e.entity_type {
                    crate::state::entity::EntityType::Roboport => {
                        roboports += 1;
                        if let crate::state::entity::EntityData::Roboport(data) = &e.data {
                            available_construction_robots += data.available_construction_robots;
                            available_logistic_robots += data.available_logistic_robots;
                        }
                    }
                    crate::state::entity::EntityType::LogisticContainer => {
                        logistics_chests += 1;
                    }
                    _ => {}
                }
            }
        }
        return CommandResult::ok(serde_json::json!({
            "source": "simulated",
            "center": { "x": x, "y": y },
            "radius": radius,
            "roboports": roboports,
            "logistic_containers": logistics_chests,
            "available_logistic_robots": available_logistic_robots,
            "available_construction_robots": available_construction_robots
        }));
    }
    CommandResult::err("Simulation state not available yet")
}

fn cmd_pollution(conn: &Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let radius = arg_f64(args, "radius", 50.0);
    let r2 = radius * radius;
    if let Some(world) = conn.sim_world() {
        let mut sources = serde_json::Map::new();
        let mut estimate = 0.0f64;
        if let Some(surface) = world.nauvis() {
            for e in surface.entities.values() {
                let (ex, ey) = e.position.to_tiles();
                let dx = ex - x;
                let dy = ey - y;
                if dx * dx + dy * dy > r2 {
                    continue;
                }
                let (name, value) = match e.entity_type {
                    crate::state::entity::EntityType::MiningDrill => ("mining_drill", 10.0),
                    crate::state::entity::EntityType::Furnace => ("furnace", 5.0),
                    crate::state::entity::EntityType::AssemblingMachine => ("assembler", 3.0),
                    crate::state::entity::EntityType::Boiler => ("boiler", 6.0),
                    _ => continue,
                };
                estimate += value;
                let entry = sources.entry(name.to_string()).or_insert(serde_json::json!(0));
                if let Some(v) = entry.as_u64() {
                    *entry = serde_json::json!(v + 1);
                }
            }
        }
        return CommandResult::ok(serde_json::json!({
            "source": "simulated",
            "center": { "x": x, "y": y },
            "radius": radius,
            "pollution_estimate": estimate,
            "sources": sources
        }));
    }
    CommandResult::err("Simulation state not available yet")
}

async fn cmd_build(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let direction = arg_u64(args, "direction", 0) as u8;
    match conn.actions().send_build(x, y, direction).await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_place(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let direction = arg_u64(args, "direction", 0) as u8;
    let Some(item_id) = lookup_item_id(conn, args) else {
        return CommandResult::err("Missing or unknown item (use item_id or item name)");
    };
    let slot = match arg_u64_opt(args, "slot") {
        Some(v) => v as u16,
        None => return CommandResult::err("place requires --slot"),
    };
    let kind = arg_u64(args, "kind", 0) as u8;
    let inventory = arg_u64(args, "inventory", 1) as u8;
    let source = arg_u64(args, "source", 2) as u8;
    let quality_id = arg_u64_opt(args, "quality_id").map(|v| v as u8).unwrap_or(0);
    let stack_id = arg_u64(args, "stack_id", 0);
    let clear_cursor = arg_bool(args, "clear_cursor", true);

    let location = RelativeItemStackLocation {
        kind,
        inventory_index: inventory,
        slot_index: slot,
        source,
    };
    let loc = ClientItemStackLocation {
        item_id,
        quality_id,
        stack_id,
        location,
    };

    let _ = conn.actions().send_clear_cursor().await;
    if let Err(e) = conn.actions().send_cursor_transfer(loc).await {
        return CommandResult::err(e.to_string());
    }
    let _ = conn.actions().send_selected_entity_changed(x, y).await;
    if let Err(e) = conn.actions().send_build(x, y, direction).await {
        return CommandResult::err(e.to_string());
    }
    if clear_cursor {
        let _ = conn.actions().send_clear_cursor().await;
    }

    CommandResult::ok(serde_json::json!({
        "item_id": item_id,
        "quality_id": quality_id,
        "stack_id": stack_id,
        "position": { "x": x, "y": y },
        "direction": direction,
        "clear_cursor": clear_cursor,
        "location": {
            "kind": kind,
            "inventory_index": inventory,
            "slot_index": slot,
            "source": source
        }
    }))
}

async fn cmd_build_blueprint(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let direction = arg_u64(args, "direction", 0) as u8;
    let blueprint = args
        .get("blueprint")
        .or_else(|| args.get("blueprint_string"))
        .or_else(|| args.get("string"))
        .and_then(|v| v.as_str());
    let Some(blueprint) = blueprint else {
        return CommandResult::err("Missing blueprint string (use blueprint or blueprint_string)");
    };
    let flags = arg_u64(args, "flags", 0) as u16;
    let mode = arg_u64(args, "mode", 0) as u8;
    let clear_cursor = arg_bool(args, "clear_cursor", true);

    if let Err(e) = conn.actions().send_import_blueprint_string(blueprint, flags, mode).await {
        return CommandResult::err(e.to_string());
    }

    let result = conn.actions().send_build(x, y, direction).await;

    if clear_cursor {
        let _ = conn.actions().send_clear_cursor().await;
    }

    match result {
        Ok(_) => CommandResult::ok(serde_json::json!({
            "position": { "x": x, "y": y },
            "direction": direction,
            "flags": flags,
            "mode": mode,
            "clear_cursor": clear_cursor
        })),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_set_logistics(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let slot_index = arg_u64(args, "slot", 0) as u16;
    let Some(min) = arg_u64_opt(args, "min") else {
        return CommandResult::err("Missing min count");
    };
    let max = arg_u64_opt(args, "max").unwrap_or(min);
    let mode = arg_u64(args, "mode", 0) as u32;
    let section_index = arg_u64(args, "section", 0) as u8;
    let section_type = arg_u64(args, "section_type", 0) as u8;
    let space_location_id = arg_u64(args, "space_location_id", 0) as u16;
    let quality_id = arg_u64_opt(args, "quality_id").map(|v| v as u8).unwrap_or(0);
    let quality_extra = arg_u64_opt(args, "quality_extra").map(|v| v as u8).unwrap_or(2);

    let item_specified = args.get("item").is_some() || args.get("item_id").is_some();
    let signal = if item_specified {
        let Some(item_id) = lookup_item_id(conn, args) else {
            return CommandResult::err("Unknown item (use item_id or item name)");
        };
        SignalId::item(item_id)
    } else if let Some(signal_id) = arg_u64_opt(args, "signal_id") {
        let signal_type = arg_u64(args, "signal_type", 0) as u8;
        SignalId { kind: signal_type, id: signal_id as u16 }
    } else {
        return CommandResult::err("Missing item (item_id/item) or signal_id");
    };

    if let (Some(x), Some(y)) = (arg_f64_opt(args, "x"), arg_f64_opt(args, "y")) {
        let _ = conn.actions().send_selected_entity_changed(x, y).await;
    }

    if arg_bool(args, "open_gui", false) {
        let _ = conn.actions().send_open_logistics_gui().await;
    }

    let filter = LogisticFilter {
        signal,
        quality_id,
        quality_extra,
        min: min as u32,
        max: max as u32,
        mode,
        space_location_id,
    };

    match conn
        .actions()
        .send_set_logistic_filter(filter, section_type, section_index, slot_index)
        .await
    {
        Ok(_) => CommandResult::ok(serde_json::json!({
            "slot": slot_index,
            "signal_type": signal.kind,
            "signal_id": signal.id,
            "min": min,
            "max": max,
            "mode": mode,
            "section_index": section_index,
            "section_type": section_type,
            "space_location_id": space_location_id,
            "quality_id": quality_id,
            "quality_extra": quality_extra
        })),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_cursor_transfer(
    conn: &mut Connection,
    args: &serde_json::Value,
    split: bool,
) -> CommandResult {
    let Some(item_id) = lookup_item_id(conn, args) else {
        return CommandResult::err("Missing or unknown item (use item_id or item name)");
    };
    let quality_id = arg_u64_opt(args, "quality_id").map(|v| v as u8).unwrap_or(0);
    let stack_id = arg_u64(args, "stack_id", 0);
    let location = build_relative_location(args);
    let loc = ClientItemStackLocation {
        item_id,
        quality_id,
        stack_id,
        location,
    };
    let result = if split {
        conn.actions().send_cursor_split(loc).await
    } else {
        conn.actions().send_cursor_transfer(loc).await
    };
    match result {
        Ok(_) => CommandResult::ok(serde_json::json!({
            "item_id": item_id,
            "quality_id": quality_id,
            "stack_id": stack_id,
            "location": {
                "kind": location.kind,
                "inventory_index": location.inventory_index,
                "slot_index": location.slot_index,
                "source": location.source
            },
            "mode": if split { "split" } else { "transfer" }
        })),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_stack_transfer(
    conn: &mut Connection,
    args: &serde_json::Value,
    inventory_only: bool,
    split: bool,
) -> CommandResult {
    let Some(item_id) = lookup_item_id(conn, args) else {
        return CommandResult::err("Missing or unknown item (use item_id or item name)");
    };
    let quality_id = arg_u64_opt(args, "quality_id").map(|v| v as u8).unwrap_or(0);
    let stack_id = arg_u64(args, "stack_id", 0);
    let mode = arg_u64(args, "mode", 0) as u8;
    let location = build_relative_location(args);
    let spec = ItemStackTransferSpecification {
        item_id,
        quality_id,
        stack_id,
        location,
        mode,
    };
    let result = match (inventory_only, split) {
        (true, true) => conn.actions().send_inventory_split(spec).await,
        (true, false) => conn.actions().send_inventory_transfer(spec).await,
        (false, true) => conn.actions().send_stack_split(spec).await,
        (false, false) => conn.actions().send_stack_transfer(spec).await,
    };
    match result {
        Ok(_) => CommandResult::ok(serde_json::json!({
            "item_id": item_id,
            "quality_id": quality_id,
            "stack_id": stack_id,
            "mode": mode,
            "inventory_only": inventory_only,
            "split": split,
            "location": {
                "kind": location.kind,
                "inventory_index": location.inventory_index,
                "slot_index": location.slot_index,
                "source": location.source
            }
        })),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_rotate(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let reverse = arg_bool(args, "reverse", false);
    match conn.actions().send_rotate(x, y, reverse).await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_filter(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let slot = arg_u64(args, "slot", 0) as u16;
    let inventory_index = arg_u64(args, "inventory", 0) as u8;
    let source = arg_u64(args, "source", 0) as u8;
    let kind = arg_u64(args, "kind", 0) as u8;
    let quality_id = arg_u64_opt(args, "quality_id").map(|v| v as u8).unwrap_or(0);
    let quality_extra = arg_u64_opt(args, "quality_extra").map(|v| v as u8);

    let Some(item_id) = lookup_item_id(conn, args) else {
        return CommandResult::err("Missing or unknown item (use item_id or item name)");
    };

    if let (Some(x), Some(y)) = (arg_f64_opt(args, "x"), arg_f64_opt(args, "y")) {
        let _ = conn.actions().send_selected_entity_changed(x, y).await;
    }

    let location = RelativeItemStackLocation {
        kind,
        inventory_index,
        slot_index: slot,
        source,
    };

    match conn.actions().send_set_filter(location, item_id, quality_id, quality_extra).await {
        Ok(_) => CommandResult::ok(serde_json::json!({
            "slot": slot,
            "inventory": inventory_index,
            "source": source,
            "item_id": item_id,
            "quality_id": quality_id,
            "quality_extra": quality_extra
        })),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_clear_filter(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let slot = arg_u64(args, "slot", 0) as u16;
    let inventory_index = arg_u64(args, "inventory", 0) as u8;
    let source = arg_u64(args, "source", 0) as u8;
    let kind = arg_u64(args, "kind", 0) as u8;

    if let (Some(x), Some(y)) = (arg_f64_opt(args, "x"), arg_f64_opt(args, "y")) {
        let _ = conn.actions().send_selected_entity_changed(x, y).await;
    }

    let location = RelativeItemStackLocation {
        kind,
        inventory_index,
        slot_index: slot,
        source,
    };

    match conn.actions().send_clear_filter(location).await {
        Ok(_) => CommandResult::ok(serde_json::json!({
            "slot": slot,
            "inventory": inventory_index,
            "source": source
        })),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_fast_transfer(
    conn: &mut Connection,
    args: &serde_json::Value,
    split: bool,
) -> CommandResult {
    if let (Some(x), Some(y)) = (arg_f64_opt(args, "x"), arg_f64_opt(args, "y")) {
        let _ = conn.actions().send_selected_entity_changed(x, y).await;
    }
    let from_player = arg_bool(args, "from_player", true);
    let count = arg_u64(args, "count", 1).max(1) as usize;
    for _ in 0..count {
        let result = if split {
            conn.actions().send_fast_split(from_player).await
        } else {
            conn.actions().send_fast_transfer(from_player).await
        };
        if let Err(e) = result {
            return CommandResult::err(e.to_string());
        }
    }
    CommandResult::ok(serde_json::json!({
        "from_player": from_player,
        "mode": if split { "split" } else { "transfer" },
        "count": count,
    }))
}

async fn cmd_insert(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let has_item_arg = args.get("item_id").is_some() || args.get("item").is_some();
    if has_item_arg {
        let Some(item_id) = lookup_item_id(conn, args) else {
            return CommandResult::err("Missing or unknown item (use item_id or item name)");
        };
        let kind = arg_u64(args, "kind", 0) as u8;
        if kind == 0 && (args.get("inventory").is_none() || args.get("slot").is_none()) {
            return CommandResult::err("insert with item requires --inventory and --slot when kind=0");
        }
        if let (Some(x), Some(y)) = (arg_f64_opt(args, "x"), arg_f64_opt(args, "y")) {
            let _ = conn.actions().send_selected_entity_changed(x, y).await;
        }
        let quality_id = arg_u64_opt(args, "quality_id").map(|v| v as u8).unwrap_or(0);
        let stack_id = arg_u64(args, "stack_id", 0);
        let mode = arg_u64(args, "mode", 0) as u8;
        let location = build_relative_location(args);
        let spec = ItemStackTransferSpecification {
            item_id,
            quality_id,
            stack_id,
            location,
            mode,
        };
        let split = arg_bool(args, "split", false);
        let count = arg_u64(args, "count", 1).max(1) as usize;
        for _ in 0..count {
            let result = if split {
                conn.actions().send_stack_split(spec).await
            } else {
                conn.actions().send_stack_transfer(spec).await
            };
            if let Err(e) = result {
                return CommandResult::err(e.to_string());
            }
        }
        return CommandResult::ok(serde_json::json!({
            "mode": if split { "stack-split" } else { "stack-transfer" },
            "count": count,
            "item_id": item_id,
            "quality_id": quality_id,
            "stack_id": stack_id,
            "kind": location.kind,
            "inventory": location.inventory_index,
            "slot": location.slot_index,
            "source": location.source,
            "transfer_mode": mode
        }));
    }
    if let (Some(x), Some(y)) = (arg_f64_opt(args, "x"), arg_f64_opt(args, "y")) {
        let _ = conn.actions().send_selected_entity_changed(x, y).await;
    }
    let split = arg_bool(args, "split", false);
    let count = arg_u64(args, "count", 1).max(1) as usize;
    for _ in 0..count {
        let result = if split {
            conn.actions().send_fast_split(true).await
        } else {
            conn.actions().send_fast_transfer(true).await
        };
        if let Err(e) = result {
            return CommandResult::err(e.to_string());
        }
    }
    CommandResult::ok(serde_json::json!({
        "mode": if split { "fast-split" } else { "fast-transfer" },
        "count": count,
        "note": "fast transfer used (no item specified). Use --item/--inventory/--slot for stack transfer."
    }))
}

async fn cmd_extract(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let has_item_arg = args.get("item_id").is_some() || args.get("item").is_some();
    if has_item_arg {
        let Some(item_id) = lookup_item_id(conn, args) else {
            return CommandResult::err("Missing or unknown item (use item_id or item name)");
        };
        let kind = arg_u64(args, "kind", 0) as u8;
        if kind == 0 && (args.get("inventory").is_none() || args.get("slot").is_none()) {
            return CommandResult::err("extract with item requires --inventory and --slot when kind=0");
        }
        if let (Some(x), Some(y)) = (arg_f64_opt(args, "x"), arg_f64_opt(args, "y")) {
            let _ = conn.actions().send_selected_entity_changed(x, y).await;
        }
        let quality_id = arg_u64_opt(args, "quality_id").map(|v| v as u8).unwrap_or(0);
        let stack_id = arg_u64(args, "stack_id", 0);
        let mode = arg_u64(args, "mode", 0) as u8;
        let location = build_relative_location(args);
        let spec = ItemStackTransferSpecification {
            item_id,
            quality_id,
            stack_id,
            location,
            mode,
        };
        let split = arg_bool(args, "split", false);
        let count = arg_u64(args, "count", 1).max(1) as usize;
        for _ in 0..count {
            let result = if split {
                conn.actions().send_stack_split(spec).await
            } else {
                conn.actions().send_stack_transfer(spec).await
            };
            if let Err(e) = result {
                return CommandResult::err(e.to_string());
            }
        }
        return CommandResult::ok(serde_json::json!({
            "mode": if split { "stack-split" } else { "stack-transfer" },
            "count": count,
            "item_id": item_id,
            "quality_id": quality_id,
            "stack_id": stack_id,
            "kind": location.kind,
            "inventory": location.inventory_index,
            "slot": location.slot_index,
            "source": location.source,
            "transfer_mode": mode
        }));
    }
    if let (Some(x), Some(y)) = (arg_f64_opt(args, "x"), arg_f64_opt(args, "y")) {
        let _ = conn.actions().send_selected_entity_changed(x, y).await;
    }
    let split = arg_bool(args, "split", false);
    let count = arg_u64(args, "count", 1).max(1) as usize;
    for _ in 0..count {
        let result = if split {
            conn.actions().send_fast_split(false).await
        } else {
            conn.actions().send_fast_transfer(false).await
        };
        if let Err(e) = result {
            return CommandResult::err(e.to_string());
        }
    }
    CommandResult::ok(serde_json::json!({
        "mode": if split { "fast-split" } else { "fast-transfer" },
        "count": count,
        "note": "fast transfer used (no item specified). Use --item/--inventory/--slot for stack transfer."
    }))
}

async fn cmd_equip(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let Some(item_id) = lookup_item_id(conn, args) else {
        return CommandResult::err("Missing or unknown item (use item_id or item name)");
    };
    let from_slot = match arg_u64_opt(args, "from_slot") {
        Some(v) => v as u16,
        None => return CommandResult::err("equip requires --from-slot"),
    };
    let from_inventory = arg_u64(args, "from_inventory", 1) as u8;
    let to_slot = arg_u64(args, "to_slot", 0) as u16;
    let source = arg_u64(args, "source", 2) as u8;
    let quality_id = arg_u64_opt(args, "quality_id").map(|v| v as u8).unwrap_or(0);
    let stack_id = arg_u64(args, "stack_id", 0);

    let mut to_inventory = arg_u64_opt(args, "to_inventory").map(|v| v as u8);
    if to_inventory.is_none() {
        if Prototypes::global().is_none() {
            if let Some(path) = default_factorio_data_path() {
                let _ = Prototypes::init_global(&path);
            }
        }
        let item_name = arg_str(args, "item")
            .map(|s| s.to_string())
            .or_else(|| {
                conn.parsed_map
                    .as_ref()
                    .and_then(|map| map.prototype_mappings.item_name(item_id).cloned())
            });
        if let (Some(proto), Some(name)) = (Prototypes::global(), item_name.as_ref()) {
            if let Some(item) = proto.item(name) {
                to_inventory = match item.item_type.as_str() {
                    "armor" => Some(5),
                    "gun" => Some(3),
                    "ammo" => Some(4),
                    _ => None,
                };
            }
        }
        if to_inventory.is_none() {
            return CommandResult::err("equip needs --to-inventory/--to-inventory-name or an armor/gun/ammo item");
        }
    }

    let from_location = RelativeItemStackLocation {
        kind: 0,
        inventory_index: from_inventory,
        slot_index: from_slot,
        source,
    };
    let to_location = RelativeItemStackLocation {
        kind: 0,
        inventory_index: to_inventory.unwrap_or(1),
        slot_index: to_slot,
        source,
    };

    let _ = conn.actions().send_clear_cursor().await;
    if let Err(e) = conn.actions().send_cursor_transfer(ClientItemStackLocation {
        item_id,
        quality_id,
        stack_id,
        location: from_location,
    }).await {
        return CommandResult::err(e.to_string());
    }
    if let Err(e) = conn.actions().send_cursor_transfer(ClientItemStackLocation {
        item_id: 0,
        quality_id: 0,
        stack_id: 0,
        location: to_location,
    }).await {
        return CommandResult::err(e.to_string());
    }

    CommandResult::ok(serde_json::json!({
        "item_id": item_id,
        "quality_id": quality_id,
        "stack_id": stack_id,
        "from": {
            "inventory": from_inventory,
            "slot": from_slot,
            "source": source
        },
        "to": {
            "inventory": to_location.inventory_index,
            "slot": to_slot,
            "source": source
        }
    }))
}

async fn cmd_drop(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let Some(item_id) = lookup_item_id(conn, args) else {
        return CommandResult::err("Missing or unknown item (use item_id or item name)");
    };
    let slot = match arg_u64_opt(args, "slot") {
        Some(v) => v as u16,
        None => return CommandResult::err("drop requires --slot"),
    };
    let kind = arg_u64(args, "kind", 0) as u8;
    let inventory = arg_u64(args, "inventory", 1) as u8;
    let source = arg_u64(args, "source", 2) as u8;
    let quality_id = arg_u64_opt(args, "quality_id").map(|v| v as u8).unwrap_or(0);
    let stack_id = arg_u64(args, "stack_id", 0);
    let clear_cursor = arg_bool(args, "clear_cursor", true);

    let (x, y) = match (arg_f64_opt(args, "x"), arg_f64_opt(args, "y")) {
        (Some(px), Some(py)) => (px, py),
        _ => {
            conn.update_position();
            conn.player_position()
        }
    };

    let location = RelativeItemStackLocation {
        kind,
        inventory_index: inventory,
        slot_index: slot,
        source,
    };
    let loc = ClientItemStackLocation {
        item_id,
        quality_id,
        stack_id,
        location,
    };

    let _ = conn.actions().send_clear_cursor().await;
    if let Err(e) = conn.actions().send_cursor_transfer(loc).await {
        return CommandResult::err(e.to_string());
    }
    if let Err(e) = conn.actions().send_drop_item(x, y).await {
        return CommandResult::err(e.to_string());
    }
    if clear_cursor {
        let _ = conn.actions().send_clear_cursor().await;
    }

    CommandResult::ok(serde_json::json!({
        "item_id": item_id,
        "quality_id": quality_id,
        "stack_id": stack_id,
        "position": { "x": x, "y": y },
        "clear_cursor": clear_cursor,
        "location": {
            "kind": kind,
            "inventory_index": inventory,
            "slot_index": slot,
            "source": source
        }
    }))
}

async fn cmd_server_command(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let _ = (conn, args);
    CommandResult::err("server-command is disabled (no RCON / /c usage)")
}

async fn cmd_eval(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let _ = (conn, args);
    CommandResult::err("eval is disabled (no RCON / /c usage)")
}

async fn cmd_reload(conn: &mut Connection) -> CommandResult {
    let _ = conn;
    CommandResult::err("reload is disabled (no RCON / /c usage)")
}

async fn cmd_test_reload(conn: &mut Connection) -> CommandResult {
    let _ = conn;
    CommandResult::err("test-reload is disabled (no RCON / /c usage)")
}

async fn cmd_pickup(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let hold_ms = arg_u64(args, "hold_ms", 80);
    let _ = conn.actions().send_selected_entity_changed(x, y).await;
    if let Err(e) = conn.actions().send_change_picking_state(true).await {
        return CommandResult::err(e.to_string());
    }
    tokio::time::sleep(Duration::from_millis(hold_ms)).await;
    if let Err(e) = conn.actions().send_change_picking_state(false).await {
        return CommandResult::err(e.to_string());
    }
    CommandResult::ok(serde_json::json!({
        "position": { "x": x, "y": y },
        "hold_ms": hold_ms
    }))
}

async fn cmd_set_recipe(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let Some(recipe_id) = lookup_recipe_id(conn, args) else {
        return CommandResult::err("Missing or unknown recipe (use recipe_id or recipe name)");
    };
    let quality_id = arg_u64_opt(args, "quality_id").map(|v| v as u8);
    let _ = conn.actions().send_selected_entity_changed(x, y).await;
    match conn.actions().send_set_recipe(recipe_id, quality_id).await {
        Ok(_) => CommandResult::ok(serde_json::json!({
            "recipe_id": recipe_id,
            "quality_id": quality_id
        })),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_shoot(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    match conn.actions().send_shoot(x, y).await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_stop_shoot(conn: &mut Connection) -> CommandResult {
    match conn.actions().send_stop_shoot().await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_toggle_driving(conn: &mut Connection) -> CommandResult {
    match conn.actions().send_toggle_driving().await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_drive(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let acceleration = arg_u64(args, "acceleration", 1) as u8;
    let direction = arg_u64(args, "direction", 0) as u8;
    match conn.actions().send_drive(acceleration, direction).await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_enter_vehicle(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    if let (Some(x), Some(y)) = (arg_f64_opt(args, "x"), arg_f64_opt(args, "y")) {
        let _ = conn.actions().send_selected_entity_changed(x, y).await;
    }
    match conn.actions().send_toggle_driving().await {
        Ok(_) => {
            let player_id = conn.player_index().unwrap_or(1);
            let (px, py) = if let (Some(x), Some(y)) = (arg_f64_opt(args, "x"), arg_f64_opt(args, "y")) {
                (x, y)
            } else {
                conn.player_position()
            };
            if let Some(world) = conn.sim_world_mut() {
                if let Some(surface) = world.nauvis() {
                    let mut best = None;
                    let mut best_dist = 9.0;
                    for e in surface.entities.values() {
                        let (ex, ey) = e.position.to_tiles();
                        let dx = ex - px;
                        let dy = ey - py;
                        let dist = dx * dx + dy * dy;
                        if dist <= best_dist {
                            match e.entity_type {
                                crate::state::entity::EntityType::Car
                                | crate::state::entity::EntityType::Locomotive
                                | crate::state::entity::EntityType::CargoWagon
                                | crate::state::entity::EntityType::FluidWagon
                                | crate::state::entity::EntityType::ArtilleryWagon
                                | crate::state::entity::EntityType::SpiderVehicle => {
                                    best_dist = dist;
                                    best = Some(e.id);
                                }
                                _ => {}
                            }
                        }
                    }
                    if let Some(id) = best {
                        if let Some(player) = world.players.get_mut(&player_id) {
                            player.riding_vehicle = Some(id);
                        }
                    }
                }
            }
            CommandResult::ok_empty()
        }
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_exit_vehicle(conn: &mut Connection) -> CommandResult {
    match conn.actions().send_toggle_driving().await {
        Ok(_) => {
            let player_id = conn.player_index().unwrap_or(1);
            if let Some(world) = conn.sim_world_mut() {
                if let Some(player) = world.players.get_mut(&player_id) {
                    player.riding_vehicle = None;
                }
            }
            CommandResult::ok_empty()
        }
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_connect_wire(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x1 = arg_f64(args, "x1", 0.0);
    let y1 = arg_f64(args, "y1", 0.0);
    let x2 = arg_f64(args, "x2", 0.0);
    let y2 = arg_f64(args, "y2", 0.0);
    let wire = arg_str(args, "wire").unwrap_or("unknown");
    let wire_quality_id = arg_u64_opt(args, "wire_quality_id").map(|v| v as u8).unwrap_or(0);
    let wire_stack_id = arg_u64(args, "wire_stack_id", 0);
    let wire_kind = arg_u64(args, "wire_kind", 0) as u8;
    let wire_inventory = arg_u64(args, "wire_inventory", 1) as u8;
    let wire_source = arg_u64(args, "wire_source", 2) as u8;
    let wire_clear_cursor = arg_bool(args, "wire_clear_cursor", true);

    let mut wire_item_id = arg_u64_opt(args, "wire_item_id").map(|v| v as u16);
    if wire_item_id.is_none() {
        if let Some(name) = arg_str(args, "wire_item") {
            wire_item_id = conn
                .parsed_map
                .as_ref()
                .and_then(|map| map.prototype_mappings.item_id_by_name(name));
        }
    }
    if wire_item_id.is_none() {
        if wire == "red" {
            wire_item_id = conn
                .parsed_map
                .as_ref()
                .and_then(|map| map.prototype_mappings.item_id_by_name("red-wire"));
        } else if wire == "green" {
            wire_item_id = conn
                .parsed_map
                .as_ref()
                .and_then(|map| map.prototype_mappings.item_id_by_name("green-wire"));
        }
    }

    if let Some(item_id) = wire_item_id {
        let Some(slot) = arg_u64_opt(args, "wire_slot") else {
            return CommandResult::err("connect-wire with wire item requires --wire-slot");
        };
        let loc = ClientItemStackLocation {
            item_id,
            quality_id: wire_quality_id,
            stack_id: wire_stack_id,
            location: RelativeItemStackLocation {
                kind: wire_kind,
                inventory_index: wire_inventory,
                slot_index: slot as u16,
                source: wire_source,
            },
        };
        let _ = conn.actions().send_clear_cursor().await;
        if let Err(e) = conn.actions().send_cursor_transfer(loc).await {
            return CommandResult::err(e.to_string());
        }
    }

    let _ = conn.actions().send_selected_entity_changed(x1, y1).await;
    if let Err(e) = conn.actions().send_wire_dragging(x1, y1).await {
        return CommandResult::err(e.to_string());
    }
    let _ = conn.actions().send_selected_entity_changed(x2, y2).await;
    if let Err(e) = conn.actions().send_wire_dragging(x2, y2).await {
        return CommandResult::err(e.to_string());
    }
    if wire_item_id.is_some() && wire_clear_cursor {
        let _ = conn.actions().send_clear_cursor().await;
    }

    let note = if wire_item_id.is_some() {
        "wire pulled from inventory slot"
    } else {
        "wire color uses current cursor selection"
    };

    CommandResult::ok(serde_json::json!({
        "from": { "x": x1, "y": y1 },
        "to": { "x": x2, "y": y2 },
        "wire": wire,
        "note": note
    }))
}

async fn cmd_disconnect_wire(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let wire = arg_str(args, "wire").unwrap_or("all");

    let _ = conn.actions().send_selected_entity_changed(x, y).await;
    match conn.actions().send_remove_cables(x, y).await {
        Ok(_) => CommandResult::ok(serde_json::json!({
            "position": { "x": x, "y": y },
            "wire": wire,
            "note": "remove-cables clears all wire connections"
        })),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_deconstruct(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let radius = arg_f64_opt(args, "radius");
    let (left_top, right_bottom) = if let Some(r) = radius {
        ((x - r, y - r), (x + r, y + r))
    } else {
        ((x - 0.5, y - 0.5), (x + 0.5, y + 0.5))
    };
    match conn.actions().send_deconstruct_area(left_top, right_bottom).await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_cancel_deconstruct(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let radius = arg_f64_opt(args, "radius");
    let (left_top, right_bottom) = if let Some(r) = radius {
        ((x - r, y - r), (x + r, y + r))
    } else {
        ((x - 0.5, y - 0.5), (x + 0.5, y + 0.5))
    };
    match conn.actions().send_cancel_deconstruct_area(left_top, right_bottom).await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_copy_settings(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let _ = conn.actions().send_selected_entity_changed(x, y).await;
    match conn.actions().send_copy_entity_settings().await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_paste_settings(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let _ = conn.actions().send_selected_entity_changed(x, y).await;
    match conn.actions().send_paste_entity_settings().await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_remove_cables(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    match conn.actions().send_remove_cables(x, y).await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_launch_rocket(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let _ = conn.actions().send_selected_entity_changed(x, y).await;
    match conn.actions().send_launch_rocket().await {
        Ok(_) => CommandResult::ok_empty(),
        Err(e) => CommandResult::err(e.to_string()),
    }
}

async fn cmd_move_to(
    conn: &mut Connection,
    path_follower: &mut PathFollower,
    action_tracker: &mut ActionTracker,
    args: &serde_json::Value,
) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let tolerance = arg_f64(args, "tolerance", 0.25);
    let max_nodes = arg_u64(args, "max_nodes", 50_000) as usize;

    if Prototypes::global().is_none() {
        if let Some(path) = default_factorio_data_path() {
            if let Err(e) = Prototypes::init_global(&path) {
                return CommandResult::err(format!("Failed to load Factorio Lua data: {}", e));
            }
        }
    }

    path_follower.clear();
    action_tracker.complete_if_action("move", "interrupted");
    let _ = conn.actions().send_stop_walk().await;

    conn.update_position();
    let (px, py) = conn.player_position();
    let start = MapPosition::from_tiles(px, py);
    let goal = MapPosition::from_tiles(x, y);
    let Some(map) = conn.parsed_map.as_ref() else {
        return CommandResult::err("Map data not available yet");
    };

    let action_id = action_tracker.start(
        "move",
        conn.server_tick(),
        serde_json::json!({
            "target_x": x,
            "target_y": y,
            "tolerance": tolerance,
            "max_nodes": max_nodes as u64,
            "replans": 0
        }),
    );

    // Clone map for spawn_blocking to avoid blocking the event loop
    let map_clone = map.clone();
    let mut path_handle = tokio::task::spawn_blocking(move || {
        let pathfinder = TilePathfinder::new(&map_clone);
        pathfinder.find_path(start, goal, max_nodes)
    });

    // Poll connection while pathfinding to keep heartbeats alive
    let path = loop {
        tokio::select! {
            biased;
            result = &mut path_handle => {
                break result.unwrap_or(None);
            }
            _ = tokio::time::sleep(Duration::from_millis(16)) => {
                for _ in 0..5 {
                    let _ = conn.poll().await;
                }
            }
        }
    };

    match path {
        Some(path) => {
            path_follower.set_path(path, tolerance);
            path_follower.reset_tracking(conn.server_tick(), (px, py));
            let _ = path_follower.tick(conn).await;

            let blocking = arg_bool(args, "blocking", true);

            let (px, py) = conn.player_position();
            CommandResult::ok(serde_json::json!({
                "action_id": action_id,
                "target": { "x": x, "y": y },
                "tolerance": tolerance,
                "max_nodes": max_nodes,
                "waypoints": path_follower.path_len(),
                "position": { "x": px, "y": py },
                "blocking": blocking
            }))
        }
        None => {
            action_tracker.complete_if_action("move", "no_path");
            CommandResult::err("No path found")
        }
    }
}

async fn try_replan_move(
    conn: &mut Connection,
    path_follower: &mut PathFollower,
    action_tracker: &mut ActionTracker,
) -> bool {
    let Some(current) = action_tracker.current.as_mut() else {
        return false;
    };
    if current.action != "move" {
        return false;
    }
    let target_x = current.details.get("target_x").and_then(|v| v.as_f64());
    let target_y = current.details.get("target_y").and_then(|v| v.as_f64());
    let Some((tx, ty)) = target_x.zip(target_y) else {
        return false;
    };
    let max_nodes = current
        .details
        .get("max_nodes")
        .and_then(|v| v.as_u64())
        .unwrap_or(50_000) as usize;
    let tolerance = current
        .details
        .get("tolerance")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.25);
    let replans = current
        .details
        .get("replans")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    if replans >= 2 {
        return false;
    }
    conn.update_position();
    let (px, py) = conn.player_position();
    let start = MapPosition::from_tiles(px, py);
    let goal = MapPosition::from_tiles(tx, ty);
    let Some(map) = conn.parsed_map.as_ref() else {
        return false;
    };

    // Clone map for spawn_blocking to avoid blocking the event loop
    let map_clone = map.clone();
    let mut path_handle = tokio::task::spawn_blocking(move || {
        let pathfinder = TilePathfinder::new(&map_clone);
        pathfinder.find_path(start, goal, max_nodes)
    });

    // Poll connection while pathfinding to keep heartbeats alive
    let path = loop {
        tokio::select! {
            biased;
            result = &mut path_handle => {
                break result.unwrap_or(None);
            }
            _ = tokio::time::sleep(Duration::from_millis(16)) => {
                for _ in 0..5 {
                    let _ = conn.poll().await;
                }
            }
        }
    };

    let Some(path) = path else {
        return false;
    };

    if let Some(obj) = current.details.as_object_mut() {
        obj.insert("replans".to_string(), serde_json::json!(replans + 1));
        obj.insert(
            "last_replan_tick".to_string(),
            serde_json::json!(conn.server_tick()),
        );
    }

    path_follower.set_path(path, tolerance);
    path_follower.reset_tracking(conn.server_tick(), (px, py));
    let _ = path_follower.tick(conn).await;
    true
}

async fn cmd_find_path(conn: &mut Connection, args: &serde_json::Value) -> CommandResult {
    let x = arg_f64(args, "x", 0.0);
    let y = arg_f64(args, "y", 0.0);
    let max_nodes = arg_u64(args, "max_nodes", 50_000) as usize;
    let max_points = arg_u64_opt(args, "max_points").map(|v| v as usize);

    if Prototypes::global().is_none() {
        if let Some(path) = default_factorio_data_path() {
            if let Err(e) = Prototypes::init_global(&path) {
                return CommandResult::err(format!("Failed to load Factorio Lua data: {}", e));
            }
        }
    }

    conn.update_position();
    let (px, py) = conn.player_position();
    let start = MapPosition::from_tiles(px, py);
    let goal = MapPosition::from_tiles(x, y);
    let Some(map) = conn.parsed_map.as_ref() else {
        return CommandResult::err("Map data not available yet");
    };

    // Clone map for spawn_blocking to avoid blocking the event loop
    let map_clone = map.clone();
    let mut path_handle = tokio::task::spawn_blocking(move || {
        let pathfinder = TilePathfinder::new(&map_clone);
        pathfinder.find_path(start, goal, max_nodes)
    });

    // Poll connection while pathfinding to keep heartbeats alive
    let path = loop {
        tokio::select! {
            biased;
            result = &mut path_handle => {
                break result.unwrap_or(None);
            }
            _ = tokio::time::sleep(Duration::from_millis(16)) => {
                for _ in 0..5 {
                    let _ = conn.poll().await;
                }
            }
        }
    };

    let path = match path {
        Some(p) => p,
        None => return CommandResult::err("No path found"),
    };

    let total_points = path.len();
    let limited = max_points.unwrap_or(total_points);
    let truncated = total_points > limited;
    let points: Vec<_> = path
        .iter()
        .take(limited)
        .map(|pos| {
            let (tx, ty) = pos.to_tiles();
            serde_json::json!({ "x": tx, "y": ty })
        })
        .collect();

    CommandResult::ok(serde_json::json!({
        "start": { "x": px, "y": py },
        "goal": { "x": x, "y": y },
        "points": points,
        "total_points": total_points,
        "truncated": truncated
    }))
}

fn arg_f64(args: &serde_json::Value, key: &str, default: f64) -> f64 {
    args.get(key).and_then(|v| v.as_f64()).unwrap_or(default)
}

fn arg_u64(args: &serde_json::Value, key: &str, default: u64) -> u64 {
    args.get(key).and_then(|v| v.as_u64()).unwrap_or(default)
}

fn arg_bool(args: &serde_json::Value, key: &str, default: bool) -> bool {
    args.get(key).and_then(|v| v.as_bool()).unwrap_or(default)
}

fn arg_f64_opt(args: &serde_json::Value, key: &str) -> Option<f64> {
    args.get(key).and_then(|v| v.as_f64())
}

fn arg_u64_opt(args: &serde_json::Value, key: &str) -> Option<u64> {
    args.get(key).and_then(|v| v.as_u64())
}

fn arg_str<'a>(args: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    args.get(key).and_then(|v| v.as_str())
}

fn is_enemy_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.contains("biter")
        || lower.contains("spitter")
        || lower.contains("worm")
        || lower.contains("spawner")
}

fn is_train_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.contains("locomotive")
        || lower.contains("cargo-wagon")
        || lower.contains("fluid-wagon")
        || lower.contains("artillery-wagon")
}

fn parse_station_position(name: &str) -> Option<(f64, f64)> {
    let parts: Vec<_> = name.split(',').collect();
    if parts.len() != 2 {
        return None;
    }
    let x = parts[0].trim().parse::<f64>().ok()?;
    let y = parts[1].trim().parse::<f64>().ok()?;
    Some((x, y))
}

fn build_relative_location(args: &serde_json::Value) -> RelativeItemStackLocation {
    let kind = arg_u64(args, "kind", 0) as u8;
    let inventory_index = arg_u64(args, "inventory", 0) as u8;
    let slot_index = arg_u64(args, "slot", 0) as u16;
    let source = arg_u64(args, "source", 0) as u8;
    RelativeItemStackLocation {
        kind,
        inventory_index,
        slot_index,
        source,
    }
}

fn lookup_recipe_id(conn: &Connection, args: &serde_json::Value) -> Option<u16> {
    if let Some(value) = args.get("recipe_id").and_then(|v| v.as_u64()) {
        return Some(value as u16);
    }
    let name = args.get("recipe").and_then(|v| v.as_str())?;
    let map = conn.parsed_map.as_ref()?;
    map.prototype_mappings.recipe_id_by_name(name)
}

fn lookup_item_id(conn: &Connection, args: &serde_json::Value) -> Option<u16> {
    if let Some(value) = args.get("item_id").and_then(|v| v.as_u64()) {
        return Some(value as u16);
    }
    let name = args.get("item").and_then(|v| v.as_str())?;
    let map = conn.parsed_map.as_ref()?;
    map.prototype_mappings.item_id_by_name(name)
}

fn lookup_tech_id(conn: &Connection, args: &serde_json::Value) -> Option<u16> {
    if let Some(value) = args.get("technology_id").and_then(|v| v.as_u64()) {
        return Some(value as u16);
    }
    let name = args
        .get("technology")
        .or_else(|| args.get("tech"))
        .and_then(|v| v.as_str())?;
    let map = conn.parsed_map.as_ref()?;
    map.prototype_mappings.technology_id_by_name(name)
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

#[derive(Clone)]
struct ActionStatus {
    id: u64,
    action: String,
    started_tick: u32,
    details: serde_json::Value,
    completed: bool,
    result: Option<String>,
}

struct ActionTracker {
    current: Option<ActionStatus>,
    completed: VecDeque<ActionStatus>,
    next_id: u64,
}

impl ActionTracker {
    fn new() -> Self {
        Self {
            current: None,
            completed: VecDeque::new(),
            next_id: 1,
        }
    }

    fn start(&mut self, action: &str, started_tick: u32, details: serde_json::Value) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        self.current = Some(ActionStatus {
            id,
            action: action.to_string(),
            started_tick,
            details,
            completed: false,
            result: None,
        });
        id
    }

    fn complete_if_action(&mut self, action: &str, result: &str) {
        if let Some(current) = self.current.as_mut() {
            if current.action == action && !current.completed {
                current.completed = true;
                current.result = Some(result.to_string());
                self.completed.push_back(current.clone());
                while self.completed.len() > 16 {
                    self.completed.pop_front();
                }
            }
        }
    }

    fn clear(&mut self) {
        self.current = None;
    }
}

struct PathFollower {
    path: Vec<MapPosition>,
    next_idx: usize,
    tolerance: f64,
    last_direction: Option<u8>,
    last_sent_tick: Option<u32>,
    last_pos: Option<(f64, f64)>,
    last_move_tick: Option<u32>,
    started_tick: Option<u32>,
    max_stall_ticks: u32,
    min_move_dist: f64,
    last_debug_tick: Option<u32>,
}

impl PathFollower {
    fn new() -> Self {
        Self {
            path: Vec::new(),
            next_idx: 0,
            tolerance: 0.25,
            last_direction: None,
            last_sent_tick: None,
            last_pos: None,
            last_move_tick: None,
            started_tick: None,
            max_stall_ticks: 120,
            min_move_dist: 0.1,
            last_debug_tick: None,
        }
    }

    fn clear(&mut self) {
        self.path.clear();
        self.next_idx = 0;
        self.last_direction = None;
        self.last_sent_tick = None;
        self.last_pos = None;
        self.last_move_tick = None;
        self.started_tick = None;
        self.last_debug_tick = None;
    }

    fn set_path(&mut self, path: Vec<MapPosition>, tolerance: f64) {
        self.path = path;
        self.next_idx = 0;
        self.tolerance = tolerance;
        self.last_direction = None;
        self.last_sent_tick = None;
        self.last_pos = None;
        self.last_move_tick = None;
        self.started_tick = None;
        self.last_debug_tick = None;
    }

    fn reset_tracking(&mut self, tick: u32, pos: (f64, f64)) {
        self.started_tick = Some(tick);
        self.last_move_tick = Some(tick);
        self.last_pos = Some(pos);
        self.last_debug_tick = None;
    }

    fn should_debug(&mut self, tick: u32) -> bool {
        if std::env::var("FACTORIO_DEBUG_MOVE").is_err() {
            return false;
        }
        match self.last_debug_tick {
            Some(last) if tick.saturating_sub(last) < 60 => false,
            _ => {
                self.last_debug_tick = Some(tick);
                true
            }
        }
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
        let server_tick = conn.server_tick();
        let confirmed_tick = conn.confirmed_tick();
        let tick = server_tick.max(confirmed_tick);
        if self.started_tick.is_none() {
            self.reset_tracking(tick, (px, py));
        } else if self.last_pos.is_none() {
            self.last_pos = Some((px, py));
            self.last_move_tick = Some(tick);
        }
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
            return conn.actions().send_stop_walk().await;
        }

        if let (Some(last_pos), Some(last_move)) = (self.last_pos, self.last_move_tick) {
            let dx = px - last_pos.0;
            let dy = py - last_pos.1;
            let dist = (dx * dx + dy * dy).sqrt();
            if dist >= self.min_move_dist {
                self.last_pos = Some((px, py));
                self.last_move_tick = Some(tick);
            } else if tick.saturating_sub(last_move) > self.max_stall_ticks {
                let _ = conn.actions().send_stop_walk().await;
                return Err(crate::error::Error::Io("move_to_stuck".into()));
            } else if self.should_debug(tick) {
                let target = self.path[self.next_idx];
                let remaining = current.distance_to(target);
                eprintln!(
                    "[move] stalling tick={} server_tick={} confirmed_tick={} idx={} remaining={:.3} pos=({:.3},{:.3}) last_pos=({:.3},{:.3}) last_move_tick={}",
                    tick,
                    server_tick,
                    confirmed_tick,
                    self.next_idx,
                    remaining,
                    px,
                    py,
                    last_pos.0,
                    last_pos.1,
                    last_move
                );
            }
        }

        let target = self.path[self.next_idx];
        let dir = direction_to((px, py), target);
        let dir_u8 = dir as u8;
        if self.last_direction != Some(dir_u8) || self.last_sent_tick != Some(tick) {
            conn.actions().send_walk(dir_u8).await?;
            self.last_direction = Some(dir_u8);
            self.last_sent_tick = Some(tick);
            if self.should_debug(tick) {
                let remaining = current.distance_to(target);
                eprintln!(
                    "[move] send_walk tick={} server_tick={} confirmed_tick={} idx={} dir={:?} remaining={:.3} pos=({:.3},{:.3}) target=({:.3},{:.3})",
                    tick,
                    server_tick,
                    confirmed_tick,
                    self.next_idx,
                    dir,
                    remaining,
                    px,
                    py,
                    target.x.to_tiles(),
                    target.y.to_tiles()
                );
            }
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

fn map_cache_path(map_blob: &[u8]) -> Option<PathBuf> {
    if std::env::var("FACTORIO_DISABLE_MAP_CACHE").is_ok() {
        return None;
    }
    let dir = if let Ok(path) = std::env::var("FACTORIO_MAP_CACHE_DIR") {
        PathBuf::from(path)
    } else {
        dirs::home_dir()?.join(".factorio-bot").join("map-cache")
    };
    let hash = crc32fast::hash(map_blob);
    let name = format!("map_{:08x}_{}.bin.zst", hash, map_blob.len());
    Some(dir.join(name))
}

fn load_cached_map(map_blob: &[u8]) -> Option<MapData> {
    let path = map_cache_path(map_blob)?;
    let data = std::fs::read(&path).ok()?;
    let decoded = zstd::stream::decode_all(data.as_slice()).ok()?;
    bincode::deserialize::<MapData>(&decoded).ok()
}

fn store_cached_map(map_blob: &[u8], map: &MapData) {
    let Some(path) = map_cache_path(map_blob) else {
        return;
    };
    if let Some(parent) = path.parent() {
        if std::fs::create_dir_all(parent).is_err() {
            return;
        }
    }
    let Ok(encoded) = bincode::serialize(map) else {
        return;
    };
    let Ok(compressed) = zstd::stream::encode_all(encoded.as_slice(), 3) else {
        return;
    };
    let _ = std::fs::write(path, compressed);
}
