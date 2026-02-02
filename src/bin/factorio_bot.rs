use std::io::{BufRead, BufReader, Write};
use std::fs::OpenOptions;
use std::os::unix::net::UnixStream;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand};
use serde_json::json;

use factorio_client::daemon::{self, Request, Response};

#[derive(Parser)]
#[command(name = "factorio-bot")]
#[command(about = "CLI tool for controlling a Factorio bot")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Connect {
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value = "34197")]
        port: u16,
        #[arg(long)]
        username: Option<String>,
    },
    Disconnect,
    Status {
        #[arg(long)]
        watch: bool,
        #[arg(long, default_value_t = 500)]
        interval_ms: u64,
        #[arg(long)]
        wait_map: bool,
    },
    ActionStatus {
        #[arg(long)]
        id: Option<u64>,
    },
    Position,
    State {
        #[arg(long)]
        radius: Option<f64>,
        #[arg(long)]
        max_entities: Option<u64>,
    },
    Walk {
        #[arg(long, default_value = "0")]
        direction: u8,
    },
    Stop,
    MoveTo {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
        #[arg(long)]
        tolerance: Option<f64>,
        #[arg(long)]
        max_nodes: Option<u64>,
        #[arg(long, default_value_t = true)]
        blocking: bool,
        #[arg(long)]
        timeout_ms: Option<u64>,
    },
    FindPath {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
        #[arg(long)]
        max_nodes: Option<u64>,
        #[arg(long)]
        max_points: Option<u64>,
    },
    Mine {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
    },
    StopMining,
    Craft {
        #[arg(long)]
        recipe_id: Option<u16>,
        #[arg(long)]
        recipe: Option<String>,
        #[arg(long, default_value = "1")]
        count: u32,
    },
    Research {
        #[arg(long)]
        technology_id: Option<u16>,
        #[arg(long)]
        technology: Option<String>,
    },
    CancelCraft {
        #[arg(long, default_value = "1")]
        index: u16,
        #[arg(long, default_value = "1")]
        count: u32,
    },
    SelectEntity {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
    },
    ClearSelection,
    ClearCursor,
    Inspect {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
        #[arg(long)]
        radius: Option<f64>,
    },
    ScanArea {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
        #[arg(long)]
        radius: f64,
        #[arg(long)]
        max_entities: Option<u64>,
    },
    FindNearest {
        query: String,
        #[arg(long)]
        max_radius: Option<f64>,
    },
    DropItem {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
    },
    Drop {
        #[arg(long)]
        x: Option<f64>,
        #[arg(long)]
        y: Option<f64>,
        #[arg(long)]
        item_id: Option<u16>,
        #[arg(long)]
        item: Option<String>,
        #[arg(long)]
        quality_id: Option<u8>,
        #[arg(long)]
        stack_id: Option<u64>,
        #[arg(long)]
        kind: Option<u8>,
        #[arg(long)]
        inventory: Option<u8>,
        #[arg(long)]
        inventory_name: Option<String>,
        #[arg(long)]
        slot: u16,
        #[arg(long)]
        source: Option<u8>,
        #[arg(long)]
        source_name: Option<String>,
        #[arg(long)]
        keep_cursor: bool,
    },
    Pickup {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
        #[arg(long)]
        hold_ms: Option<u64>,
    },
    UseItem {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
    },
    ServerCommand {
        command: String,
    },
    Eval {
        code: String,
    },
    Reload,
    TestReload,
    SetGhostCursor {
        #[arg(long)]
        item_id: Option<u16>,
        #[arg(long)]
        item: Option<String>,
        #[arg(long)]
        quality_id: Option<u8>,
    },
    PlaceGhost {
        #[arg(long)]
        item_id: Option<u16>,
        #[arg(long)]
        item: Option<String>,
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
        #[arg(long, default_value = "0")]
        direction: u8,
        #[arg(long)]
        quality_id: Option<u8>,
    },
    BuildBlueprint {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
        #[arg(long, default_value = "0")]
        direction: u8,
        blueprint: String,
        #[arg(long)]
        flags: Option<u16>,
        #[arg(long)]
        mode: Option<u8>,
        #[arg(long)]
        keep_cursor: bool,
    },
    Recipes,
    Techs,
    Alerts {
        #[arg(long)]
        radius: Option<f64>,
    },
    Spawn,
    GetTrain {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
    },
    SetTrainSchedule {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
        stations: Vec<String>,
    },
    TrainGo {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
        #[arg(long)]
        station: Option<String>,
    },
    SetCombinator {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
        params: String,
    },
    GetSignals {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
        #[arg(long)]
        wire: Option<String>,
    },
    PowerStatus {
        #[arg(long)]
        x: Option<f64>,
        #[arg(long)]
        y: Option<f64>,
    },
    LogisticsStatus {
        #[arg(long)]
        x: Option<f64>,
        #[arg(long)]
        y: Option<f64>,
    },
    Pollution {
        #[arg(long)]
        x: Option<f64>,
        #[arg(long)]
        y: Option<f64>,
    },
    Build {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
        #[arg(long, default_value = "0")]
        direction: u8,
    },
    Place {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
        #[arg(long, default_value = "0")]
        direction: u8,
        #[arg(long)]
        item_id: Option<u16>,
        #[arg(long)]
        item: Option<String>,
        #[arg(long)]
        quality_id: Option<u8>,
        #[arg(long)]
        stack_id: Option<u64>,
        #[arg(long)]
        kind: Option<u8>,
        #[arg(long)]
        inventory: Option<u8>,
        #[arg(long)]
        inventory_name: Option<String>,
        #[arg(long)]
        slot: u16,
        #[arg(long)]
        source: Option<u8>,
        #[arg(long)]
        source_name: Option<String>,
        #[arg(long)]
        keep_cursor: bool,
    },
    SetRecipe {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
        #[arg(long)]
        recipe_id: Option<u16>,
        #[arg(long)]
        recipe: Option<String>,
        #[arg(long)]
        quality_id: Option<u8>,
    },
    Rotate {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
        #[arg(long, default_value_t = false)]
        reverse: bool,
    },
    CopySettings {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
    },
    PasteSettings {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
    },
    RemoveCables {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
    },
    Filter {
        #[arg(long)]
        slot: u16,
        #[arg(long)]
        item_id: Option<u16>,
        #[arg(long)]
        item: Option<String>,
        #[arg(long)]
        inventory: Option<u8>,
        #[arg(long)]
        inventory_name: Option<String>,
        #[arg(long)]
        source: Option<u8>,
        #[arg(long)]
        source_name: Option<String>,
        #[arg(long)]
        kind: Option<u8>,
        #[arg(long)]
        quality_id: Option<u8>,
        #[arg(long)]
        quality_extra: Option<u8>,
        #[arg(long)]
        x: Option<f64>,
        #[arg(long)]
        y: Option<f64>,
    },
    ClearFilter {
        #[arg(long)]
        slot: u16,
        #[arg(long)]
        inventory: Option<u8>,
        #[arg(long)]
        inventory_name: Option<String>,
        #[arg(long)]
        source: Option<u8>,
        #[arg(long)]
        source_name: Option<String>,
        #[arg(long)]
        kind: Option<u8>,
        #[arg(long)]
        x: Option<f64>,
        #[arg(long)]
        y: Option<f64>,
    },
    SetLogistics {
        #[arg(long)]
        slot: u16,
        #[arg(long)]
        item_id: Option<u16>,
        #[arg(long)]
        item: Option<String>,
        #[arg(long)]
        min: u32,
        #[arg(long)]
        max: Option<u32>,
        #[arg(long)]
        mode: Option<u32>,
        #[arg(long)]
        signal_id: Option<u16>,
        #[arg(long)]
        signal_type: Option<u8>,
        #[arg(long)]
        section: Option<u8>,
        #[arg(long)]
        section_type: Option<u8>,
        #[arg(long)]
        space_location_id: Option<u16>,
        #[arg(long)]
        quality_id: Option<u8>,
        #[arg(long)]
        quality_extra: Option<u8>,
        #[arg(long)]
        x: Option<f64>,
        #[arg(long)]
        y: Option<f64>,
        #[arg(long)]
        open_gui: bool,
    },
    CursorTransfer {
        #[arg(long)]
        item_id: Option<u16>,
        #[arg(long)]
        item: Option<String>,
        #[arg(long)]
        quality_id: Option<u8>,
        #[arg(long)]
        stack_id: Option<u64>,
        #[arg(long, default_value = "0")]
        kind: u8,
        #[arg(long, default_value = "0")]
        inventory: u8,
        #[arg(long)]
        inventory_name: Option<String>,
        #[arg(long)]
        slot: u16,
        #[arg(long, default_value = "0")]
        source: u8,
        #[arg(long)]
        source_name: Option<String>,
    },
    CursorSplit {
        #[arg(long)]
        item_id: Option<u16>,
        #[arg(long)]
        item: Option<String>,
        #[arg(long)]
        quality_id: Option<u8>,
        #[arg(long)]
        stack_id: Option<u64>,
        #[arg(long, default_value = "0")]
        kind: u8,
        #[arg(long, default_value = "0")]
        inventory: u8,
        #[arg(long)]
        inventory_name: Option<String>,
        #[arg(long)]
        slot: u16,
        #[arg(long, default_value = "0")]
        source: u8,
        #[arg(long)]
        source_name: Option<String>,
    },
    StackTransfer {
        #[arg(long)]
        item_id: Option<u16>,
        #[arg(long)]
        item: Option<String>,
        #[arg(long)]
        quality_id: Option<u8>,
        #[arg(long)]
        stack_id: Option<u64>,
        #[arg(long)]
        mode: Option<u8>,
        #[arg(long, default_value = "0")]
        kind: u8,
        #[arg(long, default_value = "0")]
        inventory: u8,
        #[arg(long)]
        inventory_name: Option<String>,
        #[arg(long)]
        slot: u16,
        #[arg(long, default_value = "0")]
        source: u8,
        #[arg(long)]
        source_name: Option<String>,
    },
    InventoryTransfer {
        #[arg(long)]
        item_id: Option<u16>,
        #[arg(long)]
        item: Option<String>,
        #[arg(long)]
        quality_id: Option<u8>,
        #[arg(long)]
        stack_id: Option<u64>,
        #[arg(long)]
        mode: Option<u8>,
        #[arg(long, default_value = "0")]
        kind: u8,
        #[arg(long, default_value = "0")]
        inventory: u8,
        #[arg(long)]
        inventory_name: Option<String>,
        #[arg(long)]
        slot: u16,
        #[arg(long, default_value = "0")]
        source: u8,
        #[arg(long)]
        source_name: Option<String>,
    },
    StackSplit {
        #[arg(long)]
        item_id: Option<u16>,
        #[arg(long)]
        item: Option<String>,
        #[arg(long)]
        quality_id: Option<u8>,
        #[arg(long)]
        stack_id: Option<u64>,
        #[arg(long)]
        mode: Option<u8>,
        #[arg(long, default_value = "0")]
        kind: u8,
        #[arg(long, default_value = "0")]
        inventory: u8,
        #[arg(long)]
        inventory_name: Option<String>,
        #[arg(long)]
        slot: u16,
        #[arg(long, default_value = "0")]
        source: u8,
        #[arg(long)]
        source_name: Option<String>,
    },
    InventorySplit {
        #[arg(long)]
        item_id: Option<u16>,
        #[arg(long)]
        item: Option<String>,
        #[arg(long)]
        quality_id: Option<u8>,
        #[arg(long)]
        stack_id: Option<u64>,
        #[arg(long)]
        mode: Option<u8>,
        #[arg(long, default_value = "0")]
        kind: u8,
        #[arg(long, default_value = "0")]
        inventory: u8,
        #[arg(long)]
        inventory_name: Option<String>,
        #[arg(long)]
        slot: u16,
        #[arg(long, default_value = "0")]
        source: u8,
        #[arg(long)]
        source_name: Option<String>,
    },
    FastTransfer {
        #[arg(long)]
        from_player: Option<bool>,
        #[arg(long, default_value = "1")]
        count: u64,
        #[arg(long)]
        x: Option<f64>,
        #[arg(long)]
        y: Option<f64>,
    },
    FastSplit {
        #[arg(long)]
        from_player: Option<bool>,
        #[arg(long, default_value = "1")]
        count: u64,
        #[arg(long)]
        x: Option<f64>,
        #[arg(long)]
        y: Option<f64>,
    },
    Insert {
        #[arg(long)]
        x: Option<f64>,
        #[arg(long)]
        y: Option<f64>,
        #[arg(long)]
        split: bool,
        #[arg(long, default_value = "1")]
        count: u64,
        #[arg(long)]
        item_id: Option<u16>,
        #[arg(long)]
        item: Option<String>,
        #[arg(long)]
        quality_id: Option<u8>,
        #[arg(long)]
        stack_id: Option<u64>,
        #[arg(long)]
        mode: Option<u8>,
        #[arg(long)]
        kind: Option<u8>,
        #[arg(long)]
        inventory: Option<u8>,
        #[arg(long)]
        inventory_name: Option<String>,
        #[arg(long)]
        slot: Option<u16>,
        #[arg(long)]
        source: Option<u8>,
        #[arg(long)]
        source_name: Option<String>,
    },
    Extract {
        #[arg(long)]
        x: Option<f64>,
        #[arg(long)]
        y: Option<f64>,
        #[arg(long)]
        split: bool,
        #[arg(long, default_value = "1")]
        count: u64,
        #[arg(long)]
        item_id: Option<u16>,
        #[arg(long)]
        item: Option<String>,
        #[arg(long)]
        quality_id: Option<u8>,
        #[arg(long)]
        stack_id: Option<u64>,
        #[arg(long)]
        mode: Option<u8>,
        #[arg(long)]
        kind: Option<u8>,
        #[arg(long)]
        inventory: Option<u8>,
        #[arg(long)]
        inventory_name: Option<String>,
        #[arg(long)]
        slot: Option<u16>,
        #[arg(long)]
        source: Option<u8>,
        #[arg(long)]
        source_name: Option<String>,
    },
    Equip {
        #[arg(long)]
        item_id: Option<u16>,
        #[arg(long)]
        item: Option<String>,
        #[arg(long)]
        quality_id: Option<u8>,
        #[arg(long)]
        stack_id: Option<u64>,
        #[arg(long)]
        from_slot: u16,
        #[arg(long)]
        from_inventory: Option<u8>,
        #[arg(long)]
        from_inventory_name: Option<String>,
        #[arg(long)]
        to_slot: Option<u16>,
        #[arg(long)]
        to_inventory: Option<u8>,
        #[arg(long)]
        to_inventory_name: Option<String>,
        #[arg(long)]
        source: Option<u8>,
        #[arg(long)]
        source_name: Option<String>,
    },
    LaunchRocket {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
    },
    Shoot {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
    },
    StopShoot,
    ToggleDriving,
    Drive {
        #[arg(long, default_value = "accelerate")]
        acceleration: String,
        #[arg(long, default_value = "straight")]
        direction: String,
    },
    EnterVehicle {
        #[arg(long)]
        x: Option<f64>,
        #[arg(long)]
        y: Option<f64>,
    },
    ExitVehicle,
    ConnectWire {
        #[arg(long)]
        x1: f64,
        #[arg(long)]
        y1: f64,
        #[arg(long)]
        x2: f64,
        #[arg(long)]
        y2: f64,
        #[arg(long)]
        wire: Option<String>,
        #[arg(long)]
        wire_item_id: Option<u16>,
        #[arg(long)]
        wire_item: Option<String>,
        #[arg(long)]
        wire_slot: Option<u16>,
        #[arg(long)]
        wire_inventory: Option<u8>,
        #[arg(long)]
        wire_inventory_name: Option<String>,
        #[arg(long)]
        wire_source: Option<u8>,
        #[arg(long)]
        wire_source_name: Option<String>,
        #[arg(long)]
        wire_quality_id: Option<u8>,
        #[arg(long)]
        wire_stack_id: Option<u64>,
        #[arg(long)]
        wire_kind: Option<u8>,
        #[arg(long)]
        wire_keep_cursor: bool,
    },
    DisconnectWire {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
        #[arg(long)]
        wire: Option<String>,
    },
    Deconstruct {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
        #[arg(long)]
        radius: Option<f64>,
    },
    CancelDeconstruct {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
        #[arg(long)]
        radius: Option<f64>,
    },
    Chat {
        message: String,
    },
}

fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Connect { host, port, username } => {
            let username = username
                .and_then(|name| {
                    let trimmed = name.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_string())
                    }
                })
                .unwrap_or_else(random_username);
            start_daemon(&host, port, &username)
        }
        Commands::Disconnect => stop_daemon(),
        cmd @ Commands::MoveTo { blocking, timeout_ms, .. } => match send_command(cmd) {
            Ok(response) => {
                if blocking && response.success {
                    let action_id = response
                        .result
                        .as_ref()
                        .and_then(|v| v.get("action_id"))
                        .and_then(|v| v.as_u64());
                    match wait_for_action_complete(action_id, timeout_ms) {
                        Ok(action_status) => match send_command(Commands::Position) {
                            Ok(pos_resp) => {
                                let position = pos_resp.result.unwrap_or_else(|| json!({}));
                                let mut result = response.result.unwrap_or_else(|| json!({}));
                                let mut success = response.success;
                                let mut error = response.error.clone();
                                if let Some(obj) = result.as_object_mut() {
                                    obj.insert("position".to_string(), position);
                                    if let Some(status) = action_status.clone() {
                                        if let Some(result_str) = status.get("result").and_then(|v| v.as_str()) {
                                            if result_str != "arrived" {
                                                success = false;
                                                error = Some(format!("Move-to {}", result_str));
                                            }
                                        }
                                        obj.insert("action_status".to_string(), status);
                                    }
                                }
                                Ok(Response {
                                    id: response.id,
                                    success,
                                    result: Some(result),
                                    error,
                                })
                            }
                            Err(e) => Err(e),
                        },
                        Err(e) => Err(e),
                    }
                } else {
                    Ok(response)
                }
            }
            Err(e) => Err(e),
        },
        cmd @ Commands::Status { watch, interval_ms, wait_map } => {
            if watch || wait_map {
                watch_status(interval_ms, wait_map, watch)
            } else {
                match send_command(cmd) {
                    Ok(response) => {
                        if response.success {
                            if let Some(result) = response.result.as_ref() {
                                if let Some(line) = build_map_progress_line(result) {
                                    eprintln!("{}", line);
                                }
                            }
                        }
                        Ok(response)
                    }
                    Err(e) => Err(e),
                }
            }
        }
        cmd => send_command(cmd),
    };

    match result {
        Ok(response) => {
            println!("{}", serde_json::to_string_pretty(&response).unwrap());
            if !response.success {
                std::process::exit(1);
            }
        }
        Err(e) => {
            let err = json!({
                "success": false,
                "error": e.to_string()
            });
            println!("{}", serde_json::to_string_pretty(&err).unwrap());
            std::process::exit(1);
        }
    }
}

fn random_username() -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let pid = std::process::id();
    let mut buf = [0u8; 6];
    let suffix = if getrandom::getrandom(&mut buf).is_ok() {
        let mut out = String::with_capacity(buf.len() * 2);
        for b in buf {
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0x0f) as usize] as char);
        }
        out
    } else {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        format!("{:08x}", nanos)
    };
    format!("FactorioBot-{:04x}-{}", pid & 0xffff, suffix)
}

fn start_daemon(host: &str, port: u16, username: &str) -> Result<Response, Box<dyn std::error::Error>> {
    let socket_path = daemon::socket_path();
    let log_path = socket_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("daemon.log");

    if socket_path.exists() {
        if UnixStream::connect(&socket_path).is_ok() {
            if let Ok(Some(status)) = probe_daemon_status(&socket_path) {
                if status.get("connected").and_then(|v| v.as_bool()).unwrap_or(false) {
                    return Ok(Response {
                        id: "connect".into(),
                        success: true,
                        result: Some(json!({"message": "Already connected"})),
                        error: None,
                    });
                }
            }
            let _ = std::fs::remove_file(&socket_path);
        }
        let _ = std::fs::remove_file(&socket_path);
    }

    let exe = std::env::current_exe()?
        .parent()
        .unwrap()
        .join("factorio-daemon");

    if let Some(parent) = log_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    let log_file_err = log_file.try_clone()?;

    let mut child = Command::new(&exe)
        .args(["--host", host, "--port", &port.to_string(), "--username", username])
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_file_err))
        .spawn()?;

    for _ in 0..200 {
        std::thread::sleep(std::time::Duration::from_millis(100));
        if socket_path.exists() {
            if UnixStream::connect(&socket_path).is_ok() {
                return Ok(Response {
                    id: "connect".into(),
                    success: true,
                    result: Some(json!({"message": "Connected"})),
                    error: None,
                });
            }
        }
        if let Some(status) = child.try_wait()? {
            let tail = std::fs::read_to_string(&log_path)
                .ok()
                .map(|content| {
                    let lines: Vec<&str> = content.lines().collect();
                    let start = lines.len().saturating_sub(40);
                    lines[start..].join("\n")
                })
                .unwrap_or_else(|| "no daemon log available".to_string());
            return Err(format!(
                "Daemon exited early ({}) while starting. See {}.\n{}",
                status,
                log_path.display(),
                tail
            ).into());
        }
    }

    let tail = std::fs::read_to_string(&log_path)
        .ok()
        .map(|content| {
            let lines: Vec<&str> = content.lines().collect();
            let start = lines.len().saturating_sub(40);
            lines[start..].join("\n")
        })
        .unwrap_or_else(|| "no daemon log available".to_string());

    Err(format!(
        "Daemon failed to start. See {}.\n{}",
        log_path.display(),
        tail
    ).into())
}

fn probe_daemon_status(socket_path: &std::path::Path) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error>> {
    let mut stream = UnixStream::connect(socket_path)?;
    stream.set_read_timeout(Some(std::time::Duration::from_millis(300)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_millis(300)))?;
    let request = Request {
        id: "connect_probe".into(),
        command: "status".into(),
        args: json!({}),
    };
    let json = serde_json::to_string(&request)?;
    writeln!(&stream, "{}", json)?;
    stream.flush()?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        return Ok(None);
    }
    let response: Response = serde_json::from_str(&line)?;
    Ok(response.result)
}

fn stop_daemon() -> Result<Response, Box<dyn std::error::Error>> {
    let pid_path = daemon::pid_path();

    if pid_path.exists() {
        let pid: i32 = std::fs::read_to_string(&pid_path)?.trim().parse()?;
        unsafe {
            libc::kill(pid, libc::SIGTERM);
        }
        let _ = std::fs::remove_file(&pid_path);
    }

    let socket_path = daemon::socket_path();
    let _ = std::fs::remove_file(&socket_path);

    Ok(Response {
        id: "disconnect".into(),
        success: true,
        result: Some(json!({"message": "Disconnected"})),
        error: None,
    })
}

fn send_command(cmd: Commands) -> Result<Response, Box<dyn std::error::Error>> {
    let socket_path = daemon::socket_path();

    let mut stream = UnixStream::connect(&socket_path)
        .map_err(|_| "Not connected. Run 'factorio-bot connect' first.")?;

    let (command, args) = build_request(cmd)?;

    let request = Request {
        id: format!("cli_{}", std::process::id()),
        command: command.into(),
        args,
    };

    let json = serde_json::to_string(&request)?;
    writeln!(stream, "{}", json)?;
    stream.flush()?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line)?;

    let response: Response = serde_json::from_str(&line)?;
    Ok(response)
}

fn wait_for_action_complete(
    action_id: Option<u64>,
    timeout_ms: Option<u64>,
) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    loop {
        let response = send_command(Commands::ActionStatus { id: action_id })?;
        if !response.success {
            return Err(response.error.unwrap_or_else(|| "Action status failed".to_string()).into());
        }
        if let Some(payload) = response.result.clone() {
            let completed = payload
                .get("completed")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if completed {
                return Ok(Some(payload));
            }
        }
        if let Some(limit) = timeout_ms {
            if start.elapsed() >= std::time::Duration::from_millis(limit) {
                return Err("Move-to timed out".into());
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

fn watch_status(
    interval_ms: u64,
    wait_map: bool,
    watch: bool,
) -> Result<Response, Box<dyn std::error::Error>> {
    let mut last_line = String::new();
    let mut last_print = Instant::now().checked_sub(Duration::from_secs(5)).unwrap_or_else(Instant::now);
    let mut spinner_idx = 0usize;
    let spinner_chars: [char; 4] = ['|', '/', '-', '\\'];
    let poll = Duration::from_millis(interval_ms.max(50));
    loop {
        let response = send_command(Commands::Status {
            watch: false,
            interval_ms: 0,
            wait_map: false,
        })?;

        if !response.success {
            return Ok(response);
        }

        if let Some(result) = response.result.as_ref() {
            if let Some(base_line) = build_map_progress_line(result) {
                let now = Instant::now();
                if base_line != last_line {
                    eprintln!("{}", base_line);
                    last_line = base_line;
                    last_print = now;
                } else if now.duration_since(last_print) >= Duration::from_secs(1) {
                    spinner_idx = (spinner_idx + 1) % spinner_chars.len();
                    let mut line = base_line.clone();
                    line.push(' ');
                    line.push(spinner_chars[spinner_idx]);
                    eprintln!("{}", line);
                    last_print = now;
                }
            }

            if watch && !wait_map {
                println!("{}", serde_json::to_string_pretty(&response).unwrap());
            }

            if wait_map {
                let map_ready = result
                    .get("map_ready")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let map_parsing = result
                    .get("map_parsing")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if map_ready {
                    return Ok(response);
                }
                if !map_parsing {
                    return Ok(response);
                }
            } else if !watch {
                return Ok(response);
            }
        }

        std::thread::sleep(poll);
    }
}

fn build_map_progress_line(status: &serde_json::Value) -> Option<String> {
    if !status
        .get("map_parsing")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        return None;
    }

    let stage = status
        .get("map_parse_stage")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let pct = status
        .get("map_parse_progress")
        .and_then(|v| v.as_f64())
        .map(|p| p * 100.0);
    let elapsed = status
        .get("map_parse_elapsed_ms")
        .and_then(|v| v.as_u64());

    let counts = status.get("map_parse_counts");
    let (done, total, resources_cur, resources_len) = counts
        .and_then(|c| {
            let done = c.get("done")?.as_u64()?;
            let total = c.get("total")?.as_u64()?;
            let res = c.get("resources");
            let res_cur = res.and_then(|r| r.get("current")).and_then(|v| v.as_u64());
            let res_len = res
                .and_then(|r| r.get("current_len"))
                .and_then(|v| v.as_u64());
            Some((done, total, res_cur, res_len))
        })
        .unwrap_or((0, 0, None, None));

    let pct_str = pct
        .map(|p| format!("{:.1}%", p))
        .unwrap_or_else(|| "?%".to_string());
    let bar = if total > 0 {
        let width = 20usize;
        let filled = ((done as f64 / total as f64) * width as f64).round() as usize;
        let mut buf = String::with_capacity(width);
        for i in 0..width {
            buf.push(if i < filled { '#' } else { '-' });
        }
        buf
    } else {
        "????????????????????".to_string()
    };
    let mut line = format!(
        "map parse {} [{}] stage={} ({}/{})",
        pct_str, bar, stage, done, total
    );
    if let Some(ms) = elapsed {
        line.push_str(&format!(" elapsed={}ms", ms));
    }
    if let Some(cur) = resources_cur {
        line.push_str(&format!(" resources_chunk={}", cur));
        if let Some(len) = resources_len {
            line.push_str(&format!(" len={}", len));
        }
    }
    Some(line)
}

fn build_request(cmd: Commands) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    match cmd {
        Commands::Position | Commands::Status { .. } | Commands::ActionStatus { .. } | Commands::State { .. } |
        Commands::Recipes | Commands::Techs | Commands::Alerts { .. } | Commands::GetTrain { .. } |
        Commands::GetSignals { .. } | Commands::PowerStatus { .. } | Commands::LogisticsStatus { .. } |
        Commands::Pollution { .. } | Commands::Chat { .. } => build_status_request(cmd),
        Commands::Walk { .. } | Commands::Stop | Commands::MoveTo { .. } |
        Commands::FindPath { .. } | Commands::Mine { .. } | Commands::StopMining => {
            build_movement_request(cmd)
        }
        Commands::Craft { .. } | Commands::Research { .. } | Commands::CancelCraft { .. } => {
            build_crafting_request(cmd)
        }
        Commands::SelectEntity { .. } | Commands::ClearSelection | Commands::ClearCursor |
        Commands::Inspect { .. } | Commands::ScanArea { .. } | Commands::FindNearest { .. } => {
            build_selection_request(cmd)
        }
        Commands::SetGhostCursor { .. } | Commands::PlaceGhost { .. } | Commands::BuildBlueprint { .. } |
        Commands::Build { .. } | Commands::Place { .. } | Commands::SetRecipe { .. } | Commands::Rotate { .. } |
        Commands::CopySettings { .. } | Commands::PasteSettings { .. } | Commands::RemoveCables { .. } => {
            build_build_request(cmd)
        }
        Commands::Filter { .. } | Commands::ClearFilter { .. } | Commands::SetLogistics { .. } |
        Commands::CursorTransfer { .. } | Commands::CursorSplit { .. } |
        Commands::StackTransfer { .. } | Commands::InventoryTransfer { .. } |
        Commands::StackSplit { .. } | Commands::InventorySplit { .. } |
        Commands::FastTransfer { .. } | Commands::FastSplit { .. } | Commands::Insert { .. } |
        Commands::Extract { .. } | Commands::Equip { .. } | Commands::Drop { .. } => build_inventory_request(cmd),
        Commands::DropItem { .. } | Commands::UseItem { .. } | Commands::LaunchRocket { .. } |
        Commands::Shoot { .. } | Commands::StopShoot | Commands::ToggleDriving | Commands::Drive { .. } |
        Commands::EnterVehicle { .. } | Commands::ExitVehicle | Commands::ConnectWire { .. } |
        Commands::DisconnectWire { .. } | Commands::Deconstruct { .. } | Commands::CancelDeconstruct { .. } |
        Commands::ServerCommand { .. } | Commands::Eval { .. } | Commands::Reload | Commands::TestReload |
        Commands::Pickup { .. } | Commands::Spawn | Commands::SetTrainSchedule { .. } |
        Commands::TrainGo { .. } | Commands::SetCombinator { .. } => {
            build_action_request(cmd)
        }
        _ => Err("Invalid command".into()),
    }
}

fn build_status_request(cmd: Commands) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    match cmd {
        Commands::Position => Ok(("position".into(), json!({}))),
        Commands::Status { .. } => Ok(("status".into(), json!({}))),
        Commands::ActionStatus { id } => {
            let mut args = json!({});
            if let Some(value) = id {
                args["id"] = json!(value);
            }
            Ok(("action-status".into(), args))
        }
        Commands::State { radius, max_entities } => {
            let mut args = json!({});
            if let Some(r) = radius {
                args["radius"] = json!(r);
            }
            if let Some(m) = max_entities {
                args["max_entities"] = json!(m);
            }
            Ok(("state".into(), args))
        }
        Commands::Recipes => Ok(("recipes".into(), json!({}))),
        Commands::Techs => Ok(("techs".into(), json!({}))),
        Commands::Alerts { radius } => {
            let mut args = json!({});
            if let Some(r) = radius {
                args["radius"] = json!(r);
            }
            Ok(("alerts".into(), args))
        }
        Commands::GetTrain { x, y } => Ok(("get-train".into(), json!({"x": x, "y": y}))),
        Commands::GetSignals { x, y, wire } => {
            let mut args = json!({"x": x, "y": y});
            if let Some(w) = wire {
                args["wire"] = json!(w);
            }
            Ok(("get-signals".into(), args))
        }
        Commands::PowerStatus { x, y } => {
            let mut args = json!({});
            if let Some(px) = x {
                args["x"] = json!(px);
            }
            if let Some(py) = y {
                args["y"] = json!(py);
            }
            Ok(("power-status".into(), args))
        }
        Commands::LogisticsStatus { x, y } => {
            let mut args = json!({});
            if let Some(px) = x {
                args["x"] = json!(px);
            }
            if let Some(py) = y {
                args["y"] = json!(py);
            }
            Ok(("logistics-status".into(), args))
        }
        Commands::Pollution { x, y } => {
            let mut args = json!({});
            if let Some(px) = x {
                args["x"] = json!(px);
            }
            if let Some(py) = y {
                args["y"] = json!(py);
            }
            Ok(("pollution".into(), args))
        }
        Commands::Chat { message } => Ok(("chat".into(), json!({"message": message}))),
        _ => Err("Invalid command".into()),
    }
}

fn build_movement_request(cmd: Commands) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    match cmd {
        Commands::Walk { direction } => Ok(("walk".into(), json!({"direction": direction}))),
        Commands::Stop => Ok(("stop".into(), json!({}))),
        Commands::MoveTo { x, y, tolerance, max_nodes, blocking, timeout_ms } => {
            let mut args = json!({"x": x, "y": y, "blocking": blocking});
            if let Some(t) = tolerance {
                args["tolerance"] = json!(t);
            }
            if let Some(m) = max_nodes {
                args["max_nodes"] = json!(m);
            }
            if let Some(ms) = timeout_ms {
                args["timeout_ms"] = json!(ms);
            }
            Ok(("move-to".into(), args))
        }
        Commands::FindPath { x, y, max_nodes, max_points } => {
            let mut args = json!({"x": x, "y": y});
            if let Some(m) = max_nodes {
                args["max_nodes"] = json!(m);
            }
            if let Some(p) = max_points {
                args["max_points"] = json!(p);
            }
            Ok(("find-path".into(), args))
        }
        Commands::Mine { x, y } => Ok(("mine".into(), json!({"x": x, "y": y}))),
        Commands::StopMining => Ok(("stop-mining".into(), json!({}))),
        _ => Err("Invalid command".into()),
    }
}

fn build_crafting_request(cmd: Commands) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    match cmd {
        Commands::Craft { recipe_id, recipe, count } => {
            if recipe_id.is_none() && recipe.is_none() {
                return Err("craft requires --recipe-id or --recipe".into());
            }
            let mut args = json!({"count": count});
            if let Some(id) = recipe_id {
                args["recipe_id"] = json!(id);
            }
            if let Some(name) = recipe {
                args["recipe"] = json!(name);
            }
            Ok(("craft".into(), args))
        }
        Commands::Research { technology_id, technology } => {
            if technology_id.is_none() && technology.is_none() {
                return Err("research requires --technology-id or --technology".into());
            }
            let mut args = json!({});
            if let Some(id) = technology_id {
                args["technology_id"] = json!(id);
            }
            if let Some(name) = technology {
                args["technology"] = json!(name);
            }
            Ok(("research".into(), args))
        }
        Commands::CancelCraft { index, count } => {
            Ok(("cancel-craft".into(), json!({"index": index, "count": count})))
        }
        _ => Err("Invalid command".into()),
    }
}

fn build_selection_request(cmd: Commands) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    match cmd {
        Commands::SelectEntity { x, y } => Ok(("select-entity".into(), json!({"x": x, "y": y}))),
        Commands::ClearSelection => Ok(("clear-selection".into(), json!({}))),
        Commands::ClearCursor => Ok(("clear-cursor".into(), json!({}))),
        Commands::Inspect { x, y, radius } => {
            let mut args = json!({"x": x, "y": y});
            if let Some(r) = radius {
                args["radius"] = json!(r);
            }
            Ok(("inspect".into(), args))
        }
        Commands::ScanArea { x, y, radius, max_entities } => {
            let mut args = json!({"x": x, "y": y, "radius": radius});
            if let Some(m) = max_entities {
                args["max_entities"] = json!(m);
            }
            Ok(("scan-area".into(), args))
        }
        Commands::FindNearest { query, max_radius } => {
            let mut args = json!({"query": query});
            if let Some(r) = max_radius {
                args["max_radius"] = json!(r);
            }
            Ok(("find-nearest".into(), args))
        }
        _ => Err("Invalid command".into()),
    }
}

fn build_build_request(cmd: Commands) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    match cmd {
        Commands::SetGhostCursor { item_id, item, quality_id } => {
            if item_id.is_none() && item.is_none() {
                return Err("set-ghost-cursor requires --item-id or --item".into());
            }
            let mut args = json!({});
            if let Some(id) = item_id {
                args["item_id"] = json!(id);
            }
            if let Some(name) = item {
                args["item"] = json!(name);
            }
            if let Some(q) = quality_id {
                args["quality_id"] = json!(q);
            }
            Ok(("set-ghost-cursor".into(), args))
        }
        Commands::PlaceGhost { item_id, item, x, y, direction, quality_id } => {
            if item_id.is_none() && item.is_none() {
                return Err("place-ghost requires --item-id or --item".into());
            }
            let mut args = json!({"x": x, "y": y, "direction": direction});
            if let Some(id) = item_id {
                args["item_id"] = json!(id);
            }
            if let Some(name) = item {
                args["item"] = json!(name);
            }
            if let Some(q) = quality_id {
                args["quality_id"] = json!(q);
            }
            Ok(("place-ghost".into(), args))
        }
        Commands::BuildBlueprint { x, y, direction, blueprint, flags, mode, keep_cursor } => {
            let mut args = json!({"x": x, "y": y, "direction": direction, "blueprint": blueprint});
            if let Some(v) = flags {
                args["flags"] = json!(v);
            }
            if let Some(v) = mode {
                args["mode"] = json!(v);
            }
            if keep_cursor {
                args["clear_cursor"] = json!(false);
            }
            Ok(("build-blueprint".into(), args))
        }
        Commands::Build { x, y, direction } => {
            Ok(("build".into(), json!({"x": x, "y": y, "direction": direction})))
        }
        Commands::Place {
            x,
            y,
            direction,
            item_id,
            item,
            quality_id,
            stack_id,
            kind,
            inventory,
            inventory_name,
            slot,
            source,
            source_name,
            keep_cursor,
        } => {
            if item_id.is_none() && item.is_none() {
                return Err("place requires --item-id or --item".into());
            }
            let mut args = json!({"x": x, "y": y, "direction": direction, "slot": slot});
            if let Some(id) = item_id {
                args["item_id"] = json!(id);
            }
            if let Some(name) = item {
                args["item"] = json!(name);
            }
            if let Some(q) = quality_id {
                args["quality_id"] = json!(q);
            }
            if let Some(s) = stack_id {
                args["stack_id"] = json!(s);
            }
            if let Some(k) = kind {
                args["kind"] = json!(k);
            }
            if let Some(inv) = parse_inventory_arg(inventory, inventory_name)? {
                args["inventory"] = json!(inv);
            }
            if let Some(src) = parse_source_arg(source, source_name)? {
                args["source"] = json!(src);
            }
            if keep_cursor {
                args["clear_cursor"] = json!(false);
            }
            Ok(("place".into(), args))
        }
        Commands::SetRecipe { x, y, recipe_id, recipe, quality_id } => {
            if recipe_id.is_none() && recipe.is_none() {
                return Err("set-recipe requires --recipe-id or --recipe".into());
            }
            let mut args = json!({"x": x, "y": y});
            if let Some(id) = recipe_id {
                args["recipe_id"] = json!(id);
            }
            if let Some(name) = recipe {
                args["recipe"] = json!(name);
            }
            if let Some(q) = quality_id {
                args["quality_id"] = json!(q);
            }
            Ok(("set-recipe".into(), args))
        }
        Commands::Rotate { x, y, reverse } => {
            Ok(("rotate".into(), json!({"x": x, "y": y, "reverse": reverse})))
        }
        Commands::CopySettings { x, y } => Ok(("copy-settings".into(), json!({"x": x, "y": y}))),
        Commands::PasteSettings { x, y } => Ok(("paste-settings".into(), json!({"x": x, "y": y}))),
        Commands::RemoveCables { x, y } => Ok(("remove-cables".into(), json!({"x": x, "y": y}))),
        _ => Err("Invalid command".into()),
    }
}

fn build_inventory_request(cmd: Commands) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    match cmd {
        Commands::Filter { .. } | Commands::ClearFilter { .. } => build_filter_request(cmd),
        Commands::SetLogistics { .. } => build_logistics_request(cmd),
        Commands::CursorTransfer { .. } | Commands::CursorSplit { .. } |
        Commands::StackTransfer { .. } | Commands::InventoryTransfer { .. } |
        Commands::StackSplit { .. } | Commands::InventorySplit { .. } => {
            build_transfer_request(cmd)
        }
        Commands::FastTransfer { .. } | Commands::FastSplit { .. } |
        Commands::Insert { .. } | Commands::Extract { .. } => build_fast_request(cmd),
        Commands::Equip { .. } => build_equip_request(cmd),
        Commands::Drop { .. } => build_drop_request(cmd),
        _ => Err("Invalid command".into()),
    }
}

fn build_filter_request(cmd: Commands) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    match cmd {
        Commands::Filter {
            slot,
            item_id,
            item,
            inventory,
            inventory_name,
            source,
            source_name,
            kind,
            quality_id,
            quality_extra,
            x,
            y,
        } => {
            if item_id.is_none() && item.is_none() {
                return Err("filter requires --item-id or --item".into());
            }
            let mut args = json!({"slot": slot});
            if let Some(id) = item_id {
                args["item_id"] = json!(id);
            }
            if let Some(name) = item {
                args["item"] = json!(name);
            }
            if let Some(inv) = parse_inventory_arg(inventory, inventory_name)? {
                args["inventory"] = json!(inv);
            }
            if let Some(src) = parse_source_arg(source, source_name)? {
                args["source"] = json!(src);
            }
            if let Some(k) = kind {
                args["kind"] = json!(k);
            }
            if let Some(q) = quality_id {
                args["quality_id"] = json!(q);
            }
            if let Some(q) = quality_extra {
                args["quality_extra"] = json!(q);
            }
            if let Some(px) = x {
                args["x"] = json!(px);
            }
            if let Some(py) = y {
                args["y"] = json!(py);
            }
            Ok(("filter".into(), args))
        }
        Commands::ClearFilter {
            slot,
            inventory,
            inventory_name,
            source,
            source_name,
            kind,
            x,
            y,
        } => {
            let mut args = json!({"slot": slot});
            if let Some(inv) = parse_inventory_arg(inventory, inventory_name)? {
                args["inventory"] = json!(inv);
            }
            if let Some(src) = parse_source_arg(source, source_name)? {
                args["source"] = json!(src);
            }
            if let Some(k) = kind {
                args["kind"] = json!(k);
            }
            if let Some(px) = x {
                args["x"] = json!(px);
            }
            if let Some(py) = y {
                args["y"] = json!(py);
            }
            Ok(("clear-filter".into(), args))
        }
        _ => Err("Invalid command".into()),
    }
}

fn build_logistics_request(cmd: Commands) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    match cmd {
        Commands::SetLogistics {
            slot,
            item_id,
            item,
            min,
            max,
            mode,
            signal_id,
            signal_type,
            section,
            section_type,
            space_location_id,
            quality_id,
            quality_extra,
            x,
            y,
            open_gui,
        } => {
            if item_id.is_none() && item.is_none() && signal_id.is_none() {
                return Err("set-logistics requires --item-id/--item or --signal-id".into());
            }
            let mut args = json!({"slot": slot, "min": min});
            if let Some(id) = item_id {
                args["item_id"] = json!(id);
            }
            if let Some(name) = item {
                args["item"] = json!(name);
            }
            if let Some(v) = max {
                args["max"] = json!(v);
            }
            if let Some(v) = mode {
                args["mode"] = json!(v);
            }
            if let Some(id) = signal_id {
                args["signal_id"] = json!(id);
            }
            if let Some(v) = signal_type {
                args["signal_type"] = json!(v);
            }
            if let Some(v) = section {
                args["section"] = json!(v);
            }
            if let Some(v) = section_type {
                args["section_type"] = json!(v);
            }
            if let Some(v) = space_location_id {
                args["space_location_id"] = json!(v);
            }
            if let Some(v) = quality_id {
                args["quality_id"] = json!(v);
            }
            if let Some(v) = quality_extra {
                args["quality_extra"] = json!(v);
            }
            if let Some(px) = x {
                args["x"] = json!(px);
            }
            if let Some(py) = y {
                args["y"] = json!(py);
            }
            if open_gui {
                args["open_gui"] = json!(true);
            }
            Ok(("set-logistics".into(), args))
        }
        _ => Err("Invalid command".into()),
    }
}

fn build_transfer_request(cmd: Commands) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    let mut args = json!({});
    let (command, item_id, item, quality_id, stack_id, mode, kind, inventory, inventory_name, slot, source, source_name) = match cmd {
        Commands::CursorTransfer { item_id, item, quality_id, stack_id, kind, inventory, inventory_name, slot, source, source_name } => {
            ("cursor-transfer", item_id, item, quality_id, stack_id, None, kind, inventory, inventory_name, slot, source, source_name)
        }
        Commands::CursorSplit { item_id, item, quality_id, stack_id, kind, inventory, inventory_name, slot, source, source_name } => {
            ("cursor-split", item_id, item, quality_id, stack_id, None, kind, inventory, inventory_name, slot, source, source_name)
        }
        Commands::StackTransfer { item_id, item, quality_id, stack_id, mode, kind, inventory, inventory_name, slot, source, source_name } => {
            ("stack-transfer", item_id, item, quality_id, stack_id, mode, kind, inventory, inventory_name, slot, source, source_name)
        }
        Commands::InventoryTransfer { item_id, item, quality_id, stack_id, mode, kind, inventory, inventory_name, slot, source, source_name } => {
            ("inventory-transfer", item_id, item, quality_id, stack_id, mode, kind, inventory, inventory_name, slot, source, source_name)
        }
        Commands::StackSplit { item_id, item, quality_id, stack_id, mode, kind, inventory, inventory_name, slot, source, source_name } => {
            ("stack-split", item_id, item, quality_id, stack_id, mode, kind, inventory, inventory_name, slot, source, source_name)
        }
        Commands::InventorySplit { item_id, item, quality_id, stack_id, mode, kind, inventory, inventory_name, slot, source, source_name } => {
            ("inventory-split", item_id, item, quality_id, stack_id, mode, kind, inventory, inventory_name, slot, source, source_name)
        }
        _ => return Err("Invalid command".into()),
    };

    if item_id.is_none() && item.is_none() {
        return Err(format!("{command} requires --item-id or --item").into());
    }

    args["kind"] = json!(kind);
    let inv = parse_inventory_arg(Some(inventory), inventory_name)?
        .unwrap_or(inventory);
    args["inventory"] = json!(inv);
    args["slot"] = json!(slot);
    let resolved_source = parse_source_arg(Some(source), source_name)?.unwrap_or(source);
    args["source"] = json!(resolved_source);

    if let Some(id) = item_id {
        args["item_id"] = json!(id);
    }
    if let Some(name) = item {
        args["item"] = json!(name);
    }
    if let Some(q) = quality_id {
        args["quality_id"] = json!(q);
    }
    if let Some(s) = stack_id {
        args["stack_id"] = json!(s);
    }
    if let Some(m) = mode {
        args["mode"] = json!(m);
    }

    Ok((command.into(), args))
}

struct InsertExtractArgs {
    x: Option<f64>,
    y: Option<f64>,
    split: bool,
    count: u64,
    item_id: Option<u16>,
    item: Option<String>,
    quality_id: Option<u8>,
    stack_id: Option<u64>,
    mode: Option<u8>,
    kind: Option<u8>,
    inventory: Option<u8>,
    inventory_name: Option<String>,
    slot: Option<u16>,
    source: Option<u8>,
    source_name: Option<String>,
}

fn build_insert_extract_request(
    command: &str,
    args: InsertExtractArgs,
) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    let mut out = json!({"split": args.split, "count": args.count});
    if let Some(px) = args.x {
        out["x"] = json!(px);
    }
    if let Some(py) = args.y {
        out["y"] = json!(py);
    }
    if let Some(id) = args.item_id {
        out["item_id"] = json!(id);
    }
    if let Some(name) = args.item {
        out["item"] = json!(name);
    }
    if let Some(q) = args.quality_id {
        out["quality_id"] = json!(q);
    }
    if let Some(s) = args.stack_id {
        out["stack_id"] = json!(s);
    }
    if let Some(m) = args.mode {
        out["mode"] = json!(m);
    }
    if let Some(k) = args.kind {
        out["kind"] = json!(k);
    }
    if let Some(inv) = parse_inventory_arg(args.inventory, args.inventory_name)? {
        out["inventory"] = json!(inv);
    }
    if let Some(sl) = args.slot {
        out["slot"] = json!(sl);
    }
    if let Some(src) = parse_source_arg(args.source, args.source_name)? {
        out["source"] = json!(src);
    }
    Ok((command.into(), out))
}

fn build_equip_request(cmd: Commands) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    match cmd {
        Commands::Equip {
            item_id,
            item,
            quality_id,
            stack_id,
            from_slot,
            from_inventory,
            from_inventory_name,
            to_slot,
            to_inventory,
            to_inventory_name,
            source,
            source_name,
        } => {
            if item_id.is_none() && item.is_none() {
                return Err("equip requires --item-id or --item".into());
            }
            let mut args = json!({"from_slot": from_slot});
            if let Some(id) = item_id {
                args["item_id"] = json!(id);
            }
            if let Some(name) = item {
                args["item"] = json!(name);
            }
            if let Some(q) = quality_id {
                args["quality_id"] = json!(q);
            }
            if let Some(s) = stack_id {
                args["stack_id"] = json!(s);
            }
            if let Some(inv) = parse_inventory_arg(from_inventory, from_inventory_name)? {
                args["from_inventory"] = json!(inv);
            }
            if let Some(inv) = parse_inventory_arg(to_inventory, to_inventory_name)? {
                args["to_inventory"] = json!(inv);
            }
            if let Some(slot) = to_slot {
                args["to_slot"] = json!(slot);
            }
            if let Some(src) = parse_source_arg(source, source_name)? {
                args["source"] = json!(src);
            }
            Ok(("equip".into(), args))
        }
        _ => Err("Invalid command".into()),
    }
}

fn build_drop_request(cmd: Commands) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    match cmd {
        Commands::Drop {
            x,
            y,
            item_id,
            item,
            quality_id,
            stack_id,
            kind,
            inventory,
            inventory_name,
            slot,
            source,
            source_name,
            keep_cursor,
        } => {
            if item_id.is_none() && item.is_none() {
                return Err("drop requires --item-id or --item".into());
            }
            let mut args = json!({"slot": slot});
            if let Some(px) = x {
                args["x"] = json!(px);
            }
            if let Some(py) = y {
                args["y"] = json!(py);
            }
            if let Some(id) = item_id {
                args["item_id"] = json!(id);
            }
            if let Some(name) = item {
                args["item"] = json!(name);
            }
            if let Some(q) = quality_id {
                args["quality_id"] = json!(q);
            }
            if let Some(s) = stack_id {
                args["stack_id"] = json!(s);
            }
            if let Some(k) = kind {
                args["kind"] = json!(k);
            }
            if let Some(inv) = parse_inventory_arg(inventory, inventory_name)? {
                args["inventory"] = json!(inv);
            }
            if let Some(src) = parse_source_arg(source, source_name)? {
                args["source"] = json!(src);
            }
            if keep_cursor {
                args["clear_cursor"] = json!(false);
            }
            Ok(("drop".into(), args))
        }
        _ => Err("Invalid command".into()),
    }
}

fn build_fast_request(cmd: Commands) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    match cmd {
        Commands::FastTransfer { from_player, count, x, y } => {
            let mut args = json!({"count": count});
            if let Some(fp) = from_player {
                args["from_player"] = json!(fp);
            }
            if let Some(px) = x {
                args["x"] = json!(px);
            }
            if let Some(py) = y {
                args["y"] = json!(py);
            }
            Ok(("fast-transfer".into(), args))
        }
        Commands::FastSplit { from_player, count, x, y } => {
            let mut args = json!({"count": count});
            if let Some(fp) = from_player {
                args["from_player"] = json!(fp);
            }
            if let Some(px) = x {
                args["x"] = json!(px);
            }
            if let Some(py) = y {
                args["y"] = json!(py);
            }
            Ok(("fast-split".into(), args))
        }
        Commands::Insert {
            x,
            y,
            split,
            count,
            item_id,
            item,
            quality_id,
            stack_id,
            mode,
            kind,
            inventory,
            inventory_name,
            slot,
            source,
            source_name,
        } => {
            build_insert_extract_request(
                "insert",
                InsertExtractArgs {
                    x,
                    y,
                    split,
                    count,
                    item_id,
                    item,
                    quality_id,
                    stack_id,
                    mode,
                    kind,
                    inventory,
                    inventory_name,
                    slot,
                    source,
                    source_name,
                },
            )
        }
        Commands::Extract {
            x,
            y,
            split,
            count,
            item_id,
            item,
            quality_id,
            stack_id,
            mode,
            kind,
            inventory,
            inventory_name,
            slot,
            source,
            source_name,
        } => {
            build_insert_extract_request(
                "extract",
                InsertExtractArgs {
                    x,
                    y,
                    split,
                    count,
                    item_id,
                    item,
                    quality_id,
                    stack_id,
                    mode,
                    kind,
                    inventory,
                    inventory_name,
                    slot,
                    source,
                    source_name,
                },
            )
        }
        _ => Err("Invalid command".into()),
    }
}

fn build_action_request(cmd: Commands) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    match cmd {
        Commands::DropItem { x, y } => Ok(("drop-item".into(), json!({"x": x, "y": y}))),
        Commands::UseItem { x, y } => Ok(("use-item".into(), json!({"x": x, "y": y}))),
        Commands::LaunchRocket { x, y } => Ok(("launch-rocket".into(), json!({"x": x, "y": y}))),
        Commands::Shoot { x, y } => Ok(("shoot".into(), json!({"x": x, "y": y}))),
        Commands::StopShoot => Ok(("stop-shoot".into(), json!({}))),
        Commands::ToggleDriving => Ok(("toggle-driving".into(), json!({}))),
        Commands::Drive { acceleration, direction } => {
            let accel = match acceleration.as_str() {
                "nothing" | "coast" | "0" => 0u8,
                "accelerate" | "accel" | "1" => 1u8,
                "brake" | "2" => 2u8,
                "reverse" | "3" => 3u8,
                _ => 1u8,
            };
            let dir = match direction.as_str() {
                "straight" | "0" => 0u8,
                "left" | "1" => 1u8,
                "right" | "2" => 2u8,
                _ => 0u8,
            };
            Ok(("drive".into(), json!({"acceleration": accel, "direction": dir})))
        }
        Commands::EnterVehicle { x, y } => {
            let mut args = json!({});
            if let Some(px) = x {
                args["x"] = json!(px);
            }
            if let Some(py) = y {
                args["y"] = json!(py);
            }
            Ok(("enter-vehicle".into(), args))
        }
        Commands::ExitVehicle => Ok(("exit-vehicle".into(), json!({}))),
        Commands::ConnectWire {
            x1,
            y1,
            x2,
            y2,
            wire,
            wire_item_id,
            wire_item,
            wire_slot,
            wire_inventory,
            wire_inventory_name,
            wire_source,
            wire_source_name,
            wire_quality_id,
            wire_stack_id,
            wire_kind,
            wire_keep_cursor,
        } => {
            let mut args = json!({"x1": x1, "y1": y1, "x2": x2, "y2": y2});
            if let Some(w) = wire {
                args["wire"] = json!(w);
            }
            if let Some(id) = wire_item_id {
                args["wire_item_id"] = json!(id);
            }
            if let Some(name) = wire_item {
                args["wire_item"] = json!(name);
            }
            if let Some(slot) = wire_slot {
                args["wire_slot"] = json!(slot);
            }
            if let Some(inv) = parse_inventory_arg(wire_inventory, wire_inventory_name)? {
                args["wire_inventory"] = json!(inv);
            }
            if let Some(src) = parse_source_arg(wire_source, wire_source_name)? {
                args["wire_source"] = json!(src);
            }
            if let Some(q) = wire_quality_id {
                args["wire_quality_id"] = json!(q);
            }
            if let Some(s) = wire_stack_id {
                args["wire_stack_id"] = json!(s);
            }
            if let Some(k) = wire_kind {
                args["wire_kind"] = json!(k);
            }
            if wire_keep_cursor {
                args["wire_clear_cursor"] = json!(false);
            }
            Ok(("connect-wire".into(), args))
        }
        Commands::DisconnectWire { x, y, wire } => {
            let mut args = json!({"x": x, "y": y});
            if let Some(w) = wire {
                args["wire"] = json!(w);
            }
            Ok(("disconnect-wire".into(), args))
        }
        Commands::Deconstruct { x, y, radius } => {
            let mut args = json!({"x": x, "y": y});
            if let Some(r) = radius {
                args["radius"] = json!(r);
            }
            Ok(("deconstruct".into(), args))
        }
        Commands::CancelDeconstruct { x, y, radius } => {
            let mut args = json!({"x": x, "y": y});
            if let Some(r) = radius {
                args["radius"] = json!(r);
            }
            Ok(("cancel-deconstruct".into(), args))
        }
        Commands::Pickup { x, y, hold_ms } => {
            let mut args = json!({"x": x, "y": y});
            if let Some(ms) = hold_ms {
                args["hold_ms"] = json!(ms);
            }
            Ok(("pickup".into(), args))
        }
        Commands::Spawn => Ok(("spawn".into(), json!({}))),
        Commands::SetTrainSchedule { x, y, stations } => {
            if stations.is_empty() {
                return Err("set-train-schedule requires at least one station".into());
            }
            Ok(("set-train-schedule".into(), json!({"x": x, "y": y, "stations": stations})))
        }
        Commands::TrainGo { x, y, station } => {
            let mut args = json!({"x": x, "y": y});
            if let Some(s) = station {
                args["station"] = json!(s);
            }
            Ok(("train-go".into(), args))
        }
        Commands::SetCombinator { x, y, params } => Ok(("set-combinator".into(), json!({"x": x, "y": y, "params": params}))),
        Commands::ServerCommand { command } => Ok(("server-command".into(), json!({"command": command}))),
        Commands::Eval { code } => Ok(("eval".into(), json!({"code": code}))),
        Commands::Reload => Ok(("reload".into(), json!({}))),
        Commands::TestReload => Ok(("test-reload".into(), json!({}))),
        _ => Err("Invalid command".into()),
    }
}

fn parse_inventory_arg(
    inventory: Option<u8>,
    inventory_name: Option<String>,
) -> Result<Option<u8>, Box<dyn std::error::Error>> {
    if inventory.is_some() && inventory_name.is_some() {
        return Err("use either --inventory or --inventory-name, not both".into());
    }
    let Some(name) = inventory_name else {
        return Ok(inventory);
    };
    let mut key = name.to_ascii_lowercase();
    key.retain(|c| c != '-' && c != '_' && c != ' ');
    let value = match key.as_str() {
        "other" => 0,
        "main" | "character" | "characterinventory" | "inventory" => 1,
        "guns" | "gun" | "weapons" | "weapon" => 3,
        "ammo" | "ammunition" => 4,
        "armor" | "armour" => 5,
        "vehicle" | "car" | "vehicleinventory" => 7,
        "trash" | "logistictrash" | "logisticstrash" => 8,
        _ => {
            return Err(format!(
                "unknown inventory name '{}'; use main, guns, ammo, armor, trash, vehicle, or other",
                name
            ).into());
        }
    };
    Ok(Some(value))
}

fn parse_source_arg(
    source: Option<u8>,
    source_name: Option<String>,
) -> Result<Option<u8>, Box<dyn std::error::Error>> {
    if source.is_some() && source_name.is_some() {
        return Err("use either --source or --source-name, not both".into());
    }
    let Some(name) = source_name else {
        return Ok(source);
    };
    let mut key = name.to_ascii_lowercase();
    key.retain(|c| c != '-' && c != '_');
    let value = match key.as_str() {
        "empty" => 0,
        "playerexternalinventory" | "playerexternal" | "player" | "character" => 2,
        "entityinventory" | "entity" => 4,
        "vehicleinventory" | "vehicle" => 5,
        "openediteminventory" | "openeditem" => 6,
        "openedequipmentinventory" | "openedequipment" | "equipmentinventory" | "equipment" => 7,
        "openedotherplayerinventory" | "openedotherplayer" | "otherplayer" => 8,
        "playerquickbar" | "quickbar" => 11,
        _ => {
            return Err(format!(
                "unknown source name '{}'; use empty, player-external, entity, vehicle, opened-item, opened-equipment, opened-other-player, or quickbar",
                name
            ).into());
        }
    };
    Ok(Some(value))
}
