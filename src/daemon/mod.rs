pub mod protocol;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::protocol::{Connection, ConnectionState};

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

    loop {
        // Poll 5 times like play-game does
        for _ in 0..5 {
            let _ = connection.poll().await;
        }

        // Check for commands (non-blocking)
        while let Ok(cmd) = cmd_rx.try_recv() {
            match cmd {
                DaemonCommand::Execute(request, response_tx) => {
                    let response = handle_command(&mut connection, request).await;
                    let _ = response_tx.send(response);
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

async fn handle_command(conn: &mut Connection, request: Request) -> Response {
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
            match conn.send_walk(dir).await {
                Ok(_) => CommandResult::ok_empty(),
                Err(e) => CommandResult::err(e.to_string()),
            }
        }
        "stop" => {
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
        _ => CommandResult::err(format!("Unknown command: {}", request.command)),
    };

    Response {
        id: request.id,
        success: result.success,
        result: result.data,
        error: result.error,
    }
}
