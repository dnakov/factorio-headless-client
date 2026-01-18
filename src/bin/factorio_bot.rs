use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::process::{Command, Stdio};

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
        #[arg(long, default_value = "FactorioBot")]
        username: String,
    },
    Disconnect,
    Status,
    Position,
    Walk {
        #[arg(long, default_value = "0")]
        direction: u8,
    },
    Stop,
    Mine {
        #[arg(long)]
        x: f64,
        #[arg(long)]
        y: f64,
    },
    StopMining,
    Craft {
        #[arg(long)]
        recipe_id: u16,
        #[arg(long, default_value = "1")]
        count: u32,
    },
    Chat {
        message: String,
    },
}

fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Connect { host, port, username } => start_daemon(&host, port, &username),
        Commands::Disconnect => stop_daemon(),
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

fn start_daemon(host: &str, port: u16, username: &str) -> Result<Response, Box<dyn std::error::Error>> {
    let socket_path = daemon::socket_path();

    if socket_path.exists() {
        if UnixStream::connect(&socket_path).is_ok() {
            return Ok(Response {
                id: "connect".into(),
                success: true,
                result: Some(json!({"message": "Already connected"})),
                error: None,
            });
        }
        let _ = std::fs::remove_file(&socket_path);
    }

    let exe = std::env::current_exe()?
        .parent()
        .unwrap()
        .join("factorio-daemon");

    let _child = Command::new(&exe)
        .args(["--host", host, "--port", &port.to_string(), "--username", username])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    for _ in 0..50 {
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
    }

    Err("Daemon failed to start".into())
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

    let (command, args) = match cmd {
        Commands::Position => ("position", json!({})),
        Commands::Status => ("status", json!({})),
        Commands::Walk { direction } => ("walk", json!({"direction": direction})),
        Commands::Stop => ("stop", json!({})),
        Commands::Mine { x, y } => ("mine", json!({"x": x, "y": y})),
        Commands::StopMining => ("stop-mining", json!({})),
        Commands::Craft { recipe_id, count } => ("craft", json!({"recipe_id": recipe_id, "count": count})),
        Commands::Chat { message } => ("chat", json!({"message": message})),
        _ => return Err("Invalid command".into()),
    };

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
