use clap::Parser;
use factorio_client::daemon::{self, Daemon};
use std::fs::OpenOptions;
use std::os::unix::io::AsRawFd;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Parser)]
#[command(name = "factorio-daemon")]
#[command(about = "Factorio bot daemon - maintains connection and handles commands")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value = "34197")]
    port: u16,

    #[arg(long)]
    username: Option<String>,

    #[arg(long)]
    foreground: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let socket_path = daemon::socket_path();
    let pid_path = daemon::pid_path();
    let log_path = socket_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("daemon.log");

    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let _log_guard = if args.foreground {
        None
    } else {
        // Redirect stdout/stderr to log so daemon errors are visible even when spawned by CLI.
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;
        unsafe {
            let fd = log_file.as_raw_fd();
            libc::dup2(fd, libc::STDOUT_FILENO);
            libc::dup2(fd, libc::STDERR_FILENO);
        }
        Some(log_file)
    };

    eprintln!("Connecting to {}:{}...", args.host, args.port);

    let username = args
        .username
        .and_then(|name| {
            let trimmed = name.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        })
        .unwrap_or_else(random_username);

    let daemon = match Daemon::connect(&args.host, args.port, &username).await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Daemon connect failed: {}", e);
            return Err(e.into());
        }
    };

    eprintln!("Connected! Player ID: {:?}", daemon.player_id());

    std::fs::write(&pid_path, std::process::id().to_string())?;

    eprintln!("Daemon running on {:?}", socket_path);

    let result = daemon::run_daemon(daemon, socket_path.clone()).await;

    let _ = std::fs::remove_file(&socket_path);
    let _ = std::fs::remove_file(&pid_path);

    result?;
    Ok(())
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
