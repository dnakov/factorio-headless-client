use clap::Parser;
use factorio_client::daemon::{self, Daemon};

#[derive(Parser)]
#[command(name = "factorio-daemon")]
#[command(about = "Factorio bot daemon - maintains connection and handles commands")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value = "34197")]
    port: u16,

    #[arg(long, default_value = "FactorioBot")]
    username: String,

    #[arg(long)]
    foreground: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let socket_path = daemon::socket_path();
    let pid_path = daemon::pid_path();

    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    eprintln!("Connecting to {}:{}...", args.host, args.port);

    let daemon = Daemon::connect(&args.host, args.port, &args.username).await?;

    eprintln!("Connected! Player ID: {:?}", daemon.player_id());

    std::fs::write(&pid_path, std::process::id().to_string())?;

    eprintln!("Daemon running on {:?}", socket_path);

    let result = daemon::run_daemon(daemon, socket_path.clone()).await;

    let _ = std::fs::remove_file(&socket_path);
    let _ = std::fs::remove_file(&pid_path);

    result?;
    Ok(())
}
