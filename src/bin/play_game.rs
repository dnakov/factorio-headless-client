//! Just connect, download map, and stay alive
//!
//! Run with: cargo run --bin play-game -- [server:port]

use std::io::{self, BufRead};
use std::net::SocketAddr;
use std::time::Duration;
use factorio_client::protocol::Connection;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:34197".into())
        .parse()?;

    println!("=== FACTORIO CONNECTION TEST ===\n");

    let username = format!("Bot{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() % 10000);
    println!("Username: {}", username);

    let mut connection = Connection::new(addr, username.clone()).await?;

    // Connect and download map
    connection.connect().await?;
    println!("Connected! Player index: {:?}", connection.player_index());

    connection.download_map().await?;
    println!("Map downloaded!");
    println!("After download: tick={} player_index={:?}\n", connection.server_tick(), connection.player_index());

    println!("Initial position: ({:.2}, {:.2})",
        connection.player_position().0,
        connection.player_position().1);
    println!("Initial tick: {}\n", connection.server_tick());

    // Send a chat message
    println!("Sending chat message...");
    tokio::time::sleep(Duration::from_millis(500)).await;
    connection.send_chat("Hello from Rust bot!").await?;
    println!("Chat message sent!\n");

    // Exercise movement + mining actions
    println!("Walking north...");
    connection.send_walk(0).await?;
    pump_connection(&mut connection, Duration::from_millis(800)).await?;
    println!("Stopping...");
    connection.send_stop_walk().await?;
    pump_connection(&mut connection, Duration::from_millis(400)).await?;

    println!("Mining...");
    connection.send_begin_mine().await?;
    pump_connection(&mut connection, Duration::from_millis(600)).await?;
    connection.send_stop_mine().await?;
    pump_connection(&mut connection, Duration::from_millis(400)).await?;

    // Just stay alive - no actions
    println!("=== STAYING ALIVE (30 seconds) ===");
    let start = std::time::Instant::now();
    let mut heartbeat_count = 0u32;
    let mut other_count = 0u32;
    let mut none_count = 0u32;
    let mut last_tick = connection.server_tick();
    while start.elapsed() < Duration::from_secs(30) {
        for _ in 0..5 {
            match connection.poll().await {
                Ok(Some(pkt)) => {
                    if matches!(pkt, factorio_client::protocol::ReceivedPacket::Heartbeat { .. }) {
                        heartbeat_count += 1;
                    } else {
                        other_count += 1;
                    }
                }
                Ok(None) => none_count += 1,
                Err(_) => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(8)).await;

        let cur_tick = connection.server_tick();
        if start.elapsed().as_secs() % 5 == 0 && start.elapsed().subsec_millis() < 20 {
            println!("  tick={} (delta={}) hb={} other={} none={} state={:?}",
                cur_tick, cur_tick.wrapping_sub(last_tick), heartbeat_count, other_count, none_count, connection.state());
            last_tick = cur_tick;
        }
    }

    println!("\n=== FINAL STATE ===");
    println!("Position: ({:.2}, {:.2})",
        connection.player_position().0,
        connection.player_position().1);
    println!("Server tick: {}", connection.server_tick());
    println!("Player index: {:?}", connection.player_index());
    println!("State: {:?}", connection.state());

    if let Err(err) = check_server_log(&username) {
        eprintln!("[WARN] Server log check failed: {}", err);
    }

    Ok(())
}

async fn pump_connection(connection: &mut Connection, duration: Duration) -> Result<(), Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    while start.elapsed() < duration {
        let _ = connection.poll().await;
        tokio::time::sleep(Duration::from_millis(8)).await;
    }
    Ok(())
}

fn check_server_log(username: &str) -> io::Result<()> {
    let log_path = std::env::var("FACTORIO_SERVER_LOG")
        .unwrap_or_else(|_| "/private/tmp/factorio-server.log".to_string());
    let file = std::fs::File::open(&log_path)?;
    let reader = io::BufReader::new(file);
    let join_pat = format!("[JOIN] {} joined the game", username);
    let chat_pat = format!("[CHAT] {}: Hello from Rust bot!", username);
    let mut saw_join = false;
    let mut saw_chat = false;
    for line in reader.lines() {
        let line = line?;
        if line.contains(&join_pat) {
            saw_join = true;
        }
        if line.contains(&chat_pat) {
            saw_chat = true;
        }
    }

    println!("Server log check ({}): join={}, chat={}", log_path, saw_join, saw_chat);
    Ok(())
}
