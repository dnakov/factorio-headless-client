//! Download map from server and save to file
//!
//! Run with: cargo run --bin download_map -- [server:port] [output.zip]

use std::net::SocketAddr;
use factorio_client::protocol::Connection;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    let addr: SocketAddr = args.get(1)
        .unwrap_or(&"127.0.0.1:34197".to_string())
        .parse()?;

    let output_path = args.get(2)
        .map(|s| s.as_str())
        .unwrap_or("downloaded_map.zip");

    println!("Connecting to {}...", addr);

    let username = format!("MapDownloader{}", std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() % 10000);

    let mut connection = Connection::new(addr, username).await?;

    connection.connect().await?;
    println!("Connected!");

    println!("Downloading map...");
    let map_data = connection.download_map_raw().await?;

    println!("Map size: {} bytes", map_data.len());
    println!("Saving to {}...", output_path);

    std::fs::write(output_path, &map_data)?;

    println!("Done! Map saved to {}", output_path);

    Ok(())
}
