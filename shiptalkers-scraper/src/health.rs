use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Bare-bones HTTP responder for container health checks
/// Answers 200 as long as the scraper process is running.
const RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok";

pub async fn serve(port: u16) {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            tracing::error!("Health endpoint failed to bind {addr}: {e}");
            return;
        }
    };
    tracing::info!("Health endpoint listening on {addr}");
    loop {
        let Ok((mut socket, _)) = listener.accept().await else {
            continue;
        };
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let _ = socket.write_all(RESPONSE).await;
        });
    }
}
