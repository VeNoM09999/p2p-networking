#![allow(dead_code, unused)]
use std::str::FromStr;

use anyhow::Result;
use clap::Parser;
use eventsource_stream::{Event, Eventsource};
use serde::{Deserialize, Serialize};
use tokio as Runtime;

#[derive(Debug, Clone, clap::ValueEnum)]
enum Mode {
    Host,
    Peer,
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, value_enum)]
    mode: Mode,
    #[arg(long)]
    code: uuid::Uuid,
}

#[Runtime::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let udp_socket = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
    match args.mode {
        Mode::Host => {
            println!("App started as host mode");
            listen_to_relay(udp_socket).await;
        }
        Mode::Peer => {
            println!("App started as peer mode");
            request_host_addr(udp_socket, args.code).await;
        }
    }
    Ok(())
}

#[async_trait::async_trait]
trait FromResponse<T> {
    async fn from_response(resp: T) -> Result<Self>
    where
        Self: Sized;
}

use futures_util::StreamExt;
async fn listen_to_relay(socket: std::net::UdpSocket) -> Result<()> {
    let data = [0];
    socket.send_to(
        &data,
        std::net::SocketAddr::from_str("127.0.0.1:9903").unwrap(),
    );
    let mut buf = [0u8; 120];
    if let Ok((bytes_read, addr)) = socket.recv_from(&mut buf) {
        let addr =
            uuid::Uuid::from_slice(&buf[..bytes_read]).expect("Failed to parse uuid from socket");
        println!("Share Code: {}", addr);
    }
    Ok(())
}

async fn request_host_addr(socket: std::net::UdpSocket, code: uuid::Uuid) -> Result<()> {
    let mut data = Vec::with_capacity(1 + 16);
    data.push(1);

    data.extend_from_slice(code.as_bytes());

    socket.send_to(
        &data,
        std::net::SocketAddr::from_str("127.0.0.1:9903")
            .expect("failed to parse socker addr from str"),
    );

    let mut buf = [0u8; 120];
    if let Ok((bytes_read, addr)) = socket.recv_from(&mut buf) {
        let s = str::from_utf8(&buf[..bytes_read])?;
        let socket_addr: std::net::SocketAddr = s.parse()?;
        println!("Host Address: {}", socket_addr);
    }
    Ok(())
}
