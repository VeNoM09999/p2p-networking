#![allow(dead_code, unused)]
use std::{
    f64::consts::PI,
    io,
    process::exit,
    str::FromStr,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow};
use clap::Parser;
use eventsource_stream::{Event, Eventsource};
use serde::{Deserialize, Serialize};
use std::io::Write;
use tokio::{self as Runtime, net::UdpSocket, sync::mpsc};
use tracing::{Instrument, Level, error, info, info_span, instrument, trace};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, clap::ValueEnum)]
enum Mode {
    Host,
    Peer,
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, value_enum)]
    mode: Mode,
    #[arg(short, long, required_if_eq("mode", "peer"))]
    code: Option<uuid::Uuid>,
    #[arg(short, long)]
    addr: std::net::SocketAddr,
}

use common::singleconnection::{ReliableOneWayUDP, ReliableUDPBuilder};
#[Runtime::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(Level::TRACE.into()))
        .init();
    let args = Args::parse();
    let udp_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await.unwrap());

    // let arc_socket = Arc::new(udp_socket);
    // let cloned = Arc::clone(&arc_socket);
    // let (tx, mut rx) = mpsc::channel(100);
    // let (packet_tx, mut packet_rx) = mpsc::channel(100);
    // let (mut relaible_udp, tx_packet, tx_ack_pack) =
    //     ReliableUDPBuilder::new().build(&udp_socket, tx);

    // tokio::spawn(async move {
    //     //
    //     relaible_udp.recv().await;
    //     relaible_udp.send(addr, packet_rx)
    // });

    match args.mode {
        Mode::Host => {
            info!("App started as host mode");
            match listen_to_relay(&udp_socket, args.addr).await {
                Err(e) => {
                    error!("Error: {}", e);
                }
                Ok(addr) => {
                    hole_punching_wan(udp_socket, addr).await;
                }
            }
        }
        Mode::Peer => {
            info!("App started as peer mode");
            if let Some(code) = args.code {
                match request_host_addr(&udp_socket, code, args.addr).await {
                    Err(_) => {
                        error!("Failed to retrieve host addr");
                    }
                    Ok(addr) => {
                        hole_punching_wan(udp_socket, addr).await;
                    }
                }
            }
        }
    }
    Ok(())
}

use futures_util::StreamExt;
async fn listen_to_relay(
    socket: &Arc<UdpSocket>,
    server_addr: std::net::SocketAddr,
) -> Result<std::net::SocketAddr> {
    socket.send_to(&[0], server_addr).await; // Lobby Creation Packet
    let mut buf = [0u8; 120];
    if let Ok((bytes_read, addr)) = socket.recv_from(&mut buf).await {
        let addr =
            uuid::Uuid::from_slice(&buf[..bytes_read]).expect("Failed to parse uuid from socket");
        println!("Share Code: {}", addr);
    }
    loop {
        match socket.recv_from(&mut buf).await {
            Err(e) => continue,
            Ok((bytes_read, addr)) => {
                if let Ok(text) = str::from_utf8(&buf[..bytes_read]) {
                    if let Ok(socketaddr) = std::net::SocketAddr::from_str(text) {
                        println!("Peer Addr: {}", socketaddr);
                        return Ok(socketaddr);
                    }
                }

                println!("Keep Alive Packet received: {}", addr);
                socket.send_to(&[2], addr).await; //Echo Back
                continue;
            }
        }
    }
}

async fn request_host_addr(
    socket: &UdpSocket,
    code: uuid::Uuid,
    addr: std::net::SocketAddr,
) -> Result<std::net::SocketAddr> {
    let mut data = Vec::with_capacity(1 + 16);
    data.push(1); // Lobby Join Packet

    data.extend_from_slice(code.as_bytes());

    socket.send_to(&data, addr).await;

    let mut received = 0;
    let mut buf = [0u8; 120];
    loop {
        match socket.recv_from(&mut buf).await {
            Ok((bytes_read, addr)) => {
                if buf[0] == 0 {
                    println!("Echo Packet Received : {}", addr);
                    socket.send_to(&[2], addr).await; // SENDING ECHO PACKET TO SERVER
                } else {
                    match str::from_utf8(&buf[..bytes_read]) {
                        Ok(string) => {
                            let socket_addr: std::net::SocketAddr = string.parse()?;
                            println!("Host Address: {}", socket_addr);
                            return Ok(socket_addr);
                        }
                        Err(_) => {}
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to receive from socket");
            }
        }
    }
}

async fn hole_punching_wan(socket: Arc<UdpSocket>, peer_addr: std::net::SocketAddr) {
    println!("🔄 Starting WAN hole punching with {}", peer_addr);

    let sender_span = info_span!("SENDER.TASK");
    sender_span.enter();

    let socket_clone1 = socket.clone();
    let socket_clone2 = socket.clone();
    let peer_addr1 = peer_addr;
    let peer_addr2 = peer_addr;

    // Spawn first task for punching
    let punch_handle = tokio::spawn(
        async move {
            info!("Beginning punching holes...");

            for i in 1..=5 {
                let _ = socket_clone1.send_to(b"HOLE_PUNCH", peer_addr1).await;
                trace!(" Punch attempt {}/30", i);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            info!("Punching holes completed");
        }
        .instrument(sender_span),
    );

    let listen_span = info_span!("RECEIVER.TASK");
    listen_span.enter();
    // Spawn second task for listening
    let listen_handle = tokio::spawn(
        async move {
            info!("Listening for peer packets");
            let mut buf = [0u8; 1024];
            let start_time = Instant::now();
            let timeout = Duration::from_secs(5); // Longer timeout

            while start_time.elapsed() < timeout {
                match socket_clone2.recv_from(&mut buf).await {
                    Ok((size, src)) => {
                        // ACCEPT ANY PACKET from the peer's IP, regardless of port
                        if src.ip() == peer_addr2.ip() {
                            info!("✅ Peer detected at {} (any port!)", src);

                            // Send confirmation back to the EXACT port we received from
                            let _ = socket_clone2.send_to(b"CONNECTED", src).await;

                            info!("Connection Established with: {}", src);
                            return Some(src);
                        }
                    }
                    Err(_timeout) => {
                        error!("Hole Punching failed")
                        // Keep sending punches while listening
                        // let _ = socket_clone2.send_to(b"HOLE_PUNCH", peer_addr2).await;
                    }
                }
            }

            error!("hole punching failed - Timed out");
            None
        }
        .instrument(listen_span),
    );

    let _ = punch_handle.await;

    if let Ok(Some(connected_peer)) = listen_handle.await {
        start_chat(socket, connected_peer, false).await;
    } else {
        // FALLBACK:  Route traffic from TURN Server
    }
}

#[instrument(name = "CHAT", skip(peer, socket))]
async fn start_chat(socket: Arc<UdpSocket>, peer: std::net::SocketAddr, relayed: bool) {
    trace!("✅ Chat established with {}", peer);
    println!("📝 Type your messages (type 'exit' to quit):");
    println!("────────────────────────────────────");

    // Start keep-alive thread
    let keep_alive_socket = socket.clone();
    let keep_alive_peer = peer;
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(15)); // Send every 15 seconds
            let _ = keep_alive_socket.send_to(b"KEEP_ALIVE", keep_alive_peer);
        }
    });

    // Receiver thread
    let recv_socket = socket.clone();
    let recv_peer = peer;
    let receiver = tokio::task::spawn(async move {
        let mut buf = [0u8; 1024];

        loop {
            match recv_socket.recv_from(&mut buf).await {
                Ok((size, src)) => {
                    if src.ip() == recv_peer.ip() {
                        if let Ok(message) = String::from_utf8(buf[..size].to_vec()) {
                            match message.as_str() {
                                "KEEP_ALIVE" => {
                                    println!("KEEP ALIVE");
                                    // Ignore keep-alive messages
                                }
                                "GOODBYE" => {
                                    println!("Chat closed by other peer");
                                    exit(0);
                                }
                                _ => {
                                    print!("\r\x1b[K"); // Clear line
                                    println!("📨 Peer: {}", message.trim_end());
                                    print!("💬 You: ");
                                    io::stdout().flush().unwrap();
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error receiving: {}", e);
                    continue;
                }
            }
        }
    });

    // receiver.await;

    // Sender thread
    let mut input = String::new();
    loop {
        print!("💬 You: ");
        io::stdout().flush().unwrap();

        input.clear();
        match io::stdin().read_line(&mut input) {
            Ok(_) => {
                let message = input.trim();

                if message == "exit" {
                    println!("👋 Exiting...");
                    let _ = socket.send_to(b"GOODBYE", peer).await;
                    break;
                }

                if !message.is_empty() {
                    if let Err(e) = socket.send_to(message.as_bytes(), peer).await {
                        error!("Failed to send to other peer")
                    }
                }
            }
            Err(e) => {
                error!("Error reading input: {}", e);
                break;
            }
        }
    }
    info!("Stopped listening for new packets...");
    receiver.abort();
}
