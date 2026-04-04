// #![allow(dead_code, unused)]
use std::io::Write;
use std::{
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use clap::Parser;
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

use common::{
    Packet, PacketType,
    singleconnection::{ReliableOneWayUDP, ReliableOneWayUDPHandle},
};

const LOBBY_CREATE: u8 = 0;
const LOBBY_JOIN: u8 = 1;

#[Runtime::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(Level::TRACE.into()))
        .init();
    let args = Args::parse();
    let udp_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await.unwrap());

    let (packet_tx, mut packet_rx) = mpsc::channel(100);
    let (mut receiver, mut handle, mut output_rx, mut ack_rx) =
        ReliableOneWayUDP::new(udp_socket.clone());

    match args.mode {
        Mode::Host => {
            info!("App started as host mode");
            match listen_to_relay(&udp_socket, args.addr).await {
                Err(e) => {
                    error!("Error: {}", e);
                }
                Ok(addr) => {
                    let hole_punch_handle = handle.clone();
                    tokio::spawn(async move {
                        receiver.recv(addr).await; // Will block
                    });
                    tokio::spawn(async move {
                        handle.send(addr, packet_rx).await // Will block
                    });
                    let cloned_socket = udp_socket.clone();
                    tokio::task::spawn(async move {
                        ReliableOneWayUDPHandle::periodic_acks(cloned_socket, ack_rx).await;
                    });
                    hole_punching_wan(packet_tx, output_rx, hole_punch_handle, addr).await; // Needs Reliable UDP
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
                        let hole_punch_handle = handle.clone();
                        tokio::spawn(async move {
                            receiver.recv(addr).await; // Will block
                        });
                        tokio::spawn(async move {
                            handle.send(addr, packet_rx).await // Will block
                        });
                        let cloned_socket = udp_socket.clone();
                        tokio::task::spawn(async move {
                            ReliableOneWayUDPHandle::periodic_acks(cloned_socket, ack_rx).await;
                        });
                        hole_punching_wan(packet_tx, output_rx, hole_punch_handle, addr).await;
                    }
                }
            }
        }
    }
    Ok(())
}

#[instrument(name = "HOST.-.RELAY", skip_all)]
async fn listen_to_relay(
    socket: &Arc<UdpSocket>,
    server_addr: std::net::SocketAddr,
) -> Result<std::net::SocketAddr> {
    info!("Sending session creation packet...");
    socket.send_to(&[LOBBY_CREATE], server_addr).await; // Lobby Creation Packet
    let mut buf = [0u8; 120];
    if let Ok((bytes_read, addr)) = socket.recv_from(&mut buf).await {
        let addr =
            uuid::Uuid::from_slice(&buf[..bytes_read]).expect("Failed to parse uuid from socket");
        info!("Received Share Code: {}", addr);
    }
    loop {
        match socket.recv_from(&mut buf).await {
            Err(e) => continue,
            Ok((bytes_read, addr)) => {
                if let Ok(text) = str::from_utf8(&buf[..bytes_read]) {
                    if let Ok(socketaddr) = std::net::SocketAddr::from_str(text) {
                        info!("Successfully got peer addr: {}", socketaddr);
                        return Ok(socketaddr);
                    }
                }

                trace!("keep alive packet received: {}", addr);
                socket.send_to(&[2], addr).await; //Echo Back
                continue;
            }
        }
    }
}

#[instrument(name = "PEER.-.Relay", skip(socket, addr))]
async fn request_host_addr(
    socket: &UdpSocket,
    code: uuid::Uuid,
    addr: std::net::SocketAddr,
) -> Result<std::net::SocketAddr> {
    trace!("Creating lobby join packet...");
    let mut data = Vec::with_capacity(1 + 16);
    data.push(LOBBY_JOIN); // Lobby Join Packet
    data.extend_from_slice(code.as_bytes());

    trace!("Sending packet to stun server...");
    socket.send_to(&data, addr).await;

    let mut buf = [0u8; 120];
    loop {
        match socket.recv_from(&mut buf).await {
            Ok((bytes_read, addr)) => {
                if buf[0] == 0 {
                    trace!("Echo Packet Received : {}", addr);
                    socket.send_to(&[2], addr).await; // SENDING ECHO PACKET TO SERVER
                } else {
                    match str::from_utf8(&buf[..bytes_read]) {
                        Ok(string) => {
                            let socket_addr: std::net::SocketAddr = string.parse()?;
                            info!("Received host address: {}", socket_addr);
                            return Ok(socket_addr);
                        }
                        Err(_) => {
                            trace!("Failed to parse bytes...");
                        }
                    }
                }
            }
            Err(_e) => {
                trace!("Failed to receive from socket");
            }
        }
    }
}

async fn hole_punching_wan(
    handle: mpsc::Sender<PacketType>, // For sending data
    mut output_rx: mpsc::Receiver<(Packet, SocketAddr)>, // For receiving data (just Packet, no SocketAddr)
    reliable_udp_handle: ReliableOneWayUDPHandle,        // The handle for potential reuse
    peer_addr: std::net::SocketAddr,
) {
    println!("🔄 Starting WAN hole punching with {}", peer_addr);

    // Clone handle for the punching task
    let punch_handle = handle.clone();

    // Spawn task for sending punch packets
    let punch_task = tokio::spawn(async move {
        info!("Beginning punching holes...");

        for i in 0..=5 {
            if punch_handle
                .send(PacketType::Data {
                    payload: b"HOLE_PUNCH".to_vec(),
                })
                .await
                .is_err()
            {
                error!("Failed to send punch packet");
                break;
            }

            trace!("Punch attempt {}/5", i);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        info!("Punching holes completed");
    });

    // Listen for peer response
    info!("Listening for peer packets");
    let start_time = Instant::now();
    let timeout = Duration::from_secs(5);

    while start_time.elapsed() < timeout {
        tokio::select! {
            Some((packet,addr)) = output_rx.recv() => {
                // Check if this is a response from peer
                if let Some(payload) = packet.payload {
                    if payload == b"HOLE_PUNCH" || payload == b"CONNECTED" {
                        info!("✅ Peer detected!");

                        // Send confirmation
                        let _ = handle
                            .send(PacketType::Data {
                                payload: b"CONNECTED".to_vec(),
                            })
                            .await;

                        info!("Connection Established with: {}", peer_addr);

                        // Start chat using the channel-based approach, not raw sockets
                        start_chat(handle.clone(), output_rx, peer_addr,false).await;
                        return;
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {
                // Continue loop
            }
        }
    }

    error!("Hole punching failed - timeout");
    punch_task.abort();
}

#[instrument(name = "CHAT", skip(handle, output_rx))]
async fn start_chat(
    handle: mpsc::Sender<PacketType>,
    mut output_rx: mpsc::Receiver<(Packet, SocketAddr)>,
    peer: std::net::SocketAddr,
    relayed: bool,
) {
    trace!("✅ Chat established with {}", peer);
    println!("📝 Type your messages (type 'exit' to quit):");
    println!("────────────────────────────────────");

    // Start keep-alive task - use tokio::spawn, NOT spawn_blocking
    let keep_alive_handle = handle.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(15));
        loop {
            interval.tick().await;
            let _ = keep_alive_handle
                .send(PacketType::Data {
                    payload: b"KEEP_ALIVE".to_vec(),
                })
                .await;
        }
    });

    // Receiver task
    let receiver = tokio::spawn(async move {
        while let Some((packet, addr)) = output_rx.recv().await {
            if addr.ip() == peer.ip() {
                if let Some(payload) = packet.payload {
                    if let Ok(message) = String::from_utf8(payload) {
                        match message.as_str() {
                            "KEEP_ALIVE" => continue,
                            "GOODBYE" => {
                                println!("\n👋 Chat closed by other peer");
                                std::process::exit(0);
                            }
                            "CONNECTED" | "HOLE_PUNCH" => {
                                trace!(
                                    "Ignoring hole punch message: {},PACK_SEQ: {}, PACK_ACK: {}",
                                    message, packet.header.seq, packet.header.ack
                                );
                                continue;
                            }
                            _ => {
                                print!("\r\x1b[K");
                                println!("📨 Peer: {}", message);
                                print!("💬 You: ");
                                std::io::stdout().flush().unwrap();
                            }
                        }
                    }
                }
            }
        }
    });

    // Sender loop - use async stdin or tokio::spawn_blocking for stdin only
    let (stdin_tx, mut stdin_rx) = mpsc::unbounded_channel::<String>();

    // Spawn a blocking thread JUST for stdin
    std::thread::spawn(move || {
        let mut input = String::new();
        loop {
            input.clear();
            match std::io::stdin().read_line(&mut input) {
                Ok(_) => {
                    let message = input.trim().to_string();
                    if stdin_tx.send(message).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Error reading input: {}", e);
                    break;
                }
            }
        }
    });

    // Main async loop
    loop {
        tokio::select! {
            Some(message) = stdin_rx.recv() => {
                if message == "exit" {
                    println!("👋 Exiting...");
                    let _ = handle.send(PacketType::Data {
                        payload: b"GOODBYE".to_vec(),
                    }).await;
                    break;
                }

                if !message.is_empty() {
                    if let Err(e) = handle.send(PacketType::Data {
                        payload: message.as_bytes().to_vec(),
                    }).await {
                        error!("Failed to send to other peer: {}", e);
                        break;
                    }
                }
            }
        }
    }

    info!("Chat ended");
    receiver.abort();
}
