// #![allow(unused, dead_code, unused_variables)]
use anyhow::Result;
use clap::Parser;
use relay::RelayManager;
use std::{collections::HashMap, sync::Arc};
use tokio::net::UdpSocket;
use tokio::{self as Runtime};
use tracing::{Level, event};
use tracing_subscriber::EnvFilter;

mod udp;
use udp::parser::udp_handler;

struct Appstate {
    relay_manager: RelayManager,
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(short, long, default_value_t = 9903)]
    port: u16,
}

#[Runtime::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_target(true)
        .with_env_filter(EnvFilter::from_default_env().add_directive(Level::TRACE.into()))
        .init();
    let args = Args::parse();
    let (tx, rx) = tokio::sync::mpsc::channel::<relay::MessageType>(50);
    let addr = format!("0.0.0.0:{}", args.port);
    let udp_listener = UdpSocket::bind(addr)
        .await
        .expect("Failed to bind udp socket");
    let arc_socket = Arc::new(udp_listener);

    event!(Level::INFO, "RendezvousXSTUN server started...");
    tokio::spawn(async move {
        event!(Level::INFO, "Global State Manager started and listening...");
        let mut state = Appstate {
            relay_manager: RelayManager {
                session: HashMap::new(),
            },
        };
        state.relay_manager.handler(rx).await;
    });
    let app_handle = Arc::new(tx);

    event!(
        Level::INFO,
        "UDP Socket Listening on {:?}",
        arc_socket.local_addr().unwrap()
    );
    udp_handler(arc_socket.clone(), app_handle).await;
    Ok(())
}
