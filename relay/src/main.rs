// #![allow(unused, dead_code, unused_variables)]
use anyhow::Result;
use relay::RelayManager;
use std::{collections::HashMap, sync::Arc};
use tokio::net::UdpSocket;
use tokio::{self as Runtime};

mod udp;
use udp::parser::udp_handler;

struct Appstate {
    relay_manager: RelayManager,
}

#[Runtime::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<relay::MessageType>(50);
    let udp_listener = UdpSocket::bind("0.0.0.0:9903")
        .await
        .expect("Failed to bind udp socket");

    let arc_socket = Arc::new(udp_listener);

    tokio::spawn(async move {
        let mut state = Appstate {
            relay_manager: RelayManager {
                session: HashMap::new(),
            },
        };
        state.relay_manager.handler(rx).await;
    });
    let app_handle = Arc::new(tx);

    println!("UDP Socket Listening on {:?}", arc_socket.local_addr());
    udp_handler(arc_socket.clone(), app_handle).await;
    Ok(())
}
