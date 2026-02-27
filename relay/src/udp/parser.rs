#![allow(dead_code)]

use relay::{MessageType, RelayManager};
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc::Sender, oneshot};

#[derive(serde::Serialize, serde::Deserialize)]
pub enum RelayEvent {
    LobbyCreate,
    LobbyJoin { lobby_id: uuid::Uuid },
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum RelayResponse {
    LobbyCreated { lobby_id: uuid::Uuid },
    LobbyJoined,
    LobbyNotFound,
}
const LOBBY_CREATE: u8 = 0;
const LOBBY_JOIN: u8 = 1;

pub async fn udp_handler(socket: Arc<UdpSocket>, channel: Arc<Sender<MessageType>>) {
    let mut buf = [0u8; 1024];
    loop {
        if let Ok((_bytes_read, relay_observed_addr)) = socket.recv_from(&mut buf).await {
            if buf[0] == LOBBY_CREATE {
                // Lobby Create
                let code = uuid::Uuid::new_v4();
                let (tx, rx) = oneshot::channel();
                let _ = channel
                    .send(MessageType::CreateLobby(code, relay_observed_addr, tx))
                    .await;
                if let Err(e) = socket.send_to(code.as_bytes(), relay_observed_addr).await {
                    // Trace Error
                }
                RelayManager::keep_alive(relay_observed_addr, socket.clone(), rx).await;
            } else if buf[0] == LOBBY_JOIN {
                // Lobby Join
                match uuid::Uuid::from_slice(&buf[1..17]) {
                    Ok(code) => {
                        let (response_tx, response_rx) =
                            tokio::sync::oneshot::channel::<std::net::SocketAddr>(); // Getting Host Address
                        let _ = channel
                            .send(MessageType::JoinLobby(
                                code,
                                relay_observed_addr,
                                response_tx,
                            ))
                            .await;
                        if let Ok(host_info) = response_rx.await {
                            let cloned_socket = socket.clone();
                            tokio::spawn(async move {
                                let mut sent = 0;
                                loop {
                                    if let Err(e) =
                                        cloned_socket.send_to(&[0], relay_observed_addr).await
                                    {
                                        // Tracing Error
                                    }
                                    sent += 1;
                                    if sent >= 5 {
                                        break;
                                    }
                                }

                                if let Err(e) = cloned_socket
                                    .send_to(host_info.to_string().as_bytes(), relay_observed_addr)
                                    .await
                                {
                                    // Trace Error
                                } // Relay -> Client [Host Addr]
                                if let Err(e) = cloned_socket
                                    .send_to(relay_observed_addr.to_string().as_bytes(), host_info)
                                    .await
                                {
                                    // Trace Error
                                } // Relay -> Host [Client Addr]
                            });
                        }
                    }
                    Err(_) => {
                        continue;
                    }
                }
            } else {
                // Echo - Drop
                println!("Echo Packet from addr:  {}", relay_observed_addr);
            }
        }
    }
}
