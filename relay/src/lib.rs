// #![allow(dead_code)]
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use tokio::net::UdpSocket;
use tokio::{sync::oneshot, time};
use tracing::{Event, Level, event, info_span, trace_span};

type Code = uuid::Uuid;
type HostAddr = SocketAddr;
type ClientAddr = SocketAddr;
type ResponseSender = tokio::sync::oneshot::Sender<SocketAddr>;

#[derive(Debug)]
pub enum MessageType {
    CreateLobby(Code, HostAddr, oneshot::Sender<()>),
    JoinLobby(Code, ClientAddr, ResponseSender),
}

pub struct Session {
    pub host_addr: SocketAddr,
    pub shutdown_channel: Option<oneshot::Sender<()>>,
    pub client_addr: Option<Vec<SocketAddr>>,
}

pub struct RelayManager {
    pub session: HashMap<uuid::Uuid, Session>,
}
impl RelayManager {
    pub async fn handler(&mut self, mut rx: tokio::sync::mpsc::Receiver<MessageType>) {
        loop {
            if let Some(event) = rx.recv().await {
                match event {
                    MessageType::CreateLobby(code, addr, tx) => {
                        lobby_create(addr.to_string().as_str(), code.to_string().as_str());
                        // Insert new session
                        self.session.insert(
                            code,
                            Session {
                                host_addr: addr,
                                client_addr: None,
                                shutdown_channel: Some(tx),
                            },
                        );
                    }
                    MessageType::JoinLobby(code, addr, channel) => {
                        if let Some(session) = self.session.get_mut(&code) {
                            lobby_joined(addr.to_string().as_str(), code.to_string().as_str());
                            // Updated session with new client addr & sending host addr back
                            session.client_addr = Some(vec![addr]);
                            let _ = channel.send(session.host_addr);
                            if let Some(tx) = session.shutdown_channel.take() {
                                tokio::task::spawn(async move {
                                    tokio::time::sleep(Duration::from_secs(5)).await;
                                    let _ = tx.send(());
                                });
                            }
                        }
                    }
                }
            }
        }
    }
    pub async fn keep_alive(
        addr: std::net::SocketAddr,
        socket: Arc<UdpSocket>,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) {
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(1));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Send keep-alive packet
                        if let Err(e) = socket.send_to(&[0], addr).await{
                            eprintln!("Keep-alive send error to {}: {}", addr, e);
                            // Don't break - continue trying
                        }
                    }
                    _ = &mut shutdown_rx => {
                        println!("Keep-alive task shutting down for {}", addr);
                        break;
                    }
                }
            }
        });
    }
}

fn lobby_create(ip: &str, code: &str) {
    let span = trace_span!("LOBBY.CREATE",);
    let _entered = span.enter();

    event!(Level::TRACE, CODE = code, HOST_IP = ip, "Lobby Created",);
}

fn lobby_joined(ip: &str, code: &str) {
    let span = trace_span!("LOBBY.CREATE",);
    let _entered = span.enter();

    event!(Level::TRACE, IP = ip, CODE = code, "Lobby Joined!");
}
