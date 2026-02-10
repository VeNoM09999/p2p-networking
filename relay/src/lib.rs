#![allow(dead_code)]
use std::{collections::HashMap, net::SocketAddr};

type Code = uuid::Uuid;
type HostAddr = SocketAddr;
type ClientAddr = SocketAddr;
type ResponseSender = tokio::sync::oneshot::Sender<SocketAddr>;

#[derive(Debug)]
pub enum MessageType {
    CreateLobby(Code, HostAddr),
    JoinLobby(Code, ClientAddr, ResponseSender),
}

pub struct Session {
    pub host_addr: SocketAddr,
    pub client_addr: Option<Vec<SocketAddr>>,
}

pub struct RelayManager {
    pub session: HashMap<uuid::Uuid, Session>,
}
impl RelayManager {
    pub async fn handler(&mut self, mut rx: tokio::sync::mpsc::Receiver<MessageType>) {
        loop {
            println!("Waiting for event");
            if let Some(event) = rx.recv().await {
                println!("Received Event: {:?}", event);
                match event {
                    MessageType::CreateLobby(code, addr) => {
                        // Insert new session
                        self.session.insert(
                            code,
                            Session {
                                host_addr: addr,
                                client_addr: None,
                            },
                        );
                    }
                    MessageType::JoinLobby(code, addr, channel) => {
                        if let Some(session) = self.session.get_mut(&code) {
                            // Updated session with new client addr & sending host addr back
                            session.client_addr = Some(vec![addr]);
                            let _ = channel.send(session.host_addr);
                        }
                    }
                }
            }
        }
    }
}
