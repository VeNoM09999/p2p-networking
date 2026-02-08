#![allow(dead_code)]
use std::{collections::HashMap, net::SocketAddr};

type Code = uuid::Uuid;
type HostAddr = SocketAddr;
type ClientAddr = SocketAddr;
pub type ReturnChannelCreate = tokio::sync::oneshot::Sender<CreateResponse>;
pub type ReturnChannelJoin = tokio::sync::oneshot::Sender<HostAddr>;

#[derive(Debug)]
pub struct CreateResponse {
    pub code: Code,
    pub sse_receiver: tokio::sync::mpsc::Receiver<bytes::Bytes>,
}

#[derive(Debug)]
pub enum MessageType {
    CreateLobby(Code, HostAddr, ReturnChannelCreate),
    JoinLobby(Code, ClientAddr, ReturnChannelJoin),
}

pub struct Session {
    pub host_addr: SocketAddr,
    pub sse_channel: tokio::sync::mpsc::Sender<bytes::Bytes>,
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
                    /*
                       1. Create session
                       2. Spawn the SSE Connection
                       3. Return the code in first event
                    */
                    MessageType::CreateLobby(code, host_addr, retun_channel) => {
                        let (sse_tx, sse_rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(8);
                        self.session.insert(
                            code,
                            Session {
                                host_addr: host_addr,
                                sse_channel: sse_tx,
                            },
                        );
                        // Return the response
                        let _ = retun_channel.send(CreateResponse {
                            code,
                            sse_receiver: sse_rx,
                        });

                        if let Some(res) = self.session.get(&code) {
                            let sse = res.sse_channel.clone();
                            tokio::spawn(async move {
                                // tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                                println!("Sending code");
                                let format = format!("data: {{\"code\":\"{}\"}}\n\n", code);
                                let _ = sse.send(format.into()).await;
                            });
                        }
                    }
                    MessageType::JoinLobby(code, client_addr, return_channel) => {
                        if let Some(session) = self.session.get(&code) {
                            if session.sse_channel.is_closed() {
                                println!("SSE Channel is closed");
                                continue;
                            }
                            let format = format!("data: {{\"peer_addr\": \"{}\"}}\n\n", client_addr);
                            let _ = session.sse_channel.send(bytes::Bytes::from(format)).await;
                            let _ = return_channel.send(session.host_addr);
                        }
                    }
                }
            }
        }
    }
}
