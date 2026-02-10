use std::sync::Arc;

use relay::MessageType;
use tokio::sync::mpsc::Sender;

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

pub async fn udp_handler(socket: std::net::UdpSocket, channel: Arc<Sender<MessageType>>) {
    let mut buf = [0u8; 1024];
    loop {
        if let Ok((bytes_read, addr)) = socket.recv_from(&mut buf) {
            if buf[0] == LOBBY_CREATE {
                // Lobby Create
                let code = uuid::Uuid::new_v4();
                channel.send(MessageType::CreateLobby(code, addr)).await;
                socket.send_to(code.as_bytes(), addr);
            } else {
                // Lobby Join
                match uuid::Uuid::from_slice(&buf[1..17]) {
                    Ok(code) => {
                        let (response_tx, response_rx) =
                            tokio::sync::oneshot::channel::<std::net::SocketAddr>();
                        channel
                            .send(MessageType::JoinLobby(code, addr, response_tx))
                            .await;
                        if let Ok(resp) = response_rx.await {
                            socket.send_to(resp.to_string().as_bytes(), addr);
                        }
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        }
    }
}
