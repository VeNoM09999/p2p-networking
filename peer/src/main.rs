use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio as Runtime;

#[derive(Serialize, Deserialize, Debug)]
struct PeerInfo {
    connected_peer: String,
}

#[Runtime::main]
async fn main() -> Result<()>{
    println!("Peer booting up");
    // let udp_socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await;
    let info = register_with_server("http://localhost:8080").await?;

    println!("{:?}",info);
    Ok(())
}

async fn register_with_server(server_url: &str) -> Result<PeerInfo> {
    let client = reqwest::Client::new()
        .get(server_url)
        .body("Give me peer info!")
        .send()
        .await?;
    let parsed_info = PeerInfo::from_response(client).await;
    parsed_info
}

#[async_trait::async_trait]
trait FromResponse<T> {
    async fn from_response(resp: T) -> Result<Self>
    where
        Self: Sized;
}

#[async_trait::async_trait]
impl FromResponse<reqwest::Response> for PeerInfo {
    async fn from_response(resp: reqwest::Response) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(resp.json::<PeerInfo>().await?)
    }
}

