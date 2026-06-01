use std::{error::Error, time::Duration};

use common::relay::{CreateSessionRequest, relay_control_client::RelayControlClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("gRPc control pane starting...");
    let mut client = RelayControlClient::connect("http://192.168.1.40:50051").await?;

    // tokio::time::sleep(Duration::from_secs(10)).await;

    client
        .create_session(CreateSessionRequest {
            host_addr: "192.168.1.42:9001".into(),
            peer_addr: "192.168.1.42:9002".into(),
        })
        .await?;
    Ok(())
}
