#![allow(dead_code, unused)]
use anyhow::Result;
use clap::Parser;
use eventsource_stream::{Event, Eventsource};
use serde::{Deserialize, Serialize};
use tokio as Runtime;

#[derive(Debug, Clone, clap::ValueEnum)]
enum Mode {
    Host,
    Peer,
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    url: String,
    #[arg(long, value_enum)]
    mode: Mode,
}

#[Runtime::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    match args.mode {
        Mode::Host => {
            println!("App started as host mode");
            listen_to_relay(&args.url).await;
        }
        Mode::Peer => {
            println!("App started as peer mode");
            request_host_addr(&args.url).await;
        }
    }
    Ok(())
}

#[async_trait::async_trait]
trait FromResponse<T> {
    async fn from_response(resp: T) -> Result<Self>
    where
        Self: Sized;
}

use futures_util::StreamExt;
async fn listen_to_relay(url: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .header("Accept", "text/event-stream")
        .send()
        .await?;

    println!("Connected to relay, waiting for peers...");

    let mut stream = response.bytes_stream().eventsource();

    while let Some(event) = stream.next().await {
        println!("{:?}", event);
    }

    Ok(())
}

async fn request_host_addr(url: &str) -> Result<()> {
    let response = reqwest::Client::new().get(url).send().await;
    if let Ok(r) = response {
        println!("{}", r.text().await.unwrap());
        // dbg!(r);
    };
    Ok(())
}
