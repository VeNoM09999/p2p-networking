#![allow(unused, dead_code, unused_variables)]
use anyhow::Result;
use http_body_util::Full;
use hyper::{
    Error, Method, Request, Response,
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr, str::FromStr, sync::Arc};
use tokio::{self as Runtime, net::TcpListener};
static ADDR: &str = "192.168.1.40:8080";

#[derive(Debug)]
enum StateCommands {
    Insert(uuid::Uuid, String),
    Get(uuid::Uuid, tokio::sync::oneshot::Sender<Option<String>>),
}
#[derive(Debug)]
struct Appstate {
    connected_clients: HashMap<uuid::Uuid, String>,
}

impl Appstate {
    async fn state_manager(&mut self, mut rx: tokio::sync::mpsc::Receiver<StateCommands>) {
        while let Some(event) = rx.recv().await {
            match event {
                StateCommands::Get(k, tx) => {
                    let response = self.connected_clients.get(&k).cloned();
                    let _ = tx.send(response);
                }
                StateCommands::Insert(k, v) => {
                    &self.connected_clients.insert(k, v);
                }
            }
        }
    }
}

struct AppHandle {
    sender: tokio::sync::mpsc::Sender<StateCommands>,
}
impl AppHandle {
    pub async fn set(&self, addr: String) -> uuid::Uuid {
        let random_uuid = uuid::Uuid::new_v4();
        self.sender
            .send(StateCommands::Insert(random_uuid, addr))
            .await;
        random_uuid
    }
    pub async fn get(&self, id: uuid::Uuid) -> Option<String> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = self.sender.send(StateCommands::Get(id, tx)).await;
        rx.await.unwrap_or(None)
    }
}

#[Runtime::main]
async fn main() -> Result<(), std::io::Error> {
    println!("Relay Sever Listening on {}!", ADDR);
    let (tx, rx) = tokio::sync::mpsc::channel(100);
    let listener = TcpListener::bind(ADDR).await?;
    tokio::spawn(async move {
        let mut state = Appstate {
            connected_clients: HashMap::new(),
        };
        state.state_manager(rx).await
    });

    let app_handle = Arc::new(AppHandle { sender: tx });

    loop {
        let (stream, addr) = listener.accept().await?;

        let io = TokioIo::new(stream);
        let cloned_state = Arc::clone(&app_handle);

        tokio::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(|req| {
                        let cloned_state = Arc::clone(&cloned_state);
                        async move { handler(addr.to_string(), req, cloned_state).await }
                    }),
                )
                .await
            {
                println!("Error serving connection {:?}", err);
            }
        });
    }
}

async fn handler(
    addr: String,
    req: Request<Incoming>,
    state: Arc<AppHandle>,
) -> Result<Response<Full<Bytes>>> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => {
            let uuid = state.set(addr).await;
            let formated_response = format!("Share Url : http://{}/share/?share={}", ADDR, uuid);
            Ok(Response::new(Full::new(Bytes::from(formated_response))))
        }
        (&Method::GET, path) if path.starts_with("/share") => {
            let params = extract_params(&req);
            let response = format!("");
            dbg!(&params);
            if let Some(val) = params {
                if let Some(v) = state.get(val.share).await {
                    return respond_text(v);
                } else {
                    return respond_text("Missing params");
                }
            }
            respond_text("Missing params")
        }
        _ => {
            println!("URI PATH :{}", req.uri().path());
            Ok(Response::new(Full::new(Bytes::from("Unknown path"))))
        }
    }
}

fn respond<T: Into<Bytes>>(body: T) -> Result<Response<Full<Bytes>>> {
    Ok(Response::new(Full::new(body.into())))
}

fn respond_text<T: Into<String>>(body: T) -> Result<Response<Full<Bytes>>> {
    respond(Bytes::from(body.into()))
}

fn extract_params(req: &Request<Incoming>) -> Option<UrlQuery> {
    let query = req.uri().query();

    if let Some(q) = query {
        return UrlQuery::try_from(q).ok();
    }
    None
}

#[derive(Debug)]
enum UrlQueryError {
    InvalidUuid(uuid::Error),
    ParseError,
}

#[derive(Serialize, Deserialize, Debug)]
struct UrlQuery {
    share: uuid::Uuid,
}

impl TryFrom<&str> for UrlQuery {
    type Error = UrlQueryError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let collected: Vec<&str> = value.split("=").collect();
        if collected[0] == "share" {
            let parsed = uuid::Uuid::from_str(collected[1]).map_err(UrlQueryError::InvalidUuid)?;

            Ok(UrlQuery { share: parsed })
        } else {
            Err(UrlQueryError::ParseError)
        }
    }
}
