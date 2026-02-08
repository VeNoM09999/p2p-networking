#![allow(unused, dead_code, unused_variables)]
// #![deny(dead_code)]
use anyhow::Result;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::{
    Method, Request, Response,
    body::{Bytes, Frame, Incoming},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{collections::HashMap, net::SocketAddr, str::FromStr, sync::Arc};
use tokio::{self as Runtime, net::TcpListener};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
static ADDR: &str = "192.168.1.40:8080";

use relay::RelayManager;
struct Appstate {
    relay_manager: RelayManager,
}

#[Runtime::main]
async fn main() -> Result<(), std::io::Error> {
    println!("Relay Sever Listening on {}!", ADDR);
    let (tx, rx) = tokio::sync::mpsc::channel::<relay::MessageType>(50);
    let listener = TcpListener::bind(ADDR).await?;
    tokio::spawn(async move {
        let mut state = Appstate {
            relay_manager: RelayManager {
                session: HashMap::new(),
            },
        };
        state.relay_manager.handler(rx).await;
    });

    let app_handle = Arc::new(tx);

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
                        async move { handler(addr, req, cloned_state).await }
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
    addr: SocketAddr,
    req: Request<Incoming>,
    state: Arc<tokio::sync::mpsc::Sender<relay::MessageType>>,
) -> Result<
    Response<http_body_util::combinators::BoxBody<hyper::body::Bytes, std::convert::Infallible>>,
> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => {
            let share_code = uuid::Uuid::new_v4();
            let (return_channel_tx, rx) = tokio::sync::oneshot::channel::<relay::CreateResponse>();

            state
                .send(relay::MessageType::CreateLobby(
                    share_code,
                    addr,
                    return_channel_tx,
                ))
                .await;

            if let Ok(r) = rx.await {
                let receiver_stream =
                    ReceiverStream::new(r.sse_receiver).map(|b| Ok(Frame::data(b)));
                let body = StreamBody::new(receiver_stream);
                let response = Response::builder()
                    .header(hyper::header::CONTENT_TYPE, "text/event-stream")
                    .header(hyper::header::CACHE_CONTROL, "no-cache")
                    .body(body.boxed())
                    .unwrap();
                Ok(response)
            } else {
                let response = Response::builder()
                    .status(hyper::StatusCode::INTERNAL_SERVER_ERROR)
                    .body(http_body_util::combinators::BoxBody::new(Full::new(
                        Bytes::from("nothing"),
                    )))
                    .unwrap();
                Ok(response)
            }
        }
        (&Method::GET, path) if path.starts_with("/code") => {
            let params = extract_params(&req);
            dbg!(&params);
            if let Some(val) = params {
                let (tx, rx) = tokio::sync::oneshot::channel();
                state
                    .send(relay::MessageType::JoinLobby(val.code, addr, tx))
                    .await;

                if let Ok(res) = rx.await {
                    let json_object = json!({"host_addr": res});
                    let j = serde_json::to_string(&json_object).unwrap();
                    return Ok(Response::new(Full::new(Bytes::from(j)).boxed()));
                } else {
                    respond_text("Code Expired");
                }
            }
            respond_text("Missing params")
        }
        _ => {
            println!("URI PATH :{}", req.uri().path());
            Ok(Response::new(
                Full::new(Bytes::from("Unknown path")).boxed(),
            ))
        }
    }
}

fn respond_text<T: Into<String>>(
    body: T,
) -> Result<
    Response<http_body_util::combinators::BoxBody<hyper::body::Bytes, std::convert::Infallible>>,
> {
    let body = Full::new(Bytes::from(body.into()));
    Ok(Response::new(body.boxed()))
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
    code: uuid::Uuid,
}

impl TryFrom<&str> for UrlQuery {
    type Error = UrlQueryError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let collected: Vec<&str> = value.split("=").collect();
        if collected[0] == "code" {
            let parsed = uuid::Uuid::from_str(collected[1]).map_err(UrlQueryError::InvalidUuid)?;

            Ok(UrlQuery { code: parsed })
        } else {
            Err(UrlQueryError::ParseError)
        }
    }
}

#[derive(Serialize, Deserialize)]
struct HostAddr {
    host_addr: SocketAddr,
}

// async fn host_sse(
//     code: String,
//     host_addr: SocketAddr,
//     state: Arc<AppHandle>,
// ) -> Response<http_body_util::combinators::BoxBody<hyper::body::Bytes, std::convert::Infallible>> {
//     let (tx, rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(8);
//     tokio::task::spawn(async move {
//         let payload = HostAddr {
//             host_addr: host_addr,
//         };
//         let json = serde_json::to_string(&payload).unwrap();
//         let _ = tx.send(json.into()).await;
//         tokio::time::sleep(std::time::Duration::from_secs(1)).await;
//         drop(tx);
//     });
//     let stream = ReceiverStream::new(rx).map(|b| Ok(Frame::data(b)));
//     let body = StreamBody::new(stream);
//     let response = Response::builder()
//         .header(hyper::header::CONTENT_TYPE, "text/event-stream")
//         .header(hyper::header::CACHE_CONTROL, "no-cache")
//         .body(body.boxed())
//         .unwrap();
//     response
// }
