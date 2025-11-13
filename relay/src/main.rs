use std::{collections::HashMap, str::FromStr, sync::Arc};

use http_body_util::Full;
use hyper::{
    Error, Method, Request, Response,
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use tokio::{self as Runtime, net::TcpListener};

static ADDR: &str = "192.168.1.40:8080";
#[derive(Debug, Serialize, Deserialize, Clone)]
struct Appstate {
    connected_clients: HashMap<uuid::Uuid, u32>,
}

#[Runtime::main]
async fn main() -> Result<(), std::io::Error> {
    println!("Relay Sever Listening on {}!", ADDR);
    let listener = TcpListener::bind(ADDR).await?;
    let state = Arc::new(Appstate {
        connected_clients: HashMap::new(),
    });

    loop {
        let (stream, _addr) = listener.accept().await?;

        let io = TokioIo::new(stream);
        let cloned_state = Arc::clone(&state);

        tokio::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(|req| {
                        let cloned_state = Arc::clone(&cloned_state);
                        async move { handler(req, cloned_state).await }
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
    req: Request<Incoming>,
    _state: Arc<Appstate>,
) -> Result<Response<Full<Bytes>>, Error> {
    println!("{}", req.uri().path());
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => {
            let random_uuid = uuid::Uuid::new_v4();
            let formated_response =
                format!("Share Url : http://{}/share/?share={}", ADDR, random_uuid);
            Ok(Response::new(Full::new(Bytes::from(formated_response))))
        }
        (&Method::GET, path) if path.starts_with("/share") => {
            let params = extract_params(&req);
            dbg!(&params);
            if let Some(val) = params {
                println!("{:?}", val);
            }
            Ok(Response::new(Full::new(Bytes::from("Something"))))
        }
        _ => {
            println!("URI PATH :{}", req.uri().path());
            Ok(Response::new(Full::new(Bytes::from("Unknown path"))))
        }
    }
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
