use std::collections::{HashMap, BTreeMap};
use rand::prelude::*;
use ordered_float::OrderedFloat;
use serde::{Serialize,Deserialize};
use serde_json::*;
use serde_with::*;
use std::iter;
use rsocket_rust::{prelude::*, Result, async_trait};
use rsocket_rust_transport_tcp::*;
use log::info;


use futures::stream;
use futures::StreamExt;


mod server_connectivity;


#[tokio::main]
async fn main() -> rsocket_rust::Result<()> {
    // Show info-level logs without requiring RUST_LOG env var
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    RSocketFactory::receive()
        .transport(TcpServerTransport::from("127.0.0.1:7878"))
        .acceptor(Box::new(|setup, _| {
            // Setup payload: Option<&Bytes> -> Option<&[u8]> -> String
            let setup_data = setup.data().as_deref()
                .map(|s| String::from_utf8_lossy(s).to_string())
                .unwrap_or_default();

            let setup_meta = setup.metadata().as_deref()
                .map(|m| String::from_utf8_lossy(m).to_string())
                .unwrap_or_default();

            info!("incoming setup: data='{}', metadata='{}'", setup_data, setup_meta);

            Ok(Box::new(server_connectivity::RRResponder))
        }))
        .serve()
        .await
}


