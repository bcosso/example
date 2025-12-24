use std::iter;
use rsocket_rust::{prelude::*, Result, async_trait};
use rsocket_rust_transport_tcp::*;
use log::info;


use futures::stream;
use futures::StreamExt;


#[derive(Clone)]
struct RRResponder;

#[async_trait] // ← required to implement async methods in the trait
impl RSocket for RRResponder {
    // --- You *use* this one ---
    async fn request_response(&self, payload: Payload) -> Result<Option<Payload>> {
        let data = payload.data_utf8().unwrap_or_default();
        let meta = payload.metadata_utf8().unwrap_or_default();
        info!("RR received: data='{}', metadata='{}'", data, meta);

        // Return a single response (echo example)
        
        let body = format!("echo: {}", data);
        
        let resp = Payload::builder()
        .set_data_utf8(&body)
        // .set_metadata_utf8("optional-meta") // if you want metadata
        .build();

        Ok(Some(resp))

        
    }

    // --- You DON'T use these; keep them as NO-OPs so the trait is fully implemented ---
    async fn fire_and_forget(&self, _req: Payload) -> Result<()> {
        // no-op
        Ok(())
    }

    async fn metadata_push(&self, _req: Payload) -> Result<()> {
        // no-op
        Ok(())
    }

    // If your installed trait version still defines these (older API may differ),
    // provide trivial implementations. If your trait variant uses async versions
    // returning Result<Flux<Payload>>, change these signatures accordingly.
    fn request_stream(&self, _req: Payload) -> Flux<Result<Payload>> {
        // empty stream
        Box::pin(stream::empty::<Result<Payload>>())
    }

    fn request_channel(&self, _reqs: Flux<Result<Payload>>) -> Flux<Result<Payload>> {
        // echo-nothing stream
        Box::pin(stream::empty::<Result<Payload>>())
    }
}

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

            Ok(Box::new(RRResponder))
        }))
        .serve()
        .await
}
