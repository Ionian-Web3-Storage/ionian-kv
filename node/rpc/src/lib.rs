#[macro_use]
extern crate tracing;

mod config;
mod error;
mod ionian_rpc_client;
mod kv_rpc_server;
mod types;

use futures::channel::mpsc::Sender;
pub use ionian_rpc_client::IonianRpcClient;
pub use jsonrpsee::http_client::HttpClient;
use jsonrpsee::http_client::HttpClientBuilder;
use jsonrpsee::http_server::{HttpServerBuilder, HttpServerHandle};
use kv_rpc_server::KeyValueRpcServer;
use std::error::Error;
use std::sync::Arc;
use storage_with_stream::Store;
use task_executor::ShutdownReason;
use tokio::sync::RwLock;

pub use config::Config as RPCConfig;

/// A wrapper around all the items required to spawn the HTTP server.
///
/// The server will gracefully handle the case where any fields are `None`.
#[derive(Clone)]
pub struct Context {
    pub config: RPCConfig,
    pub shutdown_sender: Sender<ShutdownReason>,
    pub store: Arc<RwLock<dyn Store>>,
}

pub fn ionian_clients(ctx: &Context) -> Result<Vec<HttpClient>, Box<dyn Error>> {
    ctx.config
        .ionian_nodes
        .iter()
        .map(|url| Ok(HttpClientBuilder::default().build(url)?))
        .collect()
}

pub async fn run_server(ctx: Context) -> Result<HttpServerHandle, Box<dyn Error>> {
    let server = HttpServerBuilder::default()
        .max_response_body_size(ctx.config.max_response_body_in_bytes)
        .build(ctx.config.listen_address)
        .await?;

    let kv = (kv_rpc_server::KeyValueRpcServerImpl { ctx: ctx.clone() }).into_rpc();

    let addr = server.local_addr()?;
    let handle = server.start(kv)?;
    info!("Server started http://{}", addr);

    Ok(handle)
}
