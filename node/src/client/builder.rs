use super::{Client, RuntimeContext};
use log_entry_sync::{LogSyncConfig, LogSyncManager};
use rpc::HttpClient;
use rpc::RPCConfig;
use std::sync::Arc;
use storage::log_store::log_manager::LogConfig;
use storage::log_store::Store;
use storage::{LogManager, StorageConfig};
use stream::{StreamConfig, StreamManager};
use tokio::sync::RwLock;

macro_rules! require {
    ($component:expr, $self:ident, $e:ident) => {
        $self
            .$e
            .as_ref()
            .ok_or(format!("{} requires {}", $component, std::stringify!($e)))?
    };
}

/// Builds a `Client` instance.
///
/// ## Notes
///
/// The builder may start some services (e.g.., libp2p, http server) immediately after they are
/// initialized, _before_ the `self.build(..)` method has been called.
pub struct ClientBuilder {
    runtime_context: Option<RuntimeContext>,
    store: Option<Arc<RwLock<dyn Store>>>,
    async_store: Option<storage_async::Store>,
    ionian_client: Option<HttpClient>,
}

impl ClientBuilder {
    /// Instantiates a new, empty builder.
    pub fn new() -> Self {
        Self {
            runtime_context: None,
            store: None,
            async_store: None,
            ionian_client: None,
        }
    }

    /// Specifies the runtime context (tokio executor, logger, etc) for client services.
    pub fn with_runtime_context(mut self, context: RuntimeContext) -> Self {
        self.runtime_context = Some(context);
        self
    }

    /// Initializes in-memory storage.
    pub fn with_memory_store(mut self) -> Result<Self, String> {
        // TODO(zz): Set config.
        let store = Arc::new(RwLock::new(
            LogManager::memorydb(LogConfig::default())
                .map_err(|e| format!("Unable to start in-memory store: {:?}", e))?,
        ));

        self.store = Some(store.clone());

        if let Some(ctx) = self.runtime_context.as_ref() {
            self.async_store = Some(storage_async::Store::new(store, ctx.executor.clone()));
        }

        Ok(self)
    }

    /// Initializes RocksDB storage.
    pub fn with_rocksdb_store(mut self, config: &StorageConfig) -> Result<Self, String> {
        let store = Arc::new(RwLock::new(
            LogManager::connect_db(LogConfig::default(), &config.db_dir)
                .map_err(|e| format!("Unable to start RocksDB store: {:?}", e))?,
        ));

        self.store = Some(store.clone());

        if let Some(ctx) = self.runtime_context.as_ref() {
            self.async_store = Some(storage_async::Store::new(store, ctx.executor.clone()));
        }

        Ok(self)
    }

    pub async fn with_rpc(mut self, rpc_config: RPCConfig) -> Result<Self, String> {
        if !rpc_config.enabled {
            return Ok(self);
        }

        let executor = require!("rpc", self, runtime_context).clone().executor;

        let ctx = rpc::Context {
            config: rpc_config,
            shutdown_sender: executor.shutdown_sender(),
        };

        self.ionian_client = Some(
            rpc::ionian_client(&ctx)
                .map_err(|e| format!("Unable to create rpc client: {:?}", e))?,
        );

        let rpc_handle = rpc::run_server(ctx)
            .await
            .map_err(|e| format!("Unable to start HTTP RPC server: {:?}", e))?;

        executor.spawn(rpc_handle, "rpc");

        Ok(self)
    }

    pub async fn with_stream(self, config: &StreamConfig) -> Result<Self, String> {
        let executor = require!("stream", self, runtime_context).clone().executor;
        let store = require!("stream", self, store).clone();
        let ionian_client = require!("stream", self, ionian_client).clone();
        let (stream_data_fetcher, stream_replayer) =
            StreamManager::initialize(config, store, ionian_client)
                .await
                .map_err(|e| e.to_string())?;
        StreamManager::spawn(stream_data_fetcher, stream_replayer, executor)
            .map_err(|e| e.to_string())?;
        Ok(self)
    }

    pub async fn with_log_sync(self, config: LogSyncConfig) -> Result<Self, String> {
        let executor = require!("log_sync", self, runtime_context).clone().executor;
        let store = require!("log_sync", self, store).clone();
        LogSyncManager::spawn(config, executor, store)
            .await
            .map_err(|e| e.to_string())?;
        Ok(self)
    }

    /// Consumes the builder, returning a `Client` if all necessary components have been
    /// specified.
    pub fn build(self) -> Result<Client, String> {
        require!("client", self, runtime_context);

        Ok(Client {})
    }
}
