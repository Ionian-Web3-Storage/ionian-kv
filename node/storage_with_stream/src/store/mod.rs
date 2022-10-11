use async_trait::async_trait;
use ethereum_types::{H160, H256};
use shared_types::{AccessControlSet, StreamWriteSet, Transaction};
use storage::log_store::{Configurable, LogStoreRead, LogStoreWrite};

use crate::error::Result;

mod sqlite_db_statements;
pub mod store_manager;
mod stream_store;

pub use stream_store::AccessControlOps;

pub trait Store:
    LogStoreRead + LogStoreWrite + Configurable + Send + Sync + StreamRead + StreamWrite + 'static
{
}
impl<
        T: LogStoreRead
            + LogStoreWrite
            + Configurable
            + Send
            + Sync
            + StreamRead
            + StreamWrite
            + 'static,
    > Store for T
{
}

#[async_trait]
pub trait StreamRead {
    async fn get_holding_stream_ids(&self) -> Result<Vec<H256>>;

    async fn get_stream_data_sync_progress(&self) -> Result<u64>;

    async fn get_stream_replay_progress(&self) -> Result<u64>;

    async fn get_latest_version_before(
        &self,
        stream_id: H256,
        key: H256,
        before: u64,
    ) -> Result<u64>;

    async fn has_write_permission(
        &self,
        account: H160,
        stream_id: H256,
        key: H256,
        version: u64,
    ) -> Result<bool>;

    async fn is_new_stream(&self, stream_id: H256, version: u64) -> Result<bool>;

    async fn is_admin(&self, account: H160, stream_id: H256, version: u64) -> Result<bool>;

    async fn get_stream_key_value(
        &self,
        stream_id: H256,
        key: H256,
        version: u64,
    ) -> Result<Option<(shared_types::StreamWrite, u64)>>;
}

#[async_trait]
pub trait StreamWrite {
    async fn reset_stream_sync(&self, stream_ids: Vec<u8>) -> Result<()>;

    async fn update_stream_ids(&self, stream_ids: Vec<u8>) -> Result<()>;

    async fn update_stream_data_sync_progress(&self, from: u64, progress: u64) -> Result<u64>;

    async fn update_stream_replay_progress(&self, from: u64, progress: u64) -> Result<u64>;

    async fn put_stream(
        &self,
        tx_seq: u64,
        data_merkle_root: H256,
        result: &'static str,
        commit_data: Option<(StreamWriteSet, AccessControlSet)>,
    ) -> Result<()>;

    async fn get_tx_result(&self, tx_seq: u64) -> Result<Option<String>>;

    async fn revert_stream(&mut self, tx_seq: u64) -> Result<Vec<Transaction>>;
}
