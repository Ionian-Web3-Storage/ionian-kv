use ethereum_types::{H160, H256};
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;

use crate::types::{ValueSegment, Segment};

#[rpc(server, client, namespace = "kv")]
pub trait KeyValueRpc {
    #[method(name = "getStatus")]
    async fn get_status(&self) -> RpcResult<bool>;

    #[method(name = "getValue")]
    async fn get_value(
        &self,
        stream_id: H256,
        key: Segment,
        start_index: u64,
        len: u64,
        version: Option<u64>,
    ) -> RpcResult<Option<ValueSegment>>;

    #[method(name = "getTransactionResult")]
    async fn get_trasanction_result(&self, tx_seq: u64) -> RpcResult<Option<String>>;

    #[method(name = "getHoldingStreamIds")]
    async fn get_holding_stream_ids(&self) -> RpcResult<Vec<H256>>;

    #[method(name = "hasWritePermission")]
    async fn has_write_permission(
        &self,
        account: H160,
        stream_id: H256,
        key: Segment,
        version: Option<u64>,
    ) -> RpcResult<bool>;

    #[method(name = "isAdmin")]
    async fn is_admin(
        &self,
        account: H160,
        stream_id: H256,
        version: Option<u64>,
    ) -> RpcResult<bool>;

    #[method(name = "isSpecialKey")]
    async fn is_special_key(
        &self,
        stream_id: H256,
        key: Segment,
        version: Option<u64>,
    ) -> RpcResult<bool>;

    #[method(name = "isWriterOfKey")]
    async fn is_writer_of_key(
        &self,
        account: H160,
        stream_id: H256,
        key: Segment,
        version: Option<u64>,
    ) -> RpcResult<bool>;

    #[method(name = "isWriterOfStream")]
    async fn is_writer_of_stream(
        &self,
        account: H160,
        stream_id: H256,
        version: Option<u64>,
    ) -> RpcResult<bool>;
}
