use ethereum_types::H256;
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;

use crate::types::ValueSegment;

#[rpc(server, client, namespace = "kv")]
pub trait KeyValueRpc {
    #[method(name = "getValue")]
    async fn get_value(
        &self,
        stream_id: H256,
        key: H256,
        start_index: u64,
        len: u64,
        version: Option<u64>,
    ) -> RpcResult<Option<ValueSegment>>;

    #[method(name = "getTransactionResult")]
    async fn get_trasanction_result(&self, tx_seq: u64) -> RpcResult<Option<String>>;
}
