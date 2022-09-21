use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;

#[rpc(server, client, namespace = "kv")]
pub trait KeyValueRpc {
    #[method(name = "foo")]
    async fn foo(&self) -> RpcResult<u32>;
}
