use super::api::KeyValueRpcServer;
use jsonrpsee::core::async_trait;
use jsonrpsee::core::RpcResult;
pub struct KeyValueRpcServerImpl;

#[async_trait]
impl KeyValueRpcServer for KeyValueRpcServerImpl {
    async fn foo(&self) -> RpcResult<u32> {
        Ok(0)
    }
}
