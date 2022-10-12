use crate::error;
use crate::types::ValueSegment;
use crate::Context;
use storage_with_stream::log_store::log_manager::ENTRY_SIZE;

use super::api::KeyValueRpcServer;
use ethereum_types::H256;
use jsonrpsee::core::async_trait;
use jsonrpsee::core::RpcResult;
pub struct KeyValueRpcServerImpl {
    pub ctx: Context,
}

#[async_trait]
impl KeyValueRpcServer for KeyValueRpcServerImpl {
    async fn get_status(&self) -> RpcResult<bool> {
        debug!("kv_getStatus()");

        Ok(true)
    }

    async fn get_value(
        &self,
        stream_id: H256,
        key: H256,
        start_index: u64,
        len: u64,
        version: Option<u64>,
    ) -> RpcResult<Option<ValueSegment>> {
        debug!("kv_getValue()");

        if len > self.ctx.config.max_query_len_in_bytes {
            return Err(error::invalid_params("len", "query length too large"));
        }

        let before_version = match version {
            Some(v) => v,
            None => u64::MAX,
        };

        let store_read = self.ctx.store.read().await;
        if let Some((stream_write, latest_version)) = store_read
            .get_stream_key_value(stream_id, key, before_version)
            .await?
        {
            if start_index > stream_write.end_index - stream_write.start_index {
                return Err(error::invalid_params(
                    "start_index",
                    "start index is greater than value length",
                ));
            }
            let start_byte_index = stream_write.start_index + start_index;
            let end_byte_index = std::cmp::min(start_byte_index + len, stream_write.end_index);
            let start_entry_index = start_byte_index / ENTRY_SIZE as u64;
            let end_entry_index = if end_byte_index % ENTRY_SIZE as u64 == 0 {
                end_byte_index / ENTRY_SIZE as u64
            } else {
                end_byte_index / ENTRY_SIZE as u64 + 1
            };
            if let Some(entry_array) = store_read
                .get_chunk_by_flow_index(start_entry_index, end_entry_index - start_entry_index)?
            {
                return Ok(Some(ValueSegment {
                    version: latest_version,
                    data: entry_array.data[(start_byte_index as usize
                        - start_entry_index as usize * ENTRY_SIZE)
                        ..(end_byte_index as usize - start_entry_index as usize * ENTRY_SIZE)
                            as usize]
                        .to_vec(),
                    size: stream_write.end_index - stream_write.start_index,
                }));
            }
            Ok(None)
        } else {
            Ok(None)
        }
    }

    async fn get_trasanction_result(&self, tx_seq: u64) -> RpcResult<Option<String>> {
        debug!("kv_getTransactionResult()");

        Ok(self.ctx.store.read().await.get_tx_result(tx_seq).await?)
    }
}
