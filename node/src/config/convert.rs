#![allow(clippy::field_reassign_with_default)]

use std::{collections::HashSet, str::FromStr};

use crate::IonianKVConfig;
use ethereum_types::H256;
use log_entry_sync::{ContractAddress, LogSyncConfig};
use rpc::RPCConfig;
use storage_with_stream::{LogStorageConfig, StorageConfig};
use stream::StreamConfig;

impl IonianKVConfig {
    pub fn storage_config(&self) -> Result<StorageConfig, String> {
        Ok(StorageConfig {
            log_config: LogStorageConfig {
                db_dir: self.db_dir.clone().into(),
            },
            kv_db_file: self.kv_db_file.clone().into(),
        })
    }

    pub fn stream_config(&self) -> Result<StreamConfig, String> {
        let mut stream_ids: Vec<H256> = vec![];
        for id in &self.stream_ids {
            stream_ids.push(
                H256::from_str(id)
                    .map_err(|e| format!("Unable to parse stream id: {:?}, error: {:?}", id, e))?,
            );
        }
        stream_ids.sort();
        stream_ids.dedup();
        if stream_ids.is_empty() {
            error!("{}", format!("stream ids is empty"))
        }
        let stream_set = HashSet::from_iter(stream_ids.iter().cloned());
        Ok(StreamConfig {
            stream_ids,
            stream_set,
        })
    }

    pub fn rpc_config(&self) -> Result<RPCConfig, String> {
        let listen_address = self
            .rpc_listen_address
            .parse::<std::net::SocketAddr>()
            .map_err(|e| format!("Unable to parse rpc_listen_address: {:?}", e))?;

        Ok(RPCConfig {
            enabled: self.rpc_enabled,
            listen_address,
            chunks_per_segment: self.rpc_chunks_per_segment,
            ionian_node_url: self.ionian_node_url.clone(),
            max_query_len_in_bytes: self.max_query_len_in_bytes,
        })
    }

    pub fn log_sync_config(&self) -> Result<LogSyncConfig, String> {
        let contract_address = self
            .log_contract_address
            .parse::<ContractAddress>()
            .map_err(|e| format!("Unable to parse log_contract_address: {:?}", e))?;
        Ok(LogSyncConfig::new(
            self.blockchain_rpc_endpoint.clone(),
            contract_address,
            self.log_sync_start_block_number,
        ))
    }
}
