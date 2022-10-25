mod config_macro;

mod convert;
use config_macro::*;
use std::ops::Deref;

build_config! {
    // stream
    (stream_ids, (Vec<String>), vec![])

    // log sync
    (blockchain_rpc_endpoint, (String), "http://127.0.0.1:8545".to_string())
    (log_contract_address, (String), "".to_string())
    (log_sync_start_block_number, (u64), 0)
    (confirmation_block_count, (u64), 12)
    (log_page_size, (u64), 1000)
    (max_cache_data_size, (usize), 100 * 1024 * 1024) // 100 MB
    (cache_tx_seq_ttl, (usize), 500)

    // rpc
    (rpc_enabled, (bool), true)
    (rpc_listen_address, (String), "127.0.0.1:5678".to_string())
    (rpc_chunks_per_segment, (usize), 1024)
    (ionian_node_urls, (String), "http://127.0.0.1:5678".to_string())
    (max_query_len_in_bytes, (u64), 1024 * 256)

    // db
    (db_dir, (String), "db".to_string())
    (kv_db_file, (String), "kv.DB".to_string())

    // misc
    (log_config_file, (String), "log_config".to_string())
}

#[derive(Debug)]
pub struct IonianKVConfig {
    pub raw_conf: RawConfiguration,
}

impl Deref for IonianKVConfig {
    type Target = RawConfiguration;

    fn deref(&self) -> &Self::Target {
        &self.raw_conf
    }
}

impl IonianKVConfig {
    pub fn parse(matches: &clap::ArgMatches) -> Result<IonianKVConfig, String> {
        Ok(IonianKVConfig {
            raw_conf: RawConfiguration::parse(matches)?,
        })
    }
}
