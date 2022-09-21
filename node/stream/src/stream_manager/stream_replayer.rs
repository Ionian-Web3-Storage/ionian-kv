use crate::StreamConfig;
use anyhow::Result;
use std::{sync::Arc, time::Duration};
use storage::log_store::Store;
use tokio::sync::RwLock;

use super::RETRY_WAIT_MS;

pub struct StreamReplayer {
    config: StreamConfig,
    store: Arc<RwLock<dyn Store>>,
}

impl StreamReplayer {
    pub async fn new(config: StreamConfig, store: Arc<RwLock<dyn Store>>) -> Result<Self> {
        Ok(Self { config, store })
    }

    async fn replay(&self) -> Result<()> {
        Ok(())
    }

    pub async fn run(&self) {
        let connection;
        match self.store.read().await.get_stream_db_connection() {
            Ok(conn) => {
                connection = conn;
            }
            Err(e) => {
                error!("build sqlite connection error: e={:?}", e);
                return;
            }
        }
        let mut tx_seq;
        match self
            .store
            .read()
            .await
            .get_stream_replay_progress(&connection)
        {
            Ok(progress) => {
                tx_seq = progress;
            }
            Err(e) => {
                error!("get stream replay progress error: e={:?}", e);
                return;
            }
        }
        loop {
            let maybe_tx = self.store.read().await.get_tx_by_seq_number(tx_seq);
            match maybe_tx {
                Ok(Some(tx)) => {
                    let mut skip = false;
                    if tx.stream_ids.len() == 0 {
                        skip = true;
                    } else {
                        for id in tx.stream_ids.iter() {
                            if self.config.stream_set.get(id) == None {
                                skip = true;
                                break;
                            }
                        }
                    }
                    // sync data
                    if !skip {
                        match self.replay().await {
                            Ok(()) => {}
                            Err(e) => {
                                error!("update stream data sync progress error: e={:?}", e);
                            }
                        }
                    }
                    // update progress, get next tx_seq to sync
                    match self.store.write().await.update_stream_data_sync_progress(
                        &connection,
                        tx_seq,
                        tx_seq + 1,
                    ) {
                        Ok(next_tx_seq) => {
                            tx_seq = next_tx_seq;
                        }
                        Err(e) => {
                            error!("update stream data sync progress error: e={:?}", e);
                            break;
                        }
                    }
                }
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(RETRY_WAIT_MS)).await;
                }
                Err(e) => {
                    error!("stream data sync error: e={:?}", e);
                    tokio::time::sleep(Duration::from_millis(RETRY_WAIT_MS)).await;
                }
            }
        }
    }
}
