use crate::StreamConfig;
use anyhow::Result;
use jsonrpsee::http_client::HttpClient;
use rpc::IonianRpcClient;
use shared_types::{ChunkArray, Transaction};
use std::{cmp, sync::Arc, time::Duration};
use storage::log_store::Store;
use tokio::sync::RwLock;

const RETRY_WAIT_MS: u64 = 1000;
const ENTRIES_PER_SEGMENT: usize = 1024;
const MAX_DOWNLOAD_TASK: usize = 5;
const ALERT_CNT: i32 = 10;

pub struct StreamDataFetcher {
    config: StreamConfig,
    store: Arc<RwLock<dyn Store>>,
    client: HttpClient,
}

impl StreamDataFetcher {
    pub async fn new(
        config: StreamConfig,
        store: Arc<RwLock<dyn Store>>,
        client: HttpClient,
    ) -> Result<Self> {
        Ok(Self {
            config,
            store,
            client,
        })
    }

    async fn download(&self, tx: &Transaction, start_index: u64, end_index: u64) -> Result<()> {
        let mut fail_cnt = 0;
        loop {
            match self
                .client
                .download_segment(tx.data_merkle_root, start_index as u32, end_index as u32)
                .await
            {
                Ok(Some(segment)) => {
                    self.store.write().await.put_chunks(
                        tx.seq,
                        ChunkArray {
                            data: segment.0,
                            start_index: tx.start_entry_index + start_index,
                        },
                    )?;
                    return Ok(());
                }
                Ok(None) => {
                    fail_cnt += 1;
                    tokio::time::sleep(Duration::from_millis(RETRY_WAIT_MS)).await;
                }
                Err(_) => {
                    //todo: match error types
                    fail_cnt += 1;
                    tokio::time::sleep(Duration::from_millis(RETRY_WAIT_MS)).await;
                }
            }
            if fail_cnt % ALERT_CNT == 0 {
                warn!(
                    "Download data of tx_seq {:?} failed for {:?} times",
                    tx.seq, fail_cnt
                );
            }
        }
    }

    async fn sync_data(&self, tx: &Transaction) -> Result<()> {
        for i in (0..tx.size).step_by(ENTRIES_PER_SEGMENT * MAX_DOWNLOAD_TASK) {
            let mut tasks = vec![];
            let tasks_end_index = cmp::min(
                tx.size,
                i + (ENTRIES_PER_SEGMENT * MAX_DOWNLOAD_TASK) as u64,
            );
            for j in (i..tasks_end_index).step_by(ENTRIES_PER_SEGMENT) {
                let task_end_index = cmp::min(tasks_end_index, j + ENTRIES_PER_SEGMENT as u64);
                tasks.push(Box::pin(self.download(tx, j, task_end_index)));
            }
            for task in tasks.into_iter() {
                task.await?;
            }
        }
        self.store.write().await.finalize_tx(tx.seq)?;
        Ok(())
    }

    pub async fn run(&self) {
        let connection = match self.store.read().await.get_stream_db_connection() {
            Ok(conn) => conn,
            Err(e) => {
                error!("build sqlite connection error: e={:?}", e);
                return;
            }
        };
        let mut tx_seq;
        match self
            .store
            .read()
            .await
            .get_stream_data_sync_progress(&connection)
        {
            Ok(progress) => {
                tx_seq = progress;
            }
            Err(e) => {
                error!("get stream data sync progress error: e={:?}", e);
                return;
            }
        }
        loop {
            let maybe_tx = self.store.read().await.get_tx_by_seq_number(tx_seq);
            match maybe_tx {
                Ok(Some(tx)) => {
                    let mut skip = false;
                    if tx.stream_ids.is_empty() {
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
                        match self.sync_data(&tx).await {
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
