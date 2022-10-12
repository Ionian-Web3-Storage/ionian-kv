use crate::StreamConfig;
use anyhow::{anyhow, bail, Result};
use jsonrpsee::http_client::HttpClient;
use rpc::IonianRpcClient;
use shared_types::{ChunkArray, Transaction};
use std::{cmp, sync::Arc, time::Duration};
use storage_with_stream::{log_store::log_manager::ENTRY_SIZE, Store};
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

    async fn download_with_proof(
        &self,
        tx: &Transaction,
        start_index: usize,
        end_index: usize,
    ) -> Result<()> {
        let mut fail_cnt = 0;
        loop {
            match self
                .client
                .download_segment_with_proof(tx.data_merkle_root, start_index as usize)
                .await
            {
                Ok(Some(segment)) => {
                    if segment.data.len() % ENTRY_SIZE != 0
                        || segment.data.len() / ENTRY_SIZE != end_index - start_index
                    {
                        bail!(anyhow!("invalid data length"));
                    }

                    if segment.root != tx.data_merkle_root {
                        bail!(anyhow!("invalid file root"));
                    }

                    segment.validate(ENTRIES_PER_SEGMENT)?;
                    self.store.write().await.put_chunks(
                        tx.seq,
                        ChunkArray {
                            data: segment.data,
                            start_index: (segment.index * ENTRIES_PER_SEGMENT) as u64,
                        },
                    )?;
                    return Ok(());
                }
                Ok(None) => {
                    debug!(
                        "start_index {:?}, end_index {:?}, response is none",
                        start_index, end_index
                    );
                    fail_cnt += 1;
                    tokio::time::sleep(Duration::from_millis(RETRY_WAIT_MS)).await;
                }
                Err(e) => {
                    debug!(
                        "start_index {:?}, end_index {:?}, response error: {:?}",
                        start_index, end_index, e
                    );
                    fail_cnt += 1;
                    tokio::time::sleep(Duration::from_millis(RETRY_WAIT_MS)).await;
                }
            }
            if fail_cnt % ALERT_CNT == 0 {
                warn!(
                    "Download data of tx_seq {:?}, start_index {:?}, end_index {:?}, failed for {:?} times",
                    tx.seq, start_index, end_index, fail_cnt
                );
            }
        }
    }

    async fn sync_data(&self, tx: &Transaction) -> Result<()> {
        if self.store.read().await.check_tx_completed(tx.seq)? {
            return Ok(());
        }
        let tx_size_in_entry = if tx.size % ENTRY_SIZE as u64 == 0 {
            tx.size / ENTRY_SIZE as u64
        } else {
            tx.size / ENTRY_SIZE as u64 + 1
        };
        for i in (0..tx_size_in_entry).step_by(ENTRIES_PER_SEGMENT * MAX_DOWNLOAD_TASK) {
            let mut tasks = vec![];
            let tasks_end_index = cmp::min(
                tx_size_in_entry,
                i + (ENTRIES_PER_SEGMENT * MAX_DOWNLOAD_TASK) as u64,
            );
            debug!(
                "task_start_index: {:?}, tasks_end_index: {:?}, tx_size_in_entry: {:?}, root: {:?}",
                i, tasks_end_index, tx_size_in_entry, tx.data_merkle_root
            );
            for j in (i..tasks_end_index).step_by(ENTRIES_PER_SEGMENT) {
                let task_end_index = cmp::min(tasks_end_index, j + ENTRIES_PER_SEGMENT as u64);
                debug!(
                    "downloading start_index {:?}, end_index: {:?}",
                    j, task_end_index
                );
                tasks.push(Box::pin(self.download_with_proof(
                    tx,
                    j as usize,
                    task_end_index as usize,
                )));
            }
            for task in tasks.into_iter() {
                task.await?;
            }
        }
        self.store.write().await.finalize_tx(tx.seq)?;
        Ok(())
    }

    pub async fn run(&self) {
        let mut tx_seq;
        match self
            .store
            .read()
            .await
            .get_stream_data_sync_progress()
            .await
        {
            Ok(progress) => {
                tx_seq = progress;
            }
            Err(e) => {
                error!("get stream data sync progress error: e={:?}", e);
                return;
            }
        }

        let mut check_sync_progress = false;
        loop {
            if check_sync_progress {
                match self
                    .store
                    .read()
                    .await
                    .get_stream_data_sync_progress()
                    .await
                {
                    Ok(progress) => {
                        if tx_seq != progress {
                            debug!("reorg happend: tx_seq {}, progress {}", tx_seq, progress);
                            tx_seq = progress;
                        }
                    }
                    Err(e) => {
                        error!("get stream data sync progress error: e={:?}", e);
                    }
                }

                check_sync_progress = false;
            }

            info!("checking tx with sequence number {:?}..", tx_seq);
            let maybe_tx = self.store.read().await.get_tx_by_seq_number(tx_seq);
            match maybe_tx {
                Ok(Some(tx)) => {
                    let mut skip = false;
                    if tx.stream_ids.is_empty() {
                        skip = true;
                    } else {
                        for id in tx.stream_ids.iter() {
                            if !self.config.stream_set.contains(id) {
                                skip = true;
                                break;
                            }
                        }
                    }
                    // sync data
                    if !skip {
                        info!("syncing data of tx with sequence number {:?}..", tx.seq);
                        match self.sync_data(&tx).await {
                            Ok(()) => {
                                info!("data of tx with sequence number {:?} synced.", tx.seq);
                            }
                            Err(e) => {
                                error!("stream data sync error: e={:?}", e);
                                check_sync_progress = true;
                                continue;
                            }
                        }
                    } else {
                        info!("tx {:?} is not in stream, skipped.", tx.seq);
                    }
                    // update progress, get next tx_seq to sync
                    match self
                        .store
                        .write()
                        .await
                        .update_stream_data_sync_progress(tx_seq, tx_seq + 1)
                        .await
                    {
                        Ok(next_tx_seq) => {
                            tx_seq = next_tx_seq;
                        }
                        Err(e) => {
                            error!("update stream data sync progress error: e={:?}", e);
                        }
                    }
                }
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(RETRY_WAIT_MS)).await;
                    check_sync_progress = true;
                }
                Err(e) => {
                    error!("stream data sync error: e={:?}", e);
                    tokio::time::sleep(Duration::from_millis(RETRY_WAIT_MS)).await;
                    check_sync_progress = true;
                }
            }
        }
    }
}
