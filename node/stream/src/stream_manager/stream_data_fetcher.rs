use crate::StreamConfig;
use anyhow::{anyhow, bail, Result};
use jsonrpsee::http_client::HttpClient;
use rpc::IonianRpcClient;
use shared_types::{ChunkArray, Transaction};
use std::{
    cmp,
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};
use storage_with_stream::{log_store::log_manager::ENTRY_SIZE, Store};
use task_executor::TaskExecutor;
use tokio::sync::{
    mpsc::{self, UnboundedSender},
    RwLock,
};

const RETRY_WAIT_MS: u64 = 1000;
const ENTRIES_PER_SEGMENT: usize = 1024;
const MAX_DOWNLOAD_TASK: usize = 5;
const ALERT_CNT: i32 = 10;
const MAX_RETRY: usize = 5;

pub struct StreamDataFetcher {
    config: StreamConfig,
    store: Arc<RwLock<dyn Store>>,
    clients: Vec<HttpClient>,
    task_executor: TaskExecutor,
}

async fn download_with_proof(
    client: HttpClient,
    tx: Arc<Transaction>,
    start_index: usize,
    end_index: usize,
    store: Arc<RwLock<dyn Store>>,
    sender: UnboundedSender<Result<(), (usize, usize, bool)>>,
) {
    let mut fail_cnt = 0;
    while fail_cnt < ALERT_CNT {
        debug!("download_with_proof for {}", start_index);
        match client
            .download_segment_with_proof(tx.data_merkle_root, start_index / ENTRIES_PER_SEGMENT)
            .await
        {
            Ok(Some(segment)) => {
                if segment.data.len() % ENTRY_SIZE != 0
                    || segment.data.len() / ENTRY_SIZE != end_index - start_index
                {
                    debug!("invalid data length");
                    if let Err(e) = sender.send(Err((start_index, end_index, true))) {
                        error!("send error: {:?}", e);
                    }

                    return;
                }

                if segment.root != tx.data_merkle_root {
                    debug!("invalid file root");
                    if let Err(e) = sender.send(Err((start_index, end_index, true))) {
                        error!("send error: {:?}", e);
                    }

                    return;
                }

                if let Err(e) = segment.validate(ENTRIES_PER_SEGMENT) {
                    debug!("validate segment with error: {:?}", e);

                    if let Err(e) = sender.send(Err((start_index, end_index, true))) {
                        error!("send error: {:?}", e);
                    }
                    return;
                }

                if let Err(e) = store.write().await.put_chunks_with_tx_hash(
                    tx.seq,
                    tx.hash(),
                    ChunkArray {
                        data: segment.data,
                        start_index: (segment.index * ENTRIES_PER_SEGMENT) as u64,
                    },
                ) {
                    debug!("put segment with error: {:?}", e);

                    if let Err(e) = sender.send(Err((start_index, end_index, true))) {
                        error!("send error: {:?}", e);
                    }
                    return;
                }

                debug!("download start_index {:?} successful", start_index);
                if let Err(e) = sender.send(Ok(())) {
                    error!("send error: {:?}", e);
                }

                return;
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
    }

    if let Err(e) = sender.send(Err((start_index, end_index, false))) {
        error!("send error: {:?}", e);
    }
}

impl StreamDataFetcher {
    pub async fn new(
        config: StreamConfig,
        store: Arc<RwLock<dyn Store>>,
        clients: Vec<HttpClient>,
        task_executor: TaskExecutor,
    ) -> Result<Self> {
        Ok(Self {
            config,
            store,
            clients,
            task_executor,
        })
    }

    fn spawn_download_task(
        &self,
        client_index: &mut usize,
        tx: Arc<Transaction>,
        start_index: usize,
        end_index: usize,
        sender: &UnboundedSender<Result<(), (usize, usize, bool)>>,
    ) {
        debug!(
            "downloading start_index {:?}, end_index: {:?} from client index: {}",
            start_index, end_index, client_index
        );

        self.task_executor.spawn(
            download_with_proof(
                self.clients[*client_index].clone(),
                tx,
                start_index,
                end_index,
                self.store.clone(),
                sender.clone(),
            ),
            "download segment",
        );

        // round robin client
        *client_index = (*client_index + 1) % self.clients.len();
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

        let mut pending_entries = VecDeque::new();
        let mut task_counter = 0;
        let mut client_index = 0;
        let (sender, mut rx) = mpsc::unbounded_channel();
        let tx = Arc::new(tx.clone());

        for i in (0..tx_size_in_entry).step_by(ENTRIES_PER_SEGMENT * MAX_DOWNLOAD_TASK) {
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
                pending_entries.push_back((j as usize, task_end_index as usize));
            }
        }

        // spawn download tasks
        while task_counter < MAX_DOWNLOAD_TASK && !pending_entries.is_empty() {
            let (start_index, end_index) = pending_entries.pop_front().unwrap();
            self.spawn_download_task(
                &mut client_index,
                tx.clone(),
                start_index,
                end_index,
                &sender,
            );
            task_counter += 1;
        }

        let mut failed_tasks = HashMap::new();
        while task_counter > 0 {
            if let Some(ret) = rx.recv().await {
                match ret {
                    Ok(_) => {
                        if let Some((start_index, end_index)) = pending_entries.pop_front() {
                            self.spawn_download_task(
                                &mut client_index,
                                tx.clone(),
                                start_index,
                                end_index,
                                &sender,
                            );
                        } else {
                            task_counter -= 1;
                        }
                    }
                    Err((start_index, end_index, data_err)) => {
                        warn!("Download data of tx_seq {:?}, start_index {:?}, end_index {:?}, failed",tx.seq, start_index, end_index);

                        match failed_tasks.get_mut(&start_index) {
                            Some(c) => {
                                if data_err {
                                    *c += 1;
                                }

                                if *c == self.clients.len() * MAX_RETRY {
                                    bail!(anyhow!(format!("Download segment failed, start_index {:?}, end_index: {:?}", start_index, end_index)));
                                }
                            }
                            _ => {
                                failed_tasks.insert(start_index, 1);
                            }
                        }

                        self.spawn_download_task(
                            &mut client_index,
                            tx.clone(),
                            start_index,
                            end_index,
                            &sender,
                        );
                    }
                }
            }
        }

        self.store
            .write()
            .await
            .finalize_tx_with_hash(tx.seq, tx.hash())?;
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
