mod stream_data_fetcher;
mod stream_replayer;

use crate::StreamConfig;
use anyhow::Result;
use ethereum_types::H256;
use jsonrpsee::http_client::HttpClient;
use ssz::Encode;
use std::{collections::HashSet, sync::Arc};
use storage::log_store::Store;
use task_executor::TaskExecutor;
use tokio::sync::RwLock;

use self::{stream_data_fetcher::StreamDataFetcher, stream_replayer::StreamReplayer};

pub struct StreamManager;

pub const RETRY_WAIT_MS: u64 = 1000;

impl StreamManager {
    pub async fn initialize(
        config: &StreamConfig,
        store: Arc<RwLock<dyn Store>>,
        client: HttpClient,
    ) -> Result<(StreamDataFetcher, StreamReplayer)> {
        let connection = store.read().await.get_stream_db_connection()?;
        // initialize
        let holding_stream_ids = store.read().await.get_holding_stream_ids(&connection)?;
        let holding_stream_set: HashSet<H256> =
            HashSet::from_iter(holding_stream_ids.iter().cloned());
        // ensure current stream id set is a subset of streams maintained in db
        let mut reseted = false;
        for id in config.stream_ids.iter() {
            if holding_stream_set.get(id) == None {
                // new stream id, replay from start
                store
                    .write()
                    .await
                    .reset_stream_sync(&connection, &config.stream_ids.as_ssz_bytes())?;
                reseted = true;
                break;
            }
        }
        // is a subset, update stream ids in db
        if !reseted && config.stream_ids.len() != holding_stream_ids.len() {
            store
                .write()
                .await
                .update_stream_ids(&connection, &config.stream_ids.as_ssz_bytes())?;
        }
        // spawn data sync and stream replay threads
        let fetcher = StreamDataFetcher::new(config.clone(), store.clone(), client).await?;
        let replayer = StreamReplayer::new(config.clone(), store.clone()).await?;
        Ok((fetcher, replayer))
    }

    pub fn spawn(
        fetcher: StreamDataFetcher,
        replayer: StreamReplayer,
        executor: TaskExecutor,
    ) -> Result<()> {
        executor.spawn(
            async move { Box::pin(fetcher.run()).await },
            "stream data fetcher",
        );

        executor.spawn(
            async move { Box::pin(replayer.run()).await },
            "stream data fetcher",
        );
        Ok(())
    }
}
