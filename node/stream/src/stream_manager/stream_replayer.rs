use crate::StreamConfig;
use anyhow::{bail, Result};
use ethereum_types::{H160, H256};
use shared_types::{
    AccessControl, AccessControlSet, StreamRead, StreamReadSet, StreamWrite, StreamWriteSet,
    Transaction,
};
use ssz::Decode;
use std::collections::{HashMap, HashSet};
use std::{cmp, sync::Arc, time::Duration};
use storage_with_stream::error::Error;
use storage_with_stream::log_store::log_manager::ENTRY_SIZE;
use storage_with_stream::AccessControlOps;
use storage_with_stream::Store;
use tokio::sync::RwLock;

use super::RETRY_WAIT_MS;

const MAX_LOAD_ENTRY_SIZE: u64 = 10;
const STREAM_ID_SIZE: u64 = 32;
const STREAM_KEY_SIZE: u64 = 32;
const SET_LEN_SIZE: u64 = 4;
const DATA_LEN_SIZE: u64 = 8;
const VERSION_SIZE: u64 = 8;
const ACCESS_CONTROL_OP_TYPE_SIZE: u64 = 1;
const ADDRESS_SIZE: u64 = 20;
const MAX_SIZE_LEN: u32 = 65536;

struct StreamReader<'a> {
    store: Arc<RwLock<dyn Store>>,
    tx: &'a Transaction,
    current_position: u64, // the index of next entry to read
    buffer: Vec<u8>,       // buffered data
}

impl<'a> StreamReader<'a> {
    pub fn new(store: Arc<RwLock<dyn Store>>, tx: &'a Transaction) -> Self {
        Self {
            store,
            tx,
            current_position: 0,
            buffer: vec![],
        }
    }

    pub fn current_position_in_bytes(&self) -> u64 {
        (self.current_position + self.tx.start_entry_index) * (ENTRY_SIZE as u64)
            - (self.buffer.len() as u64)
    }

    async fn load(&mut self, length: u64) -> Result<()> {
        match self
            .store
            .read()
            .await
            .get_chunk_by_flow_index(self.current_position + self.tx.start_entry_index, length)?
        {
            Some(mut x) => {
                self.buffer.append(&mut x.data);
                self.current_position += length;
                Ok(())
            }
            None => {
                bail!("only partial data are available");
            }
        }
    }

    // read next ${size} bytes from the stream
    pub async fn next(&mut self, size: u64) -> Result<Vec<u8>> {
        if (self.buffer.len() as u64) + (self.tx.size - self.current_position) * (ENTRY_SIZE as u64)
            < size
        {
            bail!("next target position is larger than tx size");
        }
        while (self.buffer.len() as u64) < size {
            self.load(cmp::min(
                self.tx.size - self.current_position,
                MAX_LOAD_ENTRY_SIZE,
            ))
            .await?;
        }
        Ok(self.buffer.drain(0..(size as usize)).collect())
    }

    pub async fn skip(&mut self, mut size: u64) -> Result<()> {
        if (self.buffer.len() as u64) >= size {
            self.buffer.drain(0..(size as usize));
            return Ok(());
        }
        size -= self.buffer.len() as u64;
        self.buffer.clear();
        let entries_to_skip = size / (ENTRY_SIZE as u64);
        self.current_position += entries_to_skip;
        size -= entries_to_skip * (ENTRY_SIZE as u64);
        if size > 0 {
            self.next(size).await?;
        }
        Ok(())
    }
}

pub struct StreamReplayer {
    config: StreamConfig,
    store: Arc<RwLock<dyn Store>>,
}

impl StreamReplayer {
    pub async fn new(config: StreamConfig, store: Arc<RwLock<dyn Store>>) -> Result<Self> {
        Ok(Self { config, store })
    }

    async fn parse_version(&self, stream_reader: &mut StreamReader<'_>) -> Result<u64> {
        Ok(u64::from_be_bytes(
            stream_reader.next(VERSION_SIZE).await?.try_into().unwrap(),
        ))
    }

    async fn parse_stream_read_set(
        &self,
        stream_reader: &mut StreamReader<'_>,
    ) -> Result<Option<StreamReadSet>> {
        let size = u32::from_be_bytes(stream_reader.next(SET_LEN_SIZE).await?.try_into().unwrap());
        if size > MAX_SIZE_LEN {
            return Ok(None);
        }
        let mut stream_read_set = StreamReadSet {
            stream_reads: vec![],
        };
        for _ in 0..(size as usize) {
            stream_read_set.stream_reads.push(StreamRead {
                stream_id: H256::from_ssz_bytes(&stream_reader.next(STREAM_ID_SIZE).await?)
                    .map_err(Error::from)?,
                key: H256::from_ssz_bytes(&stream_reader.next(STREAM_KEY_SIZE).await?)
                    .map_err(Error::from)?,
            });
        }
        Ok(Some(stream_read_set))
    }

    async fn validate_stream_read_set(
        &self,
        stream_read_set: &StreamReadSet,
        tx: &Transaction,
        version: u64,
    ) -> Result<bool> {
        for stream_read in stream_read_set.stream_reads.iter() {
            if !self.config.stream_set.contains(&stream_read.stream_id) {
                return Ok(false);
            }
            // check version confiction
            if self
                .store
                .read()
                .await
                .get_latest_version_before(stream_read.stream_id, stream_read.key, tx.seq)
                .await?
                > version
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn parse_stream_write_set(
        &self,
        stream_reader: &mut StreamReader<'_>,
    ) -> Result<Option<StreamWriteSet>> {
        let size = u32::from_be_bytes(stream_reader.next(SET_LEN_SIZE).await?.try_into().unwrap());
        if size > MAX_SIZE_LEN {
            return Ok(None);
        }
        let stream_write_info_length = size as u64 * (32 + 32 + 8);
        let mut write_data_start_index =
            stream_reader.current_position_in_bytes() + stream_write_info_length;
        // use a hashmap to filter out the duplicate writes on same key, only the last one is reserved
        let mut stream_writes = HashMap::new();
        for _ in 0..(size as usize) {
            let stream_id = H256::from_ssz_bytes(&stream_reader.next(STREAM_ID_SIZE).await?)
                .map_err(Error::from)?;
            let key = H256::from_ssz_bytes(&stream_reader.next(STREAM_KEY_SIZE).await?)
                .map_err(Error::from)?;
            let start_index = write_data_start_index;
            let end_index = write_data_start_index
                + u64::from_be_bytes(stream_reader.next(DATA_LEN_SIZE).await?.try_into().unwrap());
            stream_writes.insert(
                (stream_id, key),
                StreamWrite {
                    stream_id,
                    key,
                    start_index,
                    end_index,
                },
            );
            write_data_start_index = end_index;
        }
        // skip the write data
        stream_reader
            .skip(write_data_start_index - stream_reader.current_position_in_bytes())
            .await?;
        Ok(Some(StreamWriteSet {
            stream_writes: stream_writes.into_values().collect(),
        }))
    }

    async fn validate_stream_write_set(
        &self,
        stream_write_set: &StreamWriteSet,
        tx: &Transaction,
        version: u64,
    ) -> Result<bool> {
        let stream_set = HashSet::<H256>::from_iter(tx.stream_ids.iter().cloned());
        for stream_write in stream_write_set.stream_writes.iter() {
            if !stream_set.contains(&stream_write.stream_id) {
                // the write set in data is conflict with tx tags
                return Ok(false);
            }
            // check write permission
            if !(self
                .store
                .read()
                .await
                .has_write_permission(tx.sender, stream_write.stream_id, stream_write.key, version)
                .await?)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn parse_access_control_data(
        &self,
        tx: &Transaction,
        stream_reader: &mut StreamReader<'_>,
    ) -> Result<Option<AccessControlSet>> {
        let size = u32::from_be_bytes(stream_reader.next(SET_LEN_SIZE).await?.try_into().unwrap());
        if size > MAX_SIZE_LEN {
            return Ok(None);
        }
        // use a hashmap to filter out the useless operations
        // all operations can be categorized by op_type & 0xf0
        // for each category, except GRANT_ADMIN_ROLE, only the last operation of each account is reserved
        let mut access_ops = HashMap::new();
        for _ in 0..(size as usize) {
            let op_type = u8::from_be_bytes(
                stream_reader
                    .next(ACCESS_CONTROL_OP_TYPE_SIZE)
                    .await?
                    .try_into()
                    .unwrap(),
            );
            // parse operation data
            let stream_id = H256::from_ssz_bytes(&stream_reader.next(STREAM_ID_SIZE).await?)
                .map_err(Error::from)?;
            let mut account = H160::zero();
            let mut key = H256::zero();
            match op_type {
                // stream_id + account
                AccessControlOps::GRANT_ADMIN_ROLE
                | AccessControlOps::GRANT_WRITER_ROLE
                | AccessControlOps::REVOKE_WRITER_ROLE => {
                    account = H160::from_ssz_bytes(&stream_reader.next(ADDRESS_SIZE).await?)
                        .map_err(Error::from)?;
                }
                // stream_id + key
                AccessControlOps::SET_KEY_TO_NORMAL | AccessControlOps::SET_KEY_TO_SPECIAL => {
                    key = H256::from_ssz_bytes(&stream_reader.next(STREAM_KEY_SIZE).await?)
                        .map_err(Error::from)?;
                }
                // stream_id + key + account
                AccessControlOps::GRANT_SPECIAL_WRITER_ROLE
                | AccessControlOps::REVOKE_SPECIAL_WRITER_ROLE => {
                    key = H256::from_ssz_bytes(&stream_reader.next(STREAM_KEY_SIZE).await?)
                        .map_err(Error::from)?;
                    account = H160::from_ssz_bytes(&stream_reader.next(ADDRESS_SIZE).await?)
                        .map_err(Error::from)?;
                }
                // renounce type
                AccessControlOps::RENOUNCE_ADMIN_ROLE | AccessControlOps::RENOUNCE_WRITER_ROLE => {
                    account = tx.sender;
                }
                AccessControlOps::RENOUNCE_SPECIAL_WRITER_ROLE => {
                    key = H256::from_ssz_bytes(&stream_reader.next(STREAM_KEY_SIZE).await?)
                        .map_err(Error::from)?;
                    account = tx.sender;
                }
                // unexpected type
                _ => {
                    bail!("unexpected access control op type");
                }
            }
            let op_meta = (op_type & 0xf0, stream_id, key, account);
            if op_type != AccessControlOps::GRANT_ADMIN_ROLE
                || (!access_ops.contains_key(&op_meta) && account != tx.sender)
            {
                access_ops.insert(
                    op_meta,
                    AccessControl {
                        op_type,
                        stream_id,
                        key,
                        account,
                    },
                );
            }
        }
        Ok(Some(AccessControlSet {
            access_controls: access_ops.into_values().collect(),
        }))
    }

    async fn validate_access_control_set(
        &self,
        access_control_set: &mut AccessControlSet,
        tx: &Transaction,
        version: u64,
    ) -> Result<bool> {
        // pad GRANT_ADMIN_ROLE prefix to handle the first write to new stream
        let mut with_prefix_grant_admin_role = vec![];
        let mut is_admin = HashSet::new();
        for id in &tx.stream_ids {
            if self.store.read().await.is_new_stream(*id, version).await? {
                with_prefix_grant_admin_role.push(AccessControl {
                    op_type: AccessControlOps::GRANT_ADMIN_ROLE,
                    stream_id: *id,
                    key: H256::zero(),
                    account: tx.sender,
                });
                is_admin.insert(*id);
            } else if self
                .store
                .read()
                .await
                .is_admin(tx.sender, *id, version)
                .await?
            {
                is_admin.insert(*id);
            }
        }
        with_prefix_grant_admin_role.append(&mut access_control_set.access_controls);
        access_control_set.access_controls = with_prefix_grant_admin_role;
        // validate
        let stream_set = HashSet::<H256>::from_iter(tx.stream_ids.iter().cloned());
        for access_control in &access_control_set.access_controls {
            if !stream_set.contains(&access_control.stream_id) {
                // the write set in data is conflict with tx tags
                return Ok(false);
            }
            match access_control.op_type {
                AccessControlOps::GRANT_ADMIN_ROLE
                | AccessControlOps::SET_KEY_TO_NORMAL
                | AccessControlOps::SET_KEY_TO_SPECIAL
                | AccessControlOps::GRANT_WRITER_ROLE
                | AccessControlOps::REVOKE_WRITER_ROLE
                | AccessControlOps::GRANT_SPECIAL_WRITER_ROLE
                | AccessControlOps::REVOKE_SPECIAL_WRITER_ROLE => {
                    if !is_admin.contains(&access_control.stream_id) {
                        return Ok(false);
                    }
                }
                _ => {}
            }
        }
        Ok(true)
    }

    async fn replay(&self, tx: &Transaction) -> Result<bool> {
        if self.store.read().await.check_tx_completed(tx.seq)? {
            return Ok(false);
        }
        let mut stream_reader = StreamReader::new(self.store.clone(), tx);
        // parse and validate
        let version = self.parse_version(&mut stream_reader).await?;
        let stream_read_set = match self.parse_stream_read_set(&mut stream_reader).await? {
            Some(x) => x,
            None => {
                return Ok(true);
            }
        };
        if !(self
            .validate_stream_read_set(&stream_read_set, tx, version)
            .await?)
        {
            // there is confliction in stream read set
            return Ok(true);
        }
        let stream_write_set = match self.parse_stream_write_set(&mut stream_reader).await? {
            Some(x) => x,
            None => {
                return Ok(true);
            }
        };
        if !(self
            .validate_stream_write_set(&stream_write_set, tx, version)
            .await?)
        {
            // there is confliction in stream write set
            // or sender does not have the write permission of all keys
            return Ok(true);
        }
        let mut access_control_set = match self
            .parse_access_control_data(tx, &mut stream_reader)
            .await?
        {
            Some(x) => x,
            None => return Ok(true),
        };
        if !(self
            .validate_access_control_set(&mut access_control_set, tx, version)
            .await?)
        {
            // there is confliction in access control set
            return Ok(true);
        }
        // update database
        self.store
            .write()
            .await
            .put_stream(tx.seq, stream_write_set, access_control_set)
            .await?;
        Ok(true)
    }

    pub async fn run(&self) {
        let mut tx_seq;
        match self.store.read().await.get_stream_replay_progress().await {
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
                    // replay data
                    if !skip {
                        info!("replaying data of tx with sequence number {:?}..", tx.seq);
                        match self.replay(&tx).await {
                            Ok(true) => {
                                info!("tx with sequence number {:?} replayed.", tx.seq);
                            }
                            Ok(false) => {
                                // data not available
                                info!("data of tx with sequence number {:?} is not available yet, wait..", tx.seq);
                                tokio::time::sleep(Duration::from_millis(RETRY_WAIT_MS)).await;
                                continue;
                            }
                            Err(e) => {
                                error!("update stream replay progress error: e={:?}", e);
                                tokio::time::sleep(Duration::from_millis(RETRY_WAIT_MS)).await;
                                continue;
                            }
                        }
                    } else {
                        info!("tx {:?} is not in stream, skipped.", tx.seq);
                    }
                    // parse success
                    // update progress, get next tx_seq to sync
                    match self
                        .store
                        .write()
                        .await
                        .update_stream_replay_progress(tx_seq, tx_seq + 1)
                        .await
                    {
                        Ok(next_tx_seq) => {
                            tx_seq = next_tx_seq;
                        }
                        Err(e) => {
                            error!("update stream replay progress error: e={:?}", e);
                        }
                    }
                }
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(RETRY_WAIT_MS)).await;
                }
                Err(e) => {
                    error!("stream replay error: e={:?}", e);
                    tokio::time::sleep(Duration::from_millis(RETRY_WAIT_MS)).await;
                }
            }
        }
    }
}
