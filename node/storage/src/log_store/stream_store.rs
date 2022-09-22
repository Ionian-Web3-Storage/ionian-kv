use anyhow::{bail, Result};
use ethereum_types::{H160, H256};
use shared_types::{StreamWriteSet, AccessControlSet};
use ssz::{Decode, Encode};
use std::path::Path;

use rusqlite::named_params;
use tokio_rusqlite::Connection;

use crate::error::Error;

use super::sqlite_db_statements::SqliteDBStatements;

pub struct StreamStore {
    connection: Connection,
}

impl StreamStore {
    pub async fn create_tables_if_not_exist(&self) -> Result<()> {
        self.connection
            .call(|conn| {
                // misc table
                conn.execute(SqliteDBStatements::CREATE_MISC_TABLE_STATEMENT, [])?;
                // stream table
                conn.execute(SqliteDBStatements::CREATE_STREAM_TABLE_STATEMENT, [])?;
                for stmt in SqliteDBStatements::CREATE_STREAM_INDEX_STATEMENTS.iter() {
                    conn.execute(stmt, [])?;
                }
                // access control table
                conn.execute(
                    SqliteDBStatements::CREATE_ACCESS_CONTROL_TABLE_STATEMENT,
                    [],
                )?;
                for stmt in SqliteDBStatements::CREATE_ACCESS_CONTROL_INDEX_STATEMENTS.iter() {
                    conn.execute(stmt, [])?;
                }
                Ok(())
            })
            .await
    }

    pub async fn new_in_memory() -> Result<Self> {
        let connection = Connection::open_in_memory().await?;
        Ok(Self { connection })
    }

    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let connection = Connection::open(path).await?;
        Ok(Self { connection })
    }

    pub async fn get_stream_ids(&self) -> Result<Vec<H256>> {
        self.connection
            .call(|conn| {
                let mut stmt = conn.prepare(SqliteDBStatements::GET_STREAM_IDS_STATEMENT)?;
                let mut rows = stmt.query_map([], |row| row.get(0))?;
                if let Some(raw_data) = rows.next() {
                    let raw_stream_ids: Vec<u8> = raw_data?;
                    return Ok(Vec::<H256>::from_ssz_bytes(&raw_stream_ids).map_err(Error::from)?);
                }
                Ok(vec![])
            })
            .await
    }

    pub async fn update_stream_ids(&self, stream_ids: Vec<u8>) -> Result<()> {
        self.connection
            .call(move |conn| {
                let mut stmt = conn.prepare(SqliteDBStatements::UPDATE_STREAM_IDS_STATEMENT)?;
                stmt.execute(named_params! {
                    ":stream_ids": stream_ids,
                    ":id": 0,
                })?;
                Ok(())
            })
            .await
    }

    pub async fn reset_stream_sync(&self, stream_ids: Vec<u8>) -> Result<()> {
        self.connection
            .call(move |conn| {
                let mut stmt = conn.prepare(SqliteDBStatements::RESET_STERAM_SYNC_STATEMENT)?;
                stmt.execute(named_params! {
                    ":data_sync_progress": 0,
                    ":stream_replay_progress": 0,
                    ":stream_ids": stream_ids,
                    ":id": 0,
                })?;
                Ok(())
            })
            .await
    }

    pub async fn get_stream_data_sync_progress(&self) -> Result<u64> {
        self.connection
            .call(|conn| {
                let mut stmt =
                    conn.prepare(SqliteDBStatements::GET_STREAM_DATA_SYNC_PROGRESS_STATEMENT)?;
                let mut rows = stmt.query_map([], |row| row.get(0))?;
                if let Some(raw_data) = rows.next() {
                    return Ok(raw_data?);
                }
                Ok(0)
            })
            .await
    }

    pub async fn update_stream_data_sync_progress(
        &self,
        from: u64,
        progress: u64,
    ) -> Result<usize> {
        self.connection
            .call(move |conn| {
                let mut stmt =
                    conn.prepare(SqliteDBStatements::UPDATE_STREAM_DATA_SYNC_PROGRESS_STATEMENT)?;
                Ok(stmt.execute(named_params! {
                    ":data_sync_progress": progress,
                    ":id": 0,
                    ":from": from,
                })?)
            })
            .await
    }

    pub async fn get_stream_replay_progress(&self) -> Result<u64> {
        self.connection
            .call(|conn| {
                let mut stmt =
                    conn.prepare(SqliteDBStatements::GET_STREAM_REPLAY_PROGRESS_STATEMENT)?;
                let mut rows = stmt.query_map([], |row| row.get(0))?;
                if let Some(raw_data) = rows.next() {
                    return Ok(raw_data?);
                }
                Ok(0)
            })
            .await
    }

    pub async fn update_stream_replay_progress(&self, from: u64, progress: u64) -> Result<usize> {
        self.connection
            .call(move |conn| {
                let mut stmt =
                    conn.prepare(SqliteDBStatements::UPDATE_STREAM_REPLAY_PROGRESS_STATEMENT)?;
                Ok(stmt.execute(named_params! {
                    ":stream_replay_progress": progress,
                    ":id": 0,
                    ":from": from,
                })?)
            })
            .await
    }

    pub async fn get_latest_version_before(
        &self,
        stream_id: H256,
        key: H256,
        before: u64,
    ) -> Result<u64> {
        self.connection
            .call(move |conn| {
                let mut stmt =
                    conn.prepare(SqliteDBStatements::GET_LATEST_VERSION_BEFORE_STATEMENT)?;
                let mut rows = stmt.query_map(
                    named_params! {
                        ":stream_id": stream_id.as_ssz_bytes(),
                        ":key": key.as_ssz_bytes(),
                        ":before": before,
                    },
                    |row| row.get(0),
                )?;
                if let Some(raw_data) = rows.next() {
                    return Ok(raw_data?);
                }
                Ok(0)
            })
            .await
    }

    pub async fn is_new_stream(&self, stream_id: H256, version: u64) -> Result<bool> {
        self.connection
            .call(move |conn| {
                let mut stmt = conn.prepare(SqliteDBStatements::IS_NEW_STREAM_STATEMENT)?;
                let mut rows = stmt.query_map(
                    named_params! {
                        ":stream_id": stream_id.as_ssz_bytes(),
                        ":version": version,
                    },
                    |row| row.get(0),
                )?;
                if let Some(raw_data) = rows.next() {
                    let num: u64 = raw_data?;
                    if num > 0 {
                        return Ok(false);
                    }
                    return Ok(true);
                } else {
                    bail!("unexpected empty rows");
                }
            })
            .await
    }

    pub async fn is_special_key(&self, stream_id: H256, key: H256, version: u64) -> Result<bool> {
        self.connection
            .call(move |conn| {
                let mut stmt = conn.prepare(SqliteDBStatements::IS_SPECIAL_KEY_STATEMENT)?;
                let mut rows = stmt.query_map(
                    named_params! {
                        ":stream_id": stream_id.as_ssz_bytes(),
                        ":key": key.as_ssz_bytes(),
                        ":version": version,
                    },
                    |row| row.get(0),
                )?;
                if let Some(raw_data) = rows.next() {
                    match raw_data? {
                        AccessControlOps::SET_KEY_TO_NORMAL => Ok(false),
                        AccessControlOps::SET_KEY_TO_SPECIAL => Ok(true),
                        _ => {
                            bail!("unexpected access control op type");
                        }
                    }
                } else {
                    return Ok(false);
                }
            })
            .await
    }

    pub async fn is_admin(&self, account: H160, stream_id: H256, version: u64) -> Result<bool> {
        self.connection
            .call(move |conn| {
                let mut stmt = conn.prepare(SqliteDBStatements::IS_ADMIN_STATEMENT)?;
                let mut rows = stmt.query_map(
                    named_params! {
                        ":stream_id": stream_id.as_ssz_bytes(),
                        ":account": account.as_ssz_bytes(),
                        ":version": version,
                    },
                    |row| row.get(0),
                )?;
                if let Some(raw_data) = rows.next() {
                    let num: u64 = raw_data?;
                    if num > 0 {
                        return Ok(false);
                    }
                    return Ok(true);
                } else {
                    bail!("unexpected empty rows");
                }
            })
            .await
    }

    pub async fn is_writer_for_key(
        &self,
        account: H160,
        stream_id: H256,
        key: H256,
        version: u64,
    ) -> Result<bool> {
        self.connection
            .call(move |conn| {
                let mut stmt = conn.prepare(SqliteDBStatements::IS_WRITER_FOR_KEY_STATEMENT)?;
                let mut rows = stmt.query_map(
                    named_params! {
                        ":stream_id": stream_id.as_ssz_bytes(),
                        ":key": key.as_ssz_bytes(),
                        ":account": account.as_ssz_bytes(),
                        ":version": version,
                    },
                    |row| row.get(0),
                )?;
                if let Some(raw_data) = rows.next() {
                    match raw_data? {
                        AccessControlOps::GRANT_SPECIAL_WRITER_ROLE => Ok(true),
                        AccessControlOps::REVOKE_SPECIAL_WRITER_ROLE
                        | AccessControlOps::RENOUNCE_SPECIAL_WRITER_ROLE => Ok(false),
                        _ => {
                            bail!("unexpected access control op type");
                        }
                    }
                } else {
                    bail!("unexpected empty rows");
                }
            })
            .await
    }

    pub async fn is_writer_for_stream(
        &self,
        account: H160,
        stream_id: H256,
        version: u64,
    ) -> Result<bool> {
        self.connection
            .call(move |conn| {
                let mut stmt = conn.prepare(SqliteDBStatements::IS_WRITER_FOR_STREAM_STATEMENT)?;
                let mut rows = stmt.query_map(
                    named_params! {
                        ":stream_id": stream_id.as_ssz_bytes(),
                        ":account": account.as_ssz_bytes(),
                        ":version": version,
                    },
                    |row| row.get(0),
                )?;
                if let Some(raw_data) = rows.next() {
                    match raw_data? {
                        AccessControlOps::GRANT_WRITER_ROLE => Ok(true),
                        AccessControlOps::REVOKE_WRITER_ROLE
                        | AccessControlOps::RENOUNCE_WRITER_ROLE => Ok(false),
                        _ => {
                            bail!("unexpected access control op type");
                        }
                    }
                } else {
                    bail!("unexpected empty rows");
                }
            })
            .await
    }

    pub async fn has_write_permission(
        &self,
        account: H160,
        stream_id: H256,
        key: H256,
        version: u64,
    ) -> Result<bool> {
        if self.is_new_stream(stream_id, version).await? {
            return Ok(true);
        }
        if self.is_admin(account, stream_id, version).await? {
            return Ok(true);
        }
        if self.is_special_key(stream_id, key, version).await? {
            self.is_writer_for_key(account, stream_id, key, version)
                .await
        } else {
            self.is_writer_for_stream(account, stream_id, version).await
        }
    }

    pub async fn put_stream(&self, version: u64, stream_write_set: StreamWriteSet, access_control_set: AccessControlSet) -> Result<()>{
        self.connection.call(move |conn| {
            let tx = conn.transaction()?;
            for stream_write in stream_write_set.stream_writes.iter() {
                tx.execute(SqliteDBStatements::PUT_STREAM_WRITE_STATEMENT, named_params! {
                    ":stream_id": stream_write.stream_id.as_ssz_bytes(),
                    ":key": stream_write.key.as_ssz_bytes(),
                    ":version": version,
                    ":start_index": stream_write.start_index,
                    ":end_index": stream_write.end_index
                })?;
            }
            for access_control in access_control_set.access_controls.iter() {
                tx.execute(SqliteDBStatements::PUT_ACCESS_CONTROL_STATEMENT, named_params! {
                    ":stream_id": access_control.stream_id.as_ssz_bytes(),
                    ":key": access_control.key.as_ssz_bytes(),
                    ":version": version,
                    ":account": access_control.account.as_ssz_bytes(),
                    ":op_type": access_control.op_type,
                })?;
            }
            tx.commit()?;
            Ok(())
        }).await
    }
}

pub struct AccessControlOps;

impl AccessControlOps {
    pub const GRANT_ADMIN_ROLE: u8 = 0x00;
    pub const RENOUNCE_ADMIN_ROLE: u8 = 0x01;
    pub const SET_KEY_TO_SPECIAL: u8 = 0x10;
    pub const SET_KEY_TO_NORMAL: u8 = 0x11;
    pub const GRANT_WRITER_ROLE: u8 = 0x20;
    pub const REVOKE_WRITER_ROLE: u8 = 0x21;
    pub const RENOUNCE_WRITER_ROLE: u8 = 0x22;
    pub const GRANT_SPECIAL_WRITER_ROLE: u8 = 0x30;
    pub const REVOKE_SPECIAL_WRITER_ROLE: u8 = 0x31;
    pub const RENOUNCE_SPECIAL_WRITER_ROLE: u8 = 0x32;
}
