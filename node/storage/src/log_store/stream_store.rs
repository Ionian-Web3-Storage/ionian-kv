use anyhow::Result;
use ethereum_types::H256;
use ssz::Decode;
use std::path::{Path, PathBuf};

use rusqlite::{named_params, Connection};

use crate::error::Error;

use super::sqlite_db_statements::SqliteDBStatements;

pub struct StreamStore {
    path: PathBuf,
    in_memory: bool,
}

impl StreamStore {
    pub fn get_connection(&self) -> Result<Connection> {
        if self.in_memory {
            return Ok(Connection::open_in_memory()?);
        }
        Ok(Connection::open(&self.path)?)
    }

    pub fn create_tables_if_not_exist(&self) -> Result<()> {
        let connection = self.get_connection()?;
        self.create_tables(&connection)?;
        Ok(())
    }

    pub fn new_in_memory() -> Self {
        Self {
            path: PathBuf::new(),
            in_memory: true,
        }
    }

    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().join("stream.db"),
            in_memory: false,
        }
    }

    pub fn get_stream_ids(&self, connection: &Connection) -> Result<Vec<H256>> {
        let mut stmt = connection.prepare(SqliteDBStatements::GET_STREAM_IDS_STATEMENT)?;
        let mut rows = stmt.query_map([], |row| row.get(0))?;
        if let Some(raw_data) = rows.next() {
            let raw_stream_ids: Vec<u8> = raw_data?;
            return Ok(Vec::<H256>::from_ssz_bytes(&raw_stream_ids).map_err(Error::from)?);
        }
        Ok(vec![])
    }

    pub fn update_stream_ids(&self, connection: &Connection, stream_ids: &[u8]) -> Result<()> {
        let mut stmt = connection.prepare(SqliteDBStatements::UPDATE_STREAM_IDS_STATEMENT)?;
        stmt.execute(named_params! {
            ":stream_ids": stream_ids,
            ":id": 0,
        })?;
        Ok(())
    }

    pub fn reset_stream_sync(&self, connection: &Connection, stream_ids: &[u8]) -> Result<()> {
        let mut stmt = connection.prepare(SqliteDBStatements::RESET_STERAM_SYNC_STATEMENT)?;
        stmt.execute(named_params! {
            ":data_sync_progress": 0,
            ":stream_replay_progress": 0,
            ":stream_ids": stream_ids,
            ":id": 0,
        })?;
        Ok(())
    }

    pub fn get_stream_data_sync_progress(&self, connection: &Connection) -> Result<u64> {
        let mut stmt =
            connection.prepare(SqliteDBStatements::GET_STREAM_DATA_SYNC_PROGRESS_STATEMENT)?;
        let mut rows = stmt.query_map([], |row| row.get(0))?;
        if let Some(raw_data) = rows.next() {
            return Ok(raw_data?);
        }
        Ok(0)
    }

    pub fn update_stream_data_sync_progress(
        &self,
        connection: &Connection,
        from: u64,
        progress: u64,
    ) -> Result<usize> {
        let mut stmt =
            connection.prepare(SqliteDBStatements::UPDATE_STREAM_DATA_SYNC_PROGRESS_STATEMENT)?;
        Ok(stmt.execute(named_params! {
            ":data_sync_progress": progress,
            ":id": 0,
            ":from": from,
        })?)
    }

    pub fn get_stream_replay_progress(&self, connection: &Connection) -> Result<u64> {
        let mut stmt =
            connection.prepare(SqliteDBStatements::GET_STREAM_REPLAY_PROGRESS_STATEMENT)?;
        let mut rows = stmt.query_map([], |row| row.get(0))?;
        if let Some(raw_data) = rows.next() {
            return Ok(raw_data?);
        }
        Ok(0)
    }

    pub fn update_stream_replay_progress(
        &self,
        connection: &Connection,
        from: u64,
        progress: u64,
    ) -> Result<usize> {
        let mut stmt =
            connection.prepare(SqliteDBStatements::UPDATE_STREAM_REPLAY_PROGRESS_STATEMENT)?;
        Ok(stmt.execute(named_params! {
            ":stream_replay_progress": progress,
            ":id": 0,
            ":from": from,
        })?)
    }

    fn create_tables(&self, connection: &Connection) -> Result<()> {
        // misc table
        connection.execute(SqliteDBStatements::CREATE_MISC_TABLE_STATEMENT, [])?;
        // stream table
        connection.execute(SqliteDBStatements::CREATE_STREAM_TABLE_STATEMENT, [])?;
        for stmt in SqliteDBStatements::CREATE_STREAM_INDEX_STATEMENTS.iter() {
            connection.execute(stmt, [])?;
        }
        // access control table
        connection.execute(
            SqliteDBStatements::CREATE_ACCESS_CONTROL_TABLE_STATEMENT,
            [],
        )?;
        for stmt in SqliteDBStatements::CREATE_ACCESS_CONTROL_INDEX_STATEMENTS.iter() {
            connection.execute(stmt, [])?;
        }
        Ok(())
    }
}
