pub struct SqliteDBStatements;

impl SqliteDBStatements {
    pub const RESET_STERAM_SYNC_STATEMENT: &'static str = "
        INSERT OR REPLACE INTO 
            t_misc (data_sync_progress, stream_replay_progress, stream_ids) 
        VALUES 
            (:data_sync_progress, :stream_replay_progress, :stream_ids)
        WHERE 
            id = :id
    ";

    pub const GET_STREAM_DATA_SYNC_PROGRESS_STATEMENT: &'static str =
        "SELECT data_sync_progress FROM t_misc WHERE id = 0";

    pub const UPDATE_STREAM_DATA_SYNC_PROGRESS_STATEMENT: &'static str =
        "UPDATE t_misc SET data_sync_progress = :data_sync_progress WHERE id = :id AND data_sync_progress = :from";

    pub const GET_STREAM_REPLAY_PROGRESS_STATEMENT: &'static str =
        "SELECT stream_replay_progress FROM t_misc WHERE id = 0";

    pub const UPDATE_STREAM_REPLAY_PROGRESS_STATEMENT: &'static str =
        "UPDATE t_misc SET stream_replay_progress = :stream_replay_progress WHERE id = :id AND stream_replay_progress = :from";

    pub const GET_STREAM_IDS_STATEMENT: &'static str = "SELECT stream_ids FROM t_misc WHERE id = 0";

    pub const UPDATE_STREAM_IDS_STATEMENT: &'static str =
        "UPDATE t_misc SET stream_ids = :stream_ids WHERE id = :id";

    pub const CREATE_MISC_TABLE_STATEMENT: &'static str = "
        CREATE TABLE IF NOT EXISTS t_misc (
            id INTEGER NOT NULL PRIMARY KEY,
            data_sync_progress INTEGER NOT NULL, 
            stream_replay_progress INTEGER NOT NULL, 
            stream_ids BLOB NOT NULL
        ) WITHOUT ROWID
    ";

    pub const CREATE_STREAM_TABLE_STATEMENT: &'static str = "
        CREATE TABLE IF NOT EXISTS t_stream (
            stream_id BLOB NOT NULL,
            key BLOB NOT NULL,
            version INTEGER NOT NULL,
            start_index INTEGER NOT NULL,
            end_index INTEGER NOT NULL,
            PRIMARY KEY (stream_id, key, version)
        ) WITHOUT ROWID
    ";

    pub const CREATE_STREAM_INDEX_STATEMENTS: [&'static str; 2] = [
        "CREATE INDEX IF NOT EXISTS stream_key_idx ON t_stream(stream_id, key)",
        "CREATE INDEX IF NOT EXISTS stream_version_idx ON t_stream(version)",
    ];

    pub const CREATE_ACCESS_CONTROL_TABLE_STATEMENT: &'static str = "
        CREATE TABLE IF NOT EXISTS t_access_control (
            stream_id BLOB NOT NULL,
            key BLOB NOT NULL,
            version INTEGER NOT NULL,
            account BLOB NOT NULL,
            op_type TEXT NOT NULL 
        )
    ";

    pub const CREATE_ACCESS_CONTROL_INDEX_STATEMENTS: [&'static str; 5] = [
        "CREATE INDEX IF NOT EXISTS ac_version_index ON t_access_control(version)",
        "CREATE INDEX IF NOT EXISTS ac_op_type_index ON t_access_control(op_type)",
        "CREATE INDEX IF NOT EXISTS ac_account_index ON t_access_control(stream_id, account)",
        "CREATE INDEX IF NOT EXISTS ac_key_index ON t_access_control(stream_id, key)",
        "CREATE INDEX IF NOT EXISTS ac_account_key_index ON t_access_control(stream_id, key, account)",
    ];
}
