use std::path::PathBuf;

#[derive(Clone)]
pub struct Config {
    pub db_dir: PathBuf,
    pub kv_db_file: PathBuf,
}
