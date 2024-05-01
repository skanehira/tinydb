use std::path::PathBuf;

use crate::{file_manager::FileManager, log_manager::LogManager};
use anyhow::Result;

static LOG_FILE: &str = "tinydb.log";

pub struct TinyDB {
    pub log_manager: LogManager,
}

impl TinyDB {
    pub fn new(dir: impl Into<PathBuf>, block_size: u64) -> Result<Self> {
        let db_dir = dir.into();
        let log_manager = LogManager::new(FileManager::new(db_dir, block_size)?, LOG_FILE.into())?;
        Ok(Self { log_manager })
    }
}
