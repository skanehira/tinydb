use crate::{
    buffer::buffer_manager::BufferManager,
    file::file_manager::FileManager,
    log::log_manager::LogManager,
    tx::{concurrency::lock_table::LockTable, transaction::Transaction},
    LOG_FILE,
};
use anyhow::Result;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

pub struct TinyDB {
    pub file_manager: Arc<Mutex<FileManager>>,
    pub log_manager: Arc<Mutex<LogManager>>,
    pub buffer_manager: Arc<Mutex<BufferManager>>,
    pub lock_table: Arc<Mutex<LockTable>>,
}

impl TinyDB {
    pub fn new(dir: impl Into<PathBuf>, block_size: i32, buffer_size: u64) -> Result<Self> {
        let db_dir = dir.into();
        let file_manager = Arc::new(Mutex::new(FileManager::new(db_dir, block_size)?));
        let log_manager = Arc::new(Mutex::new(LogManager::new(
            file_manager.clone(),
            LOG_FILE.into(),
        )?));
        let buffer_manager = Arc::new(Mutex::new(BufferManager::new(
            file_manager.clone(),
            log_manager.clone(),
            buffer_size,
        )));
        let lock_table = Arc::new(Mutex::new(LockTable::default()));
        Ok(Self {
            file_manager,
            log_manager,
            buffer_manager,
            lock_table,
        })
    }

    pub fn transaction(&self) -> Result<Arc<Mutex<Transaction>>> {
        let tx = Arc::new(Mutex::new(Transaction::new(
            self.file_manager.clone(),
            self.log_manager.clone(),
            self.buffer_manager.clone(),
            self.lock_table.clone(),
        )?));
        Ok(tx)
    }
}
