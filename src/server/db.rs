use crate::{
    buffer::buffer_manager::BufferManager,
    file::file_manager::FileManager,
    log::log_manager::LogManager,
    metadata::metadata_manager::MetadataManager,
    plan::{
        basic_query_plan::BasicQueryPlanner, basic_update_planner::BasicUpdatePlanner,
        planner::Planner, query_planner::QueryPlanner, update_planner::UpdatePlanner,
    },
    tx::{concurrency::lock_table::LockTable, transaction::Transaction},
    unlock, LOG_FILE,
};
use anyhow::Result;
use std::{
    path::PathBuf,
    sync::{Arc, Condvar, Mutex},
};

pub struct TinyDB {
    pub file_manager: Arc<Mutex<FileManager>>,
    pub log_manager: Arc<Mutex<LogManager>>,
    pub buffer_manager: Arc<Mutex<BufferManager>>,
    pub lock_table: Arc<(Mutex<LockTable>, Condvar)>,
    pub planner: Arc<Mutex<Planner>>,
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
        let lock_table = Arc::new((Mutex::new(LockTable::default()), Condvar::new()));

        let tx = Arc::new(Mutex::new(Transaction::new(
            file_manager.clone(),
            log_manager.clone(),
            buffer_manager.clone(),
            lock_table.clone(),
        )?));

        let is_new = unlock!(file_manager).is_new;
        if !is_new {
            unlock!(tx).recover()?;
        }
        let metadata_manager = Arc::new(Mutex::new(MetadataManager::new(is_new, tx.clone())?));

        let query_planner = Arc::new(Mutex::new(BasicQueryPlanner::new(metadata_manager.clone())))
            as Arc<Mutex<dyn QueryPlanner>>;
        let update_planner = Arc::new(Mutex::new(BasicUpdatePlanner::new(
            metadata_manager.clone(),
        ))) as Arc<Mutex<dyn UpdatePlanner>>;

        let planner = Arc::new(Mutex::new(Planner::new(query_planner, update_planner)));

        unlock!(tx).commit()?;

        Ok(Self {
            file_manager,
            log_manager,
            buffer_manager,
            lock_table,
            planner,
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
