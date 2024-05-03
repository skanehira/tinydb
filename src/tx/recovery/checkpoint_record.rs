use anyhow::Result;

use crate::{
    file::page::Page, log::log_manager::LogManager, tx::transaction::Transaction, I32_SIZE,
};

use super::record::{LogRecord, LogRecordType};

#[derive(Default)]
pub struct CheckpointRecord;

impl std::fmt::Display for CheckpointRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<CHECKPOINT>")
    }
}

impl LogRecord for CheckpointRecord {
    fn op(&self) -> LogRecordType {
        LogRecordType::Checkpoint
    }

    fn tx_number(&self) -> i32 {
        -1
    }

    fn undo(&mut self, _tx: &mut Transaction) -> Result<()> {
        Ok(())
    }
}

impl CheckpointRecord {
    pub fn write_to_log(log_manager: &mut LogManager) -> Result<()> {
        let record = vec![0; I32_SIZE];
        let mut page: Page = record.into();
        page.set_int(0, LogRecordType::Checkpoint as i32);
        log_manager.append(page.contents())?;
        Ok(())
    }
}
