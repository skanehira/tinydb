use anyhow::Result;

use crate::{file::page::Page, log::log_manager::LogManager, tx::transaction::Transaction, I32_SIZE};

use super::record::{LogRecord, LogRecordType};

#[derive(Default)]
pub struct RollbackRecord {
    tx_num: i32,
}

impl RollbackRecord {
    pub fn new(page: &mut Page) -> Self {
        let tx_num = page.get_int(0);
        Self { tx_num }
    }
}

impl std::fmt::Display for RollbackRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<ROLLBACK {}>", self.tx_num)
    }
}

impl LogRecord for RollbackRecord {
    fn op(&self) -> LogRecordType {
        LogRecordType::Rollback
    }

    fn tx_number(&self) -> i32 {
        self.tx_num
    }

    fn undo(&mut self, _tx: &mut Transaction) -> Result<()> {
        Ok(())
    }
}

impl RollbackRecord {
    pub fn write_to_log(log_manager: &mut LogManager, tx_num: i32) -> Result<()> {
        let record = vec![0; 2 * I32_SIZE];
        let mut page: Page = record.into();
        page.set_int(0, LogRecordType::Checkpoint as i32);
        page.set_int(I32_SIZE, tx_num);
        log_manager.append(page.contents())?;
        Ok(())
    }
}
