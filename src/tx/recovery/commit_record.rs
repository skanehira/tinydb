use anyhow::Result;

use crate::{
    file::page::Page, log::log_manager::LogManager, tx::transaction::Transaction, I32_SIZE,
};

use super::record::{LogRecord, LogRecordType};

#[derive(Default)]
pub struct CommitRecord {
    tx_num: i32,
}

impl CommitRecord {
    pub fn new(page: &mut Page) -> Self {
        let tx_num = page.get_int(0);
        Self { tx_num }
    }
}

impl std::fmt::Display for CommitRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<Commit {}>", self.tx_num)
    }
}

impl LogRecord for CommitRecord {
    fn op(&self) -> LogRecordType {
        LogRecordType::Commit
    }

    fn tx_number(&self) -> i32 {
        self.tx_num
    }

    fn undo(&mut self, _tx: &mut Transaction) -> Result<()> {
        Ok(())
    }
}

impl CommitRecord {
    pub fn write_to_log(log_manager: &mut LogManager, tx_num: i32) -> Result<i32> {
        let record = vec![0; 2 * I32_SIZE];
        let mut page: Page = record.into();
        page.set_int(0, LogRecordType::Commit as i32);
        page.set_int(I32_SIZE, tx_num);
        let lsn = log_manager.append(page.contents())?;
        Ok(lsn)
    }
}
