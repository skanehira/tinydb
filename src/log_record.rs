use anyhow::Result;

use crate::{log_record_set_string::SetStringRecord, page::Page, transaction::Transaction};

pub enum LogRecordType {
    Checkpoint = 0,
    Start = 1,
    Commit = 2,
    Rollback = 3,
    SetInt = 4,
    SetString = 5,
    Unknown,
}

impl From<u8> for LogRecordType {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Checkpoint,
            1 => Self::Start,
            2 => Self::Commit,
            3 => Self::Rollback,
            4 => Self::SetInt,
            5 => Self::SetString,
            _ => Self::Unknown,
        }
    }
}

pub trait LogRecord {
    fn op(&self) -> LogRecordType;
    fn tx_number(&self) -> i32;
    fn undo(&mut self, tx: &mut Transaction) -> Result<()>;

    fn create_log_record(bytes: &[u8]) -> Option<impl LogRecord> {
        let mut page: Page = bytes.to_vec().into();
        let op = page.get_int(0) as u8;
        match LogRecordType::from(op) {
            LogRecordType::Checkpoint => todo!(),
            LogRecordType::Start => todo!(),
            LogRecordType::Commit => todo!(),
            LogRecordType::Rollback => todo!(),
            LogRecordType::SetInt => todo!(),
            LogRecordType::SetString => Some(SetStringRecord::new(&mut page).unwrap()),
            LogRecordType::Unknown => None,
        }
    }
}
