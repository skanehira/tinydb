use anyhow::{bail, Result};

use crate::{file::page::Page, tx::transaction::Transaction};

use super::{
    checkpoint_record::CheckpointRecord, commit_record::CommitRecord,
    rollback_record::RollbackRecord, set_int_record::SetIntRecord,
    set_string_record::SetStringRecord, start_record::StartRecord,
};

#[derive(PartialEq, Eq)]
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
}

pub fn create_log_record(bytes: &[u8]) -> Result<Box<dyn LogRecord>> {
    let mut page: Page = bytes.to_vec().into();
    let op = page.get_int(0) as u8;
    match LogRecordType::from(op) {
        LogRecordType::Checkpoint => Ok(Box::<CheckpointRecord>::default()),
        LogRecordType::Start => Ok(Box::new(StartRecord::new(&mut page))),
        LogRecordType::Commit => Ok(Box::new(CommitRecord::new(&mut page))),
        LogRecordType::Rollback => Ok(Box::new(RollbackRecord::new(&mut page))),
        LogRecordType::SetInt => Ok(Box::new(SetIntRecord::new(&mut page))),
        LogRecordType::SetString => Ok(Box::new(SetStringRecord::new(&mut page))),
        LogRecordType::Unknown => bail!("Unknown log record type '{:X}'", op),
    }
}
