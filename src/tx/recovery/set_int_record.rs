use crate::{
    file::{block::BlockId, page::Page},
    log::log_manager::LogManager,
    tx::transaction::Transaction,
    I32_SIZE,
};
use anyhow::Result;

use super::record::{LogRecord, LogRecordType};

pub struct SetIntRecord {
    tx_num: i32,
    offset: i32,
    value: i32,
    block: BlockId,
}

impl std::fmt::Display for SetIntRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<SETINT {} {} {} {}>",
            self.tx_num, self.block, self.offset, self.value
        )
    }
}

impl SetIntRecord {
    pub fn new(page: &mut Page) -> Self {
        let tpos = I32_SIZE;
        let tx_num = page.get_int(tpos);

        let fpos = tpos + I32_SIZE;
        let filename = page.get_string(fpos);

        let bpos = fpos + Page::max_length(filename.len());
        let block_num = page.get_int(bpos);

        let block = BlockId::new(filename, block_num);

        let opos = bpos + I32_SIZE;
        let offset = page.get_int(opos);

        let vpos = opos + I32_SIZE;
        let value = page.get_int(vpos);

        Self {
            tx_num,
            offset,
            value,
            block,
        }
    }

    /// Write a setInt record to the log
    /// log record is formatted as follows:
    /// ```markdown
    /// | Type      | txnum     | filename length   | filename       | blocknum   | offset   | value          |
    /// | --------- | --------- | ----------------- | -------------- | ---------- | -------- | -------------- |
    /// | 4 bytes   | 4 bytes   | 4 bytes           | length bytes   | 4 bytes    | 4 bytes  | 4 bytes        |
    /// ```
    pub fn write_to_log(
        log_manager: &mut LogManager,
        tx_num: i32,
        block: &BlockId,
        offset: i32,
        value: i32,
    ) -> Result<i32> {
        let tpos = I32_SIZE;
        let fpos = tpos + I32_SIZE;
        let bpos = fpos + Page::max_length(block.filename.len());
        let opos = bpos + I32_SIZE;
        let vpos = opos + I32_SIZE;
        let record_len = vpos + I32_SIZE;
        let mut page = Page::new(record_len as i32);
        page.set_int(0, LogRecordType::SetInt as i32);
        page.set_int(tpos, tx_num);
        page.set_string(fpos, &block.filename);
        page.set_int(bpos, block.num);
        page.set_int(opos, offset);
        page.set_int(vpos, value);
        log_manager.append(page.contents())
    }
}

impl LogRecord for SetIntRecord {
    fn op(&self) -> LogRecordType {
        LogRecordType::SetInt
    }

    fn tx_number(&self) -> i32 {
        self.tx_num
    }

    fn undo(&mut self, tx: &mut Transaction) -> Result<()> {
        tx.pin(&self.block);
        tx.set_int(&self.block, self.offset, self.value, false)?;
        tx.unpin(&self.block);
        Ok(())
    }
}
