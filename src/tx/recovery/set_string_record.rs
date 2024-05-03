use crate::{
    file::{block::BlockId, page::Page},
    log::log_manager::LogManager,
    transaction::Transaction,
    I32_SIZE,
};
use anyhow::Result;

use super::record::{LogRecord, LogRecordType};

pub struct SetStringRecord {
    tx_num: i32,
    offset: i32,
    value: String,
    block: BlockId,
}

impl std::fmt::Display for SetStringRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<SETSTRING {} {} {} {}>",
            self.tx_num, self.block, self.offset, self.value
        )
    }
}

impl SetStringRecord {
    /// Construct a setString log record from a page
    /// The page should contain the following format:
    /// ```markdown
    /// | Type    | txnum   | filename length | filename     | blocknum | offset | value length | value        |
    /// |---------|---------|-----------------|--------------|----------|--------|--------------|--------------|
    /// | 4 bytes | 4 bytes | 4 bytes         | length bytes | 4 bytes  | 4      | 4 bytes      | length bytes |
    /// ```
    pub fn new(page: &mut Page) -> Result<Self> {
        let tpos = I32_SIZE;
        let tx_num = page.get_int(tpos);

        let fpos = tpos + I32_SIZE;
        let filename = page.get_string(fpos)?;

        let bpos = fpos + Page::max_length(filename.len());
        let block_num = page.get_int(bpos);

        let block = BlockId::new(filename, block_num as u64);

        let opos = bpos + I32_SIZE;
        let offset = page.get_int(opos);

        let vpos = opos + I32_SIZE;
        let value = page.get_string(vpos)?;

        Ok(Self {
            tx_num,
            offset,
            value,
            block,
        })
    }

    /// Write a setString record to the log
    /// log record is formatted as follows:
    /// ```markdown
    /// | Type    | txnum   | filename length | filename     | blocknum | offset | value length | value        |
    /// |---------|---------|-----------------|--------------|----------|--------|--------------|--------------|
    /// | 4 bytes | 4 bytes | 4 bytes         | length bytes | 4 bytes  | 4      | 4 bytes      | length bytes |
    /// ```
    pub fn write_to_log(
        log_manager: &mut LogManager,
        tx_num: i32,
        block: &BlockId,
        offset: i32,
        value: String,
    ) -> Result<()> {
        let tpos = I32_SIZE;
        let fpos = tpos + I32_SIZE;
        let bpos = fpos + Page::max_length(block.filename.len());
        let opos = bpos + I32_SIZE;
        let vpos = opos + I32_SIZE;
        let record_len = vpos + Page::max_length(value.len());
        let record = vec![0; record_len];
        let mut page: Page = record.into();
        page.set_int(0, LogRecordType::SetString as i32);
        page.set_int(tpos, tx_num);
        page.set_string(fpos, &block.filename);
        page.set_int(bpos, block.num as i32);
        page.set_int(opos, offset);
        page.set_string(vpos, &value);
        log_manager.append(page.contents())?;
        Ok(())
    }
}

impl LogRecord for SetStringRecord {
    fn op(&self) -> LogRecordType {
        LogRecordType::SetString
    }

    fn tx_number(&self) -> i32 {
        self.tx_num
    }

    fn undo(&mut self, tx: &mut Transaction) -> Result<()> {
        tx.pin(&self.block);
        tx.set_string(&self.block, self.offset, self.value.clone(), false);
        tx.unpin(&self.block);
        todo!()
    }
}
