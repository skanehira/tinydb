use super::layout::Layout;
use crate::{file::block::BlockId, record::schema::FieldTypes, tx::transaction::Transaction};
use anyhow::{anyhow, Result};
use std::sync::{Arc, Mutex};

#[derive(PartialEq, Eq)]
pub enum RecordType {
    Empty,
    Used,
}

impl From<RecordType> for i32 {
    fn from(val: RecordType) -> Self {
        match val {
            RecordType::Empty => 0,
            RecordType::Used => 1,
        }
    }
}

/// RecordPage はスロットの集まりで構成される
/// スロットはレコードを保持していて、1スロット:1レコードの関係
/// ファイル・ブロック・スロット・レコードの関係性は以下のとおり
///
/// ```text
///                                                             file
/// ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
///                                            block                                                    other bloks
/// ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┻━━━━━━━━━━━┓
///                                slot                                         other slots
/// ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┻━━━━━━━━━━━┓
///                                          record
///                 ┏━━━━━━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━━━━━━━━━━┓
/// ┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐
/// │ 1 │ 0 │ 0 │ 0 │ 6 │ 0 │ 0 │ 0 │ 5 │ 0 │ 0 │ 0 │ h │ e │ l │ l │ o │...│...│...│...│...│...│...│...│...│...│...│...│
/// └───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘
/// ┗━━━━━━━┳━━━━━━━┻━━━━━━━┳━━━━━━━┻━━━━━━━┳━━━━━━━┻━━━━━━━━━┳━━━━━━━━━┛
///    record type       integer      varchar length     varchar(5)
/// (0: emtpy, 1: used)
/// ```
pub struct RecordPage {
    tx: Arc<Mutex<Transaction>>,
    pub block: BlockId,
    pub layout: Arc<Layout>,
}

impl RecordPage {
    pub fn new(tx: Arc<Mutex<Transaction>>, block: BlockId, layout: Arc<Layout>) -> Self {
        Self { tx, block, layout }
    }

    pub fn get_int(&self, slot: i32, field_name: &str) -> i32 {
        let field_pos = self.offset(slot) + self.layout.offset(field_name);
        self.tx.lock().unwrap().get_int(&self.block, field_pos)
    }

    pub fn get_string(&self, slot: i32, field_name: &str) -> String {
        let field_pos = self.offset(slot) + self.layout.offset(field_name);
        self.tx.lock().unwrap().get_string(&self.block, field_pos)
    }

    pub fn set_int(&mut self, slot: i32, field_name: &str, value: i32) -> Result<()> {
        let field_pos = self.offset(slot) + self.layout.offset(field_name);
        self.tx
            .lock()
            .unwrap()
            .set_int(&self.block, field_pos, value, true)
    }

    pub fn set_string(&mut self, slot: i32, field_name: &str, value: String) -> Result<()> {
        let field_pos = self.offset(slot) + self.layout.offset(field_name);
        self.tx
            .lock()
            .unwrap()
            .set_string(&self.block, field_pos, value, true)
    }

    pub fn delete(&mut self, slot: i32) -> Result<()> {
        self.set_record_type(slot, RecordType::Empty)
    }

    pub fn format(&mut self) -> Result<()> {
        let mut slot = 0;
        while self.is_valid_slot(slot) {
            let mut tx = self.tx.lock().unwrap();
            tx.set_int(
                &self.block,
                self.offset(slot),
                RecordType::Empty.into(),
                false,
            )?;

            let schema = &self.layout.schema;
            for field_name in &schema.fields {
                // ブロックにあるスロットのオフセット + フィールドのオフセット = フィールドの位置
                // フィールドのオフセット自体は変わらないが、ブロックにあるスロットの断片化を防ぐためスロットの位置が調整されることがあるため
                // スロットのオフセットは変わることがある
                let field_pos = self.offset(slot) + self.layout.offset(field_name);
                let field_type = schema
                    .r#type(field_name)
                    .ok_or_else(|| anyhow!("field type not found"))?;
                match field_type.into() {
                    FieldTypes::Integer => {
                        tx.set_int(&self.block, field_pos, 0, false)?;
                    }
                    FieldTypes::Varchar => {
                        tx.set_string(&self.block, field_pos, "".into(), false)?;
                    }
                }
            }
            slot += 1;
        }
        Ok(())
    }

    pub fn next_after(&self, slot: i32) -> i32 {
        self.search_after(slot, RecordType::Used)
    }

    pub fn insert_ater(&mut self, slot: i32) -> Result<i32> {
        let new_slot = self.search_after(slot, RecordType::Empty);
        if new_slot >= 0 {
            self.set_record_type(new_slot, RecordType::Used)?;
        }
        Ok(new_slot)
    }

    fn set_record_type(&self, slot: i32, record_type: RecordType) -> Result<()> {
        let offset = self.offset(slot);
        let mut tx = self.tx.lock().unwrap();
        tx.set_int(&self.block, offset, record_type.into(), true)
    }

    fn search_after(&self, slot: i32, record_type: RecordType) -> i32 {
        let mut slot = slot + 1;
        while self.is_valid_slot(slot) {
            if self.get_record_type(&self.block, slot) == record_type {
                return slot;
            }
            slot += 1;
        }
        -1
    }

    fn get_record_type(&self, block: &BlockId, slot: i32) -> RecordType {
        let offset = self.offset(slot);
        let tx = self.tx.lock().unwrap();
        let record_type = tx.get_int(block, offset);
        if record_type == 0 {
            RecordType::Empty
        } else {
            RecordType::Used
        }
    }

    pub fn is_valid_slot(&self, slot: i32) -> bool {
        self.offset(slot + 1) <= self.tx.lock().unwrap().block_size()
    }

    pub fn offset(&self, slot: i32) -> i32 {
        self.layout.slot_size * slot
    }
}
