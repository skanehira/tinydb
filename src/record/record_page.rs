use super::layout::Layout;
use crate::{file::block::BlockId, record::schema::FieldTypes, tx::transaction::Transaction};
use anyhow::{anyhow, Result};
use std::sync::{Arc, Mutex};

#[derive(Debug, PartialEq, Eq)]
pub enum RecordType {
    Empty,
    Used,
}

impl From<i32> for RecordType {
    fn from(value: i32) -> Self {
        match value {
            0 => RecordType::Empty,
            1 => RecordType::Used,
            _ => panic!("invalid record type"),
        }
    }
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
///                                              file
/// ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
///                                   block                                             other bloks
/// ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┻━━━━━━━━━━━┓
///                        slot                                 other slots
/// ┏━━━━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┻━━━━━━━━━━━┓
///                                record
///                 ┏━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━━━┓
/// ┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐
/// │ 1 │ 0 │ 0 │ 0 │ 6 │ 0 │ 0 │ 0 │ h │ e │ l │ l │ o │...│...│...│...│...│...│...│...│...│...│...│...│
/// └───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘
/// ┗━━━━━━━┳━━━━━━━┻━━━━━━━┳━━━━━━━┻━━━━━━━━━┳━━━━━━━━━┛
///    record type       integer         varchar(5)
/// (0: emtpy, 1: used)
/// ```
pub struct RecordPage {
    tx: Arc<Mutex<Transaction>>,
    pub block: BlockId,
    pub layout: Arc<Layout>,
}

impl RecordPage {
    pub fn new(tx: Arc<Mutex<Transaction>>, block: BlockId, layout: Arc<Layout>) -> Self {
        tx.lock().unwrap().pin(&block);
        Self { tx, block, layout }
    }

    /// get_int は指定したスロットにあるフィールドの値を取得する
    /// フィールドの位置はスロットのオフセット + フィールドのオフセットで求める
    pub fn get_int(&self, slot: i32, field_name: &str) -> Result<i32> {
        let field_pos = self.offset(slot)
            + self
                .layout
                .offset(field_name)
                .ok_or_else(|| anyhow!("field offset not found"))?;
        Ok(self.tx.lock().unwrap().get_int(&self.block, field_pos))
    }

    pub fn get_string(&self, slot: i32, field_name: &str) -> Result<String> {
        let field_pos = self.offset(slot)
            + self
                .layout
                .offset(field_name)
                .ok_or_else(|| anyhow!("field offset not found"))?;
        Ok(self.tx.lock().unwrap().get_string(&self.block, field_pos))
    }

    pub fn set_int(&mut self, slot: i32, field_name: &str, value: i32) -> Result<()> {
        let field_pos = self.offset(slot)
            + self
                .layout
                .offset(field_name)
                .ok_or_else(|| anyhow!("field offset not found"))
                .unwrap();
        self.tx
            .lock()
            .unwrap()
            .set_int(&self.block, field_pos, value, true)
    }

    pub fn set_string(&mut self, slot: i32, field_name: &str, value: String) -> Result<()> {
        let field_pos = self.offset(slot)
            + self
                .layout
                .offset(field_name)
                .ok_or_else(|| anyhow!("field offset not found"))?;
        self.tx
            .lock()
            .unwrap()
            .set_string(&self.block, field_pos, value, true)
    }

    pub fn delete(&mut self, slot: i32) -> Result<()> {
        self.set_record_type(slot, RecordType::Empty)
    }

    /// format はレコードページを初期化する
    /// 具体的に以下の処理をする
    ///   - レコードタイプを Empty にする
    ///   - 各フィールドを初期値で埋める
    ///     - Integer の場合は 0
    ///     - Varchar の場合は空文字
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

            let schema = &self.layout.schema.lock().unwrap();
            for field_name in &schema.fields {
                // ブロックにあるスロットのオフセット + フィールドのオフセット = フィールドの位置
                // フィールドのオフセット自体は変わらないが、ブロックにあるスロットの断片化を防ぐためスロットの位置が調整されることがあるため
                // スロットのオフセットは変わることがある
                let field_pos = self.offset(slot)
                    + self
                        .layout
                        .offset(field_name)
                        .ok_or_else(|| anyhow!("field offset not found"))?;
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

    /// next_after は次の使われているスロット番号を返す
    pub fn next_after(&self, slot: i32) -> i32 {
        self.search_after(slot, RecordType::Used)
    }

    /// insert_after は指定したスロットのあとに新しい空きスロットを検索して
    /// 利用中に変更して、そのスロット番号を返す
    /// 空きスロットがない場合は -1 を返す
    pub fn insert_after(&mut self, slot: i32) -> Result<i32> {
        let new_slot = self.search_after(slot, RecordType::Empty);
        if new_slot >= 0 {
            self.set_record_type(new_slot, RecordType::Used)?;
        }
        Ok(new_slot)
    }

    /// set_record_type は指定したスロットのレコードタイプを変更する
    fn set_record_type(&self, slot: i32, record_type: RecordType) -> Result<()> {
        let offset = self.offset(slot);
        let mut tx = self.tx.lock().unwrap();
        tx.set_int(&self.block, offset, record_type.into(), true)
    }

    /// search_after は指定したスロットの次のスロットから指定したレコードタイプのスロットを検索して
    /// スロット番号を返す
    /// 見つからない場合は -1 を返す
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

    /// get_record_type は指定したスロットのレコードタイプを返す
    fn get_record_type(&self, block: &BlockId, slot: i32) -> RecordType {
        let offset = self.offset(slot);
        let mut tx = self.tx.lock().unwrap();
        tx.get_int(block, offset).into()
    }

    /// is_valid_slot は指定したスロットが有効かどうかを返す
    /// 有効なスロットとは、スロットの位置がブロックの範囲内に収まっているかどうか
    pub fn is_valid_slot(&self, slot: i32) -> bool {
        self.offset(slot + 1) <= self.tx.lock().unwrap().block_size()
    }

    /// offset は指定したスロットのオフセットを返す
    /// オフセットはブロックの先頭からの位置を表す
    pub fn offset(&self, slot: i32) -> i32 {
        self.layout.slot_size * slot
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        buffer::buffer_manager::BufferManager, file::file_manager::FileManager,
        log::log_manager::LogManager, record::schema::Schema,
        tx::concurrency::lock_table::LockTable, LOG_FILE,
    };
    use std::{path::Path, sync::Condvar};
    use tempfile::tempdir;

    fn new_transaction(db_dir: &Path) -> Arc<Mutex<Transaction>> {
        let block_size = 128;
        let file_manager = Arc::new(Mutex::new(FileManager::new(db_dir, block_size).unwrap()));
        let log_manager = Arc::new(Mutex::new(
            LogManager::new(file_manager.clone(), LOG_FILE.into()).unwrap(),
        ));
        let buffer_manager = Arc::new(Mutex::new(BufferManager::new(
            file_manager.clone(),
            log_manager.clone(),
            10,
        )));
        let lock_table = Arc::new((Mutex::new(LockTable::default()), Condvar::new()));

        let tx = Transaction::new(file_manager, log_manager, buffer_manager, lock_table).unwrap();

        Arc::new(Mutex::new(tx))
    }

    #[test]
    fn should_can_format() {
        let mut schema = Schema::default();
        schema.add_int_field("id".into());
        schema.add_string_field("name".into(), 8);
        let schema = Arc::new(Mutex::new(schema));
        let layout = Arc::new(Layout::try_from_schema(schema.clone()).unwrap());

        // 4bytes: record type
        // 4bytes: id
        // 8bytes: name
        assert_eq!(layout.slot_size, 16);

        let db_dir = tempdir().unwrap();
        let tx = new_transaction(db_dir.path());
        let block = BlockId::new("testfile".into(), 0);
        let mut rp = RecordPage::new(tx.clone(), block, layout);

        rp.format().unwrap();

        let slot = 0;
        assert_eq!(rp.get_int(slot, "id").unwrap(), 0);
        assert_eq!(rp.get_string(slot, "name").unwrap(), "");
    }

    #[test]
    fn should_can_set_record_date() {
        let mut schema = Schema::default();
        schema.add_int_field("id".into());
        schema.add_string_field("name".into(), 8);
        let schema = Arc::new(Mutex::new(schema));
        let layout = Arc::new(Layout::try_from_schema(schema.clone()).unwrap());

        let db_dir = tempdir().unwrap();
        let tx = new_transaction(db_dir.path());
        let block = BlockId::new("testfile".into(), 0);
        let mut rp = RecordPage::new(tx.clone(), block, layout);

        rp.format().unwrap();

        let slot = 0;
        rp.set_int(slot, "id", 1).unwrap();
        rp.set_string(slot, "name", "hello".into()).unwrap();

        assert_eq!(rp.get_int(slot, "id").unwrap(), 1);
        assert_eq!(rp.get_string(slot, "name").unwrap(), "hello");
    }

    #[test]
    fn should_can_delete() {
        let mut schema = Schema::default();
        schema.add_int_field("id".into());
        schema.add_string_field("name".into(), 8);
        let schema = Arc::new(Mutex::new(schema));
        let layout = Arc::new(Layout::try_from_schema(schema.clone()).unwrap());

        let db_dir = tempdir().unwrap();
        let tx = new_transaction(db_dir.path());
        let block = BlockId::new("testfile".into(), 0);
        let mut rp = RecordPage::new(tx.clone(), block.clone(), layout);

        rp.format().unwrap();

        let slot = 0;
        rp.set_int(slot, "id", 1).unwrap();
        rp.set_string(slot, "name", "hello".into()).unwrap();

        rp.delete(slot).unwrap();

        assert_eq!(rp.get_record_type(&block, slot), RecordType::Empty);
    }
}
