use super::{record_page::RecordPage, rid::RID, schema::FieldTypes};
use crate::{
    file::block::BlockId,
    query::{
        constant::Constant,
        scan::{Scan, UpdateScan},
    },
    record::layout::Layout,
    tx::transaction::Transaction,
};
use anyhow::{anyhow, bail, Result};
use std::sync::{Arc, Mutex};

pub struct TableScan {
    tx: Arc<Mutex<Transaction>>,
    layout: Arc<Layout>,
    rp: Option<RecordPage>,
    file_name: String,
    current_slot: i32,
}

impl TableScan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        table_name: impl Into<String>,
        layout: Arc<Layout>,
    ) -> Result<Self> {
        let file_name = table_name.into() + ".tbl";
        let mut scan = Self {
            tx: tx.clone(),
            layout,
            rp: None,
            file_name: file_name.clone(),
            current_slot: -1,
        };

        let size = tx.lock().unwrap().size(file_name)?;
        if size == 0 {
            scan.move_to_new_block()?
        } else {
            scan.move_to_block(0);
        }
        Ok(scan)
    }

    fn record_page(&mut self) -> Result<&mut RecordPage> {
        self.rp.as_mut().ok_or(anyhow!("no record page"))
    }

    // move_to_new_block はファイルに新しいブロックを追加して、そのブロックに移動
    fn move_to_new_block(&mut self) -> Result<()> {
        self.close();
        let block_id = {
            let mut tx = self.tx.lock().unwrap();
            tx.append(self.file_name.clone())?
        };
        let mut rp = RecordPage::new(self.tx.clone(), block_id, self.layout.clone());
        rp.format()?;
        self.rp = Some(rp);
        self.current_slot = -1;
        Ok(())
    }

    // move_to_block は指定したブロックに移動
    // ブロックへの操作はRecordPageを通して行うので、RecordPageを生成して保持する
    fn move_to_block(&mut self, block_num: i32) {
        self.close();
        let block_id = BlockId::new(self.file_name.clone(), block_num);
        self.rp = Some(RecordPage::new(
            self.tx.clone(),
            block_id,
            self.layout.clone(),
        ));
        self.current_slot = -1;
    }

    /// at_last_block は最後のブロックにいるかどうかを返す
    fn at_last_block(&self) -> bool {
        let size = self
            .tx
            .lock()
            .unwrap()
            .size(self.file_name.clone())
            .unwrap() as i32;
        self.rp.as_ref().unwrap().block.num == size - 1
    }
}

impl Scan for TableScan {
    fn before_first(&mut self) {
        self.move_to_block(0);
    }

    fn next(&mut self) -> Result<bool> {
        loop {
            let current_slot = self.current_slot;
            self.current_slot = self.record_page()?.next_after(current_slot);
            if self.current_slot >= 0 {
                break;
            }
            if self.at_last_block() {
                return Ok(false);
            } else {
                let block_num = self.record_page()?.block.num;
                self.move_to_block(block_num + 1);
            }
        }

        Ok(true)
    }

    fn get_int(&mut self, field_name: &str) -> Result<i32> {
        let slot = self.current_slot;
        self.record_page()?.get_int(slot, field_name)
    }

    fn get_string(&mut self, field_name: &str) -> Result<String> {
        let slot = self.current_slot;
        self.record_page()?.get_string(slot, field_name)
    }

    fn get_value(&mut self, field_name: &str) -> Result<Constant> {
        match self.layout.schema.r#type(field_name) {
            Some(FieldTypes::Integer) => {
                let val = self.get_int(field_name)?;
                Ok(Constant::Int(val))
            }
            Some(FieldTypes::Varchar) => {
                let val = self.get_string(field_name)?;
                Ok(Constant::String(val))
            }
            _ => bail!("field type not found: {}", field_name),
        }
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.layout.schema.has_field(field_name)
    }

    fn close(&mut self) {
        if let Some(rp) = self.rp.take() {
            self.tx.lock().unwrap().unpin(&rp.block);
        }
    }
}

impl UpdateScan for TableScan {
    fn set_value(&mut self, field_name: &str, value: Constant) -> Result<()> {
        let field_type = self
            .layout
            .schema
            .r#type(field_name)
            .ok_or(anyhow!("field type not found"))?;

        match (field_type, value) {
            (FieldTypes::Integer, Constant::Int(val)) => self.set_int(field_name, val),
            (FieldTypes::Varchar, Constant::String(val)) => self.set_string(field_name, &val),
            _ => bail!("type mismatch"),
        }
    }

    fn set_int(&mut self, field_name: &str, value: i32) -> Result<()> {
        let slot = self.current_slot;
        self.record_page()?.set_int(slot, field_name, value)
    }

    fn set_string(&mut self, field_name: &str, value: &str) -> Result<()> {
        let slot = self.current_slot;
        self.record_page()?
            .set_string(slot, field_name, value.into())
    }

    fn delete(&mut self) -> Result<()> {
        let slot = self.current_slot;
        self.record_page()?.delete(slot)
    }

    fn insert(&mut self) -> Result<()> {
        loop {
            let current_slot = self.current_slot;
            self.current_slot = self.record_page()?.insert_after(current_slot)?;
            if self.current_slot >= 0 {
                return Ok(());
            }
            if self.at_last_block() {
                self.move_to_new_block()?;
            } else {
                let block_num = self.record_page()?.block.num;
                self.move_to_block(block_num + 1);
            }
        }
    }

    fn get_rid(&mut self) -> Result<RID> {
        let block_num = self.record_page()?.block.num;
        Ok(RID::new(block_num, self.current_slot))
    }

    fn move_to_rid(&mut self, rid: RID) {
        self.close();
        let block_id = BlockId::new(self.file_name.clone(), rid.block_num);
        self.rp = Some(RecordPage::new(
            self.tx.clone(),
            block_id,
            self.layout.clone(),
        ));
        self.current_slot = rid.block_num;
    }

    fn as_scan(&mut self) -> &mut dyn Scan {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::TableScan;
    use crate::{
        query::scan::{Scan as _, UpdateScan as _},
        record::{layout::Layout, schema::Schema},
        server::db::TinyDB,
    };
    use anyhow::Result;
    use tempfile::tempdir;

    fn create_table_scan() -> Result<TableScan> {
        let test_directory = tempdir()?;
        let db = TinyDB::new(test_directory.path(), 100, 8)?;
        let tx = db.transaction()?;

        let mut sch = Schema::default();

        // record type: 4
        // int: 4
        // string length: 4
        // string : 8
        // total: 20
        sch.add_int_field("A");
        sch.add_string_field("B", 8);

        let layout = Layout::try_from_schema(Arc::new(sch))?;

        let mut ts = TableScan::new(tx.clone(), "T", Arc::new(layout))?;
        for n in 0..50 {
            ts.insert()?;
            ts.set_int("A", n)?;
            ts.set_string("B", &format!("rec{}", n))?;
        }

        ts.before_first();

        Ok(ts)
    }

    #[test]
    fn should_can_scan_table() -> Result<()> {
        let mut ts = create_table_scan()?;
        assert_eq!(ts.rp.as_ref().unwrap().block.num, 0);

        for i in 0..50 {
            ts.next()?;
            assert_eq!(ts.get_int("A")?, i);
            assert_eq!(ts.get_string("B")?, format!("rec{}", i));
        }
        Ok(())
    }
}
