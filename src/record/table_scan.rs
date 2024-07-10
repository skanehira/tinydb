use super::record_page::RecordPage;
use crate::{query::scan::Scan, record::layout::Layout, tx::transaction::Transaction};
use anyhow::Result;
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
        table_name: String,
        layout: Arc<Layout>,
    ) -> Result<Self> {
        let file_name = table_name + ".tbl";
        let mut scan = Self {
            tx: tx.clone(),
            layout,
            rp: None,
            file_name: file_name.clone(),
            current_slot: -1,
        };

        if tx.lock().unwrap().size(file_name)? == 0 {
            scan.move_to_new_block()?
        } else {
            scan.move_to_block(0)?
        }
        Ok(scan)
    }

    // ファイルに新しいブロックを追加
    fn move_to_new_block(&mut self) -> Result<()> {
        let mut tx = self.tx.lock().unwrap();
        let block = tx.append(self.file_name.clone())?;
        let mut rp = RecordPage::new(self.tx.clone(), block, self.layout.clone());
        rp.format()?;
        self.rp = Some(rp);
        self.current_slot = -1;
        Ok(())
    }

    fn move_to_block(&mut self, block_num: i32) -> Result<()> {
        todo!()
    }
}

impl Scan for TableScan {
    fn before_first(&mut self) {
        todo!()
    }

    fn next(&mut self) -> bool {
        todo!()
    }

    fn get_int(&mut self, field_name: &str) -> i32 {
        todo!()
    }

    fn get_value(&mut self, fieldname: &str) -> crate::query::constant::Constant {
        todo!()
    }

    fn get_string(&mut self, field_name: &str) -> String {
        todo!()
    }

    fn has_field(&self, field_name: &str) -> bool {
        todo!()
    }

    fn close(&mut self) {
        todo!()
    }

    fn set_val(&mut self, field_name: &str, val: crate::query::constant::Constant) {
        todo!()
    }

    fn set_int(&mut self, field_name: &str, val: i32) {
        todo!()
    }

    fn set_string(&mut self, field_name: &str, val: &str) {
        todo!()
    }

    fn delete(&mut self) {
        todo!()
    }

    fn insert(&mut self) {
        todo!()
    }

    fn get_rid(&self) -> super::rid::RID {
        todo!()
    }

    fn move_to_rid(&mut self, rid: super::rid::RID) {
        todo!()
    }
}
