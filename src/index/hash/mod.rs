use super::Index;
use crate::{
    query::{constant::Constant, scan::Scan as _},
    record::{layout::Layout, rid::RID, table_scan::TableScan},
    tx::transaction::Transaction,
};
use anyhow::{anyhow, Result};
use std::sync::{Arc, Mutex};

const NUM_BUCKETS: u64 = 100;

pub struct HashIndex {
    tx: Arc<Mutex<Transaction>>,
    index_name: String,
    layout: Arc<Layout>,
    search_key: Option<Constant>,
    table_scan: Option<TableScan>,
}

impl HashIndex {
    pub fn new(tx: Arc<Mutex<Transaction>>, index_name: String, layout: Arc<Layout>) -> Self {
        Self {
            tx,
            index_name,
            layout,
            search_key: None,
            table_scan: None,
        }
    }

    pub fn search_cost(num_blocks: u64, _: u64) -> u64 {
        num_blocks / NUM_BUCKETS
    }
}

impl Index for HashIndex {
    fn before_first(&mut self, search_key: Constant) -> Result<()> {
        self.close();
        let hash_code = search_key.hash_code();
        self.search_key = Some(search_key);

        let bucket = hash_code % NUM_BUCKETS;
        let table_name = format!("{}{}", self.index_name, bucket);

        self.table_scan = Some(TableScan::new(
            self.tx.clone(),
            table_name,
            self.layout.clone(),
        )?);

        Ok(())
    }

    fn next(&mut self) -> Result<bool> {
        let Some(table_scan) = self.table_scan.as_mut() else {
            return Ok(false);
        };
        let Some(search_key) = self.search_key.as_ref() else {
            return Ok(false);
        };

        while table_scan.next()? {
            if table_scan.get_value("dataval")? == *search_key {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_data_rid(&mut self) -> Result<RID> {
        let table_scan = self.table_scan.as_mut().ok_or(anyhow!("no table_scan"))?;
        let block_num = table_scan.get_int("block")?;
        let id = table_scan.get_int("id")?;
        Ok(RID::new(block_num, id))
    }

    fn insert(&mut self, data_value: Constant, data_rid: RID) -> Result<()> {
        self.before_first(data_value.clone())?;
        let table_scan = self.table_scan.as_mut().ok_or(anyhow!("no table_scan"))?;
        table_scan.insert()?;
        table_scan.set_int("block", data_rid.block_num)?;
        table_scan.set_int("id", data_rid.slot)?;
        table_scan.set_value("dataval", data_value)?;
        todo!()
    }

    fn delete(&mut self, data_value: Constant, data_rid: RID) -> Result<()> {
        self.before_first(data_value)?;
        while self.next()? {
            if self.get_data_rid()? == data_rid {
                self.table_scan
                    .as_mut()
                    .ok_or(anyhow!("no table_scan"))?
                    .delete()?;
                return Ok(());
            }
        }
        Ok(())
    }

    fn close(&mut self) {
        if let Some(table_scan) = self.table_scan.as_mut() {
            table_scan.close()
        }
    }
}
