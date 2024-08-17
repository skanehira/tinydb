use anyhow::Result;
use std::sync::{Arc, Mutex};

use crate::{
    metadata::{metadata_manager::MetadataManager, stat_info::StatInfo},
    query::scan::ArcScan,
    record::{layout::Layout, schema::Schema, table_scan::TableScan},
    tx::transaction::Transaction,
};

use super::Plan;

pub struct TablePlan {
    table_name: String,
    tx: Arc<Mutex<Transaction>>,
    layout: Arc<Layout>,
    stat_info: StatInfo,
}

impl TablePlan {
    pub fn new(
        table_name: String,
        tx: Arc<Mutex<Transaction>>,
        md: &mut MetadataManager,
    ) -> Result<Self> {
        let layout = Arc::new(md.get_layout(&table_name, tx.clone())?);
        let stat_info = md.get_stat_info(&table_name, layout.clone(), tx.clone())?;
        Ok(Self {
            table_name,
            tx,
            layout: layout.clone(),
            stat_info,
        })
    }
}

impl Plan for TablePlan {
    fn open(&mut self) -> Result<ArcScan> {
        Ok(Arc::new(Mutex::new(TableScan::new(
            self.tx.clone(),
            self.table_name.clone(),
            self.layout.clone(),
        )?)) as ArcScan)
    }

    fn blocks_accessed(&self) -> i32 {
        self.stat_info.num_blocks
    }

    fn records_output(&self) -> i32 {
        self.stat_info.num_records
    }

    fn distinct_values(&self, field_name: &str) -> i32 {
        self.stat_info.distinct_values(field_name)
    }

    fn schema(&self) -> Arc<Schema> {
        self.layout.schema.clone()
    }
}
