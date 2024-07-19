use super::{stat_info::StatInfo, table_manager::TableManager};
use crate::{
    query::scan::Scan,
    record::{layout::Layout, table_scan::TableScan},
    tx::transaction::Transaction,
};
use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub struct StatManager {
    table_manager: TableManager,
    table_stats: HashMap<String, StatInfo>,
    num_calls: i32,
}

impl StatManager {
    pub fn new(table_manager: TableManager, tx: Arc<Mutex<Transaction>>) -> Result<Self> {
        let table_stats = HashMap::new();
        let num_calls = 0;
        let mut sm = Self {
            table_manager,
            table_stats,
            num_calls,
        };

        sm.refresh_statistics(tx)?;

        Ok(sm)
    }

    pub fn get_stat_info(
        &mut self,
        table_name: String,
        layout: Arc<Layout>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<StatInfo> {
        self.num_calls += 1;
        if self.num_calls > 100 {
            self.refresh_statistics(tx.clone())?;
        }
        match self.table_stats.get(&table_name) {
            Some(stat_info) => Ok(stat_info.clone()),
            None => {
                let stat_info = self.calc_table_stats(&table_name, layout, tx.clone())?;
                self.table_stats.insert(table_name, stat_info.clone());
                Ok(stat_info)
            }
        }
    }

    pub fn refresh_statistics(&mut self, tx: Arc<Mutex<Transaction>>) -> Result<()> {
        self.table_stats = HashMap::new();
        self.num_calls = 0;

        let table_catalog_layout = Arc::new(self.table_manager.get_layout("tblcat", tx.clone())?);
        let mut ts = TableScan::new(tx.clone(), "tblcat", table_catalog_layout)?;

        while ts.next()? {
            let table_name = ts.get_string("tblname")?;
            let layout = Arc::new(self.table_manager.get_layout(&table_name, tx.clone())?);
            let stat_info = self.calc_table_stats(&table_name, layout, tx.clone())?;
            self.table_stats.insert(table_name, stat_info);
        }

        Ok(())
    }

    fn calc_table_stats(
        &mut self,
        table_name: impl Into<String>,
        layout: Arc<Layout>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<StatInfo> {
        let mut num_records = 0;
        let mut num_blocks = 0;

        let mut ts = TableScan::new(tx.clone(), table_name, layout)?;
        while ts.next()? {
            num_records += 1;
            num_blocks = ts.get_rid()?.block_num + 1;
        }

        let stat_info = StatInfo::new(num_blocks, num_records);
        Ok(stat_info)
    }
}
