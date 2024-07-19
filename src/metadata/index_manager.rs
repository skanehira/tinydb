use super::{
    index_info::IndexInfo,
    stat_manager::StatManager,
    table_manager::{TableManager, MAX_NAME},
};
use crate::{
    query::scan::Scan,
    record::{layout::Layout, schema::Schema, table_scan::TableScan},
    tx::transaction::Transaction,
    unlock,
};
use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub struct IndexManager {
    layout: Arc<Layout>,
    table_manager: Arc<Mutex<TableManager>>,
    stat_manager: Arc<Mutex<StatManager>>,
}

impl IndexManager {
    pub fn new(
        is_new: bool,
        table_manager: Arc<Mutex<TableManager>>,
        stat_manager: Arc<Mutex<StatManager>>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Self> {
        if is_new {
            let mut schema = Schema::default();
            schema.add_string_field("indexname", MAX_NAME);
            schema.add_string_field("tablename", MAX_NAME);
            schema.add_string_field("fieldname", MAX_NAME);
            unlock!(table_manager).create_table("idxcat", Arc::new(schema), tx.clone())?;
        }

        let layout = Arc::new(unlock!(table_manager).get_layout("idxcat", tx.clone())?);

        Ok(Self {
            layout,
            table_manager,
            stat_manager,
        })
    }

    pub fn create_index(
        &mut self,
        index_name: String,
        table_name: String,
        field_name: String,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<()> {
        let mut ts = TableScan::new(tx, table_name.clone(), self.layout.clone())?;
        ts.insert()?;
        ts.set_string("indexname", &index_name)?;
        ts.set_string("tablename", &table_name)?;
        ts.set_string("fieldname", &field_name)?;
        Ok(())
    }

    pub fn get_index_info(
        &mut self,
        table_name: String,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<HashMap<String, IndexInfo>> {
        let mut result = HashMap::new();

        let mut ts = TableScan::new(tx.clone(), table_name.clone(), self.layout.clone())?;

        while ts.next()? {
            if ts.get_string("tablename")? == table_name.clone() {
                let index_name = ts.get_string("indexname")?;
                let field_name = ts.get_string("fieldname")?;
                let table_layout = Arc::new(
                    unlock!(self.table_manager).get_layout(table_name.clone(), tx.clone())?,
                );
                let table_stat_info = self.stat_manager.lock().unwrap().get_stat_info(
                    table_name.clone(),
                    table_layout.clone(),
                    tx.clone(),
                )?;
                let index_info = IndexInfo::new(
                    index_name.clone(),
                    field_name,
                    table_layout.schema.clone(),
                    tx.clone(),
                    table_stat_info,
                )?;
                result.insert(index_name, index_info);
            }
        }

        Ok(result)
    }
}
