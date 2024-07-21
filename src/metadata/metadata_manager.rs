use crate::{
    record::{layout::Layout, schema::Schema},
    tx::transaction::Transaction,
    unlock,
};

use super::{
    index_info::IndexInfo, index_manager::IndexManager, stat_info::StatInfo,
    stat_manager::StatManager, table_manager::TableManager, view_manager::ViewManager,
};
use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub struct MetadataManager {
    table_manager: Arc<Mutex<TableManager>>,
    view_manager: Arc<Mutex<ViewManager>>,
    stat_manager: Arc<Mutex<StatManager>>,
    index_manager: Arc<Mutex<IndexManager>>,
}

impl MetadataManager {
    pub fn new(is_new: bool, tx: Arc<Mutex<Transaction>>) -> Result<Self> {
        let table_manager = Arc::new(Mutex::new(TableManager::new(is_new, tx.clone())?));
        let view_manager = Arc::new(Mutex::new(ViewManager::new(
            is_new,
            table_manager.clone(),
            tx.clone(),
        )?));
        let stat_manager = Arc::new(Mutex::new(StatManager::new(
            table_manager.clone(),
            tx.clone(),
        )?));
        let index_manager = Arc::new(Mutex::new(
            IndexManager::new(
                is_new,
                table_manager.clone(),
                stat_manager.clone(),
                tx.clone(),
            )
            .unwrap(),
        ));

        Ok(Self {
            table_manager,
            view_manager,
            stat_manager,
            index_manager,
        })
    }

    pub fn create_table(
        &self,
        table_name: &str,
        schema: Arc<Schema>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<()> {
        unlock!(self.table_manager).create_table(table_name, schema, tx.clone())
    }

    pub fn get_layout(&mut self, table_name: &str, tx: Arc<Mutex<Transaction>>) -> Result<Layout> {
        unlock!(self.table_manager).get_layout(table_name, tx.clone())
    }

    pub fn create_view(&self, vname: &str, vdef: &str, tx: Arc<Mutex<Transaction>>) -> Result<()> {
        unlock!(self.view_manager).create_view(vname, vdef, tx.clone())
    }

    pub fn get_view_def(&self, vname: &str, tx: Arc<Mutex<Transaction>>) -> Result<Option<String>> {
        unlock!(self.view_manager).get_view_def(vname, tx.clone())
    }

    pub fn create_index(
        &self,
        index_name: String,
        table_name: String,
        field_name: String,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<()> {
        unlock!(self.index_manager).create_index(index_name, table_name, field_name, tx.clone())
    }

    pub fn get_index_info(
        &self,
        table_name: String,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<HashMap<String, IndexInfo>> {
        unlock!(self.index_manager).get_index_info(table_name, tx.clone())
    }

    pub fn get_stat_info(
        &self,
        table_name: String,
        layout: Arc<Layout>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<StatInfo> {
        unlock!(self.stat_manager).get_stat_info(table_name, layout, tx.clone())
    }
}
