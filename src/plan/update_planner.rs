use crate::query::create_index_data::CreateIndexData;
use crate::query::create_table_data::CreateTableData;
use crate::query::create_view_data::CreateViewData;
use crate::query::modify_data::ModifyData;
use crate::query::{delete_data::DeleteData, insert_data::InsertData};
use crate::tx::transaction::Transaction;
use anyhow::Result;
use std::sync::{Arc, Mutex};

pub trait UpdatePlanner {
    fn execute_insert(&mut self, data: InsertData, tx: Arc<Mutex<Transaction>>) -> Result<i32>;
    fn execute_delete(&mut self, data: DeleteData, tx: Arc<Mutex<Transaction>>) -> Result<i32>;
    fn execute_modify(&mut self, data: ModifyData, tx: Arc<Mutex<Transaction>>) -> Result<i32>;
    fn execute_create_table(
        &mut self,
        data: CreateTableData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<i32>;
    fn execute_create_view(
        &mut self,
        data: CreateViewData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<i32>;
    fn execute_create_index(
        &mut self,
        data: CreateIndexData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<i32>;
}
