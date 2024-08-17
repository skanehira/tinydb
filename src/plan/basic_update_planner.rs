use crate::{
    metadata::metadata_manager::MetadataManager,
    plan::{select_plan::SelectPlan, table_plan::TablePlan, Plan},
    query::{
        create_index_data::CreateIndexData, create_table_data::CreateTableData,
        create_view_data::CreateViewData, delete_data::DeleteData, insert_data::InsertData,
        modify_data::ModifyData,
    },
    tx::transaction::Transaction,
    unlock,
};
use anyhow::Result;
use std::sync::{Arc, Mutex};

use super::{update_planner::UpdatePlanner, ArcPlan};

pub struct BasicUpdatePlanner {
    metadata_manager: MetadataManager,
}

impl BasicUpdatePlanner {
    pub fn new(metadata_manager: MetadataManager) -> Self {
        Self { metadata_manager }
    }
}

impl UpdatePlanner for BasicUpdatePlanner {
    fn execute_insert(&mut self, data: InsertData, tx: Arc<Mutex<Transaction>>) -> Result<i32> {
        let mut plan = TablePlan::new(data.table_name.clone(), tx, &mut self.metadata_manager)?;
        let scan = plan.open()?;
        let mut scan = unlock!(scan);
        scan.insert()?;
        for (field, value) in data.fields.into_iter().zip(data.values) {
            scan.set_value(&field, value)?;
        }
        scan.close();
        Ok(1)
    }

    fn execute_delete(&mut self, data: DeleteData, tx: Arc<Mutex<Transaction>>) -> Result<i32> {
        let plan = Arc::new(Mutex::new(TablePlan::new(
            data.table_name.clone(),
            tx,
            &mut self.metadata_manager,
        )?)) as ArcPlan;
        let mut plan = SelectPlan::new(plan, data.pred.clone());
        let scan = plan.open()?;
        let mut count = 0;
        while unlock!(scan).next()? {
            unlock!(scan).delete()?;
            count += 1;
        }
        unlock!(scan).close();
        Ok(count)
    }

    fn execute_modify(&mut self, data: ModifyData, tx: Arc<Mutex<Transaction>>) -> Result<i32> {
        let plan = Arc::new(Mutex::new(TablePlan::new(
            data.table_name.clone(),
            tx,
            &mut self.metadata_manager,
        )?)) as ArcPlan;
        let mut plan = SelectPlan::new(plan, data.pred.clone());
        let scan = plan.open()?;
        let mut count = 0;
        while unlock!(scan).next()? {
            let value = data.new_value.evaluate(scan.clone())?;
            unlock!(scan).set_value(&data.field_name, value.clone())?;
            count += 1;
        }
        unlock!(scan).close();
        Ok(count)
    }

    fn execute_create_table(
        &mut self,
        data: CreateTableData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<i32> {
        self.metadata_manager
            .create_table(&data.table_name, Arc::new(data.schema), tx)?;
        Ok(0)
    }

    fn execute_create_view(
        &mut self,
        data: CreateViewData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<i32> {
        self.metadata_manager
            .create_view(&data.view_name, &data.view_def(), tx)?;
        Ok(0)
    }

    fn execute_create_index(
        &mut self,
        data: CreateIndexData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<i32> {
        self.metadata_manager.create_index(
            &data.index_name,
            &data.table_name,
            &data.field_name,
            tx,
        )?;
        Ok(0)
    }
}
