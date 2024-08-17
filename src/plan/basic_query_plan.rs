use super::{query_planner::QueryPlanner, ArcPlan, Plan};
use crate::{
    metadata::metadata_manager::MetadataManager,
    parse::parser::Parser,
    plan::{
        product_plan::ProductPlan, project_plan::ProjectPlan, select_plan::SelectPlan,
        table_plan::TablePlan,
    },
    query::query_data::QueryData,
    tx::transaction::Transaction,
    unlock,
};
use anyhow::Result;
use std::sync::{Arc, Mutex};

pub struct BasicQueryPlanner {
    metadata_manager: Arc<Mutex<MetadataManager>>,
}

impl BasicQueryPlanner {
    pub fn new(metadata_manager: Arc<Mutex<MetadataManager>>) -> Self {
        Self { metadata_manager }
    }
}

impl QueryPlanner for BasicQueryPlanner {
    fn create_plan(
        &mut self,
        data: QueryData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Arc<Mutex<dyn Plan>>> {
        let mut plans = vec![];

        for table_name in data.tables {
            let view_def = unlock!(self.metadata_manager).get_view_def(&table_name, tx.clone())?;
            if let Some(view_def) = view_def {
                let mut parser = Parser::new(&view_def);
                let view_data = parser.query()?;
                plans.push(self.create_plan(view_data, tx.clone())?);
            } else {
                let plan = TablePlan::new(table_name, tx.clone(), self.metadata_manager.clone())?;
                plans.push(Arc::new(Mutex::new(plan)) as ArcPlan);
            }
        }

        let mut plan = plans.remove(0);
        for next_plan in plans {
            plan = Arc::new(Mutex::new(ProductPlan::new(
                plan.clone(),
                next_plan.clone(),
            )?)) as ArcPlan;
        }

        plan = Arc::new(Mutex::new(SelectPlan::new(plan, data.pred.clone()))) as ArcPlan;
        plan = Arc::new(Mutex::new(ProjectPlan::new(plan, data.fields.clone())?)) as ArcPlan;

        Ok(plan)
    }
}
