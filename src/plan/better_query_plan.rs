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

pub struct BetterQueryPlanner {
    metadata_manager: MetadataManager,
}

impl BetterQueryPlanner {
    pub fn new(metadata_manager: MetadataManager) -> Self {
        Self { metadata_manager }
    }
}

impl QueryPlanner for BetterQueryPlanner {
    fn create_plan(
        &mut self,
        data: QueryData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Arc<Mutex<dyn Plan>>> {
        let mut plans = vec![];

        for table_name in data.tables {
            if let Some(view_def) = self
                .metadata_manager
                .get_view_def(&table_name, tx.clone())?
            {
                let mut parser = Parser::new(&view_def);
                let view_data = parser.query()?;
                plans.push(self.create_plan(view_data, tx.clone())?);
            } else {
                let plan = TablePlan::new(table_name, tx.clone(), &mut self.metadata_manager)?;
                plans.push(Arc::new(Mutex::new(plan)) as ArcPlan);
            }
        }

        let mut plan = plans.remove(0);
        for next_plan in plans {
            let choice1 = Arc::new(Mutex::new(ProductPlan::new(
                plan.clone(),
                next_plan.clone(),
            )?)) as ArcPlan;
            let choice2 = Arc::new(Mutex::new(ProductPlan::new(
                next_plan.clone(),
                plan.clone(),
            )?)) as ArcPlan;
            if unlock!(choice1).blocks_accessed() < unlock!(choice2).blocks_accessed() {
                plan = choice1;
            } else {
                plan = choice2;
            }
        }

        plan = Arc::new(Mutex::new(SelectPlan::new(plan, data.pred.clone()))) as ArcPlan;
        plan = Arc::new(Mutex::new(ProjectPlan::new(plan, data.fields.clone())?)) as ArcPlan;

        Ok(plan)
    }
}
