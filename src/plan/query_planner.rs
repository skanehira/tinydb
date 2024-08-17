use super::Plan;
use crate::{query::query_data::QueryData, tx::transaction::Transaction};
use anyhow::Result;
use std::sync::{Arc, Mutex};

pub trait QueryPlanner {
    fn create_plan(
        &mut self,
        data: QueryData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Arc<Mutex<dyn Plan>>>;
}
