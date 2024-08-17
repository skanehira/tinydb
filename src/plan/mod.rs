pub mod basic_query_plan;
pub mod basic_update_planner;
pub mod better_query_plan;
pub mod product_plan;
pub mod project_plan;
pub mod query_planner;
pub mod select_plan;
pub mod table_plan;
pub mod update_planner;

use crate::{query::scan::ArcScan, record::schema::Schema};
use anyhow::Result;
use std::sync::{Arc, Mutex};

pub trait Plan {
    fn open(&mut self) -> Result<ArcScan>;
    fn blocks_accessed(&self) -> i32;
    fn records_output(&self) -> i32;
    fn distinct_values(&self, field_name: &str) -> i32;
    fn schema(&self) -> Arc<Schema>;
}

pub type ArcPlan = Arc<Mutex<dyn Plan>>;
