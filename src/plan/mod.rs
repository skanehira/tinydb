use crate::{query::scan::Scan, record::schema::Schema};
use anyhow::Result;
use std::sync::Arc;

pub trait Plan {
    fn open(&mut self) -> Result<Box<dyn Scan>>;
    fn blocks_accessed(&self) -> i32;
    fn records_output(&self) -> i32;
    fn distinct_values(&self, field_name: &str) -> i32;
    fn schema(&self) -> Arc<Schema>;
}
