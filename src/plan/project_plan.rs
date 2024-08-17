use super::Plan;
use crate::{
    query::{project_scan::ProjectScan, scan::ArcScan},
    record::schema::Schema,
    unlock,
};
use anyhow::Result;
use std::sync::{Arc, Mutex};

pub struct ProjectPlan {
    plan: Arc<Mutex<dyn Plan>>,
    schema: Schema,
}

impl ProjectPlan {
    pub fn new(plan: Arc<Mutex<dyn Plan>>, fields: Vec<String>) -> Result<Self> {
        let mut schema = Schema::default();
        for field in fields {
            schema.add(field, unlock!(plan).schema())?;
        }
        Ok(Self { plan, schema })
    }
}

unsafe impl Send for ProjectPlan {}
unsafe impl Sync for ProjectPlan {}

impl Plan for ProjectPlan {
    fn open(&mut self) -> Result<ArcScan> {
        let s = unlock!(self.plan).open()?;
        Ok(Arc::new(Mutex::new(ProjectScan::new(s, self.schema.fields.clone()))) as ArcScan)
    }

    fn blocks_accessed(&self) -> i32 {
        unlock!(self.plan).blocks_accessed()
    }

    fn records_output(&self) -> i32 {
        unlock!(self.plan).records_output()
    }

    fn distinct_values(&self, field_name: &str) -> i32 {
        unlock!(self.plan).distinct_values(field_name)
    }

    fn schema(&self) -> Arc<Schema> {
        unlock!(self.plan).schema()
    }
}
