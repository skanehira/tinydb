use super::{ArcPlan, Plan};
use crate::{
    query::{product_scan::ProductScan, scan::ArcScan},
    record::schema::Schema,
    unlock,
};
use anyhow::Result;
use std::sync::{Arc, Mutex};

pub struct ProductPlan {
    plan1: Arc<Mutex<dyn Plan>>,
    plan2: Arc<Mutex<dyn Plan>>,
    schema: Arc<Schema>,
}

unsafe impl Send for ProductPlan {}
unsafe impl Sync for ProductPlan {}

impl ProductPlan {
    pub fn new(plan1: ArcPlan, plan2: ArcPlan) -> Result<Self> {
        let mut schema = Schema::default();

        schema.add_all(unlock!(plan1).schema())?;
        schema.add_all(unlock!(plan2).schema())?;
        Ok(Self {
            plan1,
            plan2,
            schema: Arc::new(schema),
        })
    }
}

impl Plan for ProductPlan {
    fn open(&mut self) -> Result<ArcScan> {
        let s1 = unlock!(self.plan1).open()?;
        let s2 = unlock!(self.plan2).open()?;
        Ok(Arc::new(Mutex::new(ProductScan::new(s1, s2))) as ArcScan)
    }

    fn blocks_accessed(&self) -> i32 {
        unlock!(self.plan1).blocks_accessed()
            + unlock!(self.plan1).records_output() * unlock!(self.plan2).blocks_accessed()
    }

    fn records_output(&self) -> i32 {
        unlock!(self.plan1).records_output() * unlock!(self.plan2).records_output()
    }

    fn distinct_values(&self, field_name: &str) -> i32 {
        if self.schema().has_field(field_name) {
            unlock!(self.plan1).distinct_values(field_name)
        } else {
            unlock!(self.plan2).distinct_values(field_name)
        }
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}
