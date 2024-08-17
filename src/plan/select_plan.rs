use super::{ArcPlan, Plan};
use crate::{
    query::{predicate::Predicate, scan::ArcScan, select_scan::SelectScan},
    record::schema::Schema,
    unlock,
};
use anyhow::Result;
use std::{
    cmp,
    sync::{Arc, Mutex},
};

pub struct SelectPlan {
    plan: ArcPlan,
    pred: Predicate,
}

impl SelectPlan {
    pub fn new(plan: ArcPlan, pred: Predicate) -> Self {
        Self { plan, pred }
    }
}

unsafe impl Send for SelectPlan {}
unsafe impl Sync for SelectPlan {}

impl Plan for SelectPlan {
    fn open(&mut self) -> Result<ArcScan> {
        let s = unlock!(self.plan).open()?;
        Ok(Arc::new(Mutex::new(SelectScan::new(s, self.pred.clone()))) as ArcScan)
    }

    fn blocks_accessed(&self) -> i32 {
        unlock!(self.plan).blocks_accessed()
    }

    fn records_output(&self) -> i32 {
        unlock!(self.plan).records_output() / self.pred.reduction_factor(self.plan.clone())
    }

    fn distinct_values(&self, field_name: &str) -> i32 {
        if self.pred.equates_with_constant(field_name).is_some() {
            1
        } else if let Some(field_name2) = self.pred.equates_with_field(field_name) {
            cmp::min(
                unlock!(self.plan).distinct_values(field_name),
                unlock!(self.plan).distinct_values(&field_name2),
            )
        } else {
            unlock!(self.plan).distinct_values(field_name)
        }
    }

    fn schema(&self) -> Arc<Schema> {
        unlock!(self.plan).schema()
    }
}
