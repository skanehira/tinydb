use crate::unlock;

use super::{
    predicate::Predicate,
    scan::{ArcScan, Scan},
};
use anyhow::Result;

pub struct SelectScan {
    scan: ArcScan,
    pred: Predicate,
}

impl SelectScan {
    pub fn new(scan: ArcScan, pred: Predicate) -> SelectScan {
        SelectScan { scan, pred }
    }
}

unsafe impl Send for SelectScan {}
unsafe impl Sync for SelectScan {}

impl Scan for SelectScan {
    fn before_first(&mut self) {
        unlock!(self.scan).before_first();
    }

    fn next(&mut self) -> Result<bool> {
        while unlock!(self.scan).next()? {
            if self.pred.is_satisfied(self.scan.clone())? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_int(&mut self, field_name: &str) -> Result<i32> {
        unlock!(self.scan).get_int(field_name)
    }

    fn get_string(&mut self, field_name: &str) -> Result<String> {
        unlock!(self.scan).get_string(field_name)
    }

    fn get_value(&mut self, fieldname: &str) -> Result<super::constant::Constant> {
        unlock!(self.scan).get_value(fieldname)
    }

    fn has_field(&self, field_name: &str) -> bool {
        unlock!(self.scan).has_field(field_name)
    }

    fn close(&mut self) {
        unlock!(self.scan).close();
    }

    fn set_value(&mut self, field_name: &str, val: super::constant::Constant) -> Result<()> {
        unlock!(self.scan).set_value(field_name, val)
    }

    fn set_int(&mut self, field_name: &str, val: i32) -> Result<()> {
        unlock!(self.scan).set_int(field_name, val)
    }

    fn set_string(&mut self, field_name: &str, val: &str) -> Result<()> {
        unlock!(self.scan).set_string(field_name, val)
    }

    fn delete(&mut self) -> Result<()> {
        unlock!(self.scan).delete()
    }

    fn insert(&mut self) -> Result<()> {
        unlock!(self.scan).insert()
    }

    fn get_rid(&mut self) -> Result<crate::record::rid::RID> {
        unlock!(self.scan).get_rid()
    }

    fn move_to_rid(&mut self, rid: crate::record::rid::RID) {
        unlock!(self.scan).move_to_rid(rid)
    }
}
