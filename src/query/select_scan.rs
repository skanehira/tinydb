use super::{
    predicate::Predicate,
    scan::{Scan, UpdateScan},
};
use anyhow::Result;

pub struct SelectScan {
    scan: Box<dyn UpdateScan>,
    pred: Predicate,
}

impl SelectScan {
    pub fn new(scan: Box<dyn UpdateScan>, pred: Predicate) -> SelectScan {
        SelectScan { scan, pred }
    }
}

impl Scan for SelectScan {
    fn before_first(&mut self) {
        self.scan.before_first();
    }

    fn next(&mut self) -> Result<bool> {
        while self.scan.next()? {
            if self.pred.is_satisfied(self.scan.as_scan())? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_int(&mut self, field_name: &str) -> Result<i32> {
        self.scan.get_int(field_name)
    }

    fn get_string(&mut self, field_name: &str) -> Result<String> {
        self.scan.get_string(field_name)
    }

    fn get_value(&mut self, fieldname: &str) -> Result<super::constant::Constant> {
        self.scan.get_value(fieldname)
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.scan.has_field(field_name)
    }

    fn close(&mut self) {
        self.scan.close();
    }
}

impl UpdateScan for SelectScan {
    fn set_value(&mut self, field_name: &str, val: super::constant::Constant) -> Result<()> {
        self.scan.set_value(field_name, val)
    }

    fn set_int(&mut self, field_name: &str, val: i32) -> Result<()> {
        self.scan.set_int(field_name, val)
    }

    fn set_string(&mut self, field_name: &str, val: &str) -> Result<()> {
        self.scan.set_string(field_name, val)
    }

    fn delete(&mut self) -> Result<()> {
        self.scan.delete()
    }

    fn insert(&mut self) -> Result<()> {
        self.scan.insert()
    }

    fn get_rid(&mut self) -> Result<crate::record::rid::RID> {
        self.scan.get_rid()
    }

    fn move_to_rid(&mut self, rid: crate::record::rid::RID) {
        self.scan.move_to_rid(rid)
    }

    fn as_scan(&mut self) -> &mut dyn Scan {
        self
    }
}
