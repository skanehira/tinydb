use super::{
    predicate::Predicate,
    scan::{Scan, UpdateScan},
};
use anyhow::{bail, Result};

pub struct ProductScan {
    scan1: Box<dyn UpdateScan>,
    scan2: Box<dyn UpdateScan>,
}

impl ProductScan {
    pub fn new(scan1: Box<dyn UpdateScan>, scan2: Box<dyn UpdateScan>) -> ProductScan {
        ProductScan { scan1, scan2 }
    }
}

impl Scan for ProductScan {
    fn before_first(&mut self) {
        self.scan1.before_first();
        self.scan1.next();
        self.scan2.before_first();
    }

    fn next(&mut self) -> Result<bool> {
        if (self.scan2.next()?) {
            return Ok(true);
        } else {
            self.scan2.before_first();
            Ok(self.scan1.next()? && self.scan1.next()?)
        }
    }

    fn get_int(&mut self, field_name: &str) -> Result<i32> {
        if self.has_field(field_name) {
            self.scan1.get_int(field_name)
        } else {
            self.scan2.get_int(field_name)
        }
    }

    fn get_string(&mut self, field_name: &str) -> Result<String> {
        if self.has_field(field_name) {
            self.scan1.get_string(field_name)
        } else {
            self.scan2.get_string(field_name)
        }
    }

    fn get_value(&mut self, fieldname: &str) -> Result<super::constant::Constant> {
        if self.has_field(fieldname) {
            self.scan1.get_value(fieldname)
        } else {
            self.scan2.get_value(fieldname)
        }
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.scan1.has_field(&field_name.into()) || self.scan2.has_field(field_name)
    }

    fn close(&mut self) {
        self.scan1.close();
        self.scan2.close();
    }
}
