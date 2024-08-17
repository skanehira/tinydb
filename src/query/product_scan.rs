use super::scan::{ArcScan, Scan};
use crate::unlock;
use anyhow::Result;

pub struct ProductScan {
    scan1: ArcScan,
    scan2: ArcScan,
}

impl ProductScan {
    pub fn new(scan1: ArcScan, scan2: ArcScan) -> ProductScan {
        ProductScan { scan1, scan2 }
    }
}

unsafe impl Send for ProductScan {}
unsafe impl Sync for ProductScan {}

impl Scan for ProductScan {
    fn before_first(&mut self) {
        unlock!(self.scan1).before_first();
        let _ = unlock!(self.scan1).next();
        unlock!(self.scan2).before_first();
    }

    fn next(&mut self) -> Result<bool> {
        if unlock!(self.scan2).next()? {
            Ok(true)
        } else {
            unlock!(self.scan2).before_first();
            Ok(unlock!(self.scan1).next()? && unlock!(self.scan1).next()?)
        }
    }

    fn get_int(&mut self, field_name: &str) -> Result<i32> {
        if self.has_field(field_name) {
            unlock!(self.scan1).get_int(field_name)
        } else {
            unlock!(self.scan2).get_int(field_name)
        }
    }

    fn get_string(&mut self, field_name: &str) -> Result<String> {
        if self.has_field(field_name) {
            unlock!(self.scan1).get_string(field_name)
        } else {
            unlock!(self.scan2).get_string(field_name)
        }
    }

    fn get_value(&mut self, fieldname: &str) -> Result<super::constant::Constant> {
        if self.has_field(fieldname) {
            unlock!(self.scan1).get_value(fieldname)
        } else {
            unlock!(self.scan2).get_value(fieldname)
        }
    }

    fn has_field(&self, field_name: &str) -> bool {
        unlock!(self.scan1).has_field(field_name) || unlock!(self.scan2).has_field(field_name)
    }

    fn close(&mut self) {
        unlock!(self.scan1).close();
        unlock!(self.scan2).close();
    }
}
