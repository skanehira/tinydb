use super::scan::{Scan, UpdateScan};
use anyhow::{bail, Result};

pub struct ProjectScan {
    scan: Box<dyn UpdateScan>,
    fields: Vec<String>,
}

impl ProjectScan {
    pub fn new(scan: Box<dyn UpdateScan>, fields: Vec<String>) -> ProjectScan {
        ProjectScan { scan, fields }
    }
}

impl Scan for ProjectScan {
    fn before_first(&mut self) {
        self.scan.before_first();
    }

    fn next(&mut self) -> Result<bool> {
        self.scan.next()
    }

    fn get_int(&mut self, field_name: &str) -> Result<i32> {
        if self.has_field(field_name) {
            self.scan.get_int(field_name)
        } else {
            bail!("field not found: {}", field_name);
        }
    }

    fn get_string(&mut self, field_name: &str) -> Result<String> {
        if self.has_field(field_name) {
            self.scan.get_string(field_name)
        } else {
            bail!("field not found: {}", field_name);
        }
    }

    fn get_value(&mut self, fieldname: &str) -> Result<super::constant::Constant> {
        if self.has_field(fieldname) {
            self.scan.get_value(fieldname)
        } else {
            bail!("field not found: {}", fieldname);
        }
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.fields.contains(&field_name.into())
    }

    fn close(&mut self) {
        self.scan.close();
    }
}
