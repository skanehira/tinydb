use super::constant::Constant;
use crate::record::rid::RID;
use anyhow::Result;

pub trait Scan {
    fn before_first(&mut self);
    fn next(&mut self) -> Result<bool>;
    fn get_int(&mut self, field_name: &str) -> Result<i32>;
    fn get_string(&mut self, field_name: &str) -> Result<String>;
    fn get_value(&mut self, fieldname: &str) -> Result<Constant>;
    fn has_field(&self, field_name: &str) -> bool;
    fn close(&mut self);
}

pub trait UpdateScan: Scan {
    fn as_scan(&mut self) -> &mut dyn Scan;
    fn set_value(&mut self, field_name: &str, val: Constant) -> Result<()>;
    fn set_int(&mut self, field_name: &str, val: i32) -> Result<()>;
    fn set_string(&mut self, field_name: &str, val: &str) -> Result<()>;
    fn delete(&mut self) -> Result<()>;
    fn insert(&mut self) -> Result<()>;
    fn get_rid(&mut self) -> Result<RID>;
    fn move_to_rid(&mut self, rid: RID);
}
