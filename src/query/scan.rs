use super::constant::Constant;
use crate::record::rid::RID;

pub trait Scan {
    // for scan
    fn before_first(&mut self);
    fn next(&mut self) -> bool;
    fn get_int(&mut self, field_name: &str) -> i32;
    fn get_value(&mut self, fieldname: &str) -> Constant;
    fn get_string(&mut self, field_name: &str) -> String;
    fn has_field(&self, field_name: &str) -> bool;
    fn close(&mut self);

    // for update scan
    fn set_val(&mut self, field_name: &str, val: Constant);
    fn set_int(&mut self, field_name: &str, val: i32);
    fn set_string(&mut self, field_name: &str, val: &str);
    fn delete(&mut self);
    fn insert(&mut self);
    fn get_rid(&self) -> RID;
    fn move_to_rid(&mut self, rid: RID);
}
