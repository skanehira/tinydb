#![allow(unused_variables)]

use super::constant::Constant;
use crate::record::rid::RID;
use anyhow::Result;
use std::sync::{Arc, Mutex};

pub trait Scan {
    fn before_first(&mut self);
    fn next(&mut self) -> Result<bool>;
    fn get_int(&mut self, field_name: &str) -> Result<i32>;
    fn get_string(&mut self, field_name: &str) -> Result<String>;
    fn get_value(&mut self, fieldname: &str) -> Result<Constant>;
    fn has_field(&self, field_name: &str) -> bool;
    fn close(&mut self);

    fn set_value(&mut self, field_name: &str, val: Constant) -> Result<()> {
        unimplemented!();
    }
    fn set_int(&mut self, field_name: &str, val: i32) -> Result<()> {
        unimplemented!();
    }
    fn set_string(&mut self, field_name: &str, val: &str) -> Result<()> {
        unimplemented!();
    }
    fn delete(&mut self) -> Result<()> {
        unimplemented!();
    }
    fn insert(&mut self) -> Result<()> {
        unimplemented!();
    }
    fn get_rid(&mut self) -> Result<RID> {
        unimplemented!();
    }
    fn move_to_rid(&mut self, rid: RID) {
        unimplemented!();
    }
}

pub type ArcScan = Arc<Mutex<dyn Scan>>;
