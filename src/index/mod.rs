use crate::{query::constant::Constant, record::rid::RID};
use anyhow::Result;

pub mod hash;

pub trait Index {
    fn before_first(&mut self, search_key: Constant) -> Result<()>;
    fn next(&mut self) -> Result<bool>;
    fn get_data_rid(&mut self) -> Result<RID>;
    fn delete(&mut self, data_value: Constant, data_rid: RID) -> Result<()>;
    fn insert(&mut self, data_value: Constant, data_rid: RID) -> Result<()>;
    fn close(&mut self);
}
