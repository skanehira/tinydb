use super::stat_info::StatInfo;
use crate::{
    index::hash::HashIndex,
    record::{
        layout::Layout,
        schema::{FieldTypes, Schema},
    },
    tx::transaction::Transaction,
};
use anyhow::{bail, Result};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct IndexInfo {
    index_name: String,
    field_name: String,
    tx: Arc<Mutex<Transaction>>,
    table_schema: Arc<Schema>,
    index_layout: Arc<Layout>,
    stat_info: StatInfo,
}

impl IndexInfo {
    pub fn new(
        index_name: String,
        field_name: String,
        table_schema: Arc<Schema>,
        tx: Arc<Mutex<Transaction>>,
        stat_info: StatInfo,
    ) -> Result<Self> {
        let mut schema = Schema::default();
        schema.add_int_field("block");
        schema.add_int_field("id");
        match table_schema.r#type(&field_name) {
            Some(FieldTypes::Integer) => {
                schema.add_int_field("dataval");
            }
            Some(FieldTypes::Varchar) => {
                let length = table_schema.length(&field_name).unwrap();
                schema.add_string_field("dataval", length);
            }
            None => bail!("field not found"),
        }

        let index_info = Self {
            index_name,
            field_name,
            tx,
            table_schema,
            index_layout: Arc::new(Layout::try_from_schema(Arc::new(schema))?),
            stat_info,
        };

        Ok(index_info)
    }

    pub fn open(&mut self) -> HashIndex {
        HashIndex::new(
            self.tx.clone(),
            self.index_name.clone(),
            self.index_layout.clone(),
        )
    }

    pub fn blocks_accessed(&self) -> u64 {
        let rpb = self.tx.lock().unwrap().block_size() / self.index_layout.slot_size;
        let num_blocks = self.stat_info.num_records / rpb;
        HashIndex::search_cost(num_blocks as u64, rpb as u64)
    }

    pub fn records_output(&self) -> i32 {
        self.stat_info.num_records / self.stat_info.distinct_values(&self.field_name)
    }

    pub fn distinct_values(&self, field_name: &str) -> i32 {
        if field_name != self.field_name {
            return 1;
        }
        self.stat_info.num_records / self.stat_info.distinct_values(field_name)
    }
}
