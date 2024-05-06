use crate::I32_SIZE;
use anyhow::{anyhow, Result};

use super::schema::{FieldTypes, Schema};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// Layout はテーブルレコードのレイアウトを表す
/// フィールド名と型、テーブル内の各フィールドのオフセットを保持する
pub struct Layout {
    pub schema: Arc<Mutex<Schema>>,
    pub offsets: HashMap<String, i32>,
    pub slot_size: i32,
}

impl Layout {
    pub fn try_from_schema(schema: Arc<Mutex<Schema>>) -> Result<Self> {
        let mut pos = I32_SIZE as i32;
        let mut offsets = HashMap::new();
        let sch = schema.lock().unwrap();
        for field in &sch.fields {
            offsets.insert(field.clone(), pos);
            pos += Self::length_in_bytes(&sch, field)?;
        }
        Ok(Self {
            schema: schema.clone(),
            offsets,
            slot_size: pos,
        })
    }

    pub fn try_from_metadata(
        schema: Arc<Mutex<Schema>>,
        offsets: HashMap<String, i32>,
        slot_size: i32,
    ) -> Result<Self> {
        Ok(Self {
            schema: schema.clone(),
            offsets,
            slot_size,
        })
    }

    pub fn offset(&self, field_name: &str) -> i32 {
        self.offsets.get(field_name).copied().unwrap_or(-1)
    }

    pub fn length_in_bytes(schema: &Schema, field_name: &str) -> Result<i32> {
        let field_type = schema
            .r#type(field_name)
            .ok_or_else(|| anyhow!("field type not found"))?;
        match field_type.into() {
            FieldTypes::Integer => Ok(I32_SIZE as i32),
            FieldTypes::Varchar => {
                let length = schema
                    .length(field_name)
                    .ok_or_else(|| anyhow!("field length not found"))?;
                Ok(length)
            }
        }
    }
}
