use anyhow::{anyhow, Result};
use std::collections::HashMap;

pub enum FieldTypes {
    Integer,
    Varchar,
}

/// From java.sql.Types
impl From<FieldTypes> for i32 {
    fn from(value: FieldTypes) -> i32 {
        match value {
            FieldTypes::Integer => 4,
            FieldTypes::Varchar => 12,
        }
    }
}

impl From<i32> for FieldTypes {
    fn from(value: i32) -> FieldTypes {
        match value {
            4 => FieldTypes::Integer,
            12 => FieldTypes::Varchar,
            _ => unreachable!("unknown type"),
        }
    }
}

#[derive(Clone, Copy)]
pub struct FieldInto {
    r#type: i32,
    length: i32,
}

/// Schema はテーブルレコードのスキーマを表す
/// フィールド名と型、長さを保持する
#[derive(Default, Clone)]
pub struct Schema {
    pub fields: Vec<String>,
    info: HashMap<String, FieldInto>,
}

impl Schema {
    /// add_field はフィールド名、型、長さを追加する
    pub fn add_field(&mut self, field_name: String, r#type: FieldTypes, length: i32) {
        let field = FieldInto {
            r#type: r#type.into(),
            length,
        };
        self.fields.push(field_name.clone());
        self.info.insert(field_name, field);
    }

    /// add_int_field は整数型のフィールドを追加する
    pub fn add_int_field(&mut self, field_name: String) {
        self.add_field(field_name, FieldTypes::Integer, 0);
    }

    /// add_string_field は文字列型のフィールドを追加する
    pub fn add_string_field(&mut self, field_name: String, length: i32) {
        self.add_field(field_name, FieldTypes::Varchar, length);
    }

    /// add はスキーマにフィールドを追加する
    /// スキーマにフィールドの定義がない場合はエラーを返す
    pub fn add(&mut self, field_name: String, schema: &Schema) -> Result<()> {
        let r#type = schema
            .r#type(&field_name)
            .ok_or(anyhow!("field type not found"))?;
        let length = schema
            .length(&field_name)
            .ok_or(anyhow!("field length not found"))?;
        self.add_field(field_name, r#type.into(), length);
        Ok(())
    }

    pub fn add_all(&mut self, schema: &Schema) -> Result<()> {
        for field in &schema.fields {
            self.add(field.clone(), schema)?
        }
        Ok(())
    }

    /// has_field は指定したフィールド名がスキーマに存在するかを返す
    pub fn has_field(&self, field_name: &str) -> bool {
        self.info.contains_key(field_name)
    }

    /// r#type は指定したフィールドの型を返す
    pub fn r#type(&self, field_name: &str) -> Option<i32> {
        self.info.get(field_name)?.r#type.into()
    }

    /// length は指定したフィールドの長さを返す
    pub fn length(&self, field_name: &str) -> Option<i32> {
        self.info.get(field_name)?.length.into()
    }
}
