use super::{constant::Constant, scan::Scan};
use crate::record::schema::Schema;
use anyhow::Result;
use std::{fmt::Display, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expression {
    Value(Constant),
    FieldName(String),
}

impl From<Constant> for Expression {
    fn from(value: Constant) -> Self {
        Self::Value(value)
    }
}

impl From<String> for Expression {
    fn from(field_name: String) -> Self {
        Self::FieldName(field_name)
    }
}

impl Expression {
    pub fn value(&self) -> Option<Constant> {
        match self {
            Expression::Value(value) => Some(value.clone()),
            _ => None,
        }
    }

    pub fn field_name(&self) -> Option<String> {
        match self {
            Expression::FieldName(field_name) => Some(field_name.clone()),
            _ => None,
        }
    }

    pub fn applies_to(&self, schema: Arc<Schema>) -> bool {
        match self {
            Expression::FieldName(field_name) => schema.has_field(field_name),
            _ => true,
        }
    }

    pub fn evaluate(&self, scan: &mut dyn Scan) -> Result<Constant> {
        match self {
            Expression::Value(value) => Ok(value.clone()),
            Expression::FieldName(field_name) => scan.get_value(field_name),
        }
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::Value(value) => write!(f, "{}", value),
            Expression::FieldName(field_name) => write!(f, "{}", field_name),
        }
    }
}
