use std::fmt::Display;

use super::predicate::Predicate;

#[derive(Debug, PartialEq, Eq)]
pub struct QueryData {
    pub fields: Vec<String>,
    pub tables: Vec<String>,
    pub pred: Predicate,
}

impl QueryData {
    pub fn new(fields: Vec<String>, tables: Vec<String>, pred: Predicate) -> QueryData {
        QueryData {
            fields,
            tables,
            pred,
        }
    }
}

impl Display for QueryData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SELECT ")?;
        for (i, field) in self.fields.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", field)?;
        }
        write!(f, " FROM ")?;
        for (i, table) in self.tables.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", table)?;
        }
        write!(f, " WHERE {}", self.pred)
    }
}
