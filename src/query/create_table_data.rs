use crate::record::schema::Schema;

#[derive(Debug, PartialEq, Eq)]
pub struct CreateTableData {
    pub table_name: String,
    pub schema: Schema,
}
