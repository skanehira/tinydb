#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndexData {
    pub index_name: String,
    pub table_name: String,
    pub field_name: String,
}
