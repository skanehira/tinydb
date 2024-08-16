use super::constant::Constant;

#[derive(Debug, PartialEq, Eq)]
pub struct InsertData {
    pub table_name: String,
    pub fields: Vec<String>,
    pub values: Vec<Constant>,
}
