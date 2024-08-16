use super::predicate::Predicate;

#[derive(Debug, PartialEq, Eq)]
pub struct DeleteData {
    pub table_name: String,
    pub pred: Predicate,
}
