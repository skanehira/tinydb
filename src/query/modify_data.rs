use super::{expression::Expression, predicate::Predicate};

#[derive(Debug, PartialEq, Eq)]
pub struct ModifyData {
    pub table_name: String,
    pub field_name: String,
    pub new_value: Expression,
    pub pred: Predicate,
}
