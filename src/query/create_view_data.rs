use super::query_data::QueryData;

#[derive(Debug, PartialEq, Eq)]
pub struct CreateViewData {
    pub view_name: String,
    pub query: QueryData,
}
