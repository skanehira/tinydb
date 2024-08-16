use super::{
    create_index_data::CreateIndexData, create_table_data::CreateTableData,
    create_view_data::CreateViewData, delete_data::DeleteData, insert_data::InsertData,
    modify_data::ModifyData, query_data::QueryData,
};

pub enum CreateStatement {
    CreateTable(CreateTableData),
    CreateView(CreateViewData),
    CreateIndex(CreateIndexData),
}

pub enum Statement {
    Select(QueryData),
    Create(CreateStatement),
    Insert(InsertData),
    Update(ModifyData),
    Delete(DeleteData),
}
