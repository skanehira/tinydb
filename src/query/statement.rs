use super::{
    create_index_data::CreateIndexData, create_table_data::CreateTableData,
    create_view_data::CreateViewData, delete_data::DeleteData, insert_data::InsertData,
    modify_data::ModifyData,
};

pub enum CreateStatement {
    CreateTable(CreateTableData),
    CreateView(CreateViewData),
    CreateIndex(CreateIndexData),
}

pub enum Statement {
    Create(CreateStatement),
    Insert(InsertData),
    Update(ModifyData),
    Delete(DeleteData),
}
