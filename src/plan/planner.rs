use super::{query_planner::QueryPlanner, update_planner::UpdatePlanner, Plan};
use crate::{
    parse::parser::Parser,
    query::statement::{CreateStatement, Statement},
    tx::transaction::Transaction,
    unlock,
};
use anyhow::Result;
use std::sync::{Arc, Mutex};

pub struct Planner {
    query_planner: Arc<Mutex<dyn QueryPlanner>>,
    update_planner: Arc<Mutex<dyn UpdatePlanner>>,
}

unsafe impl Send for Planner {}
unsafe impl Sync for Planner {}

impl Planner {
    pub fn new(
        query_planner: Arc<Mutex<dyn QueryPlanner>>,
        update_planner: Arc<Mutex<dyn UpdatePlanner>>,
    ) -> Self {
        Self {
            query_planner,
            update_planner,
        }
    }

    pub fn create_query_plan(
        &mut self,
        query: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Arc<Mutex<dyn Plan>>> {
        let mut parser = Parser::new(query);
        let query_data = parser.query()?;
        unlock!(self.query_planner).create_plan(query_data, tx)
    }

    pub fn execute_update(&mut self, query: &str, tx: Arc<Mutex<Transaction>>) -> Result<i32> {
        let mut parser = Parser::new(query);
        let update_data = parser.update_cmd()?;
        match update_data {
            Statement::Insert(data) => unlock!(self.update_planner).execute_insert(data, tx),
            Statement::Delete(data) => unlock!(self.update_planner).execute_delete(data, tx),
            Statement::Update(data) => unlock!(self.update_planner).execute_modify(data, tx),
            Statement::Create(create) => match create {
                CreateStatement::CreateTable(data) => {
                    unlock!(self.update_planner).execute_create_table(data, tx)
                }
                CreateStatement::CreateView(data) => {
                    unlock!(self.update_planner).execute_create_view(data, tx)
                }
                CreateStatement::CreateIndex(data) => {
                    unlock!(self.update_planner).execute_create_index(data, tx)
                }
            },
        }
    }
}
