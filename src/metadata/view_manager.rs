use super::table_manager::{TableManager, MAX_NAME};
use crate::{
    query::scan::Scan as _,
    record::{schema::Schema, table_scan::TableScan},
    tx::transaction::Transaction,
};
use anyhow::Result;
use std::sync::{Arc, Mutex};

static MAX_VIEWDEF: i32 = 100;

pub struct ViewMgr {
    table_manager: Arc<Mutex<TableManager>>,
    max_viewdef: i32,
}

impl ViewMgr {
    pub fn new(
        is_new: bool,
        table_manager: Arc<Mutex<TableManager>>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Self> {
        let max_viewdef = 100;
        if is_new {
            let mut sch = Schema::default();
            sch.add_string_field("viewname", MAX_NAME);
            sch.add_string_field("viewdef", max_viewdef);
            table_manager
                .lock()
                .unwrap()
                .create_table("viewcat", Arc::new(sch), tx.clone())?;
        }
        Ok(Self {
            table_manager,
            max_viewdef,
        })
    }

    pub fn create_view(
        &self,
        vname: &str,
        vdef: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<()> {
        let layout = Arc::new(
            self.table_manager
                .lock()
                .unwrap()
                .get_layout("viewcat", tx.clone())?,
        );
        let mut ts = TableScan::new(tx, "viewcat", layout)?;
        ts.insert()?;
        ts.set_string("viewname", vname)?;
        ts.set_string("viewdef", vdef)?;
        Ok(())
    }

    pub fn get_view_def(
        &self,
        vname: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Option<String>> {
        let layout = Arc::new(
            self.table_manager
                .lock()
                .unwrap()
                .get_layout("viewcat", tx.clone())?,
        );
        let mut ts = TableScan::new(tx, "viewcat", layout)?;
        while ts.next()? {
            if ts.get_string("viewname")? == vname {
                let result = ts.get_string("viewdef")?;
                return Ok(Some(result));
            }
        }
        Ok(None)
    }
}
