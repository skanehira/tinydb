use super::table_manager::{TableManager, MAX_NAME};
use crate::{
    query::scan::Scan as _,
    record::{schema::Schema, table_scan::TableScan},
    tx::transaction::Transaction,
    unlock,
};
use anyhow::Result;
use std::sync::{Arc, Mutex};

static MAX_VIEWDEF: i32 = 100;

pub struct ViewManager {
    table_manager: Arc<Mutex<TableManager>>,
    max_viewdef: i32,
}

impl ViewManager {
    pub fn new(
        is_new: bool,
        table_manager: Arc<Mutex<TableManager>>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Self> {
        if is_new {
            let mut sch = Schema::default();
            sch.add_string_field("viewname", MAX_NAME);
            sch.add_string_field("viewdef", MAX_VIEWDEF);
            unlock!(table_manager).create_table("viewcat", Arc::new(sch), tx.clone())?;
        }
        Ok(Self {
            table_manager,
            max_viewdef: MAX_VIEWDEF,
        })
    }

    pub fn create_view(
        &self,
        vname: &str,
        view_def: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<()> {
        let layout = Arc::new(unlock!(self.table_manager).get_layout("viewcat", tx.clone())?);
        let mut ts = TableScan::new(tx, "viewcat", layout)?;
        ts.insert()?;
        ts.set_string("viewname", vname)?;
        ts.set_string("viewdef", view_def)?;
        Ok(())
    }

    pub fn get_view_def(
        &self,
        view_name: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Option<String>> {
        let layout = Arc::new(unlock!(self.table_manager).get_layout("viewcat", tx.clone())?);
        let mut ts = TableScan::new(tx, "viewcat", layout)?;
        while ts.next()? {
            if ts.get_string("viewname")? == view_name {
                let result = ts.get_string("viewdef")?;
                return Ok(Some(result));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, Mutex};

    use anyhow::Result;
    use tempfile::tempdir;

    use crate::{metadata::table_manager::TableManager, server::db::TinyDB};

    use super::ViewManager;

    #[test]
    fn should_can_create_view_table() -> Result<()> {
        let test_directory = tempdir()?.path().join("should_can_create_view_table");
        let db = TinyDB::new(test_directory, 400, 8)?;
        let tx = db.transaction()?;

        let table_manager = Arc::new(Mutex::new(TableManager::new(true, tx.clone())?));

        let view_manager = ViewManager::new(true, table_manager.clone(), tx.clone())?;

        let view_name = "view1";
        let view_def = "x";
        view_manager.create_view(view_name, view_def, tx.clone())?;

        assert_eq!(
            view_manager.get_view_def(view_name, tx.clone())?,
            Some(view_def.into())
        );

        Ok(())
    }
}
