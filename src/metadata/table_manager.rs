use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    query::scan::Scan as _,
    record::{layout::Layout, schema::Schema, table_scan::TableScan},
    tx::transaction::Transaction,
};
use anyhow::Result;

static MAX_NAME: i32 = 16;

pub struct TableManager {
    /// テーブルごとのメタデータを保持する
    /// メタデータは以下となる
    ///   - テーブル名
    ///   - スロット（レコード）のサイズ
    table_catlog_layout: Arc<Layout>,
    /// テーブルのカラムごとのメタデータを保持する
    /// メタデータは以下となる
    ///   - テーブル名
    ///   - フィールド名（カラム名）
    ///   - フィールドの種類（FieldTypesの値）
    ///   - フィールドの長さ
    ///   - フィールドのオフセット（スロットの先頭からの位置）
    field_catlog_layout: Arc<Layout>,
}

impl TableManager {
    pub fn new(is_new: bool, tx: Arc<Mutex<Transaction>>) -> Result<Self> {
        let mut tcs = Schema::default();
        tcs.add_string_field("tblname", MAX_NAME);
        tcs.add_int_field("slotsize");
        let table_catlog_layout = Arc::new(Layout::try_from_schema(Arc::new(tcs))?);

        let mut fcs = Schema::default();
        fcs.add_string_field("tblname", MAX_NAME);
        fcs.add_string_field("fldname", MAX_NAME);
        fcs.add_int_field("type");
        fcs.add_int_field("length");
        fcs.add_int_field("offset");
        let field_catlog_layout = Arc::new(Layout::try_from_schema(Arc::new(fcs))?);

        let mut tm = Self {
            table_catlog_layout,
            field_catlog_layout,
        };

        if is_new {
            tm.create_table("tblcat", tm.table_catlog_layout.schema.clone(), tx.clone())?;
            tm.create_table("fldcat", tm.field_catlog_layout.schema.clone(), tx.clone())?;
        }

        Ok(tm)
    }

    pub fn create_table(
        &mut self,
        table_name: impl Into<String>,
        schema: Arc<Schema>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<()> {
        let table_name = table_name.into();
        let layout = Arc::new(Layout::try_from_schema(schema)?);
        let mut tcat = TableScan::new(tx.clone(), "tblcat", self.table_catlog_layout.clone())?;
        tcat.insert()?;
        tcat.set_string("tblname", &table_name)?;
        tcat.set_int("slotsize", layout.slot_size)?;
        tcat.close();

        let mut fcat = TableScan::new(tx.clone(), "fldcat", self.field_catlog_layout.clone())?;
        for field_name in layout.schema.fields.iter() {
            fcat.insert()?;
            fcat.set_string("tblname", &table_name)?;
            fcat.set_string("fldname", field_name)?;
            fcat.set_int("type", layout.schema.r#type(field_name).unwrap() as i32)?;
            fcat.set_int("length", layout.schema.length(field_name).unwrap())?;
            fcat.set_int("offset", layout.offset(field_name).unwrap())?;
        }
        fcat.close();

        Ok(())
    }

    pub fn get_layout(
        &mut self,
        table_name: impl Into<String>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Layout> {
        let mut size = -1;
        let table_name = table_name.into();

        let mut tcat = TableScan::new(tx.clone(), "tblcat", self.table_catlog_layout.clone())?;

        while tcat.next()? {
            if tcat.get_string("tblname")? == table_name {
                size = tcat.get_int("slotsize")?;
                break;
            }
        }
        tcat.close();

        let mut schema = Schema::default();
        let mut offsets: HashMap<String, i32> = HashMap::default();

        let mut fcat = TableScan::new(tx, "fldcat", self.field_catlog_layout.clone())?;

        while fcat.next()? {
            if fcat.get_string("tblname")? == table_name {
                let field_name = fcat.get_string("fldname")?;
                let field_type = fcat.get_int("type")?;
                let length = fcat.get_int("length")?;
                let offset = fcat.get_int("offset")?;
                schema.add_field(field_name.clone(), field_type.into(), length);
                offsets.insert(field_name, offset);
            }
        }

        fcat.close();
        Layout::try_from_metadata(Arc::new(schema), offsets, size)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::TableManager;
    use crate::{
        query::scan::Scan as _,
        record::{schema::FieldTypes, table_scan::TableScan},
        server::db::TinyDB,
    };
    use anyhow::Result;
    use tempfile::tempdir;

    #[test]
    fn should_can_get_layout() -> Result<()> {
        let test_directory = tempdir()?;
        let db = TinyDB::new(test_directory.path(), 400, 8)?;
        let tx = db.transaction()?;

        let mut table_manager = TableManager::new(true, tx.clone())?;

        let table_catlog_layout = Arc::new(table_manager.get_layout("tblcat", tx.clone())?);

        let mut ts = TableScan::new(tx.clone(), "tblcat", table_catlog_layout.clone())?;

        let wants = vec![("tblcat", 28), ("fldcat", 56)];

        for want in wants {
            ts.next()?;
            assert_eq!(ts.get_string("tblname")?, want.0);
            assert_eq!(ts.get_int("slotsize")?, want.1);
        }
        ts.close();

        let layout = table_manager.get_layout("fldcat", tx.clone())?;
        let mut ts = TableScan::new(tx.clone(), "fldcat", Arc::new(layout))?;

        let wants = vec![
            ("tblcat", "tblname", FieldTypes::Varchar, 16, 4),
            ("tblcat", "slotsize", FieldTypes::Integer, 0, 24),
        ];

        for want in wants {
            ts.next()?;
            assert_eq!(ts.get_string("tblname")?, want.0);
            assert_eq!(ts.get_string("fldname")?, want.1);
            assert_eq!(ts.get_int("type")?, want.2.into());
            assert_eq!(ts.get_int("length")?, want.3);
            assert_eq!(ts.get_int("offset")?, want.4);
        }

        let wants = vec![
            ("fldcat", "tblname", FieldTypes::Varchar, 16, 4),
            ("fldcat", "fldname", FieldTypes::Varchar, 16, 24),
            ("fldcat", "type", FieldTypes::Integer, 0, 44),
            ("fldcat", "length", FieldTypes::Integer, 0, 48),
            ("fldcat", "offset", FieldTypes::Integer, 0, 52),
        ];

        for want in wants {
            ts.next()?;
            assert_eq!(ts.get_string("tblname")?, want.0);
            assert_eq!(ts.get_string("fldname")?, want.1);
            assert_eq!(ts.get_int("type")?, want.2.into());
            assert_eq!(ts.get_int("length")?, want.3);
            assert_eq!(ts.get_int("offset")?, want.4);
        }
        Ok(())
    }
}
