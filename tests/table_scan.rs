use std::sync::Arc;

use anyhow::Result;
use tempfile::tempdir;
use tinydb::{
    query::scan::Scan as _,
    record::{layout::Layout, schema::Schema, table_scan::TableScan},
    server::db::TinyDB,
};

#[test]
fn table_scan_test() -> Result<()> {
    let test_directory = tempdir()?;
    let db = TinyDB::new(test_directory.path(), 400, 8)?;
    let tx = db.transaction()?;

    let mut sch = Schema::default();
    sch.add_int_field("A");
    sch.add_string_field("B", 9);

    let layout = Layout::try_from_schema(Arc::new(sch))?;
    println!("slot size: {}", layout.slot_size);

    for field_name in layout.schema.fields.iter() {
        let offset = layout.offset(field_name).unwrap();
        println!("{} has offset {}", field_name, offset);
    }

    println!("Filling the table with 50 random records.");
    let mut ts = TableScan::new(tx.clone(), "T", Arc::new(layout))?;
    for n in 0..50 {
        ts.insert()?;
        ts.set_int("A", n)?;
        ts.set_string("B", &format!("rec{}", n))?;
    }

    println!("Deleting these records, whose A-values are less than 25.");
    let mut count = 0;
    ts.before_first();
    while ts.next()? {
        let a = ts.get_int("A")?;
        if a < 25 {
            count += 1;
            ts.delete()?;
        }
    }
    println!("{} values under 10 were deleted.", count);

    println!("Here are the remaining records.");
    ts.before_first();
    while ts.next()? {
        let a = ts.get_int("A")?;
        assert!(a >= 25, "remaining values should be less than 25");
    }
    tx.lock().unwrap().commit()?;
    Ok(())
}
