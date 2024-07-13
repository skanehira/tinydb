use std::sync::Arc;

use tempfile::tempdir;
use tinydb::file::block::BlockId;
use tinydb::record::layout::Layout;
use tinydb::record::record_page::RecordPage;
use tinydb::record::schema::Schema;
use tinydb::server::db::TinyDB;

#[test]
fn record_test() {
    let test_directory = tempdir().unwrap();
    let db = TinyDB::new(test_directory.path(), 400, 8).unwrap();

    let transaction = db.transaction().unwrap();

    // Initialize Schema and Layout.
    let mut schema = Schema::default();
    schema.add_int_field("A".to_string());
    schema.add_string_field("B".to_string(), 9);
    let schema = Arc::new(schema);
    let layout = Arc::new(Layout::try_from_schema(schema.clone()).unwrap());

    for field_name in &layout.schema.fields {
        let offset = layout.offset(field_name).unwrap();
        println!("{} has offset {}", field_name, offset);
    }

    let block = BlockId::new("testfile".to_string(), 0);
    let mut record_page = RecordPage::new(transaction.clone(), block, layout.clone());
    record_page.format().unwrap();

    // Insert records into the page until it's full
    let mut n = 0;
    let initial_slot = -1;
    let mut slot = record_page.insert_after(initial_slot).unwrap();

    loop {
        if slot < 0 {
            break;
        }

        record_page.set_int(slot, "A", n).unwrap();
        record_page
            .set_string(slot, "B", format!("rec{}", n))
            .unwrap();
        println!("inserting into slot {}: {{ {}, rec{} }}", slot, n, n);
        n += 1;
        slot = record_page.insert_after(slot).unwrap();
    }

    // Delete records where A < 10
    println!("Deleting these records, whose A-values are less than 10.");
    let mut count = 0;
    let mut slot = -1;

    loop {
        slot = record_page.next_after(slot);

        if slot < 0 {
            break;
        }

        let a = record_page.get_int(slot, "A").unwrap();
        let b = record_page.get_string(slot, "B").unwrap();
        if a < 10 {
            count += 1;
            println!("slot {}: {{ {}, {} }}", slot, a, b);
            record_page.delete(slot).unwrap();
        }
    }

    println!("{} values under 10 were deleted.", count);

    // Check remaining records
    println!("Here are the remaining records.");
    let mut slot = -1;

    loop {
        slot = record_page.next_after(slot);

        if slot < 0 {
            break;
        }

        let a = record_page.get_int(slot, "A").unwrap();
        let b = record_page.get_string(slot, "B").unwrap();

        println!("slot {}: {{ {}, {} }}", slot, a, b);
        assert!(a >= 10, "Assertion failed for remaining records",);
    }

    transaction.lock().unwrap().commit().unwrap();
}
