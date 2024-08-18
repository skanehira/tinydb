use tempfile::tempdir;
use tinydb::{file::block::BlockId, server::db::TinyDB, tx::transaction::Transaction};

#[test]
fn tx_test() {
    let test_directory = tempdir().unwrap().path().join("tx_test");
    let db = TinyDB::new(test_directory, 400, 8).unwrap();
    let file_manager = db.file_manager;
    let log_manager = db.log_manager;
    let buffer_manager = db.buffer_manager;
    let lock_table = db.lock_table;

    let mut tx1 = Transaction::new(
        file_manager.clone(),
        log_manager.clone(),
        buffer_manager.clone(),
        lock_table.clone(),
    )
    .unwrap();

    let block = BlockId::new("testfile".into(), 1);
    tx1.pin(&block);
    tx1.set_int(&block, 80, 1, false).unwrap();
    tx1.set_string(&block, 40, "one".into(), false).unwrap();
    tx1.commit().unwrap();

    let mut tx2 = Transaction::new(
        file_manager.clone(),
        log_manager.clone(),
        buffer_manager.clone(),
        lock_table.clone(),
    )
    .unwrap();
    tx2.pin(&block);
    let ivalue = tx2.get_int(&block, 80);
    let svalue = tx2.get_string(&block, 40);
    assert_eq!(ivalue, 1);
    assert_eq!(svalue, "one");
    println!("initial value at location 80 = {}", ivalue);
    println!("initial value at location 40 = {}", svalue);

    let newvalue = ivalue + 1;
    let newsvalue = svalue + "!";
    tx2.set_int(&block, 80, newvalue, false).unwrap();
    tx2.set_string(&block, 40, newsvalue, false).unwrap();
    tx2.commit().unwrap();

    let mut tx3 = Transaction::new(
        file_manager.clone(),
        log_manager.clone(),
        buffer_manager.clone(),
        lock_table.clone(),
    )
    .unwrap();
    tx3.pin(&block);
    let ivalue = tx3.get_int(&block, 80);
    let svalue = tx3.get_string(&block, 40);
    assert_eq!(ivalue, 2);
    assert_eq!(svalue, "one!");
    println!("new value at location 80 = {}", ivalue);
    println!("new value at location 40 = {}", svalue);

    tx3.set_int(&block, 80, 9999, true).unwrap();
    println!(
        "pre-rollback value at location 80 = {}",
        tx3.get_int(&block, 80)
    );
    tx3.rollback().unwrap();

    let mut tx4 = Transaction::new(
        file_manager.clone(),
        log_manager.clone(),
        buffer_manager.clone(),
        lock_table.clone(),
    )
    .unwrap();
    tx4.pin(&block);
    println!(
        "post-rollback value at location 80 = {}",
        tx4.get_int(&block, 80)
    );
    tx4.commit().unwrap();
}
