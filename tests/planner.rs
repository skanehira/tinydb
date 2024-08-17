use anyhow::Result;
use tempfile::tempdir;
use tinydb::{server::db::TinyDB, unlock};

#[test]
fn test_planner() -> Result<()> {
    let test_directory = tempdir()?.path().join("test_planner");
    let db = TinyDB::new(test_directory, 400, 8)?;
    let tx = db.transaction()?;
    let planner = db.planner;

    let query = "create table T(A int, B varchar(9))";
    let mut planner = unlock!(planner);
    planner.execute_update(query, tx.clone())?;

    for i in 0..20 {
        let b = format!("rec{}", i);
        let query = format!("insert into T(A, B) values ({}, '{}')", i, b);
        planner.execute_update(&query, tx.clone())?;
    }

    let query = "select B from T where A = 10";
    let plan = planner.create_query_plan(query, tx.clone())?;
    let mut plan = unlock!(plan);

    let scan = plan.open()?;
    let mut scan = unlock!(scan);

    scan.next()?;
    let b = scan.get_string("B")?;
    assert_eq!(b, "rec10");

    unlock!(tx).commit()?;

    Ok(())
}
