use anyhow::Result;
use tempfile::tempdir;
use tinydb::{log_manager::LogManager, page::Page, server::db::TinyDB};

#[test]
fn log_test() -> Result<()> {
    let test_directory = tempdir()?;
    let db = TinyDB::new(test_directory.path(), 400)?;
    let mut log_manager = db.log_manager;

    print_log_records(&mut log_manager, "The initial empty log file:", &[])?;

    create_records(&mut log_manager, 1, 35)?;

    let expected_after_first_batch = (1..=35)
        .rev()
        .map(|i| (format!("record{}", i), i + 100))
        .collect::<Vec<_>>();
    print_log_records(
        &mut log_manager,
        "The log file now has these records:",
        &expected_after_first_batch,
    )?;

    create_records(&mut log_manager, 36, 70)?;

    log_manager.flush(65).unwrap();

    let expected_after_second_batch = (1..=70)
        .rev()
        .map(|i| (format!("record{}", i), i + 100))
        .collect::<Vec<_>>();
    print_log_records(
        &mut log_manager,
        "The log file now has these records:",
        &expected_after_second_batch,
    )?;

    Ok(())
}

fn print_log_records(
    log_manager: &mut LogManager,
    msg: &str,
    expected_records: &[(String, i32)],
) -> Result<()> {
    println!("{}", msg);
    let mut index = 0;
    let iterator = log_manager.iter();
    for bytes in iterator {
        let mut page = Page::from(bytes);
        let string = page.get_string(0)?;
        let number_position = Page::max_length(string.len());
        let value = page.get_int(number_position);

        assert_eq!((string, value), expected_records[index],);
        index += 1;
    }
    assert_eq!(index, expected_records.len());
    Ok(())
}

fn create_records(log_manager: &mut LogManager, start: i32, end: i32) -> Result<()> {
    println!("Creating records: ");
    for i in start..=end {
        let record = create_log_record(format!("record{}", i), i + 100)?;
        let lsn = log_manager.append(&record)?;
        println!("{} ", lsn);
    }
    println!();
    Ok(())
}

fn create_log_record(s: String, n: i32) -> Result<Vec<u8>> {
    let string_position = 0;
    let number_position = string_position + Page::max_length(s.len());
    let blocksize = number_position + std::mem::size_of::<i32>();

    let mut page = Page::new(blocksize as u64);

    page.set_string(string_position, &s);
    page.set_int(number_position, n);

    let bytes = page.read_bytes(0, blocksize)?;

    Ok(bytes)
}
