use anyhow::Result;
use tempfile::tempdir;
use tinydb::{file::block::BlockId, server::db::TinyDB};

#[test]
fn buffer_test() -> Result<()> {
    let test_directory = tempdir()?.path().join("buffer_test");
    let db = TinyDB::new(test_directory, 400, 3)?;
    let mut buffer_manager = db.buffer_manager.lock().unwrap();

    let buf1 = buffer_manager.pin(&BlockId::new("testfile".into(), 1))?;
    {
        let mut buf1 = buf1.lock().unwrap();
        let page = buf1.contents_mut();
        let n = page.get_int(80);
        page.set_int(80, n + 1);
        buf1.set_modified(1, 0);
        println!("The new value is {}", n + 1);
    }
    buffer_manager.unpin(buf1);

    let buf2 = buffer_manager.pin(&BlockId::new("testfile".into(), 2))?;
    buffer_manager.pin(&BlockId::new("testfile".into(), 3))?;
    buffer_manager.pin(&BlockId::new("testfile".into(), 4))?;

    buffer_manager.unpin(buf2.clone());

    {
        let mut buf2 = buf2.lock().unwrap();
        let page2 = buf2.contents_mut();
        page2.set_int(80, 9999);
        buf2.set_modified(1, 0);
    }

    buffer_manager.unpin(buf2);

    Ok(())
}
