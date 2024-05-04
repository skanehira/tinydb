use anyhow::Result;
use tempfile::tempdir;
use tinydb::{file::block::BlockId, server::db::TinyDB};

#[test]
fn buffer_manager_test() -> Result<()> {
    let test_directory = tempdir()?;
    let db = TinyDB::new(test_directory.path(), 400, 3)?;
    let mut buffer_manager = db.buffer_manager.lock().unwrap();
    let mut buffers = vec![
        buffer_manager.pin(&BlockId::new("testfile".into(), 0))?,
        buffer_manager.pin(&BlockId::new("testfile".into(), 1))?,
        buffer_manager.pin(&BlockId::new("testfile".into(), 2))?,
    ];
    buffers.append(&mut vec![
        buffer_manager.pin(&BlockId::new("testfile".into(), 0))?,
        buffer_manager.pin(&BlockId::new("testfile".into(), 1))?,
    ]);

    println!("Available buffers: {}", buffer_manager.num_available);
    {
        println!("Attempting to pin block 3...");
        let result = buffer_manager.pin(&BlockId::new("testfile".into(), 3));
        assert!(result.is_err());
    }
    buffer_manager.unpin(buffers[2].clone());
    buffers.push(buffer_manager.pin(&BlockId::new("testfile".into(), 3))?);

    print!("Final Buffer Allocation:");
    (0..buffers.len()).for_each(|i| {
        println!(
            "buffer[{}] pinned to block {}",
            i,
            buffers[i].lock().unwrap().block().unwrap()
        );
    });

    Ok(())
}
