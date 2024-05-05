use std::{
    thread::{self, sleep},
    time::Duration,
};

use tempfile::tempdir;
use tinydb::{file::block::BlockId, server::db::TinyDB, tx::transaction::Transaction};

/// 本テストは以下のシナリオを再現して
/// デッドロックが発生しないことを確認する
///
/// ```text
/// txA: sLock(blk1), sleep(1000), sLock(blk2), unlock(blk1), unlock(blk2)
/// txB: xLock(blk2), sleep(1000), sLock(blk1), unlock(blk1), unlock(blk2)
/// txC: sleep(500) , xLock(blk1), sleep(1000), sLock(blk2),  unlock(blk1), unlock(blk2)
/// ```
///
/// 時系列的に考えると以下のようになる
/// 1. txA: sLock(blk1)
/// 2. txB: xLock(blk2)
/// 3. txC: sleep(500)
/// 4. txA: sleep(1000)
/// 5. txB: sleep(1000)
/// 6. txC: xLock(blk1) -> blk1はtxAによってsLockされているので待機
/// 7. txA: sLock(blk2) -> blk2はtxBによってxLockされているので待機
/// 8. txB: sLock(blk1) -> blk1はtxAによってsLockされているのでそのまま継続
/// 9. txB: unlock(blk1) -> blk1のロックを解放
/// 9. txB: unlock(blk1) -> blk2のロックを解放
/// 11. txA: sLock(blk2) -> txBがblk2のロックがを開放したのでblk2のロックを取得
/// 12. txA: unlock(blk1) -> blk1のロックを解放
/// 13. txC: xLock(blk1) -> txAがblk1のロックを解放したのでblk1のロックを取得
/// 14: txC: sleep(1000)
/// 15: txA: unlock(blk2) -> blk2のロックを解放
/// 16: txC: sLock(blk2) -> blk2のロックを取得
/// 17: txC: unlock(blk1) -> blk1のロックを解放
/// 18: txC: unlock(blk2) -> blk2のロックを解放
///
/// 上記の時系列で動くため、デッドロックは発生しない
#[test]
fn concurrency_test() {
    let test_directory = tempdir().unwrap();
    let db = TinyDB::new(test_directory.path(), 400, 8).unwrap();
    let file_manager = db.file_manager;
    let log_manager = db.log_manager;
    let buffer_manager = db.buffer_manager;
    let lock_table = db.lock_table;

    let handle_a = thread::Builder::new()
        .name("Thread-A".into())
        .spawn({
            let file_manager = file_manager.clone();
            let log_manager = log_manager.clone();
            let buffer_manager = buffer_manager.clone();
            let lock_table = lock_table.clone();

            move || {
                let mut transaction_a =
                    Transaction::new(file_manager, log_manager, buffer_manager, lock_table)
                        .unwrap();
                let block1 = BlockId::new("testfile".into(), 1);
                let block2 = BlockId::new("testfile".into(), 2);
                transaction_a.pin(&block1);
                transaction_a.pin(&block2);
                println!("Transaction A: request slock 1");
                transaction_a.get_int(&block1, 0);
                println!("Transaction A: receive slock 1");
                sleep(Duration::from_millis(1000));
                println!("Transaction A: request slock 2");
                transaction_a.get_int(&block2, 0);
                println!("Transaction A: receive slock 2");
                transaction_a.commit().unwrap();
                println!("Transaction A: commit");
            }
        })
        .unwrap();

    let handle_b = thread::Builder::new()
        .name("Thread-B".into())
        .spawn({
            let file_manager = file_manager.clone();
            let log_manager = log_manager.clone();
            let buffer_manager = buffer_manager.clone();
            let lock_table = lock_table.clone();

            move || {
                let mut transaction_b =
                    Transaction::new(file_manager, log_manager, buffer_manager, lock_table)
                        .unwrap();
                let block1 = BlockId::new("testfile".into(), 1);
                let block2 = BlockId::new("testfile".into(), 2);
                transaction_b.pin(&block1);
                transaction_b.pin(&block2);
                println!("Transaction B: request xlock 2");
                transaction_b.set_int(&block2, 0, 0, false).unwrap();
                println!("Transaction B: receive xlock 2");
                sleep(Duration::from_millis(1000));
                println!("Transaction B: request slock 1");
                transaction_b.get_int(&block1, 0);
                println!("Transaction B: receive slock 1");
                transaction_b.commit().unwrap();
                println!("Transaction B: commit");
            }
        })
        .unwrap();

    let handle_c = thread::Builder::new()
        .name("Thread-C".into())
        .spawn({
            let file_manager = file_manager.clone();
            let log_manager = log_manager.clone();
            let buffer_manager = buffer_manager.clone();
            let lock_table = lock_table.clone();

            move || {
                let mut transaction_c =
                    Transaction::new(file_manager, log_manager, buffer_manager, lock_table)
                        .unwrap();
                let block1 = BlockId::new("testfile".into(), 1);
                let block2 = BlockId::new("testfile".into(), 2);
                transaction_c.pin(&block1);
                transaction_c.pin(&block2);
                sleep(Duration::from_millis(500));
                println!("Transaction C: request xlock 1");
                transaction_c
                    .set_int(&block1, 0, 0, false)
                    .expect("Transaction C: Error setting int in block1");
                println!("Transaction C: receive xlock 1");
                sleep(Duration::from_millis(1000));
                println!("Transaction C: request slock 2");
                transaction_c.get_int(&block2, 0);
                println!("Transaction C: receive slock 2");
                transaction_c.commit().unwrap();
                println!("Transaction C: commit");
            }
        })
        .unwrap();

    handle_a.join().unwrap();
    handle_b.join().unwrap();
    handle_c.join().unwrap();
}
