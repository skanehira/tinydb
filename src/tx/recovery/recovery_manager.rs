use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    buffer::{buffer::Buffer, buffer_manager::BufferManager},
    log::log_manager::LogManager,
    tx::transaction::Transaction,
};

use super::{
    commit_record::CommitRecord,
    record::{create_log_record, LogRecordType},
    set_int_record::SetIntRecord,
    set_string_record::SetStringRecord,
    start_record::StartRecord,
};

pub struct RecoveryManager {
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
    tx_num: i32,
}

impl RecoveryManager {
    pub fn new(
        tx_num: i32,
        log_manager: Arc<Mutex<LogManager>>,
        buffer_manager: Arc<Mutex<BufferManager>>,
    ) -> Result<RecoveryManager> {
        StartRecord::write_to_log(&mut log_manager.lock().unwrap(), tx_num)?;
        Ok(RecoveryManager {
            log_manager,
            buffer_manager,
            tx_num,
        })
    }

    pub fn set_int(&self, buffer: &mut Buffer, offset: i32) -> Result<i32> {
        let old_value = buffer.contents_mut().get_int(offset as usize);
        let block = buffer.block().unwrap();
        let mut log_manager = self.log_manager.lock().unwrap();
        SetIntRecord::write_to_log(&mut log_manager, self.tx_num, block, offset, old_value)
    }

    pub fn set_string(&self, buffer: &mut Buffer, offset: i32) -> Result<i32> {
        let old_value = buffer.contents_mut().get_string(offset as usize);
        let block = buffer.block().unwrap();
        let mut log_manager = self.log_manager.lock().unwrap();
        SetStringRecord::write_to_log(&mut log_manager, self.tx_num, block, offset, old_value)
    }

    pub fn commit(&mut self) -> Result<()> {
        self.buffer_manager.lock().unwrap().flush_all(self.tx_num);
        let lm = &mut self.log_manager.lock().unwrap();
        let lsn = CommitRecord::write_to_log(lm, self.tx_num)?;
        lm.flush(lsn)?;
        Ok(())
    }

    pub fn rollback(&mut self, tx: &mut Transaction) -> Result<()> {
        self.do_rollback(tx)?;
        self.buffer_manager.lock().unwrap().flush_all(self.tx_num);
        let lm = &mut self.log_manager.lock().unwrap();
        let lsn = CommitRecord::write_to_log(lm, self.tx_num)?;
        lm.flush(lsn)?;
        Ok(())
    }

    /// do_rollback はロールバック処理を行います
    ///
    /// 例えばロールバック前のデータは以下のようになっているとします:
    ///
    /// file1:
    /// ```text
    /// +----- block 0 +------- block N --+
    /// | 5 |  hello   | 10 |     ...     |
    /// +--------------+------------------+
    /// ```
    ///
    /// file2:
    /// ```text
    /// +-- block 0 --+-- block 1 --+
    /// |     ...     |     234     |
    /// +-------------+-------------+
    /// ```
    ///
    /// ブロックのデータを更新すると以下のようになります:
    ///
    /// file1:
    /// ```text
    /// +------ block 0 -----+------- block N --+
    /// | 11 |  hello world  | 10 |     ...     |
    /// +--------------------+------------------+
    /// ```
    ///
    /// file2:
    /// ```text
    /// +-- block 0 --+-- block 1 --+
    /// |     ...     |     234     |
    /// +-------------+-------------+
    /// ```
    ///
    /// rog record:
    /// ```text
    /// <START 1>
    /// <START 2>
    /// <SETINT     1, 5, 'file1', 0, 0, 3>
    /// <SETSTRING  1, 5, 'file1', 0, 3, 3, "hello">
    /// <SETSTRING  2, 5, 'file2', 1, 0, 234>
    /// <COMMIT 2>
    /// <ROLLBACK 1>
    /// ```
    ///
    /// ロールバックのプロセスは次のとおりです。
    /// 1. ロールバックするトランザクション（今回は1）のレコードを逆の順序でロールバックします。
    ///     - ロールバックは古い値を元に戻す処理です。
    /// 2. ログ レコードが「開始」の場合は、ロールバック プロセスを停止します。
    /// 3. その他は元に戻します。
    ///     1. `ROLLBACK 1` レコードをロールバック
    ///     2. `SETSTRING 1` レコードをロールバック
    ///     3. `SETINT 1` レコードをロールバック
    fn do_rollback(&mut self, tx: &mut Transaction) -> Result<()> {
        let iter = self.log_manager.lock().unwrap().iter();
        for bytes in iter {
            let mut record = create_log_record(&bytes)?;
            if record.tx_number() == self.tx_num {
                if record.op() == LogRecordType::Start {
                    break;
                }
                record.undo(tx)?;
            }
        }
        Ok(())
    }

    pub fn recover(&mut self, tx: &mut Transaction) -> Result<()> {
        self.do_recover(tx)?;
        self.buffer_manager.lock().unwrap().flush_all(self.tx_num);
        let lm = &mut self.log_manager.lock().unwrap();
        let lsn = CommitRecord::write_to_log(lm, self.tx_num)?;
        lm.flush(lsn)?;
        Ok(())
    }

    /// do_recover はリカバリ処理を行います
    /// ログを逆順に読み取り、コミット済みとロールバック済み以外のトランザクションをロールバックします
    fn do_recover(&mut self, tx: &mut Transaction) -> Result<()> {
        let mut finished = HashMap::new();
        let iter = self.log_manager.lock().unwrap().iter();
        for bytes in iter {
            let mut record = create_log_record(&bytes)?;
            match record.op() {
                LogRecordType::Checkpoint => break,
                LogRecordType::Commit | LogRecordType::Rollback => {
                    finished.insert(record.tx_number(), true);
                }
                _ => {
                    if !finished.contains_key(&record.tx_number()) {
                        record.undo(tx)?;
                    }
                }
            }
        }
        Ok(())
    }
}
