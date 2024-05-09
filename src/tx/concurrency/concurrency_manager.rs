use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::file::block::BlockId;

use super::lock_table::LockTable;

#[derive(Clone)]
pub struct ConcurrencyManager {
    lock_table: Arc<Mutex<LockTable>>,
    locks: HashMap<BlockId, String>,
}

impl ConcurrencyManager {
    pub fn new(lock_table: Arc<Mutex<LockTable>>) -> Self {
        Self {
            lock_table,
            locks: HashMap::new(),
        }
    }

    pub fn s_lock(&mut self, block: &BlockId) -> Result<()> {
        if !self.locks.contains_key(block) {
            let mut locked = self.lock_table.lock().unwrap();
            locked.s_lock(block)?;
            self.locks.insert(block.clone(), "S".to_string());
        }
        Ok(())
    }

    /// 何もロックが取得されていない場合、排他ロックを取得する
    /// デッドロックを検知するため、共有ロックも取得する
    ///
    /// 例えば、以下のようなトランザクションがある場合、デッドロックが発生する可能性がある
    ///
    /// ```text
    /// T1: S(block1), X(block2)
    /// T2: S(block2), X(block1)
    /// ```
    ///
    /// 上記が以下のようなシリアルスケジュールになる場合、デッドロックが発生する
    ///
    /// ```text
    /// T1: S(block1)
    /// T2: S(block2)
    /// T2: X(block1) => T1が共有ロックを取得しているためT2は待機する
    /// T1: X(block2) => T2が共有ロックを取得しているためT1は待機する
    /// ```
    ///
    /// このようなデッドロックを検知するため、共有ロックを取得してから排他ロックを取得する
    /// 自分以外が握っている共有ロックがある場合、排他ロック時に一度タイムアウトになるまで待機する
    /// タイムアウト後はデッドロックとして扱いエラーを返す
    pub fn x_lock(&mut self, block: &BlockId) -> Result<()> {
        if !self.has_x_lock(block) {
            self.s_lock(block)?;
            self.lock_table.lock().unwrap().x_lock(block)?;
            self.locks.insert(block.clone(), "X".to_string());
        }
        Ok(())
    }

    pub fn release(&mut self) {
        for block in self.locks.keys() {
            self.lock_table.lock().unwrap().unlock(block);
        }

        self.locks.clear();
    }

    // 同一トランザクションですでに排他ロックがある場合はtrueを返す
    pub fn has_x_lock(&self, block: &BlockId) -> bool {
        let Some(lock_typee) = self.locks.get(block) else {
            return false;
        };

        lock_typee == "X"
    }
}
