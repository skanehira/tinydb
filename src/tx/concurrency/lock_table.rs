use crate::{file::block::BlockId, TIMEOUT};
use anyhow::{bail, Result};
use std::{
    collections::HashMap,
    sync::{Condvar, Mutex},
    time::SystemTime,
};

pub struct LockTable {
    cond_var: std::sync::Condvar,
    locks: Mutex<HashMap<BlockId, i32>>, // 1: S lock, -1: X lock
}

impl Default for LockTable {
    fn default() -> Self {
        Self {
            cond_var: Condvar::new(),
            locks: Mutex::default(),
        }
    }
}

impl LockTable {
    pub fn s_lock(&mut self, block: &BlockId) -> Result<()> {
        let now = SystemTime::now();
        let mut locks = self.locks.lock().unwrap();
        while self.has_x_lock(block, &locks) && !Self::waiting_too_long(now) {
            locks = self.cond_var.wait_timeout(locks, TIMEOUT).unwrap().0;
        }
        if self.has_x_lock(block, &locks) {
            bail!("Lock timeout")
        }
        let value = self.get_lock_value(block, &locks);
        locks.insert(block.clone(), value + 1);
        Ok(())
    }

    pub fn x_lock(&mut self, block: &BlockId) -> Result<()> {
        let now = SystemTime::now();
        let mut locks = self.locks.lock().unwrap();
        while self.has_other_s_lock(block, &locks) && !Self::waiting_too_long(now) {
            locks = self.cond_var.wait_timeout(locks, TIMEOUT).unwrap().0;
        }
        if self.has_other_s_lock(block, &locks) {
            bail!("Lock timeout")
        }
        locks.insert(block.clone(), -1);
        Ok(())
    }

    pub fn unlock(&mut self, block: &BlockId) {
        let mut locks = self.locks.lock().unwrap();
        let value = self.get_lock_value(block, &locks);
        if value > 1 {
            locks.insert(block.clone(), value - 1);
        } else {
            locks.remove(block);
            self.cond_var.notify_all();
        }
    }

    pub fn has_x_lock(&self, block: &BlockId, locks: &HashMap<BlockId, i32>) -> bool {
        self.get_lock_value(block, locks) < 0
    }

    pub fn has_other_s_lock(&self, block: &BlockId, locks: &HashMap<BlockId, i32>) -> bool {
        self.get_lock_value(block, locks) > 1
    }

    pub fn waiting_too_long(start_time: SystemTime) -> bool {
        SystemTime::now().duration_since(start_time).unwrap() > TIMEOUT
    }

    pub fn get_lock_value(&self, block: &BlockId, locks: &HashMap<BlockId, i32>) -> i32 {
        *locks.get(block).unwrap_or(&0)
    }
}
