use anyhow::{bail, Result};
use std::{collections::HashMap, sync::OnceLock, time::SystemTime};

use crate::file::block::BlockId;

static MAX_TIME: OnceLock<u128> = OnceLock::new();

pub struct LockTable {
    locks: HashMap<BlockId, i32>, // 1: S lock, -1: X lock
}

impl Default for LockTable {
    fn default() -> Self {
        // NOTE: This is a hack to allow setting the MAX_TIME environment variable for testing
        let _ = MAX_TIME.set(
            std::env::var("MAX_TIME")
                .unwrap_or_else(|_| "10000".to_string())
                .parse()
                .unwrap(),
        );
        Self {
            locks: HashMap::new(),
        }
    }
}

impl LockTable {
    pub fn s_lock(&mut self, block: &BlockId) -> Result<()> {
        let now = SystemTime::now();
        while self.has_x_lock(block) && !Self::waiting_too_long(now) {
            // This implementation maybe wrong
            // Should I use condvar?
            std::thread::sleep(std::time::Duration::from_millis(
                *MAX_TIME.get().unwrap() as u64
            ));
        }
        if self.has_x_lock(block) {
            bail!("Deadlock")
        }
        let value = self.get_lock_value(block);
        self.locks.insert(block.clone(), value + 1);
        Ok(())
    }

    pub fn x_lock(&mut self, block: &BlockId) -> Result<()> {
        let now = SystemTime::now();
        while self.has_other_s_lock(block) && !Self::waiting_too_long(now) {
            // This implementation maybe wrong
            // Should I use condvar?
            std::thread::sleep(std::time::Duration::from_millis(
                *MAX_TIME.get().unwrap() as u64
            ));
        }
        if self.has_other_s_lock(block) {
            bail!("Deadlock")
        }
        self.locks.insert(block.clone(), -1);
        Ok(())
    }

    pub fn unlock(&mut self, block: &BlockId) {
        let value = self.get_lock_value(block);
        if value > 1 {
            self.locks.insert(block.clone(), value - 1);
        } else {
            self.locks.remove(block);
        }
    }

    pub fn has_x_lock(&self, block: &BlockId) -> bool {
        self.get_lock_value(block) < 0
    }

    pub fn has_other_s_lock(&self, block: &BlockId) -> bool {
        self.get_lock_value(block) > 1
    }

    pub fn waiting_too_long(start_time: SystemTime) -> bool {
        SystemTime::now()
            .duration_since(start_time)
            .unwrap()
            .as_millis()
            > *MAX_TIME.get().unwrap()
    }

    pub fn get_lock_value(&self, block: &BlockId) -> i32 {
        match self.locks.get(block) {
            Some(value) => *value,
            None => 0,
        }
    }
}
