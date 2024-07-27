use crate::{file::block::BlockId, TIMEOUT};
use anyhow::{bail, Result};
use std::{collections::HashMap, time::SystemTime};

#[derive(Debug, Default)]
pub struct LockTable {
    locks: HashMap<BlockId, i32>, // 1: S lock, -1: X lock
}

impl LockTable {
    pub fn s_lock(&mut self, block: &BlockId) -> Result<()> {
        if self.has_x_lock(block) {
            bail!("Lock timeout")
        }
        let value = self.get_lock_value(block);
        self.locks.insert(block.clone(), value + 1);
        Ok(())
    }

    pub fn x_lock(&mut self, block: &BlockId) -> Result<()> {
        if self.has_other_s_lock(block) {
            bail!("Lock timeout")
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
        SystemTime::now().duration_since(start_time).unwrap() > TIMEOUT
    }

    pub fn get_lock_value(&self, block: &BlockId) -> i32 {
        *self.locks.get(block).unwrap_or(&0)
    }
}
