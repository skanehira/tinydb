use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    buffer::{buffer::Buffer, buffer_manager::BufferManager},
    file::block::BlockId,
};

pub struct BufferList {
    buffers: HashMap<BlockId, Arc<Mutex<Buffer>>>,
    pins: Vec<BlockId>,
    buffer_manager: Arc<Mutex<BufferManager>>,
}

impl BufferList {
    pub fn new(buffer_manager: Arc<Mutex<BufferManager>>) -> Self {
        Self {
            buffers: HashMap::new(),
            pins: Vec::new(),
            buffer_manager,
        }
    }

    pub fn get_buffer(&self, block: &BlockId) -> Option<&Arc<Mutex<Buffer>>> {
        self.buffers.get(block)
    }

    pub fn pin(&mut self, block: &BlockId) -> Result<()> {
        let Ok(buffer) = self.buffer_manager.lock().unwrap().pin(block) else {
            return Ok(());
        };

        self.buffers.insert(block.clone(), buffer);
        self.pins.push(block.clone());
        Ok(())
    }

    pub fn unpin(&mut self, block: &BlockId) -> Result<()> {
        if let Some(buffer) = self.buffers.get(block) {
            self.buffer_manager.lock().unwrap().unpin(buffer.clone());
        }
        self.pins.retain(|b| b.id != block.id);
        if !self.pins.contains(block) {
            self.buffers.remove(block);
        }
        Ok(())
    }

    pub fn unpin_all(&mut self) {
        for block in &self.pins {
            if let Some(buffer) = self.buffers.get(block) {
                self.buffer_manager.lock().unwrap().unpin(buffer.clone());
            }
        }
        self.buffers.clear();
        self.pins.clear();
    }
}
