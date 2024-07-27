use crate::{
    file::{block::BlockId, file_manager::FileManager},
    log::log_manager::LogManager,
    TIMEOUT,
};
use anyhow::{bail, Result};
use std::{
    sync::{Arc, Mutex},
    time::SystemTime,
};

use super::buffer::Buffer;

#[derive(Debug)]
pub struct BufferManager {
    buffer_pool: Vec<Arc<Mutex<Buffer>>>,
    pub num_available: u64,
}

impl BufferManager {
    pub fn new(
        file_manager: Arc<Mutex<FileManager>>,
        log_manager: Arc<Mutex<LogManager>>,
        num_buffers: u64,
    ) -> Self {
        let mut buffer_pool = Vec::with_capacity(num_buffers as usize);
        for _ in 0..num_buffers {
            buffer_pool.push(Arc::new(Mutex::new(Buffer::new(
                file_manager.clone(),
                log_manager.clone(),
            ))));
        }

        Self {
            buffer_pool,
            num_available: num_buffers,
        }
    }

    pub fn flush_all(&mut self, txnum: i32) {
        for buffer in &mut self.buffer_pool {
            let mut x = buffer.lock().unwrap();
            if x.modifying_tx() == txnum {
                x.flush();
            }
        }
    }

    pub fn unpin(&mut self, buffer: Arc<Mutex<Buffer>>) {
        let mut buffer = buffer.lock().unwrap();
        buffer.unpin();
        if !buffer.is_pinned() {
            self.num_available += 1;
        }
    }

    pub fn pin(&mut self, block: &BlockId) -> Result<Arc<Mutex<Buffer>>> {
        let now = SystemTime::now();
        let mut buffer = self.try_pin(block);
        while buffer.is_none() && !self.waiting_too_long(now) {
            std::thread::sleep(TIMEOUT);
            buffer = self.try_pin(block);
        }
        let Some(buffer) = buffer else {
            bail!("buffer pool is full");
        };
        Ok(buffer)
    }

    pub fn try_pin(&mut self, block: &BlockId) -> Option<Arc<Mutex<Buffer>>> {
        let buffer = self.find_existing_buffer(block);

        let buffer = match buffer {
            Some(buffer) => buffer,
            None => {
                let buffer = self.choose_unpinned_buffer()?;
                buffer.lock().unwrap().assign_to_block(block);
                buffer
            }
        };

        if !buffer.lock().unwrap().is_pinned() {
            self.num_available -= 1;
        }
        buffer.lock().unwrap().pin();

        Some(buffer)
    }

    pub fn waiting_too_long(&self, start_time: SystemTime) -> bool {
        SystemTime::now().duration_since(start_time).unwrap() > TIMEOUT
    }

    pub fn find_existing_buffer(&self, block: &BlockId) -> Option<Arc<Mutex<Buffer>>> {
        self.buffer_pool
            .iter()
            .find(|buffer| buffer.lock().unwrap().block() == Some(block))
            .cloned()
    }

    pub fn choose_unpinned_buffer(&mut self) -> Option<Arc<Mutex<Buffer>>> {
        self.buffer_pool
            .iter()
            .find(|buffer| !buffer.lock().unwrap().is_pinned())
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_can_pin() {
        let tempdir = tempfile::tempdir().unwrap();
        let file_manager = Arc::new(Mutex::new(FileManager::new(tempdir.path(), 32).unwrap()));
        let log_manager = Arc::new(Mutex::new(
            LogManager::new(file_manager.clone(), "log".to_string()).unwrap(),
        ));
        let mut buffer_manager = BufferManager::new(file_manager, log_manager, 3);
        assert_eq!(buffer_manager.num_available, 3);
        let block = BlockId::new("test".to_string(), 0);
        let buf = buffer_manager.pin(&block).unwrap();
        assert_eq!(buf.lock().unwrap().block(), Some(&block));
        assert_eq!(buffer_manager.num_available, 2);
    }

    #[test]
    fn should_cannot_pin_when_buffer_pool_is_full() {
        let tempdir = tempfile::tempdir().unwrap();
        let file_manager = Arc::new(Mutex::new(FileManager::new(tempdir.path(), 32).unwrap()));
        let log_manager = Arc::new(Mutex::new(
            LogManager::new(file_manager.clone(), "log".to_string()).unwrap(),
        ));
        let mut buffer_manager = BufferManager::new(file_manager, log_manager, 1);
        assert_eq!(buffer_manager.num_available, 1);
        let block = BlockId::new("test".to_string(), 0);
        let buf = buffer_manager.pin(&block).unwrap();
        assert_eq!(buf.lock().unwrap().block(), Some(&block));
        let block = BlockId::new("test".to_string(), 1);
        let buf = buffer_manager.pin(&block);
        assert!(buf.is_err());
    }

    #[test]
    fn should_can_pin_same_buffer_mulitple_times() {
        let tempdir = tempfile::tempdir().unwrap();
        let file_manager = Arc::new(Mutex::new(FileManager::new(tempdir.path(), 32).unwrap()));
        let log_manager = Arc::new(Mutex::new(
            LogManager::new(file_manager.clone(), "log".to_string()).unwrap(),
        ));
        let mut buffer_manager = BufferManager::new(file_manager, log_manager, 3);
        assert_eq!(buffer_manager.num_available, 3);
        let block = BlockId::new("test".to_string(), 0);
        let buf = buffer_manager.pin(&block).unwrap();
        assert_eq!(buf.lock().unwrap().block(), Some(&block));
        let buf = buffer_manager.pin(&block).unwrap();
        assert_eq!(buf.lock().unwrap().block(), Some(&block));
    }

    #[test]
    fn should_can_unpin() {
        let tempdir = tempfile::tempdir().unwrap();
        let file_manager = Arc::new(Mutex::new(FileManager::new(tempdir.path(), 32).unwrap()));
        let log_manager = Arc::new(Mutex::new(
            LogManager::new(file_manager.clone(), "log".to_string()).unwrap(),
        ));
        let mut buffer_manager = BufferManager::new(file_manager, log_manager, 3);
        assert_eq!(buffer_manager.num_available, 3);
        let block = BlockId::new("test".to_string(), 0);
        let buf = buffer_manager.pin(&block).unwrap();
        assert_eq!(buf.lock().unwrap().block(), Some(&block));
        assert_eq!(buffer_manager.num_available, 2);
        buffer_manager.unpin(buf);
        assert_eq!(buffer_manager.num_available, 3);
    }
}
