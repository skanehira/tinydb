use std::sync::{Arc, Mutex};

use crate::{
    file::{block::BlockId, file_manager::FileManager, page::Page},
    log::log_manager::LogManager,
};

#[derive(Default)]
pub struct Buffer {
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    contents: Page,         // buffer contents
    block: Option<BlockId>, // block to which this buffer is assigned
    pins: i32,              // number of times this buffer has been pinned
    txnum: i32,             // transaction number, if not -1, then this buffer is modified?
    lsn: i32,               // log sequence number
}

impl Buffer {
    pub fn new(file_manager: Arc<Mutex<FileManager>>, log_manager: Arc<Mutex<LogManager>>) -> Self {
        let contents = Page::new(file_manager.lock().unwrap().block_size);
        Self {
            file_manager,
            log_manager,
            contents,
            txnum: -1,
            lsn: -1,
            ..Default::default()
        }
    }

    pub fn contents_mut(&mut self) -> &mut Page {
        &mut self.contents
    }

    pub fn block(&self) -> Option<&BlockId> {
        self.block.as_ref()
    }

    pub fn set_modified(&mut self, txnum: i32, lsn: i32) {
        self.txnum = txnum;
        if lsn >= 0 {
            self.lsn = lsn;
        }
    }

    pub fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    pub fn modifying_tx(&self) -> i32 {
        self.txnum
    }

    pub fn assign_to_block(&mut self, block: &BlockId) {
        self.flush();
        self.block = Some(block.clone());
        self.file_manager
            .lock()
            .unwrap()
            .read(block, &mut self.contents)
            .unwrap();
        self.pins = 0;
    }

    pub fn flush(&mut self) {
        if self.txnum >= 0 {
            self.log_manager.lock().unwrap().flush(self.lsn).unwrap();
            self.file_manager
                .lock()
                .unwrap()
                .write(self.block.as_ref().unwrap(), &mut self.contents)
                .unwrap();
            self.txnum = -1;
        }
    }

    pub fn pin(&mut self) {
        self.pins += 1;
    }

    pub fn unpin(&mut self) {
        self.pins -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_can_new_buffer() {
        let tempdir = tempfile::tempdir().unwrap();
        let file_manager = Arc::new(Mutex::new(FileManager::new(tempdir.path(), 32).unwrap()));
        let log_manager = Arc::new(Mutex::new(
            LogManager::new(file_manager.clone(), "log".to_string()).unwrap(),
        ));
        let mut buffer = Buffer::new(file_manager, log_manager);
        assert_eq!(buffer.contents.contents().len(), 32);
        assert_eq!(buffer.block(), None);
        assert!(!buffer.is_pinned());
    }

    #[test]
    fn should_can_assign_to_block() {
        let tempdir = tempfile::tempdir().unwrap();
        let file_manager = Arc::new(Mutex::new(FileManager::new(tempdir.path(), 32).unwrap()));
        let log_manager = Arc::new(Mutex::new(
            LogManager::new(file_manager.clone(), "log".to_string()).unwrap(),
        ));
        let mut buffer = Buffer::new(file_manager.clone(), log_manager.clone());
        let block = BlockId::new("test".to_string(), 0);
        buffer.assign_to_block(&block);

        buffer.contents_mut().set_string(0, "hello");
        buffer.set_modified(0, 1);
        buffer.flush();

        let mut new_buffer = Buffer::new(file_manager, log_manager);
        new_buffer.assign_to_block(&block);
        assert_eq!(new_buffer.contents.get_string(0).unwrap(), "hello");
    }
}
