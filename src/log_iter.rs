use crate::{block::BlockId, file_manager::FileManager, page::Page};
use std::{
    mem::size_of,
    sync::{Arc, Mutex},
};

pub struct LogIterator {
    file_manager: Arc<Mutex<FileManager>>,
    block: BlockId,
    page: Page,
    current_pos: usize,
    boundary: usize,
}

impl LogIterator {
    pub fn new(file_manager: Arc<Mutex<FileManager>>, block: BlockId) -> Self {
        let block_size = file_manager.lock().unwrap().block_size;
        let page = Page::new(block_size);
        let mut iter = LogIterator {
            file_manager: file_manager.clone(),
            block: block.clone(),
            page,
            current_pos: 0,
            boundary: 0,
        };
        iter.move_to_block(block);

        iter
    }

    pub fn has_next(&self) -> bool {
        self.current_pos < self.file_manager.lock().unwrap().block_size as usize
            || self.block.num > 0
    }

    pub fn move_to_block(&mut self, block: BlockId) {
        self.file_manager
            .lock()
            .unwrap()
            .read(&block, &mut self.page)
            .unwrap();
        self.boundary = self.page.get_int(0) as usize;
        self.current_pos = self.boundary;
    }
}

impl Iterator for LogIterator {
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_next() {
            return None;
        }

        if self.current_pos == self.file_manager.lock().unwrap().block_size as usize {
            let block = BlockId::new(self.block.filename.clone(), self.block.num - 1);
            self.block = block.clone();
            self.move_to_block(block);
        }

        let record = self.page.get_bytes(self.current_pos);
        self.current_pos += record.len() + size_of::<i32>();
        Some(record)
    }
}
