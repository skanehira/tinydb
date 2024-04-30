use std::mem::size_of;

use crate::{block::BlockId, file_manager::FileManager, page::Page};

pub struct LogIterator<'a> {
    file_manager: &'a mut FileManager,
    block: BlockId,
    page: Page,
    current_pos: usize,
    boundary: usize,
}

impl<'a> LogIterator<'a> {
    pub fn new(file_manager: &'a mut FileManager, block: BlockId) -> LogIterator<'a> {
        let page = Page::new(file_manager.block_size);
        let mut iter = LogIterator {
            file_manager,
            block: block.clone(),
            page,
            current_pos: 0,
            boundary: 0,
        };
        iter.move_to_block(block);

        iter
    }

    pub fn has_next(&self) -> bool {
        self.current_pos < self.file_manager.block_size as usize || self.block.num > 0
    }

    pub fn move_to_block(&mut self, block: BlockId) {
        self.file_manager.read(&block, &mut self.page).unwrap();
        self.boundary = self.page.get_int(0) as usize;
        self.current_pos = self.boundary;
    }
}

impl<'a> Iterator for LogIterator<'a> {
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_next() {
            return None;
        }

        if self.current_pos == self.file_manager.block_size as usize {
            let block = BlockId::new(self.block.filename.clone(), self.block.num - 1);
            self.block = block.clone();
            self.move_to_block(block);
        }

        let record = self.page.get_bytes(self.current_pos);
        self.current_pos += record.len() + size_of::<i32>();
        Some(record)
    }
}
