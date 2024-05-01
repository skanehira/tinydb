use anyhow::Result;
use std::{
    io::{Cursor, Read, Write},
    mem::size_of,
};

const I32_SIZE: usize = size_of::<i32>();

#[derive(Default)]
pub struct Page {
    buffer: Cursor<Vec<u8>>,
}

impl From<Vec<u8>> for Page {
    fn from(value: Vec<u8>) -> Self {
        Self {
            buffer: Cursor::new(value),
        }
    }
}

impl Page {
    pub fn new(block_size: u64) -> Page {
        Page {
            buffer: Cursor::new(vec![0; block_size as usize]),
        }
    }

    pub fn get_int(&mut self, offset: usize) -> i32 {
        self.buffer.set_position(offset as u64);
        let mut bytes = [0; I32_SIZE];
        self.buffer.read_exact(&mut bytes).unwrap();
        i32::from_le_bytes(bytes)
    }

    pub fn set_int(&mut self, offset: usize, value: i32) {
        self.buffer.set_position(offset as u64);
        self.buffer.write_all(&value.to_le_bytes()).unwrap();
    }

    pub fn get_bytes(&mut self, offset: usize) -> Vec<u8> {
        let length = self.get_int(offset) as usize;
        let mut bytes = vec![0; length];
        self.buffer.read_exact(&mut bytes).unwrap();
        bytes
    }

    pub fn set_bytes(&mut self, offset: usize, bytes: &[u8]) {
        self.buffer.set_position(offset as u64);
        let length = bytes.len() as i32;
        self.set_int(offset, length);
        self.buffer.write_all(bytes).unwrap();
    }

    pub fn get_string(&mut self, offset: usize) -> Result<String> {
        let bytes = self.get_bytes(offset);
        Ok(String::from_utf8(bytes.to_vec())?)
    }

    pub fn set_string(&mut self, offset: usize, value: &str) {
        self.set_bytes(offset, value.as_bytes());
    }

    pub fn max_length(str_len: usize) -> usize {
        size_of::<u32>() + (str_len * size_of::<u8>())
    }

    pub fn contents(&mut self) -> &[u8] {
        self.buffer.set_position(0);
        self.buffer.get_ref()
    }

    pub fn contents_mut(&mut self) -> &mut [u8] {
        self.buffer.set_position(0);
        self.buffer.get_mut()
    }

    pub fn read_bytes(&mut self, offset: usize, len: usize) -> Result<Vec<u8>> {
        self.buffer.set_position(offset as u64);
        let mut bytes = vec![0; len];
        self.buffer.read_exact(&mut bytes)?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_can_new_page() {
        let mut page = Page::new(10);
        assert_eq!(page.contents().len(), 10);
    }

    #[test]
    fn should_can_set_and_get_string() {
        let mut page = Page::new(12);
        page.set_string(2, "hello");
        assert_eq!(page.get_string(2).unwrap(), "hello");
    }

    #[test]
    fn should_can_get_contents() {
        let mut page = Page::new(10);
        page.set_string(0, "hello");
        assert_eq!(page.contents(), &[5, 0, 0, 0, 104, 101, 108, 108, 111, 0]);
        assert_eq!(page.buffer.position(), 0);
    }
}
