use crate::buffer::Buffer;
use anyhow::Result;

pub struct Page {
    buffer: Buffer,
}

impl Page {
    pub fn new(block_size: u64) -> Page {
        Page {
            buffer: Buffer::new(block_size as usize),
        }
    }

    pub fn get_int(&mut self, offset: usize) -> i32 {
        self.buffer.get_int(Some(offset))
    }

    pub fn set_int(&mut self, offset: usize, value: i32) {
        self.buffer.put_int(Some(offset), value);
    }

    pub fn get_bytes(&mut self, offset: usize) -> &[u8] {
        self.buffer.get_bytes(offset)
    }

    pub fn set_bytes(&mut self, offset: usize, value: &[u8]) {
        self.buffer.set_bytes(offset, value);
    }

    pub fn get_string(&mut self, offset: usize) -> Result<String> {
        let bytes = self.get_bytes(offset);
        Ok(String::from_utf8(bytes.to_vec())?)
    }

    pub fn set_string(&mut self, offset: usize, value: &str) {
        self.set_bytes(offset, value.as_bytes());
    }

    pub fn max_length(&mut self, str_len: usize) -> usize {
        4 + str_len
    }

    pub fn contents(&mut self) -> &[u8] {
        self.buffer.set_pos(0);
        self.buffer.as_ref()
    }

    pub fn contents_mut(&mut self) -> &mut [u8] {
        self.buffer.set_pos(0);
        self.buffer.as_mut()
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
        assert_eq!(page.contents(), &[0, 0, 0, 5, 104, 101, 108, 108, 111, 0]);
        assert_eq!(page.buffer.pos(), 0);
    }
}
