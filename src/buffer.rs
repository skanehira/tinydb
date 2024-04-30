pub struct Buffer {
    buffer: Vec<u8>,
    pos: usize,
}

impl From<Vec<u8>> for Buffer {
    fn from(value: Vec<u8>) -> Self {
        Buffer {
            buffer: value,
            pos: 0,
        }
    }
}

impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        &self.buffer[self.pos..]
    }
}

impl AsMut<[u8]> for Buffer {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.buffer[self.pos..]
    }
}

impl Buffer {
    pub fn new(size: usize) -> Buffer {
        Buffer {
            buffer: vec![0; size],
            pos: 0,
        }
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn set_pos(&mut self, pos: usize) {
        self.pos = pos;
    }

    pub fn put(&mut self, buf: &[u8]) {
        self.buffer[self.pos..self.pos + buf.len()].copy_from_slice(buf);
        self.pos += buf.len();
    }

    pub fn put_int(&mut self, offset: Option<usize>, value: i32) {
        if let Some(offset) = offset {
            self.buffer[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
        } else {
            self.buffer[self.pos..self.pos + 4].copy_from_slice(&value.to_be_bytes());
            self.pos += 4;
        }
    }

    pub fn get_int(&mut self, offset: Option<usize>) -> i32 {
        let mut bytes = [0; 4];
        if let Some(offset) = offset {
            bytes.copy_from_slice(self.buffer[offset..offset + 4].as_ref());
        } else {
            bytes.copy_from_slice(self.buffer[self.pos..self.pos + 4].as_ref());
            self.pos += 4;
        }
        i32::from_be_bytes(bytes)
    }

    pub fn get(&mut self, buf: &mut [u8]) {
        buf.copy_from_slice(&self.buffer[self.pos..self.pos + buf.len()]);
        self.pos += buf.len();
    }

    pub fn get_bytes(&mut self, offset: usize) -> &[u8] {
        self.pos = offset;
        let size = self.get_int(None) as usize;
        let start = offset + 4;
        let end = start + size;
        self.pos = end;
        &self.buffer[start..end]
    }

    pub fn set_bytes(&mut self, offset: usize, buf: &[u8]) {
        self.put_int(Some(offset), buf.len() as i32);
        self.pos = offset + 4;
        self.put(buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_can_new_buffer() {
        let buffer = Buffer::new(10);
        assert_eq!(buffer.buffer.len(), 10);
        assert_eq!(buffer.pos, 0);
    }

    #[test]
    fn should_can_put_buffer() {
        let mut buffer = Buffer::new(10);
        buffer.put(&[1, 2, 3]);
        assert_eq!(buffer.buffer, vec![1, 2, 3, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(buffer.pos, 3);
    }

    #[test]
    fn should_can_put_int_buffer_without_offset() {
        let mut buffer = Buffer::new(10);
        buffer.put_int(None, 1);
        assert_eq!(buffer.buffer, vec![0, 0, 0, 1, 0, 0, 0, 0, 0, 0]);
        assert_eq!(buffer.pos, 4);
    }

    #[test]
    fn should_can_put_int_buffer_with_offset() {
        let mut buffer = Buffer::new(10);
        buffer.put_int(Some(4), 1);
        assert_eq!(buffer.buffer, vec![0, 0, 0, 0, 0, 0, 0, 1, 0, 0]);
        assert_eq!(buffer.pos, 0);
    }

    #[test]
    fn should_can_get_int_buffer_without_offset() {
        let mut buffer = Buffer::new(10);
        buffer.put_int(None, 1);
        assert_eq!(buffer.get_int(Some(0)), 1);
        assert_eq!(buffer.pos, 4);
    }

    #[test]
    fn should_can_get_int_buffer_with_offset() {
        let mut buffer = Buffer::new(10);
        buffer.put_int(Some(4), 1);
        assert_eq!(buffer.get_int(Some(4)), 1);
        assert_eq!(buffer.pos, 0)
    }

    #[test]
    fn should_can_get_buffer() {
        let mut buffer = Buffer::new(10);
        buffer.put(&[1, 2, 3]);
        let mut buf = vec![0; 3];
        buffer.pos = 0;
        buffer.get(&mut buf);
        assert_eq!(buf, vec![1, 2, 3]);
        assert_eq!(buffer.pos, 3)
    }

    #[test]
    fn should_can_get_bytes_buffer() {
        let mut buffer = Buffer::new(10);
        buffer.put_int(None, 3);
        buffer.put(&[1, 2, 3]);
        assert_eq!(buffer.get_bytes(0), vec![1, 2, 3]);
        assert_eq!(buffer.pos, 7)
    }

    #[test]
    fn should_can_set_bytes_buffer() {
        let mut buffer = Buffer::new(10);
        let buf = vec![1, 2, 3];
        buffer.set_bytes(2, &buf);
        assert_eq!(buffer.buffer, vec![0, 0, 0, 0, 0, 3, 1, 2, 3, 0]);
        assert_eq!(buffer.pos, 9)
    }
}
