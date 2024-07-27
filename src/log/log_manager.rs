use anyhow::Result;
use std::sync::{Arc, Mutex};

use crate::file::{block::BlockId, file_manager::FileManager, page::Page};

use super::log_iter::LogIterator;

/// LogManager is responsible for managing the log records
/// in the log file. The log file is a sequence of blocks
/// where each block contains a sequence of log records.
///
/// ```text
///                         block                                     blocks
/// ┏━━━━━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┻━━━━━━━━━━━┓
/// ┌────┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐
/// │ 14 │ 0 │ 0 │ 0 │ 6 │ 0 │ 0 │ 0 │ h │ e │ l │ l │ o │ 0 │...│...│...│...│...│...│
/// └────┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘
/// ┗━━━━━━━┳━━━━━━━━┻━━━━━━━┳━━━━━━━┻━━━━━━━━━━━┳━━━━━━━━━━━┛
///  record boundary    record size        record data
///                  ┗━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┛
///                                    record
/// ```
#[derive(Debug, Default)]
pub struct LogManager {
    file_manager: Arc<Mutex<FileManager>>,
    log_file: String,
    log_page: Page,
    current_block: BlockId,
    // lsn is log sequence number, a unique identifier for each log record
    latest_lsn: i32,
    last_saved_lsn: i32,
}

impl LogManager {
    pub fn new(file_manager: Arc<Mutex<FileManager>>, log_file: String) -> Result<Self> {
        let mut fm = file_manager.lock().unwrap();
        let mut log_page = Page::new(fm.block_size);
        let block_count = fm.block_count(&log_file)?;
        // if block_count is 0, means that the log file is empty
        let current_block = if block_count == 0 {
            Self::append_new_block(&mut fm, &mut log_page, &log_file)?
        } else {
            // if block_count is not 0, read the last block of the log file
            let block = BlockId::new(log_file.clone(), block_count as i32 - 1);

            fm.read(&block, &mut log_page)?;
            block
        };

        Ok(Self {
            file_manager: file_manager.clone(),
            log_file: log_file.clone(),
            log_page,
            current_block,
            latest_lsn: 0,
            last_saved_lsn: 0,
        })
    }

    pub fn iter(&mut self) -> LogIterator {
        self.inner_flush().unwrap();
        LogIterator::new(self.file_manager.clone(), self.current_block.clone())
    }

    // appends a new log record to the log page or flush the log page if the log record does not fit
    pub fn append(&mut self, record: &[u8]) -> Result<i32> {
        // boundary is the position of the last log record in the log page
        let mut boundary = self.log_page.get_int(0);
        // record_size is the size of the log record
        let record_size = record.len() as i32;
        // bytes_needed is the size of the log record plus 4 bytes for the boundary
        // record size on the first 4 bytes of the block
        let bytes_needed = record_size + 4;
        // if the log record does not fit in the current block, flush the log page
        if boundary - bytes_needed < 4 {
            self.inner_flush()?;
            self.current_block = Self::append_new_block(
                &mut self.file_manager.lock().unwrap(),
                &mut self.log_page,
                &self.log_file,
            )?;
            boundary = self.log_page.get_int(0);
        }
        // record_pos is the position of the log record in the log page
        let record_pos = boundary - bytes_needed;
        // set the log record in the log page
        self.log_page.set_bytes(record_pos as usize, record);
        // set the boundary in the log page
        self.log_page.set_int(0, record_pos);
        self.latest_lsn += 1;
        Ok(self.latest_lsn)
    }

    pub fn flush(&mut self, lsn: i32) -> Result<()> {
        // if lsn >= last_saved_lsn, means that the log record is not saved yet
        if lsn >= self.last_saved_lsn {
            self.inner_flush()?;
        }
        Ok(())
    }

    // inner_flush saves the log record to the log file
    fn inner_flush(&mut self) -> Result<()> {
        self.file_manager
            .lock()
            .unwrap()
            .write(&self.current_block, &mut self.log_page)?;
        self.last_saved_lsn = self.latest_lsn;
        Ok(())
    }

    pub fn append_new_block(
        file_manager: &mut FileManager,
        log_page: &mut Page,
        log_file: &str,
    ) -> Result<BlockId> {
        let block_id = file_manager.append_block(log_file)?;
        log_page.set_int(0, file_manager.block_size);
        // why write the log page to the new block?
        file_manager.write(&block_id, log_page)?;
        Ok(block_id)
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use super::*;

    #[test]
    fn should_can_new_log_manager() {
        let tempdir = tempfile::tempdir().unwrap();
        let file_manager = Arc::new(Mutex::new(FileManager::new(tempdir.path(), 32).unwrap()));
        let mut log_manager = LogManager::new(file_manager, "log".to_string()).unwrap();
        assert_eq!(
            log_manager.current_block,
            BlockId::new("log".to_string(), 0)
        );
        assert_eq!(log_manager.log_page.get_int(0), 32);
        assert_eq!(log_manager.log_page.contents().len(), 32);
    }

    #[test]
    fn should_can_append_record() {
        let tempdir = tempfile::tempdir().unwrap();
        let block_size = 32;
        let record = b"hello";
        let boundary = block_size - record.len() as i32 - 4;
        let file_manager = Arc::new(Mutex::new(
            FileManager::new(tempdir.path(), block_size).unwrap(),
        ));
        let mut log_manager = LogManager::new(file_manager, "log".to_string()).unwrap();
        let lsn = log_manager.append(record).unwrap();
        assert_eq!(lsn, 1);
        assert_eq!(log_manager.latest_lsn, 1);
        assert_eq!(log_manager.log_page.get_int(0), boundary);
        let contents = log_manager.log_page.contents();
        assert_eq!(
            contents[boundary as usize..],
            [5, 0, 0, 0, b'h', b'e', b'l', b'l', b'o']
        );
    }

    #[test]
    fn should_can_append_multiple_record() {
        let tempdir = tempfile::tempdir().unwrap();
        let block_size = 32;
        let record = b"hello";
        let record2 = b"world";
        let file_manager = Arc::new(Mutex::new(
            FileManager::new(tempdir.path(), block_size).unwrap(),
        ));
        let mut log_manager = LogManager::new(file_manager, "log".to_string()).unwrap();
        let lsn = log_manager.append(record).unwrap();
        assert_eq!(lsn, 1);
        assert_eq!(log_manager.latest_lsn, 1);
        let lsn = log_manager.append(record2).unwrap();
        assert_eq!(lsn, 2);
        assert_eq!(log_manager.latest_lsn, 2);
        let contents = log_manager.log_page.contents();
        let boundary = block_size as usize - record.len() - record2.len() - 8;
        assert_eq!(
            contents[boundary..],
            [5, 0, 0, 0, b'w', b'o', b'r', b'l', b'd', 5, 0, 0, 0, b'h', b'e', b'l', b'l', b'o']
        );
    }

    #[test]
    fn should_can_flush_record() {
        let tempdir = tempfile::tempdir().unwrap();
        let block_size = 20;
        let record = b"hello";
        let file_manager = Arc::new(Mutex::new(
            FileManager::new(tempdir.path(), block_size).unwrap(),
        ));
        let mut log_manager = LogManager::new(file_manager, "log".to_string()).unwrap();
        log_manager.append(record).unwrap();
        let data = std::fs::read(tempdir.path().join("log")).unwrap();
        assert_eq!(
            data,
            [20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
        log_manager.inner_flush().unwrap();
        let data = std::fs::read(tempdir.path().join("log")).unwrap();
        let boundary = block_size as usize - record.len() - size_of::<i32>();
        assert_eq!(
            data.get(boundary..).unwrap(),
            [5, 0, 0, 0, b'h', b'e', b'l', b'l', b'o']
        );
    }

    #[test]
    fn should_can_iter_record() {
        let tempdir = tempfile::tempdir().unwrap();
        let block_size = 32;
        let record = b"hello";
        let record2 = b"world";
        let file_manager = Arc::new(Mutex::new(
            FileManager::new(tempdir.path(), block_size).unwrap(),
        ));
        let mut log_manager = LogManager::new(file_manager, "log".to_string()).unwrap();
        log_manager.append(record).unwrap();
        log_manager.append(record2).unwrap();
        let mut iter = log_manager.iter();
        let record = iter.next().unwrap();
        assert_eq!(record, b"world");
        let record = iter.next().unwrap();
        assert_eq!(record, b"hello");
        assert_eq!(iter.next(), None);
    }

    // FIXME: this should passed?
    //#[test]
    //fn should_can_iter_records_in_multiple_block() {
    //    let tempdir = tempfile::tempdir().unwrap();
    //    let block_size = 9;
    //    let mut file_manager = FileManager::new(tempdir.path(), block_size).unwrap();
    //    let block1 = file_manager.append_block("log").unwrap();
    //    let block2 = file_manager.append_block("log").unwrap();

    //    let mut page = Page::new(block_size);
    //    page.set_string(0, "hello");
    //    file_manager.write(&block1, &mut page).unwrap();

    //    let mut page = Page::new(block_size);
    //    page.set_string(0, "world");
    //    file_manager.write(&block2, &mut page).unwrap();

    //    let mut iter = LogIterator::new(&mut file_manager, block2);
    //    assert_eq!(iter.next().unwrap(), b"world");
    //    assert_eq!(iter.next().unwrap(), b"hello");
    //    assert_eq!(iter.next(), None);
    //}
}
