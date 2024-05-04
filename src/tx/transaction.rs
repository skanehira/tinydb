use anyhow::{bail, Result};
use std::sync::{
    atomic::{AtomicI32, Ordering},
    Arc, Mutex,
};

use crate::{
    buffer::buffer_manager::BufferManager,
    file::{block::BlockId, file_manager::FileManager},
};

use super::{
    buffer_list::BufferList, concurrency::concurrency_manager::ConcurrencyManager,
    recovery::recovery_manager::RecoveryManager,
};

static NEXT_TX_NUM: AtomicI32 = AtomicI32::new(0);

pub struct Transaction {
    recovery_manager: Arc<Mutex<RecoveryManager>>,
    concurrency_manager: Arc<Mutex<ConcurrencyManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
    file_manager: Arc<Mutex<FileManager>>,
    tx_num: i32,
    buffer_list: Arc<Mutex<BufferList>>,
}

impl Transaction {
    pub fn new(
        recovery_manager: Arc<Mutex<RecoveryManager>>,
        concurrency_manager: Arc<Mutex<ConcurrencyManager>>,
        buffer_manager: Arc<Mutex<BufferManager>>,
        file_manager: Arc<Mutex<FileManager>>,
    ) -> Self {
        let tx_num = NEXT_TX_NUM.fetch_add(1, Ordering::SeqCst);
        let buffer_list = Arc::new(Mutex::new(BufferList::new(buffer_manager.clone())));
        Self {
            recovery_manager,
            concurrency_manager,
            buffer_manager,
            file_manager,
            tx_num,
            buffer_list,
        }
    }

    pub fn commit(&mut self) -> Result<()> {
        self.recovery_manager.lock().unwrap().commit()?;
        println!("transaction {} committed", self.tx_num);
        self.concurrency_manager.lock().unwrap().release();
        self.buffer_list.lock().unwrap().unpin_all();
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<()> {
        self.recovery_manager.lock().unwrap().rollback()?;
        println!("transaction {} rolled back", self.tx_num);
        self.concurrency_manager.lock().unwrap().release();
        self.buffer_list.lock().unwrap().unpin_all();
        Ok(())
    }

    pub fn recover(&mut self) -> Result<()> {
        self.buffer_manager.lock().unwrap().flush_all(self.tx_num);
        self.recovery_manager.lock().unwrap().recover()?;
        Ok(())
    }

    pub fn pin(&mut self, block: &BlockId) {
        self.buffer_list.lock().unwrap().pin(block).unwrap();
    }

    pub fn unpin(&mut self, block: &BlockId) {
        self.buffer_list.lock().unwrap().unpin(block).unwrap();
    }

    pub fn get_int(&self, block: &BlockId, offset: i32) -> i32 {
        let buffers = self.buffer_list.lock().unwrap();
        let buffer = buffers.get_buffer(block).unwrap();
        let mut buffer = buffer.lock().unwrap();
        buffer.contents_mut().get_int(offset as usize)
    }

    pub fn get_string(&self, block: &BlockId, offset: i32) -> String {
        let buffers = self.buffer_list.lock().unwrap();
        let buffer = buffers.get_buffer(block).unwrap();
        let mut buffer = buffer.lock().unwrap();
        buffer.contents_mut().get_string(offset as usize).unwrap()
    }

    pub fn set_int(
        &mut self,
        block: &BlockId,
        offset: i32,
        value: i32,
        ok_to_log: bool,
    ) -> Result<()> {
        let mut concurrency_manager = self.concurrency_manager.lock().unwrap();
        concurrency_manager.x_lock(block)?;

        let buffer_list = self.buffer_list.lock().unwrap();
        let Some(buffer) = buffer_list.get_buffer(block) else {
            bail!("buffer not found");
        };

        let mut buffer = buffer.lock().unwrap();
        let mut lsn = -1;
        if ok_to_log {
            lsn = self
                .recovery_manager
                .lock()
                .unwrap()
                .set_int(&mut buffer, offset)?;
        }
        let page = buffer.contents_mut();
        page.set_int(offset as usize, value);
        buffer.set_modified(self.tx_num, lsn);
        Ok(())
    }

    pub fn set_string(
        &mut self,
        block: &BlockId,
        offset: i32,
        value: String,
        ok_to_log: bool,
    ) -> Result<()> {
        let mut concurrency_manager = self.concurrency_manager.lock().unwrap();
        concurrency_manager.x_lock(block).unwrap();

        let buffer_list = self.buffer_list.lock().unwrap();
        let Some(buffer) = buffer_list.get_buffer(block) else {
            bail!("buffer not found");
        };

        let mut buffer = buffer.lock().unwrap();
        let mut lsn = -1;
        if ok_to_log {
            lsn = self
                .recovery_manager
                .lock()
                .unwrap()
                .set_string(&mut buffer, offset)
                .unwrap();
        }
        let page = buffer.contents_mut();
        page.set_string(offset as usize, &value);
        buffer.set_modified(self.tx_num, lsn);
        Ok(())
    }

    pub fn size(&mut self, filename: String) -> Result<u64> {
        let dummy_block = BlockId::new(filename.clone(), -1);
        self.concurrency_manager
            .lock()
            .unwrap()
            .s_lock(&dummy_block)?;
        let mut file_manager = self.file_manager.lock().unwrap();
        file_manager.block_count(&filename)
    }

    pub fn append(&mut self, filename: String) -> Result<BlockId> {
        let dummy_block = BlockId::new(filename.clone(), -1);
        self.concurrency_manager
            .lock()
            .unwrap()
            .x_lock(&dummy_block)?;
        let mut file_manager = self.file_manager.lock().unwrap();
        file_manager.append_block(&filename)
    }

    pub fn block_size(&self) -> i32 {
        self.file_manager.lock().unwrap().block_size
    }

    pub fn available_buffers(&self) -> u64 {
        self.buffer_manager.lock().unwrap().num_available
    }
}
