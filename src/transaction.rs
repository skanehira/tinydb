use crate::file::block::BlockId;

pub struct Transaction {
    //next_tx_num: i32,
    //end_of_file: i32,
}

impl Transaction {
    pub fn pin(&mut self, block: &BlockId) {
        todo!();
    }

    pub fn set_string(&mut self, block: &BlockId, offset: i32, value: String, ok_to_log: bool) {
        todo!();
    }

    pub fn unpin(&mut self, block: &BlockId) {
        todo!();
    }
}
