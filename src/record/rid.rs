#[derive(PartialEq, Eq)]
pub struct RID {
    pub block_num: i32,
    pub slot: i32,
}

impl RID {
    pub fn new(block_num: i32, slot: i32) -> Self {
        Self { block_num, slot }
    }
}

impl std::fmt::Display for RID {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[block {}, slot {}]", self.block_num, self.slot)
    }
}
