#[derive(Clone)]
pub struct StatInfo {
    pub num_blocks: i32,
    pub num_records: i32,
}

impl StatInfo {
    pub fn new(num_blocks: i32, num_records: i32) -> Self {
        Self {
            num_blocks,
            num_records,
        }
    }

    pub fn distinct_values(&self, _field_name: String) -> i32 {
        1 + (self.num_records / 3)
    }
}
