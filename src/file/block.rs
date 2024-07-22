use std::hash::{DefaultHasher, Hasher as _};
use uuid::Uuid;

#[derive(Default, Debug, Clone, Eq)]
pub struct BlockId {
    pub id: String,
    pub filename: String,
    pub num: i32,
}

impl BlockId {
    pub fn new(filename: String, num: i32) -> BlockId {
        BlockId {
            id: Uuid::new_v4().to_string(),
            filename,
            num,
        }
    }

    pub fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write(self.to_string().as_bytes());
        hasher.finish()
    }
}

impl std::cmp::PartialEq for BlockId {
    fn eq(&self, other: &Self) -> bool {
        self.filename == other.filename && self.num == other.num
    }
}

impl std::fmt::Display for BlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[file {}, block {}]", self.filename, self.num)
    }
}

impl std::hash::Hash for BlockId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.filename.hash(state);
        self.num.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_can_new_blockid() {
        let block_id = BlockId::new("file1".to_string(), 1);
        assert_eq!(block_id.to_string(), "[file file1, block 1]");
        assert_eq!(block_id.hash(), 13928275507101178956);
    }

    #[test]
    fn should_caan_compare_blockid() {
        let block_id1 = BlockId::new("file1".to_string(), 1);
        let block_id2 = BlockId::new("file1".to_string(), 1);
        assert_eq!(block_id1, block_id2);
    }
}
