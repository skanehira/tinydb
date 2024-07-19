use std::hash::{DefaultHasher, Hash, Hasher};

#[derive(Clone, PartialEq, Eq)]
pub enum Constant {
    Int(i32),
    String(String),
}

impl Constant {
    pub fn hash_code(&self) -> u64 {
        let mut state = DefaultHasher::new();
        match self {
            Constant::Int(i) => i.hash(&mut state),
            Constant::String(s) => s.hash(&mut state),
        }
        state.finish()
    }
}
