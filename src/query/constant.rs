use std::{
    fmt::Display,
    hash::{DefaultHasher, Hash, Hasher},
};

#[derive(Debug, Clone, PartialEq, Eq)]
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

impl Display for Constant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Constant::Int(i) => write!(f, "{}", i),
            Constant::String(s) => write!(f, "{}", s),
        }
    }
}
