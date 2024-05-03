use std::mem::size_of;

pub mod buffer;
pub mod file;
pub mod log;
pub mod server;
pub mod tx;

const I32_SIZE: usize = size_of::<i32>();
