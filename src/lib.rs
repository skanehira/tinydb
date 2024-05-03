use std::mem::size_of;

pub mod buffer;
pub mod file;
pub mod log;
pub mod recovery_manager;
pub mod server;
pub mod transaction;
pub mod tx;

const I32_SIZE: usize = size_of::<i32>();
