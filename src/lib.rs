use std::mem::size_of;

pub mod block;
pub mod buffer;
pub mod buffer_manager;
pub mod file_manager;
pub mod log_iter;
pub mod log_manager;
pub mod log_record;
pub mod log_record_set_string;
pub mod page;
pub mod recovery_manager;
pub mod server;
pub mod transaction;

const I32_SIZE: usize = size_of::<i32>();
