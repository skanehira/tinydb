use std::mem::size_of;

pub mod buffer;
pub mod file;
pub mod log;
pub mod record;
pub mod server;
pub mod tx;
pub mod query;

const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
const I32_SIZE: usize = size_of::<i32>();

static LOG_FILE: &str = "tinydb.log";
