pub mod consts;
pub mod ports;
pub mod types;
pub mod variants;

pub mod assembler;
pub mod control;
pub mod data_kind;
pub mod dispatch;
pub mod featurepack;
pub mod jobs;
pub mod reports;
pub mod search_json;
pub mod sources;
pub mod trainconfig;

pub mod server;

#[cfg(any(test, feature = "test-util"))]
pub mod test_support;

#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("store: {0}")]
    Store(String),
    #[error("storage: {0}")]
    Storage(String),
    #[error("internal: {0}")]
    Internal(String),
}

pub type CoreResult<T> = Result<T, CoreError>;

pub fn is_online(last_seen: i64, now: i64, window: i64) -> bool {
    now - last_seen < window
}
