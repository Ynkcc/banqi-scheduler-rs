pub mod consts;
pub mod ports;
pub mod types;
pub mod variants;

pub mod assembler;
pub mod balance;
pub mod control;
pub mod dispatch;
pub mod evalcoord;
pub mod evalopponents;
pub mod featurepack;
pub mod gatekeeper;
pub mod jobs;
pub mod reanalysis;
pub mod reports;
pub mod sources;
pub mod trainconfig;

pub mod server;

#[cfg(test)]
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
