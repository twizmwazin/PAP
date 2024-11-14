pub(crate) mod queries;
pub mod server;
pub mod step;

use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum ExecutorError {
    #[error("Executor does not have command: {0}")]
    CommandNotFound(String),
}
