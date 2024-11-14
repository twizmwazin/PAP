mod config;
mod context;
#[cfg(test)]
mod test;

pub use config::{load_config, Config, Job, LoaderConfig, MMIOEntry, Project, Step};
pub use context::Context;

use serde::{Deserialize, Serialize};
use strum::EnumString;
use thiserror::Error;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, EnumString, strum::Display)]
pub enum ExecutionStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineStatus {
    pub id: u32,
    pub config: Config,
    pub status: ExecutionStatus,
    pub jobs: Vec<u32>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobStatus {
    pub id: u32,
    pub config: Job,
    pub steps: Vec<StepStatus>,
    pub status: ExecutionStatus,
    pub current_step: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StepStatus {
    pub id: u32,
    pub config: Step,
    pub status: ExecutionStatus,
    pub output: Option<Vec<u8>>,
}

#[derive(Error, Debug, Serialize, Deserialize)]
pub enum PapError {
    #[error("Resource not found: {0}")]
    NotFound(String),
    #[error("Database error: {0}")]
    Database(String),
    #[error("Invalid configuration: {0}")]
    Configuration(String),
    #[error("Execution error: {0}")]
    Execution(String),
    #[error("Internal error: {0}")]
    Internal(String),
}

#[cfg(feature = "serde_json")]
impl From<serde_json::Error> for PapError {
    fn from(err: serde_json::Error) -> Self {
        PapError::Internal(err.to_string())
    }
}

#[cfg(feature = "sqlx")]
impl From<sqlx::Error> for PapError {
    fn from(err: sqlx::Error) -> Self {
        match err {
            sqlx::Error::RowNotFound => PapError::NotFound("Resource not found".to_string()),
            _ => PapError::Database(err.to_string()),
        }
    }
}

impl From<anyhow::Error> for PapError {
    fn from(err: anyhow::Error) -> Self {
        PapError::Internal(err.to_string())
    }
}

impl<T> From<std::sync::PoisonError<T>> for PapError {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        PapError::Internal(err.to_string())
    }
}

impl From<strum::ParseError> for PapError {
    fn from(err: strum::ParseError) -> Self {
        PapError::Internal(err.to_string())
    }
}

/// PapApi represents the public functionality of Program Analysis Pipelines.
/// Functionality is split into three categories: pipeline management, job
/// management, and object storage.
#[tarpc::service]
#[allow(async_fn_in_trait)]
pub trait PapApi {
    // Pipeline management

    /// Submits a new pipeline for execution.
    ///
    /// # Arguments
    /// * `pipeline_context` - The pipeline context containing configuration and execution details
    ///
    /// # Returns
    /// The unique ID of the submitted pipeline
    async fn submit_pipeline(pipeline_context: Context) -> Result<u32, PapError>;

    /// Retrieves information about a specific pipeline.
    ///
    /// # Arguments
    /// * `id` - The unique identifier of the pipeline
    ///
    /// # Returns
    /// Pipeline information if found, None otherwise
    async fn get_pipeline(id: u32) -> Result<PipelineStatus, PapError>;

    /// Retrieves a list of all pipeline IDs in the system.
    ///
    /// # Returns
    /// A vector containing IDs of all pipelines
    async fn get_pipelines() -> Result<Vec<u32>, PapError>;

    /// Cancels the execution of a running pipeline.
    ///
    /// # Arguments
    /// * `id` - The unique ID of the pipeline to cancel
    async fn cancel_pipeline(id: u32) -> Result<(), PapError>;

    /// Deletes a pipeline and its associated data from the system.
    ///
    /// # Arguments
    /// * `id` - The unique ID of the pipeline to delete
    async fn delete_pipeline(id: u32) -> Result<(), PapError>;

    // Job management
    /// Retrieves information about a specific job.
    ///
    /// # Arguments
    /// * `id` - The unique ID of the job
    ///
    /// # Returns
    /// Job information including name, status, and current step
    async fn get_job(id: u32) -> Result<JobStatus, PapError>;

    /// Retrieves the log output of a specific step.
    ///
    /// # Arguments
    /// * `id` - The unique identifier of the step
    ///
    /// # Returns
    /// The complete log output as a byte vector
    async fn get_step_log(id: u32) -> Result<Vec<u8>, PapError>;

    /// Retrieves a list of all job IDs in the system.
    ///
    /// # Returns
    /// A vector containing IDs of all jobs
    async fn get_jobs() -> Result<Vec<u32>, PapError>;

    /// Cancels the execution of a running job.
    ///
    /// # Arguments
    /// * `id` - The unique identifier of the job to cancel
    async fn cancel_job(id: u32) -> Result<(), PapError>;

    // Object storage
    /// Retrieves an object from the storage system.
    ///
    /// # Arguments
    /// * `namespace` - The namespace where the object is stored
    /// * `key` - The unique key identifying the object
    ///
    /// # Returns
    /// The object's data as a byte vector
    async fn get_object(namespace: String, key: Vec<u8>) -> Result<Vec<u8>, PapError>;

    /// Stores an object in the storage system.
    ///
    /// # Arguments
    /// * `namespace` - The namespace where to store the object
    /// * `key` - The unique key to identify the object
    /// * `value` - The object's data as a byte vector
    async fn put_object(namespace: String, key: Vec<u8>, value: Vec<u8>) -> Result<(), PapError>;
}
