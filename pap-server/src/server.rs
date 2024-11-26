use std::{collections::HashMap, sync::Arc};
use tokio::task;
use tokio::{sync::Mutex, task::JoinHandle};

use anyhow::{bail, Result};
use pap_api::{ExecutionStatus, JobStatus, PapApi, PapError, PipelineStatus, StepStatus};
use sqlx::{Pool, Sqlite};
use tarpc::context::Context;

use crate::db::{init_pool, with_pool};
use crate::{queries, step::StepContext, step::StepExecutorRegistry};

#[derive(Clone)]
pub struct PipelineServer {
    registry: Arc<StepExecutorRegistry>,
    handles: Arc<Mutex<HashMap<u32, JoinHandle<()>>>>,
}

impl PipelineServer {
    pub async fn new(pool: Pool<Sqlite>, registry: StepExecutorRegistry) -> Result<Self> {
        // Initialize the thread-local pool
        init_pool(pool)?;

        // Ensure tables are created
        queries::init_tables().await?;

        Ok(Self {
            registry: Arc::new(registry),
            handles: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        })
    }

    pub fn validate(&self, context: &pap_api::Context) -> Result<()> {
        for job in &context.config.jobs {
            for step in &job.steps {
                if self.registry.get(&step.call).is_none() {
                    bail!("step executor not found: {}", step.call);
                }
            }
        }
        // TODO: ensure context has all expected fields
        Ok(())
    }

    pub async fn setup_pipeline(&self, context: &pap_api::Context) -> Result<PipelineStatus> {
        let pipeline_id = sqlx::query_scalar::<_, u32>(
            "INSERT INTO pipelines (config, context) VALUES (?, ?) RETURNING id",
        )
        .bind(serde_json::to_string(&context.config)?)
        .bind(serde_json::to_vec(&context)?)
        .fetch_one(&with_pool()?)
        .await?;

        let mut job_ids = Vec::new();
        for job in &context.config.jobs {
            let job_id = sqlx::query_scalar::<_, u32>(
                "INSERT INTO jobs (pipeline_id, name) VALUES (?, ?) RETURNING id",
            )
            .bind(pipeline_id)
            .bind(serde_json::to_string(&job)?)
            .fetch_one(&with_pool()?)
            .await?;
            job_ids.push(job_id);

            for step in &job.steps {
                sqlx::query_scalar::<_, u32>(
                    "INSERT INTO steps (job_id, name, call, args, io) VALUES (?, ?, ?, ?, ?) RETURNING id",
                )
                .bind(job_id)
                .bind(&step.name)
                .bind(&step.call)
                .bind(serde_json::to_string(&step.args)?)
                .bind(serde_json::to_string(&step.io)?)  // Add IO configuration
                .fetch_one(&with_pool()?)
                .await?;
            }
        }

        Ok(PipelineStatus {
            id: pipeline_id,
            config: context.config.clone(),
            jobs: job_ids,
            status: ExecutionStatus::Running,
            error: None,
        })
    }

    async fn execute_step(&self, step: &StepStatus, pipeline: &PipelineStatus) -> Result<()> {
        let executor = self
            .registry
            .get(&step.config.call)
            .ok_or_else(|| anyhow::anyhow!("step executor not found: {}", step.config.call))?;

        // Get context data from database
        let context: pap_api::Context =
            sqlx::query_scalar::<_, Vec<u8>>("SELECT context FROM pipelines WHERE id = ?")
                .bind(pipeline.id)
                .fetch_one(&with_pool()?)
                .await
                .map(|data| serde_json::from_slice(&data))??;

        let mut context = StepContext::new(step, pipeline, &context);

        let result = task::block_in_place(|| executor.execute(&mut context));

        // Store the log regardless of execution result
        queries::set_step_log(step.id, &context.get_log()).await?;

        result
    }

    async fn execute(&self, pipeline: &PipelineStatus) -> Result<()> {
        queries::set_pipeline_status(pipeline.id, ExecutionStatus::Running).await?;

        for job_id in &pipeline.jobs {
            // Check if pipeline was cancelled
            let pipeline_status = queries::get_pipeline_status(pipeline.id).await?;
            if pipeline_status.status == ExecutionStatus::Cancelled {
                return Ok(());
            }

            let job_status = queries::get_job_status(*job_id).await?;
            queries::set_job_status(*job_id, ExecutionStatus::Running).await?;

            for step in &job_status.steps {
                // Check if job was cancelled
                let current_job = queries::get_job_status(*job_id).await?;
                if current_job.status == ExecutionStatus::Cancelled {
                    break;
                }

                queries::set_step_status(step.id, ExecutionStatus::Running).await?;

                match self.execute_step(step, pipeline).await {
                    Ok(_) => {
                        queries::set_step_status(step.id, ExecutionStatus::Completed).await?;
                    }
                    Err(e) => {
                        queries::set_step_status(step.id, ExecutionStatus::Failed).await?;
                        queries::set_job_status(*job_id, ExecutionStatus::Failed).await?;
                        queries::set_pipeline_status(pipeline.id, ExecutionStatus::Failed).await?;
                        return Err(e);
                    }
                }
            }

            // If we got here and weren't cancelled, the job succeeded
            if queries::get_job_status(*job_id).await?.status != ExecutionStatus::Cancelled {
                queries::set_job_status(*job_id, ExecutionStatus::Completed).await?;
            }
        }

        // If we got here and weren't cancelled, the pipeline succeeded
        if queries::get_pipeline_status(pipeline.id).await?.status != ExecutionStatus::Cancelled {
            queries::set_pipeline_status(pipeline.id, ExecutionStatus::Completed).await?;
        }

        Ok(())
    }

    pub async fn execute_blocking(&self, pipeline: &PipelineStatus) {
        if let Err(e) = self.execute(pipeline).await {
            if let Err(store_err) = queries::store_error(pipeline.id, &e.to_string()).await {
                eprintln!("Failed to store error: {}", store_err);
            }
        }
    }

    pub async fn execute_background(&self, pipeline: &PipelineStatus) {
        let server = self.clone();
        let move_pipeline = pipeline.clone();
        let handle = tokio::spawn(async move {
            server.execute_blocking(&move_pipeline).await;
        });
        self.handles.lock().await.insert(pipeline.id, handle);
    }
}

impl PapApi for PipelineServer {
    async fn submit_pipeline(
        self,
        _: Context,
        pipeline_context: pap_api::Context,
    ) -> Result<u32, PapError> {
        self.validate(&pipeline_context)?;
        let status = queries::setup_pipeline(&pipeline_context).await?;
        self.execute_background(&status).await;
        Ok(status.id)
    }

    async fn get_pipeline(self, _: Context, id: u32) -> Result<PipelineStatus, PapError> {
        Ok(queries::get_pipeline_status(id).await?)
    }

    async fn get_pipelines(self, _: Context) -> Result<Vec<u32>, PapError> {
        Ok(sqlx::query_scalar("SELECT id FROM pipelines")
            .fetch_all(&with_pool()?)
            .await?)
    }

    async fn cancel_pipeline(self, _: Context, id: u32) -> Result<(), PapError> {
        queries::cancel_pipeline(id).await?;
        Ok(())
    }

    async fn delete_pipeline(self, _: Context, id: u32) -> Result<(), PapError> {
        queries::delete_pipeline(id).await?;
        Ok(())
    }

    async fn get_job(self, _: Context, id: u32) -> Result<JobStatus, PapError> {
        Ok(queries::get_job_status(id).await?)
    }

    async fn get_jobs(self, _: Context) -> Result<Vec<u32>, PapError> {
        Ok(sqlx::query_scalar("SELECT id FROM jobs")
            .fetch_all(&with_pool()?)
            .await?)
    }

    async fn cancel_job(self, _: Context, id: u32) -> Result<(), PapError> {
        queries::cancel_job(id).await?;
        Ok(())
    }

    async fn get_step_log(self, _: Context, id: u32) -> Result<Vec<u8>, PapError> {
        sqlx::query_scalar::<_, Vec<u8>>("SELECT log_data FROM steps WHERE id = ?")
            .bind(id)
            .fetch_optional(&with_pool()?)
            .await?
            .ok_or_else(|| PapError::NotFound(format!("Step log for {}", id)))
    }

    async fn get_object(
        self,
        _: Context,
        namespace: String,
        key: Vec<u8>,
    ) -> Result<Vec<u8>, PapError> {
        queries::get_object(&namespace, &key).await
    }

    async fn put_object(
        self,
        _: Context,
        namespace: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), PapError> {
        queries::put_object(&namespace, &key, &value)
            .await
            .map_err(Into::into)
    }
}
