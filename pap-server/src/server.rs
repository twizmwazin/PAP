use std::{collections::HashMap, sync::Arc};
use tokio::{sync::Mutex, task::JoinHandle};

use anyhow::{bail, Result};
use pap_api::{ExecutionStatus, JobStatus, PapApi, PapError, PipelineStatus, StepStatus};
use sqlx::{Pool, Sqlite};
use tarpc::context::Context;

use crate::{queries, step::StepContext, step::StepExecutorRegistry, storage::SqlStorage};

#[derive(Clone)]
pub struct PipelineServer {
    db: Pool<Sqlite>,
    registry: Arc<StepExecutorRegistry>,
    handles: Arc<Mutex<HashMap<u32, JoinHandle<()>>>>,
}

impl PipelineServer {
    pub async fn new(pool: Pool<Sqlite>, registry: StepExecutorRegistry) -> Result<Self> {
        // Ensure tables are created
        queries::init_tables(&pool).await?;

        Ok(Self {
            db: pool,
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
        .fetch_one(&self.db)
        .await?;

        let mut job_ids = Vec::new();
        for job in &context.config.jobs {
            let job_id = sqlx::query_scalar::<_, u32>(
                "INSERT INTO jobs (pipeline_id, name) VALUES (?, ?) RETURNING id",
            )
            .bind(pipeline_id)
            .bind(serde_json::to_string(&job)?)
            .fetch_one(&self.db)
            .await?;
            job_ids.push(job_id);

            for step in &job.steps {
                sqlx::query_scalar::<_, u32>(
                    "INSERT INTO steps (job_id, name, call, args) VALUES (?, ?, ?, ?) RETURNING id",
                )
                .bind(job_id)
                .bind(&step.name)
                .bind(&step.call)
                .bind(serde_json::to_string(&step.args)?)
                .fetch_one(&self.db)
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

    async fn execute_step(&self, step: &StepStatus, storage: &SqlStorage) -> Result<()> {
        let executor = self
            .registry
            .get(&step.config.call)
            .ok_or_else(|| anyhow::anyhow!("step executor not found: {}", step.config.call))?;

        let mut context = StepContext::new(&step.config.args, storage);

        let result = executor.execute(&mut context);

        // Store the log regardless of execution result
        queries::set_step_log(&self.db, step.id, &context.take_log()).await?;

        result
    }

    async fn execute(&self, pipeline: &PipelineStatus) -> Result<()> {
        let storage = SqlStorage::new(self.db.clone());

        queries::set_pipeline_status(&self.db, pipeline.id, ExecutionStatus::Running).await?;

        for job_id in &pipeline.jobs {
            // Check if pipeline was cancelled
            let pipeline_status = queries::get_pipeline_status(&self.db, pipeline.id).await?;
            if pipeline_status.status == ExecutionStatus::Cancelled {
                return Ok(());
            }

            let job_status = queries::get_job_status(&self.db, *job_id).await?;
            queries::set_job_status(&self.db, *job_id, ExecutionStatus::Running).await?;

            for step in &job_status.steps {
                // Check if job was cancelled
                let current_job = queries::get_job_status(&self.db, *job_id).await?;
                if current_job.status == ExecutionStatus::Cancelled {
                    break;
                }

                queries::set_step_status(&self.db, step.id, ExecutionStatus::Running).await?;

                match self.execute_step(step, &storage).await {
                    Ok(_) => {
                        queries::set_step_status(&self.db, step.id, ExecutionStatus::Completed)
                            .await?;
                    }
                    Err(e) => {
                        queries::set_step_status(&self.db, step.id, ExecutionStatus::Failed)
                            .await?;
                        queries::set_job_status(&self.db, *job_id, ExecutionStatus::Failed).await?;
                        queries::set_pipeline_status(
                            &self.db,
                            pipeline.id,
                            ExecutionStatus::Failed,
                        )
                        .await?;
                        return Err(e);
                    }
                }
            }

            // If we got here and weren't cancelled, the job succeeded
            if queries::get_job_status(&self.db, *job_id).await?.status
                != ExecutionStatus::Cancelled
            {
                queries::set_job_status(&self.db, *job_id, ExecutionStatus::Completed).await?;
            }
        }

        // If we got here and weren't cancelled, the pipeline succeeded
        if queries::get_pipeline_status(&self.db, pipeline.id)
            .await?
            .status
            != ExecutionStatus::Cancelled
        {
            queries::set_pipeline_status(&self.db, pipeline.id, ExecutionStatus::Completed).await?;
        }

        Ok(())
    }

    pub async fn execute_blocking(&self, pipeline: &PipelineStatus) {
        if let Err(e) = self.execute(pipeline).await {
            if let Err(store_err) =
                queries::store_error(&self.db, pipeline.id, &e.to_string()).await
            {
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
        let status = queries::setup_pipeline(&self.db, &pipeline_context).await?;
        self.execute_background(&status).await;
        Ok(status.id)
    }

    async fn get_pipeline(self, _: Context, id: u32) -> Result<PipelineStatus, PapError> {
        Ok(queries::get_pipeline_status(&self.db, id).await?)
    }

    async fn get_pipelines(self, _: Context) -> Result<Vec<u32>, PapError> {
        Ok(sqlx::query_scalar("SELECT id FROM pipelines")
            .fetch_all(&self.db)
            .await?)
    }

    async fn cancel_pipeline(self, _: Context, id: u32) -> Result<(), PapError> {
        queries::cancel_pipeline(&self.db, id).await?;
        Ok(())
    }

    async fn delete_pipeline(self, _: Context, id: u32) -> Result<(), PapError> {
        queries::delete_pipeline(&self.db, id).await?;
        Ok(())
    }

    async fn get_job(self, _: Context, id: u32) -> Result<JobStatus, PapError> {
        Ok(queries::get_job_status(&self.db, id).await?)
    }

    async fn get_jobs(self, _: Context) -> Result<Vec<u32>, PapError> {
        Ok(sqlx::query_scalar("SELECT id FROM jobs")
            .fetch_all(&self.db)
            .await?)
    }

    async fn cancel_job(self, _: Context, id: u32) -> Result<(), PapError> {
        sqlx::query("UPDATE jobs SET status = 'Cancelled' WHERE id = ?")
            .bind(id)
            .execute(&self.db)
            .await?;
        Ok(())
    }

    async fn get_step_log(self, _: Context, id: u32) -> Result<Vec<u8>, PapError> {
        sqlx::query_scalar::<_, Vec<u8>>("SELECT log_data FROM steps WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.db)
            .await?
            .ok_or_else(|| PapError::NotFound(format!("Step log for {}", id)))
    }

    async fn get_object(
        self,
        _: Context,
        namespace: String,
        key: Vec<u8>,
    ) -> Result<Vec<u8>, PapError> {
        queries::get_object(&self.db, &namespace, &key).await
    }

    async fn put_object(
        self,
        _: Context,
        namespace: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), PapError> {
        queries::put_object(&self.db, &namespace, &key, &value)
            .await
            .map_err(Into::into)
    }
}
