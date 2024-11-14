use std::str::FromStr;

use pap_api::{ExecutionStatus, JobStatus, PapError, PipelineStatus, Step, StepStatus};
use sqlx::{Error, Row, SqlitePool};

pub(crate) async fn init_tables(pool: &SqlitePool) -> Result<(), Error> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS pipelines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            config TEXT,
            context BLOB,
            execution_status TEXT DEFAULT 'Pending'
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id INTEGER,
            name TEXT,
            status TEXT DEFAULT 'Pending',
            current_step INTEGER DEFAULT 0,
            FOREIGN KEY(pipeline_id) REFERENCES pipelines(id)
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS steps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            name TEXT,
            call TEXT,
            args TEXT,
            status TEXT DEFAULT 'Pending',
            log_data BLOB,
            FOREIGN KEY(job_id) REFERENCES jobs(id)
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS objects (
            namespace TEXT,
            key BLOB,
            value BLOB,
            PRIMARY KEY (namespace, key)
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS global_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id INTEGER,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            error_message TEXT,
            FOREIGN KEY(pipeline_id) REFERENCES pipelines(id)
        )
        "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}

pub(crate) async fn set_pipeline_status(
    db: &SqlitePool,
    pipeline_id: u32,
    status: ExecutionStatus,
) -> Result<(), Error> {
    sqlx::query(
        r#"
        UPDATE pipelines SET execution_status = ? WHERE id = ?
        "#,
    )
    .bind(status.to_string())
    .bind(pipeline_id)
    .execute(db)
    .await?;

    Ok(())
}

pub(crate) async fn set_job_status(
    db: &SqlitePool,
    job_id: u32,
    status: ExecutionStatus,
) -> Result<(), Error> {
    sqlx::query(
        r#"
        UPDATE jobs SET status = ? WHERE id = ?
        "#,
    )
    .bind(status.to_string())
    .bind(job_id)
    .execute(db)
    .await?;
    Ok(())
}

pub(crate) async fn set_step_status(
    db: &SqlitePool,
    step_id: u32,
    status: ExecutionStatus,
) -> Result<(), Error> {
    sqlx::query(
        r#"
        UPDATE steps SET status = ? WHERE id = ?
        "#,
    )
    .bind(status.to_string())
    .bind(step_id)
    .execute(db)
    .await?;
    Ok(())
}

pub(crate) async fn set_step_log(
    db: &SqlitePool,
    step_id: u32,
    log_data: &[u8],
) -> Result<(), Error> {
    sqlx::query(
        r#"
        UPDATE steps SET log_data = ? WHERE id = ?
        "#,
    )
    .bind(log_data)
    .bind(step_id)
    .execute(db)
    .await?;
    Ok(())
}

pub(crate) async fn store_error(
    db: &SqlitePool,
    pipeline_id: u32,
    error: &str,
) -> Result<(), Error> {
    let mut tx = db.begin().await?;

    sqlx::query(r#"UPDATE pipelines SET execution_status = ? WHERE id = ?"#)
        .bind(ExecutionStatus::Failed.to_string())
        .bind(pipeline_id)
        .execute(&mut *tx)
        .await?;

    sqlx::query(r#"INSERT INTO global_errors (pipeline_id, error_message) VALUES (?, ?)"#)
        .bind(pipeline_id)
        .bind(error)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;

    // This is here as a backup for now in case the transaction fails
    eprintln!("Error: {:?}", error);
    Ok(())
}

pub(crate) async fn get_pipeline_status(
    db: &SqlitePool,
    id: u32,
) -> anyhow::Result<PipelineStatus> {
    let pipeline = sqlx::query(
        r#"
            SELECT config, context, execution_status
            FROM pipelines
            WHERE id = ?
            "#,
    )
    .bind(id)
    .fetch_optional(db)
    .await?
    .ok_or_else(|| PapError::NotFound(format!("Pipeline {}", id)))?;

    let jobs = sqlx::query_scalar(
        r#"
            SELECT id
            FROM jobs
            WHERE pipeline_id = ?
            "#,
    )
    .bind(id)
    .fetch_all(db)
    .await?;

    Ok(PipelineStatus {
        id,
        config: serde_json::from_str(pipeline.get(0))?,
        jobs,
        status: ExecutionStatus::from_str(&pipeline.get::<String, _>(2))?,
        error: None,
    })
}

pub(crate) async fn get_job_status(db: &SqlitePool, id: u32) -> anyhow::Result<JobStatus> {
    let job = sqlx::query(
        r#"
            SELECT pipeline_id, name, status, current_step
            FROM jobs
            WHERE id = ?
            "#,
    )
    .bind(id)
    .fetch_optional(db)
    .await?
    .ok_or_else(|| PapError::NotFound(format!("Job {}", id)))?;

    let steps = sqlx::query(
        r#"
            SELECT id, name, call, args, status, log_data
            FROM steps
            WHERE job_id = ?
            ORDER BY id ASC
            "#,
    )
    .bind(id)
    .fetch_all(db)
    .await?;

    let step_statuses = steps
        .into_iter()
        .map(|step| {
            Ok(StepStatus {
                id: step.get(0),
                config: Step {
                    name: step.get(1),
                    call: step.get(2),
                    args: serde_json::from_str(step.get(3))?,
                    io: Default::default(),
                },
                status: ExecutionStatus::from_str(&step.get::<String, _>(4))?,
                output: step.get(5),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(JobStatus {
        id,
        config: serde_json::from_str(job.get(1))?,
        steps: step_statuses,
        status: ExecutionStatus::from_str(&job.get::<String, _>(2))?,
        current_step: job.get(3),
    })
}

#[allow(dead_code)]
pub(crate) async fn get_step_status(db: &SqlitePool, id: u32) -> anyhow::Result<StepStatus> {
    let step = sqlx::query(
        r#"
            SELECT job_id, name, call, args, status, log_data
            FROM steps
            WHERE id = ?
            "#,
    )
    .bind(id)
    .fetch_optional(db)
    .await?
    .ok_or_else(|| PapError::NotFound(format!("Step {}", id)))?;

    Ok(StepStatus {
        id,
        config: Step {
            name: step.get(1),
            call: step.get(2),
            args: serde_json::from_str(step.get(3))?,
            io: Default::default(),
        },
        status: ExecutionStatus::from_str(&step.get::<String, _>(4))?,
        output: step.get(5),
    })
}

pub(crate) async fn get_object(
    db: &SqlitePool,
    namespace: &str,
    key: &[u8],
) -> Result<Vec<u8>, PapError> {
    sqlx::query_scalar::<_, Vec<u8>>("SELECT value FROM objects WHERE namespace = ? AND key = ?")
        .bind(namespace)
        .bind(key)
        .fetch_optional(db)
        .await?
        .ok_or_else(|| {
            PapError::NotFound(format!(
                "Object in namespace {} with key {:?}",
                namespace, key
            ))
        })
}

pub(crate) async fn put_object(
    db: &SqlitePool,
    namespace: &str,
    key: &[u8],
    value: &[u8],
) -> Result<(), Error> {
    sqlx::query("INSERT OR REPLACE INTO objects (namespace, key, value) VALUES (?, ?, ?)")
        .bind(namespace)
        .bind(key)
        .bind(value)
        .execute(db)
        .await?;
    Ok(())
}

pub(crate) async fn setup_pipeline(
    db: &SqlitePool,
    context: &pap_api::Context,
) -> anyhow::Result<PipelineStatus> {
    let mut tx = db.begin().await?;

    let pipeline_id = sqlx::query_scalar::<_, u32>(
        "INSERT INTO pipelines (config, context) VALUES (?, ?) RETURNING id",
    )
    .bind(serde_json::to_string(&context.config)?)
    .bind(serde_json::to_vec(&context)?)
    .fetch_one(&mut *tx)
    .await?;

    let mut job_ids = Vec::new();
    for job in &context.config.jobs {
        let job_id = sqlx::query_scalar::<_, u32>(
            "INSERT INTO jobs (pipeline_id, name) VALUES (?, ?) RETURNING id",
        )
        .bind(pipeline_id)
        .bind(serde_json::to_string(&job)?)
        .fetch_one(&mut *tx)
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
            .fetch_one(&mut *tx)
            .await?;
        }
    }

    tx.commit().await?;

    Ok(PipelineStatus {
        id: pipeline_id,
        config: context.config.clone(),
        jobs: job_ids,
        status: ExecutionStatus::Running,
        error: None,
    })
}

pub(crate) async fn cancel_pipeline(db: &SqlitePool, id: u32) -> Result<(), Error> {
    let mut tx = db.begin().await?;

    sqlx::query("UPDATE pipelines SET execution_status = ? WHERE id = ?")
        .bind(ExecutionStatus::Cancelled.to_string())
        .bind(id)
        .execute(&mut *tx)
        .await?;

    sqlx::query("UPDATE jobs SET status = ? WHERE pipeline_id = ?")
        .bind(ExecutionStatus::Cancelled.to_string())
        .bind(id)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(())
}

pub(crate) async fn delete_pipeline(db: &SqlitePool, id: u32) -> Result<(), Error> {
    let mut tx = db.begin().await?;

    // Delete steps belonging to jobs in this pipeline
    sqlx::query(r#"DELETE FROM steps WHERE job_id IN (SELECT id FROM jobs WHERE pipeline_id = ?)"#)
        .bind(id)
        .execute(&mut *tx)
        .await?;

    // Delete jobs belonging to this pipeline
    sqlx::query("DELETE FROM jobs WHERE pipeline_id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await?;

    // Delete the pipeline itself
    sqlx::query("DELETE FROM pipelines WHERE id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(())
}
