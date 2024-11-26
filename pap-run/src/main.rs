use std::fs::File;
use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use futures_util::stream::StreamExt;
use pap_api::{load_config, Config, Context, ExecutionStatus, PapApi, PapApiClient};
use pap_server::{server::PipelineServer, step::builtin_executors};
use sqlx::SqlitePool;
use tarpc::{client, context, server::Channel};

#[tokio::main]
async fn main() -> Result<()> {
    let file = "../sample.yaml";

    // Load config and create context
    let config_file = File::open(file).expect("Could not open file");
    let config: Config = load_config(config_file).expect("Failed to parse config");
    let config_dir = Path::new(file)
        .parent()
        .ok_or_else(|| anyhow::anyhow!("Config file has no parent directory"))?;
    let context = Context::build_with_config(config, config_dir.to_path_buf())?;

    // Setup server with database URL from environment or fallback to in-memory
    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite::memory:".to_string());

    let db = SqlitePool::connect(&database_url).await?;
    let service = PipelineServer::new(db, builtin_executors()).await?;

    // Create channel-based transport
    let (client_transport, server_transport) = tarpc::transport::channel::unbounded();

    // Spawn server
    let server = tarpc::server::BaseChannel::with_defaults(server_transport);
    tokio::spawn(
        server
            .execute(service.serve())
            // Handle all requests concurrently.
            .for_each(|response| async move {
                tokio::spawn(response);
            }),
    );

    // Create client
    let client = PapApiClient::new(client::Config::default(), client_transport).spawn();

    // Submit pipeline
    let pipeline_id = client
        .submit_pipeline(context::current(), context)
        .await??;

    // Wait for pipeline completion
    loop {
        let pipeline = client
            .get_pipeline(context::current(), pipeline_id)
            .await??;

        match pipeline.status {
            ExecutionStatus::Completed | ExecutionStatus::Failed | ExecutionStatus::Cancelled => {
                break
            }
            _ => {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        }
    }

    // Print execution results
    println!("\nPipeline {} execution results:", pipeline_id);
    let pipeline = client
        .get_pipeline(context::current(), pipeline_id)
        .await??;

    println!("\nPipeline {}: {:?}", pipeline_id, pipeline.status);
    if let Some(error) = pipeline.error {
        println!("\nPipeline Error:\n{}", error);
    }

    for job_id in pipeline.jobs {
        let job = client.get_job(context::current(), job_id).await??;
        println!("\nJob {} ({}): {:?}", job_id, job.config.name, job.status);

        for step in job.steps {
            println!(
                "\n  Step {} ({}): {:?}",
                step.id, step.config.name, step.status
            );
            if let Some(output) = step.output {
                println!("  Output:\n    {}", String::from_utf8_lossy(&output));
            }
        }
    }

    Ok(())
}
