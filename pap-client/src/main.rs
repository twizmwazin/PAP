use colored::*;
use std::env;
use std::io::{stdout, Write};
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use pap_api::{load_config, Context};
use pap_api::{ExecutionStatus, PapApiClient};
use tarpc::{client, context, tokio_serde::formats::Json};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Host address for PapApi server (default: 127.0.0.1:9090)
    /// Can also be set using PAP_HOST environment variable
    #[arg(short = 'H', long)]
    host: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Pipeline management commands
    Pipeline {
        #[command(subcommand)]
        command: PipelineCommands,
    },
    /// Job management commands
    Job {
        #[command(subcommand)]
        command: JobCommands,
    },
    /// Log access commands
    Log {
        #[command(subcommand)]
        command: LogCommands,
    },
    /// Object storage commands
    Object {
        #[command(subcommand)]
        command: ObjectCommands,
    },
}

#[derive(Subcommand)]
enum PipelineCommands {
    /// Submit a new pipeline
    Submit {
        /// Path to the pipeline configuration file
        config: PathBuf,
    },
    /// Get pipeline information
    Get {
        /// Pipeline ID
        id: u32,
    },
    /// List all pipelines
    List,
    /// Cancel a pipeline
    Cancel {
        /// Pipeline ID
        id: u32,
    },
    /// Delete a pipeline
    Delete {
        /// Pipeline ID
        id: u32,
    },
    /// Show detailed summary of a pipeline
    Summary {
        /// Pipeline ID
        id: u32,
    },
}

#[derive(Subcommand)]
enum JobCommands {
    /// Get job information
    Get {
        /// Job ID
        id: u32,
    },
    /// List all jobs
    List,
    /// Cancel a job
    Cancel {
        /// Job ID
        id: u32,
    },
}

#[derive(Subcommand)]
enum LogCommands {
    /// Get log output for a step
    Get {
        /// Step ID
        id: u32,
    },
}

#[derive(Subcommand)]
enum ObjectCommands {
    /// Get an object
    Get {
        /// Object namespace
        namespace: String,
        /// Object key
        key: String,
    },
    /// Put an object
    Put {
        /// Object namespace
        namespace: String,
        /// Object key
        key: String,
        /// Path to file containing object data
        #[arg(short, long)]
        file: PathBuf,
    },
}

async fn handle_pipeline_command(
    command: PipelineCommands,
    client: &PapApiClient,
) -> anyhow::Result<()> {
    match command {
        PipelineCommands::Submit { config } => {
            let base_path = config
                .parent()
                .ok_or_else(|| anyhow::anyhow!("Config file must have a parent directory"))?
                .to_path_buf();

            let config_file = File::open(&config).await?;
            let config = load_config(config_file.into_std().await)?;
            let context = Context::build_with_config(config, base_path)?;
            let id = client
                .submit_pipeline(context::current(), context)
                .await??;
            println!("Submitted pipeline with ID: {}", id);
        }
        PipelineCommands::Get { id } => {
            let info = client.get_pipeline(context::current(), id).await?;
            println!("{:#?}", info);
        }
        PipelineCommands::List => {
            let pipelines = client.get_pipelines(context::current()).await?;
            println!("Pipelines: {:?}", pipelines);
        }
        PipelineCommands::Cancel { id } => {
            client.cancel_pipeline(context::current(), id).await??;
            println!("Cancelled pipeline {}", id);
        }
        PipelineCommands::Delete { id } => {
            client.delete_pipeline(context::current(), id).await??;
            println!("Deleted pipeline {}", id);
        }
        PipelineCommands::Summary { id } => {
            print_summary(client, id).await?;
        }
    }
    Ok(())
}

async fn handle_job_command(command: JobCommands, client: &PapApiClient) -> anyhow::Result<()> {
    match command {
        JobCommands::Get { id } => {
            let job = client.get_job(context::current(), id).await??;
            println!("Job {} ({}):", job.id, job.config.name);
            println!("Status: {:?}", job.status);
            println!("Current step: {:?}", job.current_step);
            println!("\nSteps:");
            for step in job.steps {
                println!("  - {} ({}): {:?}", step.id, step.config.name, step.status);
            }
        }
        JobCommands::List => {
            let jobs = client.get_jobs(context::current()).await?;
            println!("Jobs: {:?}", jobs);
        }
        JobCommands::Cancel { id } => {
            client.cancel_job(context::current(), id).await??;
            println!("Cancelled job {}", id);
        }
    }
    Ok(())
}

async fn handle_log_command(command: LogCommands, client: &PapApiClient) -> anyhow::Result<()> {
    match command {
        LogCommands::Get { id } => {
            let log = client.get_step_log(context::current(), id).await??;
            std::io::stdout().write_all(&log)?;
        }
    }
    Ok(())
}

async fn handle_object_command(
    command: ObjectCommands,
    client: &PapApiClient,
) -> anyhow::Result<()> {
    match command {
        ObjectCommands::Get { namespace, key } => {
            let data = client
                .get_object(context::current(), namespace, key.into_bytes())
                .await??;
            std::io::stdout().write_all(&data)?;
        }
        ObjectCommands::Put {
            namespace,
            key,
            file,
        } => {
            let mut file = File::open(file).await?;
            let mut data = Vec::new();
            file.read_to_end(&mut data).await?;
            client
                .put_object(context::current(), namespace, key.into_bytes(), data)
                .await??;
            println!("Object stored successfully");
        }
    }
    Ok(())
}

async fn print_summary(client: &PapApiClient, pipeline_id: u32) -> anyhow::Result<()> {
    let pipeline = client
        .get_pipeline(context::current(), pipeline_id)
        .await??;

    println!(
        "\nPipeline {} ({})",
        pipeline_id,
        pipeline.status.to_string().color(match pipeline.status {
            ExecutionStatus::Completed => "green",
            ExecutionStatus::Failed => "red",
            ExecutionStatus::Cancelled => "yellow",
            _ => "blue",
        })
    );

    for job_id in pipeline.jobs {
        let job = client.get_job(context::current(), job_id).await??;
        println!(
            "\n  Job {} - {} ({})",
            job_id,
            job.config.name,
            job.status.to_string().color(match job.status {
                ExecutionStatus::Completed => "green",
                ExecutionStatus::Failed => "red",
                ExecutionStatus::Cancelled => "yellow",
                _ => "blue",
            })
        );

        for step in job.steps {
            println!(
                "\n    Step {} - {} ({})",
                step.id,
                step.config.name,
                step.status.to_string().color(match step.status {
                    ExecutionStatus::Completed => "green",
                    ExecutionStatus::Failed => "red",
                    ExecutionStatus::Cancelled => "yellow",
                    _ => "blue",
                })
            );

            // If there's log output, display it indented
            if let Ok(Ok(log)) = client.get_step_log(context::current(), step.id).await {
                if !log.is_empty() {
                    println!("\n      Log output:");
                    for line in String::from_utf8_lossy(&log).lines() {
                        println!("        {}", line);
                    }
                }
            }
        }
    }

    // Print pipeline error if present
    if let Some(error) = pipeline.error {
        println!("\n  {}", "Pipeline Error:".red());
        println!("    {}", error);
    }

    stdout().flush()?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let host = cli
        .host
        .or_else(|| env::var("PAP_HOST").ok())
        .unwrap_or_else(|| "127.0.0.1:9090".to_string());

    let transport = tarpc::serde_transport::tcp::connect(host, Json::default).await?;

    let client = PapApiClient::new(client::Config::default(), transport).spawn();

    match cli.command {
        Commands::Pipeline { command } => handle_pipeline_command(command, &client).await?,
        Commands::Job { command } => handle_job_command(command, &client).await?,
        Commands::Log { command } => handle_log_command(command, &client).await?,
        Commands::Object { command } => handle_object_command(command, &client).await?,
    }

    Ok(())
}
