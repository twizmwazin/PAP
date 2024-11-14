use anyhow::Result;
use clap::Parser;
use futures::{future, prelude::*};
use pap_api::PapApi;
use pap_server::{server::PipelineServer, step::builtin_executors};
use sqlx::sqlite::SqlitePoolOptions;
use std::net::SocketAddr;
use tarpc::{server::Channel, tokio_serde::formats::Json};
use tokio::spawn;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Config {
    /// Address to bind the server to
    #[arg(short, long, default_value = "127.0.0.1:9090")]
    bind_addr: String,

    /// Path to SQLite database file
    #[arg(short, long, default_value = "sqlite::memory:")]
    database: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let config = Config::parse();

    // Initialize logging
    env_logger::init();

    // Initialize the step executor registry
    let registry = builtin_executors();

    // Create SQLite connection pool with default settings
    let pool = SqlitePoolOptions::new()
        .connect(&format!("sqlite:{}", config.database))
        .await?;

    // Create server instance
    let server = PipelineServer::new(pool, registry).await?;

    // Set up transport
    let addr: SocketAddr = config.bind_addr.parse()?;
    let listener = tarpc::serde_transport::tcp::listen(addr, Json::default).await?;

    // Start serving
    listener
        .filter_map(|r| future::ready(r.ok()))
        .map(tarpc::server::BaseChannel::with_defaults)
        .map(|channel| {
            channel.execute(server.clone().serve()).for_each(|x| async {
                spawn(x);
            })
        })
        .buffer_unordered(10)
        .for_each(|_| async {})
        .await;

    println!("Server listening on {}", addr);

    // Keep the main thread running
    tokio::signal::ctrl_c().await?;
    println!("Shutting down server...");
    Ok(())
}
