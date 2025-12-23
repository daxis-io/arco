//! Arco CLI - Command-line interface for orchestration.
//!
//! The main entry point for the `arco` CLI binary.

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use arco_cli::{Cli, Commands};

fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    // Parse CLI arguments
    let cli = Cli::parse();
    let config = cli.config();

    // Create runtime and execute
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async {
        match cli.command {
            Commands::Deploy(args) => arco_cli::commands::deploy::execute(args, &config).await,
            Commands::Run(args) => arco_cli::commands::run::execute(args, &config).await,
            Commands::Status(args) => arco_cli::commands::status::execute(args, &config).await,
            Commands::Logs(args) => arco_cli::commands::logs::execute(&args, &config),
            Commands::BackfillRunKey(args) => {
                arco_cli::commands::run_key_backfill::execute(args, &config).await
            }
        }
    })
}
