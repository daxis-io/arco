//! # arco-cli
//!
//! Command-line interface for Arco orchestration.
//!
//! ## Commands
//!
//! - `arco deploy` - Deploy an asset manifest
//! - `arco run` - Trigger a materialization run
//! - `arco status` - Check run status
//! - `arco logs` - View task logs
//! - `arco backfill-run-key` - Backfill `run_key` fingerprints
//!
//! ## Configuration
//!
//! The CLI uses environment variables or command-line flags for settings:
//!
//! - `ARCO_API_URL` - API endpoint (default: `http://localhost:8080`)
//! - `ARCO_WORKSPACE_ID` - Workspace ID
//! - `ARCO_API_TOKEN` - API authentication token

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(rust_2018_idioms)]
#![warn(clippy::pedantic)]
// CLI uses print! macros intentionally
#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]

pub mod client;
pub mod commands;

use clap::{Parser, Subcommand};

/// Arco CLI - Orchestration command-line interface.
#[derive(Debug, Parser)]
#[command(name = "arco")]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// API server URL.
    #[arg(long, env = "ARCO_API_URL", default_value = "http://localhost:8080")]
    pub api_url: String,

    /// Workspace ID.
    #[arg(long, env = "ARCO_WORKSPACE_ID")]
    pub workspace_id: Option<String>,

    /// API authentication token.
    #[arg(long, env = "ARCO_API_TOKEN")]
    pub api_token: Option<String>,

    /// Output format.
    #[arg(long, default_value = "text")]
    pub format: OutputFormat,

    /// Subcommand to execute.
    #[command(subcommand)]
    pub command: Commands,
}

impl Cli {
    /// Get the effective configuration.
    #[must_use]
    pub fn config(&self) -> Config {
        Config {
            api_url: self.api_url.clone(),
            workspace_id: self.workspace_id.clone(),
            api_token: self.api_token.clone(),
            format: self.format.clone(),
        }
    }
}

/// CLI subcommands.
#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Deploy an asset manifest.
    Deploy(commands::deploy::DeployArgs),
    /// Trigger a materialization run.
    Run(commands::run::RunArgs),
    /// Check run status.
    Status(commands::status::StatusArgs),
    /// View task logs.
    Logs(commands::logs::LogsArgs),
    /// Backfill missing `run_key` fingerprints.
    BackfillRunKey(commands::run_key_backfill::RunKeyBackfillArgs),
}

/// Output format.
#[derive(Debug, Clone, Default, clap::ValueEnum)]
pub enum OutputFormat {
    /// Human-readable text output.
    #[default]
    Text,
    /// JSON output.
    Json,
    /// Table output.
    Table,
}

/// CLI configuration.
#[derive(Debug, Clone, Default)]
pub struct Config {
    /// API server URL.
    pub api_url: String,
    /// Workspace ID.
    pub workspace_id: Option<String>,
    /// API authentication token.
    pub api_token: Option<String>,
    /// Output format.
    pub format: OutputFormat,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_config_from_flags() {
        let cli = Cli::parse_from([
            "arco",
            "--api-url",
            "https://api.example.com",
            "--workspace-id",
            "workspace-123",
            "--api-token",
            "token-abc",
            "--format",
            "json",
            "status",
            "--list",
        ]);

        let config = cli.config();
        assert_eq!(config.api_url, "https://api.example.com");
        assert_eq!(config.workspace_id.as_deref(), Some("workspace-123"));
        assert_eq!(config.api_token.as_deref(), Some("token-abc"));
        assert!(matches!(config.format, OutputFormat::Json));
    }
}
