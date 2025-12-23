//! Logs command - view task logs.

use anyhow::{Context, Result};
use clap::Args;

use crate::Config;

/// Arguments for the logs command.
#[derive(Debug, Args)]
pub struct LogsArgs {
    /// Run ID to view logs for.
    #[arg()]
    pub run_id: String,

    /// Task key to filter logs (optional).
    #[arg(long, short = 't')]
    pub task: Option<String>,

    /// Follow logs in real-time.
    #[arg(long, short = 'f')]
    pub follow: bool,

    /// Number of lines to show (tail).
    #[arg(long, short = 'n', default_value = "100")]
    pub lines: u32,
}

/// Execute the logs command.
///
/// # Errors
///
/// Returns an error if the workspace ID is missing.
pub fn execute(args: &LogsArgs, config: &Config) -> Result<()> {
    let _workspace_id = config
        .workspace_id
        .as_ref()
        .context("Workspace ID is required. Set ARCO_WORKSPACE_ID or use --workspace-id")?;

    // For M1, logs are limited - we don't have log streaming infrastructure yet
    // This provides a placeholder implementation

    println!("Run: {}", args.run_id);
    if let Some(task) = &args.task {
        println!("Task: {task}");
    }
    println!();
    println!("Log streaming is not yet available in M1.");
    println!();
    println!("To view logs, check the Cloud Run or Cloud Tasks console for your tasks.");
    println!("Task logs will be available via the API in a future release.");

    if args.follow {
        println!();
        println!("Note: --follow flag will be supported when log streaming is implemented.");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logs_args_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(flatten)]
            args: LogsArgs,
        }

        let cli = TestCli::parse_from(["test", "run-123", "--task", "task-456", "-f"]);
        assert_eq!(cli.args.run_id, "run-123");
        assert_eq!(cli.args.task, Some("task-456".to_string()));
        assert!(cli.args.follow);
    }
}
