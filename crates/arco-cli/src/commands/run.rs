//! Run command - trigger a materialization run.

use anyhow::{Context, Result};
use clap::Args;

use crate::client::{ApiClient, RerunMode, RerunRunRequest, TriggerRunRequest};
use crate::{Config, OutputFormat};

/// Arguments for the run command.
#[derive(Debug, Args)]
#[allow(clippy::struct_excessive_bools)]
pub struct RunArgs {
    /// Asset keys to materialize (comma-separated or multiple --asset flags).
    ///
    /// When used with `--rerun`, this becomes the subset selection roots.
    #[arg(long = "asset", short = 'a', value_delimiter = ',')]
    pub assets: Vec<String>,

    /// Parent run ID to rerun.
    #[arg(long)]
    pub rerun: Option<String>,

    /// Rerun all tasks that did not succeed in the parent run.
    #[arg(long)]
    pub from_failure: bool,

    /// Include upstream dependencies of the selection.
    #[arg(long)]
    pub include_upstream: bool,

    /// Include downstream dependents of the selection.
    #[arg(long)]
    pub include_downstream: bool,

    /// Idempotency key for deduplication.
    #[arg(long)]
    pub run_key: Option<String>,

    /// Wait for run to complete.
    #[arg(long, short = 'w')]
    pub wait: bool,

    /// Poll interval when waiting (in seconds).
    #[arg(long, default_value = "5")]
    pub poll_interval: u64,
}

/// Execute the run command.
///
/// # Errors
///
/// Returns an error if the workspace ID is missing or the API request fails.
#[allow(clippy::too_many_lines)]
pub async fn execute(args: RunArgs, config: &Config) -> Result<()> {
    let workspace_id = config
        .workspace_id
        .as_ref()
        .context("Workspace ID is required. Set ARCO_WORKSPACE_ID or use --workspace-id")?;

    let client = ApiClient::new(config)?;

    let selection: Vec<String> = args
        .assets
        .iter()
        .map(|value| normalize_asset_key(value))
        .collect();

    if let Some(parent_run_id) = &args.rerun {
        let request = if args.from_failure {
            if !selection.is_empty() {
                anyhow::bail!("--from-failure cannot be combined with --asset");
            }
            if args.include_upstream || args.include_downstream {
                anyhow::bail!(
                    "--from-failure cannot be combined with --include-upstream/--include-downstream"
                );
            }

            RerunRunRequest {
                mode: RerunMode::FromFailure,
                selection: Vec::new(),
                include_upstream: false,
                include_downstream: false,
                run_key: args.run_key.clone(),
                labels: std::collections::HashMap::new(),
            }
        } else {
            if selection.is_empty() {
                anyhow::bail!("subset rerun requires at least one --asset (or use --from-failure)");
            }

            RerunRunRequest {
                mode: RerunMode::Subset,
                selection: selection.clone(),
                include_upstream: args.include_upstream,
                include_downstream: args.include_downstream,
                run_key: args.run_key.clone(),
                labels: std::collections::HashMap::new(),
            }
        };

        let response = client
            .rerun_run(workspace_id, parent_run_id, request)
            .await?;

        match config.format {
            OutputFormat::Json => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "runId": response.run_id,
                        "planId": response.plan_id,
                        "state": response.state.to_string(),
                        "created": response.created,
                        "createdAt": response.created_at.to_rfc3339(),
                        "parentRunId": response.parent_run_id,
                        "rerunKind": response.rerun_kind.to_string(),
                    }))
                    .context("Failed to serialize response")?
                );
            }
            OutputFormat::Text | OutputFormat::Table => {
                if response.created {
                    println!("Rerun created!");
                } else {
                    println!("Existing rerun found (run_key match)");
                }
                println!();
                println!("  Run ID:     {}", response.run_id);
                println!("  Plan ID:    {}", response.plan_id);
                println!("  Parent Run: {}", response.parent_run_id);
                println!("  Rerun Kind: {}", response.rerun_kind);
                println!("  State:      {}", response.state);
                println!("  Created:    {}", response.created_at);
            }
        }

        if args.wait {
            println!();
            println!("Waiting for run to complete...");
            wait_for_completion(&client, workspace_id, &response.run_id, args.poll_interval)
                .await?;
        }

        return Ok(());
    }

    if selection.is_empty() {
        anyhow::bail!("At least one --asset is required");
    }

    let request = TriggerRunRequest {
        selection,
        include_upstream: args.include_upstream,
        include_downstream: args.include_downstream,
        partitions: Vec::new(),
        run_key: args.run_key.clone(),
        labels: std::collections::HashMap::new(),
    };

    let response = client.trigger_run(workspace_id, request).await?;

    match config.format {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "runId": response.run_id,
                    "planId": response.plan_id,
                    "state": response.state.to_string(),
                    "created": response.created,
                    "createdAt": response.created_at.to_rfc3339(),
                }))
                .context("Failed to serialize response")?
            );
        }
        OutputFormat::Text | OutputFormat::Table => {
            if response.created {
                println!("Run triggered successfully!");
            } else {
                println!("Existing run found (run_key match)");
            }
            println!();
            println!("  Run ID:  {}", response.run_id);
            println!("  Plan ID: {}", response.plan_id);
            println!("  State:   {}", response.state);
            println!("  Created: {}", response.created_at);
        }
    }

    if args.wait {
        println!();
        println!("Waiting for run to complete...");
        wait_for_completion(&client, workspace_id, &response.run_id, args.poll_interval).await?;
    }

    Ok(())
}

fn normalize_asset_key(value: &str) -> String {
    value.trim().replace('/', ".")
}

async fn wait_for_completion(
    client: &ApiClient,
    workspace_id: &str,
    run_id: &str,
    poll_interval: u64,
) -> Result<()> {
    use std::io::{self, Write};
    use std::time::Duration;
    use tokio::time::sleep;

    loop {
        let run = client.get_run(workspace_id, run_id).await?;

        match run.state {
            crate::client::RunState::Succeeded => {
                println!("Run completed successfully!");
                println!(
                    "  Tasks: {}/{} succeeded",
                    run.task_counts.succeeded, run.task_counts.total
                );
                return Ok(());
            }
            crate::client::RunState::Failed => {
                println!("Run failed!");
                println!(
                    "  Tasks: {}/{} succeeded, {}/{} failed",
                    run.task_counts.succeeded,
                    run.task_counts.total,
                    run.task_counts.failed,
                    run.task_counts.total
                );
                anyhow::bail!("Run failed");
            }
            crate::client::RunState::Cancelled => {
                println!("Run was cancelled");
                anyhow::bail!("Run cancelled");
            }
            crate::client::RunState::TimedOut => {
                println!("Run timed out");
                anyhow::bail!("Run timed out");
            }
            crate::client::RunState::Pending | crate::client::RunState::Running => {
                print!(
                    "\r  Progress: {}/{} tasks running, {}/{} succeeded",
                    run.task_counts.running,
                    run.task_counts.total,
                    run.task_counts.succeeded,
                    run.task_counts.total
                );
                let _ = io::stdout().flush();
                sleep(Duration::from_secs(poll_interval)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_args_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(flatten)]
            args: RunArgs,
        }

        let cli = TestCli::parse_from([
            "test",
            "--asset",
            "analytics/users",
            "--run-key",
            "test-key",
        ]);
        assert_eq!(cli.args.assets, vec!["analytics/users"]);
        assert_eq!(cli.args.run_key, Some("test-key".to_string()));
        assert!(cli.args.rerun.is_none());
        assert!(!cli.args.from_failure);
        assert!(!cli.args.include_upstream);
        assert!(!cli.args.include_downstream);
        assert!(!cli.args.wait);
    }

    #[test]
    fn test_run_args_multiple_assets() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(flatten)]
            args: RunArgs,
        }

        let cli = TestCli::parse_from([
            "test",
            "--asset",
            "analytics/users",
            "--asset",
            "analytics/orders",
        ]);
        assert_eq!(cli.args.assets, vec!["analytics/users", "analytics/orders"]);
    }

    #[test]
    fn test_run_args_comma_separated_assets() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(flatten)]
            args: RunArgs,
        }

        let cli = TestCli::parse_from(["test", "--asset", "analytics/users,analytics/orders"]);
        assert_eq!(cli.args.assets, vec!["analytics/users", "analytics/orders"]);
    }

    #[test]
    fn test_run_args_rerun_from_failure_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(flatten)]
            args: RunArgs,
        }

        let cli = TestCli::parse_from(["test", "--rerun", "run-123", "--from-failure"]);
        assert_eq!(cli.args.rerun.as_deref(), Some("run-123"));
        assert!(cli.args.from_failure);
        assert!(cli.args.assets.is_empty());
    }

    #[test]
    fn test_run_args_rerun_subset_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(flatten)]
            args: RunArgs,
        }

        let cli = TestCli::parse_from([
            "test",
            "--rerun",
            "run-123",
            "--asset",
            "analytics/b",
            "--include-downstream",
        ]);
        assert_eq!(cli.args.rerun.as_deref(), Some("run-123"));
        assert!(!cli.args.from_failure);
        assert_eq!(cli.args.assets, vec!["analytics/b"]);
        assert!(cli.args.include_downstream);
    }
}
