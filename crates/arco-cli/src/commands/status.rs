//! Status command - check run status.

use anyhow::{Context, Result};
use clap::Args;
use owo_colors::OwoColorize;

use crate::client::ApiClient;
use crate::{Config, OutputFormat};

/// Arguments for the status command.
#[derive(Debug, Args)]
pub struct StatusArgs {
    /// Run ID to check status for.
    #[arg()]
    pub run_id: Option<String>,

    /// List recent runs instead of checking a specific run.
    #[arg(long, short = 'l')]
    pub list: bool,

    /// Number of runs to list (when using --list).
    #[arg(long, default_value = "10")]
    pub limit: u32,

    /// Show task details.
    #[arg(long, short = 't')]
    pub tasks: bool,
}

/// Execute the status command.
///
/// # Errors
///
/// Returns an error if the workspace ID is missing, arguments are invalid,
/// or the API request fails.
pub async fn execute(args: StatusArgs, config: &Config) -> Result<()> {
    let workspace_id = config
        .workspace_id
        .as_ref()
        .context("Workspace ID is required. Set ARCO_WORKSPACE_ID or use --workspace-id")?;

    let client = ApiClient::new(config)?;

    if args.list {
        list_runs(&client, workspace_id, args.limit, config).await
    } else if let Some(run_id) = &args.run_id {
        show_run(&client, workspace_id, run_id, args.tasks, config).await
    } else {
        anyhow::bail!("Either provide a run ID or use --list to see recent runs")
    }
}

async fn list_runs(
    client: &ApiClient,
    workspace_id: &str,
    limit: u32,
    config: &Config,
) -> Result<()> {
    let response = client.list_runs(workspace_id, Some(limit)).await?;

    match config.format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&response)?);
        }
        OutputFormat::Text => {
            if response.runs.is_empty() {
                println!("No runs found");
                return Ok(());
            }

            println!("Recent runs:");
            println!();
            for run in &response.runs {
                let state_str = format_state_colored(&run.state.to_string());
                println!(
                    "  {} {} ({}/{} tasks succeeded)",
                    run.run_id, state_str, run.tasks_succeeded, run.task_count
                );
            }
        }
        OutputFormat::Table => {
            use tabled::{Table, Tabled};

            #[derive(Tabled)]
            struct RunRow {
                #[tabled(rename = "Run ID")]
                run_id: String,
                #[tabled(rename = "State")]
                state: String,
                #[tabled(rename = "Tasks")]
                tasks: String,
                #[tabled(rename = "Created")]
                created: String,
            }

            let rows: Vec<_> = response
                .runs
                .iter()
                .map(|r| RunRow {
                    run_id: r.run_id.clone(),
                    state: r.state.to_string(),
                    tasks: format!("{}/{}", r.tasks_succeeded, r.task_count),
                    created: r.created_at.format("%Y-%m-%d %H:%M:%S").to_string(),
                })
                .collect();

            if rows.is_empty() {
                println!("No runs found");
            } else {
                println!("{}", Table::new(rows));
            }
        }
    }

    Ok(())
}

async fn show_run(
    client: &ApiClient,
    workspace_id: &str,
    run_id: &str,
    show_tasks: bool,
    config: &Config,
) -> Result<()> {
    let run = client.get_run(workspace_id, run_id).await?;

    match config.format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&run)?);
        }
        OutputFormat::Text | OutputFormat::Table => {
            let state_str = format_state_colored(&run.state.to_string());

            println!("Run: {}", run.run_id);
            println!("State: {state_str}");
            println!("Plan ID: {}", run.plan_id);
            println!("Created: {}", run.created_at);
            if let Some(started) = run.started_at {
                println!("Started: {started}");
            }
            if let Some(completed) = run.completed_at {
                println!("Completed: {completed}");
            }
            println!();
            println!("Task Summary:");
            println!("  Total:     {}", run.task_counts.total);
            println!("  Pending:   {}", run.task_counts.pending);
            println!("  Queued:    {}", run.task_counts.queued);
            println!("  Running:   {}", run.task_counts.running);
            println!(
                "  Succeeded: {}",
                format!("{}", run.task_counts.succeeded).green()
            );
            if run.task_counts.failed > 0 {
                println!(
                    "  Failed:    {}",
                    format!("{}", run.task_counts.failed).red()
                );
            } else {
                println!("  Failed:    {}", run.task_counts.failed);
            }
            println!("  Skipped:   {}", run.task_counts.skipped);
            println!("  Cancelled: {}", run.task_counts.cancelled);

            if show_tasks && !run.tasks.is_empty() {
                println!();
                println!("Tasks:");
                for task in &run.tasks {
                    let task_state = format_state_colored(&task.state.to_string());
                    let asset_info = task
                        .asset_key
                        .as_ref()
                        .map_or(String::new(), |k| format!(" ({k})"));
                    println!("  {} {}{}", task.task_key, task_state, asset_info);
                    if let Some(error) = &task.error_message {
                        println!("    Error: {}", error.red());
                    }
                }
            }
        }
    }

    Ok(())
}

fn format_state_colored(state: &str) -> String {
    match state {
        "SUCCEEDED" => state.green().to_string(),
        "FAILED" => state.red().to_string(),
        "RUNNING" => state.blue().to_string(),
        "PENDING" | "QUEUED" => state.yellow().to_string(),
        "CANCELLED" | "TIMED_OUT" | "SKIPPED" => state.dimmed().to_string(),
        _ => state.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_args_parsing() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(flatten)]
            args: StatusArgs,
        }

        let cli = TestCli::parse_from(["test", "run-123", "--tasks"]);
        assert_eq!(cli.args.run_id, Some("run-123".to_string()));
        assert!(cli.args.tasks);
        assert!(!cli.args.list);
    }

    #[test]
    fn test_status_args_list() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(flatten)]
            args: StatusArgs,
        }

        let cli = TestCli::parse_from(["test", "--list", "--limit", "20"]);
        assert!(cli.args.list);
        assert_eq!(cli.args.limit, 20);
    }
}
