//! Backfill `run_key` fingerprints for legacy reservations.

use std::collections::HashMap;
use std::str::FromStr;

use anyhow::{Context, Result};
use clap::Args;

use crate::client::{ApiClient, PartitionValue, RunKeyBackfillRequest};
use crate::{Config, OutputFormat};

/// Arguments for the `run_key` backfill command.
#[derive(Debug, Args)]
pub struct RunKeyBackfillArgs {
    /// Run key to backfill.
    #[arg(long)]
    pub run_key: String,

    /// Asset keys to materialize (comma-separated or multiple --asset flags).
    #[arg(long = "asset", short = 'a', value_delimiter = ',')]
    pub assets: Vec<String>,

    /// Partition key-value pairs (key=value).
    #[arg(long = "partition", value_parser = KeyValue::from_str)]
    pub partitions: Vec<KeyValue>,

    /// Label key-value pairs (key=value).
    #[arg(long = "label", value_parser = KeyValue::from_str)]
    pub labels: Vec<KeyValue>,
}

/// Execute the `run_key` backfill command.
///
/// # Errors
///
/// Returns an error if required arguments are missing or the API request fails.
pub async fn execute(args: RunKeyBackfillArgs, config: &Config) -> Result<()> {
    let workspace_id = config
        .workspace_id
        .as_ref()
        .context("Workspace ID is required. Set ARCO_WORKSPACE_ID or use --workspace-id")?;

    if args.assets.is_empty() {
        anyhow::bail!("At least one --asset is required to compute the fingerprint");
    }

    let client = ApiClient::new(config)?;

    let partitions = args
        .partitions
        .into_iter()
        .map(|kv| PartitionValue {
            key: kv.key,
            value: kv.value,
        })
        .collect();

    let mut labels = HashMap::new();
    for kv in args.labels {
        labels.insert(kv.key, kv.value);
    }

    let request = RunKeyBackfillRequest {
        run_key: args.run_key,
        selection: args.assets,
        partitions,
        labels,
    };

    let response = client.backfill_run_key(workspace_id, request).await?;

    match config.format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&response)?);
        }
        OutputFormat::Text | OutputFormat::Table => {
            if response.updated {
                println!("run_key fingerprint backfilled");
            } else {
                println!("run_key fingerprint already present");
            }
            println!();
            println!("  Run Key:     {}", response.run_key);
            println!("  Run ID:      {}", response.run_id);
            println!("  Fingerprint: {}", response.request_fingerprint);
            println!("  Created At:  {}", response.created_at);
        }
    }

    Ok(())
}

/// Parsed key=value pair for partitions and labels.
#[derive(Debug, Clone)]
pub struct KeyValue {
    /// Key portion of the pair.
    pub key: String,
    /// Value portion of the pair.
    pub value: String,
}

impl FromStr for KeyValue {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let (key, value) = s
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("expected key=value, got '{s}'"))?;
        if key.is_empty() || value.is_empty() {
            anyhow::bail!("expected key=value, got '{s}'");
        }
        Ok(Self {
            key: key.to_string(),
            value: value.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_backfill_args_parsing() {
        #[derive(Parser)]
        struct TestCli {
            #[command(flatten)]
            args: RunKeyBackfillArgs,
        }

        let cli = TestCli::parse_from([
            "test",
            "--run-key",
            "daily:2024-01-01",
            "--asset",
            "analytics/users",
            "--partition",
            "date=2024-01-01",
            "--label",
            "team=infra",
        ]);

        assert_eq!(cli.args.run_key, "daily:2024-01-01");
        assert_eq!(cli.args.assets, vec!["analytics/users".to_string()]);
        assert_eq!(cli.args.partitions.len(), 1);
        assert_eq!(cli.args.labels.len(), 1);
    }
}
