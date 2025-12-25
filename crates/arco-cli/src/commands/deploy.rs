//! Deploy command - upload and register asset manifests.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;

use crate::client::{ApiClient, DeployManifestRequest};
use crate::{Config, OutputFormat};

/// Arguments for the deploy command.
#[derive(Debug, Args)]
pub struct DeployArgs {
    /// Path to manifest file (JSON).
    #[arg(long, short = 'f')]
    pub manifest_file: PathBuf,

    /// Deployed by (optional, defaults to current user).
    #[arg(long)]
    pub deployed_by: Option<String>,
}

/// Execute the deploy command.
///
/// # Errors
///
/// Returns an error if the workspace ID is missing, manifest file is invalid,
/// or the API request fails.
pub async fn execute(args: DeployArgs, config: &Config) -> Result<()> {
    let workspace_id = config
        .workspace_id
        .as_ref()
        .context("Workspace ID is required. Set ARCO_WORKSPACE_ID or use --workspace-id")?;

    // Read manifest file
    let manifest_content = std::fs::read_to_string(&args.manifest_file)
        .with_context(|| format!("Failed to read manifest file: {:?}", args.manifest_file))?;

    // Parse manifest
    let mut request: DeployManifestRequest =
        serde_json::from_str(&manifest_content).with_context(|| "Failed to parse manifest JSON")?;

    // Override deployed_by if specified
    if let Some(deployed_by) = args.deployed_by {
        request.deployed_by = Some(deployed_by);
    }

    let client = ApiClient::new(config)?;
    let response = client.deploy_manifest(workspace_id, request).await?;

    match config.format {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "manifestId": response.manifest_id,
                    "workspaceId": response.workspace_id,
                    "codeVersionId": response.code_version_id,
                    "fingerprint": response.fingerprint,
                    "assetCount": response.asset_count,
                    "deployedAt": response.deployed_at.to_rfc3339(),
                }))
                .context("Failed to serialize response")?
            );
        }
        OutputFormat::Text => {
            let fingerprint_preview: String = response.fingerprint.chars().take(12).collect();
            let ellipsis = if response.fingerprint.len() > 12 {
                "..."
            } else {
                ""
            };
            println!("Manifest deployed successfully!");
            println!();
            println!("  Manifest ID:     {}", response.manifest_id);
            println!("  Workspace ID:    {}", response.workspace_id);
            println!("  Code Version:    {}", response.code_version_id);
            println!("  Fingerprint:     {fingerprint_preview}{ellipsis}");
            println!("  Asset Count:     {}", response.asset_count);
            println!("  Deployed At:     {}", response.deployed_at);
        }
        OutputFormat::Table => {
            use tabled::{Table, Tabled};

            #[derive(Tabled)]
            struct Row {
                #[tabled(rename = "Field")]
                field: String,
                #[tabled(rename = "Value")]
                value: String,
            }

            let rows = vec![
                Row {
                    field: "Manifest ID".to_string(),
                    value: response.manifest_id,
                },
                Row {
                    field: "Workspace ID".to_string(),
                    value: response.workspace_id,
                },
                Row {
                    field: "Code Version".to_string(),
                    value: response.code_version_id,
                },
                Row {
                    field: "Fingerprint".to_string(),
                    value: response.fingerprint,
                },
                Row {
                    field: "Asset Count".to_string(),
                    value: response.asset_count.to_string(),
                },
                Row {
                    field: "Deployed At".to_string(),
                    value: response.deployed_at.to_string(),
                },
            ];

            println!("{}", Table::new(rows));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_deploy_args_parsing() {
        #[derive(Parser)]
        struct TestCli {
            #[command(flatten)]
            args: DeployArgs,
        }

        let cli = TestCli::parse_from(["test", "-f", "manifest.json"]);
        assert_eq!(cli.args.manifest_file, PathBuf::from("manifest.json"));
        assert!(cli.args.deployed_by.is_none());
    }

    #[test]
    fn test_deploy_args_with_deployed_by() {
        #[derive(Parser)]
        struct TestCli {
            #[command(flatten)]
            args: DeployArgs,
        }

        let cli = TestCli::parse_from([
            "test",
            "--manifest-file",
            "manifest.json",
            "--deployed-by",
            "ci-pipeline",
        ]);
        assert_eq!(cli.args.manifest_file, PathBuf::from("manifest.json"));
        assert_eq!(cli.args.deployed_by, Some("ci-pipeline".to_string()));
    }
}
