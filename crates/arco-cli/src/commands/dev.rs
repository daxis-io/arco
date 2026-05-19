//! Local orchestration development readiness checks.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use clap::Args;
use serde::Serialize;

use crate::{Config, OutputFormat};

/// Arguments for the dev command.
#[derive(Debug, Args)]
pub struct DevArgs {
    /// Check local orchestration prerequisites without starting a run.
    #[arg(long, required = true)]
    pub check: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DevCheckReport {
    command: &'static str,
    ready: bool,
    status: &'static str,
    verifies: Vec<&'static str>,
    does_not_start: Vec<&'static str>,
    required_for_true_loop: Vec<&'static str>,
    configured: BTreeMap<&'static str, bool>,
    execution_location_identity: &'static str,
}

/// Execute the dev command.
///
/// # Errors
///
/// Returns an error if output serialization fails.
pub fn execute(args: DevArgs, config: &Config) -> Result<()> {
    if !args.check {
        anyhow::bail!(
            "arco dev is not an end-to-end local orchestration loop yet; use `arco dev --check`"
        );
    }

    let report = build_check_report(config);

    match config.format {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report).context("failed to serialize dev check")?
            );
        }
        OutputFormat::Text | OutputFormat::Table => print_text_report(&report),
    }

    Ok(())
}

fn build_check_report(config: &Config) -> DevCheckReport {
    DevCheckReport {
        command: "arco dev --check",
        ready: false,
        status: "check-only",
        verifies: vec![
            "CLI command wiring",
            "API URL configuration",
            "workspace configuration presence",
            "known missing pieces for a production-equivalent local loop",
        ],
        does_not_start: vec![
            "API/control-plane process",
            "flow dispatcher or scheduler runtime",
            "local dispatch queue",
            "worker shim",
            "sample run",
            "worker callbacks",
            "projection compaction or system-table evidence",
        ],
        required_for_true_loop: vec![
            "local API/control-plane startup or connection",
            "local flow runtime using the same dispatch controllers",
            "local execution-location planning or registry source",
            "dispatch adapter that delivers WorkerDispatchEnvelope to a worker",
            "sample worker shim that calls the task callback API",
            "sample run creation through the API",
            "published run/task/dispatch evidence through existing API or system.orchestration.*",
        ],
        configured: BTreeMap::from([
            ("api_url", !config.api_url.trim().is_empty()),
            (
                "workspace_id",
                config
                    .workspace_id
                    .as_ref()
                    .is_some_and(|value| !value.trim().is_empty()),
            ),
        ]),
        execution_location_identity: "not inferred; execution_location_id remains absent until planning supplies one",
    }
}

fn print_text_report(report: &DevCheckReport) {
    println!("{}", report.command);
    println!("Ready: {}", if report.ready { "yes" } else { "no" });
    println!("Status: {}", report.status);

    print_bullet_section("Verifies", &report.verifies);
    print_bullet_section("Does not start", &report.does_not_start);

    println!();
    println!("Configured:");
    for (key, present) in &report.configured {
        let value = if *present { "present" } else { "missing" };
        println!("  - {key}: {value}");
    }

    print_bullet_section(
        "Required for a true local loop",
        &report.required_for_true_loop,
    );

    println!();
    println!(
        "Execution-location identity: {}",
        report.execution_location_identity
    );
}

fn print_bullet_section(heading: &str, items: &[&'static str]) {
    println!();
    println!("{heading}:");
    for item in items {
        println!("  - {item}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_report_is_explicitly_not_an_end_to_end_loop() {
        let report = build_check_report(&Config {
            api_url: "http://localhost:8080".to_string(),
            workspace_id: Some("workspace".to_string()),
            api_token: None,
            format: OutputFormat::Text,
        });

        assert!(!report.ready);
        assert_eq!(report.status, "check-only");
        assert!(report.does_not_start.contains(&"worker callbacks"));
        assert!(
            report
                .required_for_true_loop
                .contains(&"dispatch adapter that delivers WorkerDispatchEnvelope to a worker")
        );
        assert!(
            report
                .required_for_true_loop
                .contains(&"local execution-location planning or registry source")
        );
        let json = serde_json::to_value(&report).expect("serialize report");
        assert_eq!(json["ready"], serde_json::json!(false));
    }

    #[test]
    fn check_report_does_not_infer_execution_location_identity() {
        let report = build_check_report(&Config {
            api_url: "http://localhost:8080".to_string(),
            workspace_id: Some("workspace".to_string()),
            api_token: None,
            format: OutputFormat::Text,
        });

        assert!(report.execution_location_identity.contains("not inferred"));
        assert!(
            report
                .execution_location_identity
                .contains("execution_location_id remains absent")
        );
    }
}
