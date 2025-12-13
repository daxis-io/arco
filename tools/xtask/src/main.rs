//! Workspace automation tasks.
//!
//! Run with: `cargo xtask <command>`

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::process::Command;

#[derive(Parser)]
#[command(name = "xtask", about = "Arco workspace automation")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run all CI checks locally
    Ci,
    /// Validate workspace conventions
    Lint,
    /// Generate coverage report
    Coverage,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Ci => run_ci(),
        Commands::Lint => run_lint(),
        Commands::Coverage => run_coverage(),
    }
}

fn run_ci() -> Result<()> {
    println!("Running CI checks...\n");

    run_cmd("cargo", &["fmt", "--check"])?;
    run_cmd("cargo", &["clippy", "--workspace", "--", "-D", "warnings"])?;
    run_cmd("cargo", &["test", "--workspace"])?;
    run_cmd("cargo", &["doc", "--workspace", "--no-deps"])?;
    run_cmd("cargo", &["deny", "check"])?;

    println!("\nAll CI checks passed!");
    Ok(())
}

fn run_lint() -> Result<()> {
    println!("Validating workspace conventions...\n");

    // Check crate naming
    let crates = std::fs::read_dir("crates")?;
    for entry in crates {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("arco-") {
            anyhow::bail!("Crate '{}' does not follow arco-* naming", name);
        }
    }

    println!("All conventions validated!");
    Ok(())
}

fn run_coverage() -> Result<()> {
    run_cmd("cargo", &["llvm-cov", "--workspace", "--html"])?;
    println!("\nCoverage report: target/llvm-cov/html/index.html");
    Ok(())
}

fn run_cmd(cmd: &str, args: &[&str]) -> Result<()> {
    println!("$ {} {}", cmd, args.join(" "));
    let status = Command::new(cmd)
        .args(args)
        .status()
        .with_context(|| format!("Failed to run: {} {}", cmd, args.join(" ")))?;

    if !status.success() {
        anyhow::bail!("Command failed: {} {}", cmd, args.join(" "));
    }
    Ok(())
}
