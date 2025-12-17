//! Workspace automation tasks.
//!
//! Run with: `cargo xtask <command>`

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::process::Command;

/// Expected tool versions (should match CI)
mod versions {
    pub const RUST_CHANNEL: &str = "1.85";
    pub const CARGO_DENY_MIN: &str = "0.18.0";
    pub const BUF_VERSION: &str = "1.47.2";
}

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
    /// Validate toolchain + configs are consistent with CI
    Doctor,
    /// Check ADR conformance
    AdrCheck,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Ci => run_ci(),
        Commands::Lint => run_lint(),
        Commands::Coverage => run_coverage(),
        Commands::Doctor => run_doctor(),
        Commands::AdrCheck => run_adr_check(),
    }
}

fn run_ci() -> Result<()> {
    println!("Running CI checks...\n");

    run_doctor()?;
    run_adr_check()?;

    run_cmd("cargo", &["check", "--workspace", "--all-features"])?;
    run_cmd("cargo", &["fmt", "--all", "--check"])?;
    run_cmd(
        "cargo",
        &[
            "clippy",
            "--workspace",
            "--all-features",
            "--",
            "-D",
            "warnings",
        ],
    )?;
    run_cmd(
        "cargo",
        &[
            "test",
            "--workspace",
            "--all-features",
            "--exclude",
            "arco-flow",
        ],
    )?;
    run_cmd(
        "cargo",
        &["test", "-p", "arco-flow", "--features", "test-utils"],
    )?;
    run_cmd_with_env(
        "cargo",
        &["doc", "--workspace", "--no-deps"],
        &[("RUSTDOCFLAGS", "-D warnings")],
    )?;
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

fn run_doctor() -> Result<()> {
    println!("Checking development environment...\n");

    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    // 1. Check Rust version
    print!("  Rust toolchain... ");
    match check_rust_version() {
        Ok(version) => println!("[ok] {version}"),
        Err(e) => {
            println!("[FAIL]");
            errors.push(format!("Rust: {e}"));
        }
    }

    // 2. Check cargo-deny
    print!("  cargo-deny...     ");
    match check_cargo_deny() {
        Ok(version) => println!("[ok] {version}"),
        Err(e) => {
            println!("[FAIL]");
            errors.push(format!("cargo-deny: {e}"));
        }
    }

    // 3. Check buf (optional - may not be installed locally)
    print!("  buf...            ");
    match check_buf() {
        Ok(version) => println!("[ok] {version}"),
        Err(e) => {
            println!("[warn] (optional)");
            warnings.push(format!("buf: {e}"));
        }
    }

    // 4. Validate deny.toml
    print!("  deny.toml...      ");
    match validate_deny_toml() {
        Ok(()) => println!("[ok] valid"),
        Err(e) => {
            println!("[FAIL]");
            errors.push(format!("deny.toml: {e}"));
        }
    }

    // 5. Validate buf.yaml
    print!("  buf.yaml...       ");
    match validate_buf_yaml() {
        Ok(()) => println!("[ok] valid"),
        Err(e) => {
            println!("[FAIL]");
            errors.push(format!("buf.yaml: {e}"));
        }
    }

    // 6. Check rust-toolchain.toml
    print!("  rust-toolchain... ");
    match validate_rust_toolchain() {
        Ok(()) => println!("[ok] valid"),
        Err(e) => {
            println!("[FAIL]");
            errors.push(format!("rust-toolchain.toml: {e}"));
        }
    }

    println!();

    // Print warnings
    if !warnings.is_empty() {
        println!("Warnings:");
        for w in &warnings {
            println!("  - {w}");
        }
        println!();
    }

    // Print errors and fail if any
    if !errors.is_empty() {
        println!("Errors:");
        for e in &errors {
            println!("  - {e}");
        }
        println!();
        anyhow::bail!(
            "Doctor found {} error(s). Fix them before proceeding.",
            errors.len()
        );
    }

    println!("All checks passed! Environment is ready.");
    Ok(())
}

/// Expected ADRs that must exist and be indexed
const REQUIRED_ADRS: &[(&str, &str)] = &[
    (
        "adr-001-parquet-metadata.md",
        "Parquet-first metadata storage",
    ),
    ("adr-002-id-strategy.md", "ID strategy by entity type"),
    (
        "adr-003-manifest-domains.md",
        "Manifest domain names and contention",
    ),
    (
        "adr-004-event-envelope.md",
        "Event envelope format and evolution",
    ),
    ("adr-005-storage-layout.md", "Canonical storage layout"),
];

fn run_adr_check() -> Result<()> {
    println!("Checking ADR conformance...\n");

    let adr_dir = std::path::Path::new("docs/adr");
    let readme_path = adr_dir.join("README.md");

    // Check ADR directory exists
    if !adr_dir.exists() {
        anyhow::bail!("ADR directory 'docs/adr/' does not exist");
    }

    // Check README exists
    if !readme_path.exists() {
        anyhow::bail!("ADR README 'docs/adr/README.md' does not exist");
    }

    let readme_content = std::fs::read_to_string(&readme_path)?;
    let mut errors = Vec::new();

    // Check each required ADR
    for (filename, title) in REQUIRED_ADRS {
        let adr_path = adr_dir.join(filename);
        print!("  {filename}... ");

        // Check file exists
        if !adr_path.exists() {
            println!("[MISSING]");
            errors.push(format!("ADR file missing: {filename}"));
            continue;
        }

        // Check file is indexed in README
        if !readme_content.contains(filename) {
            println!("[NOT INDEXED]");
            errors.push(format!("ADR not indexed in README: {filename}"));
            continue;
        }

        // Check ADR format
        let content = std::fs::read_to_string(&adr_path)?;
        if let Err(e) = validate_adr_format(&content) {
            println!("[INVALID FORMAT]");
            errors.push(format!("{filename}: {e}"));
            continue;
        }

        println!("[ok] {title}");
    }

    println!();

    if !errors.is_empty() {
        println!("Errors:");
        for e in &errors {
            println!("  - {e}");
        }
        anyhow::bail!("ADR check found {} error(s)", errors.len());
    }

    println!("All ADRs present and indexed!");
    Ok(())
}

fn validate_adr_format(content: &str) -> Result<()> {
    for section in ["## Status", "## Context", "## Decision", "## Consequences"] {
        if !content.contains(section) {
            anyhow::bail!("missing '{section}' section");
        }
    }

    let status = parse_adr_status(content).context("missing status value")?;
    let allowed = ["Proposed", "Accepted", "Deprecated", "Superseded"];
    if !allowed.contains(&status.as_str()) {
        anyhow::bail!(
            "invalid status '{status}' (expected one of: {})",
            allowed.join(", ")
        );
    }

    Ok(())
}

fn parse_adr_status(content: &str) -> Option<String> {
    let mut lines = content.lines();
    while let Some(line) = lines.next() {
        if line.trim() != "## Status" {
            continue;
        }

        for status_line in lines.by_ref() {
            let status_line = status_line.trim();
            if status_line.is_empty() {
                continue;
            }
            return Some(status_line.to_string());
        }
    }

    None
}

fn check_rust_version() -> Result<String> {
    let output = Command::new("rustc")
        .arg("--version")
        .output()
        .context("rustc not found")?;

    let version = String::from_utf8_lossy(&output.stdout);
    let version = version.trim();

    // Extract version number (e.g., "rustc 1.85.0 (..." -> "1.85.0")
    let parts: Vec<&str> = version.split_whitespace().collect();
    let rust_version = parts.get(1).unwrap_or(&"unknown");

    if !rust_version.starts_with(versions::RUST_CHANNEL) {
        anyhow::bail!(
            "expected Rust {}.x, found {}",
            versions::RUST_CHANNEL,
            rust_version
        );
    }

    Ok(rust_version.to_string())
}

fn check_cargo_deny() -> Result<String> {
    let output = Command::new("cargo")
        .args(["deny", "--version"])
        .output()
        .context("cargo-deny not installed. Run: cargo install cargo-deny")?;

    if !output.status.success() {
        anyhow::bail!("not installed. Run: cargo install cargo-deny");
    }

    let version = String::from_utf8_lossy(&output.stdout);
    let version = version.trim();

    let deny_version =
        parse_semver_from_output(version).context("failed to parse cargo-deny version")?;
    let min = semver::Version::parse(versions::CARGO_DENY_MIN)
        .context("invalid internal CARGO_DENY_MIN version")?;

    if deny_version < min {
        anyhow::bail!(
            "expected cargo-deny >= {}, found {}. Install: cargo install cargo-deny --version {}",
            min,
            deny_version,
            min
        );
    }

    Ok(deny_version.to_string())
}

fn check_buf() -> Result<String> {
    let output = Command::new("buf")
        .arg("--version")
        .output()
        .context("buf not installed (optional - CI will run it)")?;

    if !output.status.success() {
        anyhow::bail!("not installed (optional - CI will run it)");
    }

    let raw = String::from_utf8_lossy(&output.stdout);
    let raw = raw.trim();

    let local = parse_semver_from_output(raw).context("failed to parse buf version")?;
    let expected =
        semver::Version::parse(versions::BUF_VERSION).context("invalid internal BUF_VERSION")?;

    if local != expected {
        anyhow::bail!(
            "version mismatch: local={}, CI={}. Install: brew install bufbuild/buf/buf",
            local,
            expected
        );
    }

    Ok(local.to_string())
}

fn validate_deny_toml() -> Result<()> {
    let content = std::fs::read_to_string("deny.toml").context("deny.toml not found")?;

    // Parse as TOML to validate syntax
    let _: toml::Value = toml::from_str(&content).context("invalid TOML syntax")?;

    Ok(())
}

fn validate_buf_yaml() -> Result<()> {
    let content = std::fs::read_to_string("proto/buf.yaml").context("proto/buf.yaml not found")?;

    let doc: serde_yaml::Value = serde_yaml::from_str(&content).context("invalid YAML syntax")?;
    let version = doc
        .get("version")
        .and_then(serde_yaml::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing 'version' field"))?;

    if version != "v2" {
        anyhow::bail!("expected version: v2, found: {version}");
    }

    Ok(())
}

fn validate_rust_toolchain() -> Result<()> {
    let content =
        std::fs::read_to_string("rust-toolchain.toml").context("rust-toolchain.toml not found")?;

    // Parse and validate
    let toolchain: toml::Value = toml::from_str(&content).context("invalid TOML syntax")?;

    let channel = toolchain
        .get("toolchain")
        .and_then(|t| t.get("channel"))
        .and_then(|c| c.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing toolchain.channel"))?;

    if !channel.starts_with(versions::RUST_CHANNEL) {
        anyhow::bail!(
            "channel mismatch: file={}, expected={}.*",
            channel,
            versions::RUST_CHANNEL
        );
    }

    Ok(())
}

fn parse_semver_from_output(output: &str) -> Result<semver::Version> {
    for token in output.split_whitespace() {
        let token = token.trim().trim_start_matches('v');
        if let Ok(v) = semver::Version::parse(token) {
            return Ok(v);
        }
    }

    anyhow::bail!("unable to parse semver from: {output}");
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

fn run_cmd_with_env(cmd: &str, args: &[&str], env: &[(&str, &str)]) -> Result<()> {
    println!("$ {} {}", cmd, args.join(" "));
    let mut command = Command::new(cmd);
    command.args(args);
    for (k, v) in env {
        command.env(k, v);
    }

    let status = command
        .status()
        .with_context(|| format!("Failed to run: {} {}", cmd, args.join(" ")))?;

    if !status.success() {
        anyhow::bail!("Command failed: {} {}", cmd, args.join(" "));
    }
    Ok(())
}
