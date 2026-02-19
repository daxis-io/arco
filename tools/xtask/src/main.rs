//! Workspace automation tasks.
//!
//! Run with: `cargo xtask <command>`

use std::collections::{HashMap, HashSet};
use std::env;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand};
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use tokio::runtime::Runtime;

use arco_catalog::lock::LockInfo;
use arco_catalog::manifest::{
    CatalogDomainManifest, CommitRecord, ExecutionsManifest, LineageManifest, RootManifest,
    SearchManifest, SnapshotInfo,
};
use arco_core::storage::ObjectStoreBackend;
use arco_core::{CatalogDomain, CatalogPaths, Error as CoreError, ScopedStorage};

/// Expected tool versions (should match CI)
mod versions {
    pub const RUST_CHANNEL: &str = "1.85";
    pub const CARGO_DENY_MIN: &str = "0.18.9";
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
    /// Enforce repository hygiene invariants
    RepoHygieneCheck,
    /// Verify catalog integrity (schemas, golden files, workspace checks)
    VerifyIntegrity {
        /// Show detailed output
        #[arg(long, short)]
        verbose: bool,
        /// Report issues without failing
        #[arg(long)]
        dry_run: bool,
        /// Fail if any active lock is present (maintenance windows only)
        #[arg(long)]
        lock_strict: bool,
        /// Tenant ID for storage integrity checks
        #[arg(long, value_name = "TENANT", requires = "workspace")]
        tenant: Option<String>,
        /// Workspace ID for storage integrity checks
        #[arg(long, value_name = "WORKSPACE", requires = "tenant")]
        workspace: Option<String>,
        /// Storage bucket override (defaults to ARCO_STORAGE_BUCKET)
        #[arg(long, value_name = "BUCKET")]
        bucket: Option<String>,
    },
    ParityMatrixCheck,
    /// Generate endpoint inventory from vendored Unity Catalog OSS OpenAPI fixture
    UcOpenapiInventory,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Ci => run_ci(),
        Commands::Lint => run_lint(),
        Commands::Coverage => run_coverage(),
        Commands::Doctor => run_doctor(),
        Commands::AdrCheck => run_adr_check(),
        Commands::RepoHygieneCheck => run_repo_hygiene_check(),
        Commands::ParityMatrixCheck => run_parity_matrix_check(),
        Commands::UcOpenapiInventory => run_uc_openapi_inventory(),
        Commands::VerifyIntegrity {
            verbose,
            dry_run,
            lock_strict,
            tenant,
            workspace,
            bucket,
        } => run_verify_integrity(verbose, dry_run, lock_strict, tenant, workspace, bucket),
    }
}

fn run_uc_openapi_inventory() -> Result<()> {
    type OperationEntry = (String, String, Option<String>, Option<String>);
    type OperationsByTag = HashMap<String, Vec<OperationEntry>>;

    let spec_path = Path::new("crates/arco-uc/tests/fixtures/unitycatalog-openapi.yaml");
    let output_path = Path::new("docs/guide/src/reference/unity-catalog-openapi-inventory.md");

    let yaml = std::fs::read_to_string(spec_path)
        .with_context(|| format!("read UC OpenAPI fixture at {}", spec_path.display()))?;
    if yaml.contains("PLACEHOLDER") || yaml.contains("REPLACE_ME") {
        anyhow::bail!(
            "UC OpenAPI fixture is a placeholder; replace {} with a pinned upstream api/all.yaml and record the commit hash in the header",
            spec_path.display()
        );
    }

    let spec_yaml: serde_yaml::Value =
        serde_yaml::from_str(&yaml).context("parse UC OpenAPI fixture YAML")?;
    let spec: serde_json::Value =
        serde_json::to_value(spec_yaml).context("convert UC OpenAPI spec to JSON")?;
    let spec_sha256 = format!("{:x}", Sha256::digest(yaml.as_bytes()));

    let title = spec
        .get("info")
        .and_then(|info| info.get("title"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("Unity Catalog OSS OpenAPI");
    let version = spec
        .get("info")
        .and_then(|info| info.get("version"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");

    let servers = spec
        .get("servers")
        .and_then(serde_json::Value::as_array)
        .map(|servers| {
            servers
                .iter()
                .filter_map(|server| server.get("url").and_then(serde_json::Value::as_str))
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let spec_paths = spec
        .get("paths")
        .and_then(serde_json::Value::as_object)
        .context("UC OpenAPI spec missing `paths` object")?;

    let mut by_tag: OperationsByTag = HashMap::new();

    for (path, path_item) in spec_paths {
        let Some(path_item) = path_item.as_object() else {
            continue;
        };

        for (method, operation) in path_item {
            if !is_http_method(method) {
                continue;
            }
            let Some(operation) = operation.as_object() else {
                continue;
            };

            let tags = operation
                .get("tags")
                .and_then(serde_json::Value::as_array)
                .map(|tags| {
                    tags.iter()
                        .filter_map(|tag| tag.as_str())
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let operation_id = operation
                .get("operationId")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);
            let summary = operation
                .get("summary")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);

            let tags = if tags.is_empty() {
                vec!["Untagged".to_string()]
            } else {
                tags
            };

            for tag in tags {
                by_tag.entry(tag).or_default().push((
                    method.to_uppercase(),
                    path.to_string(),
                    operation_id.clone(),
                    summary.clone(),
                ));
            }
        }
    }

    for entries in by_tag.values_mut() {
        entries.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    }

    let mut tags: Vec<_> = by_tag.keys().cloned().collect();
    tags.sort();

    let manual_block = read_manual_block(output_path)?;

    let mut md = String::new();
    md.push_str("# Unity Catalog OSS OpenAPI Endpoint Inventory (Pinned)\n\n");
    md.push_str(&format!("**Spec SHA256:** `{spec_sha256}`  \n"));
    md.push_str(&format!("**Spec fixture:** `{}`  \n", spec_path.display()));
    md.push_str(&format!("**Spec title:** {title}  \n"));
    md.push_str(&format!("**Spec version:** {version}  \n"));
    if !servers.is_empty() {
        md.push_str("**Servers:**\n");
        for server in &servers {
            md.push_str(&format!("- `{server}`\n"));
        }
        md.push('\n');
    }

    md.push_str("<!-- BEGIN GENERATED -->\n\n");
    for tag in tags {
        md.push_str(&format!("## {tag}\n\n"));
        let Some(entries) = by_tag.get(&tag) else {
            continue;
        };
        for (method, path, operation_id, summary) in entries {
            md.push_str(&format!("- `{method} {path}`"));
            if let Some(operation_id) = operation_id {
                md.push_str(&format!(" _(operationId: `{operation_id}`)_"));
            }
            if let Some(summary) = summary {
                md.push_str(&format!(" — {summary}"));
            }
            md.push('\n');
        }
        md.push('\n');
    }
    md.push_str("<!-- END GENERATED -->\n\n");

    md.push_str("## Manual annotations\n\n");
    md.push_str("<!-- BEGIN MANUAL -->\n");
    md.push_str(manual_block.trim());
    md.push('\n');
    md.push_str("<!-- END MANUAL -->\n");

    std::fs::write(output_path, md)
        .with_context(|| format!("write inventory markdown to {}", output_path.display()))?;

    println!(
        "Wrote UC OpenAPI endpoint inventory to {}",
        output_path.display()
    );
    Ok(())
}

fn is_http_method(key: &str) -> bool {
    matches!(
        key,
        "get" | "post" | "put" | "delete" | "patch" | "head" | "options" | "trace"
    )
}

fn read_manual_block(path: &Path) -> Result<String> {
    if !path.exists() {
        return Ok(String::new());
    }
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("read existing inventory file at {}", path.display()))?;
    let Some(start) = contents.find("<!-- BEGIN MANUAL -->") else {
        return Ok(String::new());
    };
    let Some(end) = contents.find("<!-- END MANUAL -->") else {
        return Ok(String::new());
    };
    Ok(contents[start + "<!-- BEGIN MANUAL -->".len()..end]
        .trim()
        .to_string())
}

fn run_ci() -> Result<()> {
    println!("Running CI checks...\n");

    run_doctor()?;
    run_adr_check()?;
    run_parity_matrix_check()?;
    run_repo_hygiene_check()?;

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
        &["test", "-p", "arco-flow", "--all-features", "--no-run"],
    )?;
    run_cmd(
        "cargo",
        &["test", "-p", "arco-flow", "--features", "test-utils"],
    )?;
    run_cmd_with_env(
        "cargo",
        &["doc", "--workspace", "--all-features", "--no-deps"],
        &[("RUSTDOCFLAGS", "-D warnings")],
    )?;
    run_cmd("cargo", &["deny", "check", "bans", "licenses", "sources"])?;
    run_cmd("cargo", &["deny", "check", "advisories"])?;

    println!("\nAll CI checks passed!");
    Ok(())
}

fn run_repo_hygiene_check() -> Result<()> {
    println!("Running repository hygiene checks...\n");

    let mut errors = Vec::new();
    errors.extend(check_mdbook_summary_refs(Path::new("docs/guide/src/SUMMARY.md"))?);

    let forbidden_paths = forbidden_path_markers();
    for path in tracked_files()? {
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(err).with_context(|| format!("read tracked file at {path}"));
            }
        };
        let Ok(text) = String::from_utf8(bytes) else {
            continue;
        };

        for term in scan_text_for_banned_terms(&text) {
            errors.push(format!("{path}: banned term '{term}'"));
        }

        let lower = text.to_ascii_lowercase();
        for marker in &forbidden_paths {
            if lower.contains(marker) {
                errors.push(format!("{path}: forbidden path reference '{marker}'"));
            }
        }
    }

    if !errors.is_empty() {
        println!("Repository hygiene errors:");
        for err in &errors {
            println!("  - {err}");
        }
        anyhow::bail!(
            "repository hygiene check failed with {} issue(s)",
            errors.len()
        );
    }

    println!("Repository hygiene checks passed!");
    Ok(())
}

fn tracked_files() -> Result<Vec<String>> {
    let output = Command::new("git")
        .args(["ls-files"])
        .output()
        .context("run `git ls-files` for tracked file enumeration")?;
    if !output.status.success() {
        anyhow::bail!("`git ls-files` failed while enumerating tracked files");
    }

    let stdout = String::from_utf8(output.stdout).context("parse `git ls-files` output as UTF-8")?;
    Ok(stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

fn check_mdbook_summary_refs(summary_path: &Path) -> Result<Vec<String>> {
    let summary = std::fs::read_to_string(summary_path)
        .with_context(|| format!("read {}", summary_path.display()))?;
    let summary_dir = summary_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));

    let mut errors = Vec::new();
    for raw_link in extract_summary_links(&summary) {
        let link = raw_link.split('#').next().unwrap_or("").trim();
        if link.is_empty() {
            continue;
        }
        if link.starts_with("http://")
            || link.starts_with("https://")
            || link.starts_with("mailto:")
            || !link.ends_with(".md")
        {
            continue;
        }

        let resolved = summary_dir.join(link);
        if !resolved.is_file() {
            errors.push(format!(
                "{}: broken SUMMARY reference '{}'",
                summary_path.display(),
                raw_link
            ));
        }
    }

    Ok(errors)
}

fn extract_summary_links(text: &str) -> Vec<String> {
    let mut links = Vec::new();
    for line in text.lines() {
        let mut rest = line;
        while let Some(start) = rest.find("](") {
            let after = &rest[start + 2..];
            let Some(end) = after.find(')') else {
                break;
            };
            links.push(after[..end].to_string());
            rest = &after[end + 1..];
        }
    }
    links
}

fn forbidden_path_markers() -> [String; 3] {
    [
        format!("{}/{}/", "docs", "plans"),
        format!("{}_{}/", "release", "evidence"),
        format!("{}/{}/{}/", "docs", "catalog-metastore", "evidence"),
    ]
}

fn banned_term_tokens() -> [String; 7] {
    let ai_suffix: String = ['a', 'i'].into_iter().collect();
    let legacy_agent_token = ["super", "powers"].concat();
    [
        ["co", "dex"].concat(),
        ["cla", "ude"].concat(),
        ["g", "pt"].concat(),
        format!("open{ai_suffix}"),
        ["anth", "ropic"].concat(),
        legacy_agent_token.clone(),
        format!("/{legacy_agent_token}"),
    ]
}

fn scan_text_for_banned_terms(text: &str) -> Vec<String> {
    let mut hits = Vec::new();
    let lower = text.to_ascii_lowercase();
    for term in banned_term_tokens() {
        let contains = if term.starts_with('/') {
            lower.contains(&term)
        } else {
            contains_word_term(&lower, &term)
        };
        if contains {
            hits.push(term);
        }
    }

    if contains_standalone_ai(text) {
        let short_token: String = ['a', 'i'].into_iter().collect();
        hits.push(short_token);
    }

    hits
}

fn contains_word_term(lower_text: &str, term: &str) -> bool {
    if term.is_empty() {
        return false;
    }

    let text_bytes = lower_text.as_bytes();
    let mut start = 0usize;
    while let Some(found) = lower_text[start..].find(term) {
        let idx = start + found;
        let term_end = idx + term.len();

        let prev_is_word = idx > 0 && is_word_byte(text_bytes[idx - 1]);
        let next_is_word = term_end < text_bytes.len() && is_word_byte(text_bytes[term_end]);
        if !prev_is_word && !next_is_word {
            return true;
        }

        start = idx + 1;
    }

    false
}

fn contains_standalone_ai(text: &str) -> bool {
    let bytes = text.as_bytes();
    if bytes.len() < 2 {
        return false;
    }

    for idx in 0..(bytes.len() - 1) {
        if !bytes[idx].eq_ignore_ascii_case(&b'a') || !bytes[idx + 1].eq_ignore_ascii_case(&b'i')
        {
            continue;
        }

        let prev_is_word = idx > 0 && is_word_byte(bytes[idx - 1]);
        let next_is_word = idx + 2 < bytes.len() && is_word_byte(bytes[idx + 2]);
        if !prev_is_word && !next_is_word {
            return true;
        }
    }

    false
}

fn is_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
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
    (
        "adr-006-schema-evolution.md",
        "Parquet schema evolution policy",
    ),
];

fn run_adr_check() -> Result<()> {
    println!("Checking ADR conformance...\n");

    let adr_dir = Path::new("docs/adr");
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

fn run_parity_matrix_check() -> Result<()> {
    println!("Validating parity matrix evidence...\n");

    let matrix_path = "docs/parity/dagster-parity-matrix.md";
    let content = std::fs::read_to_string(matrix_path)
        .with_context(|| format!("Parity matrix not found: {matrix_path}"))?;

    let mut current_section: Option<String> = None;
    let mut header: Option<HashMap<String, usize>> = None;
    let mut in_table = false;
    let mut errors = Vec::new();

    for (idx, raw_line) in content.lines().enumerate() {
        let line_no = idx + 1;
        let line = raw_line.trim();

        if let Some(section) = line.strip_prefix("## ") {
            current_section = Some(section.trim().to_string());
            header = None;
            in_table = false;
            continue;
        }

        if line.starts_with('|') && line.contains("Capability") && line.contains("Status") {
            let headers = split_md_table_row(line);
            let header_map: HashMap<String, usize> = headers
                .into_iter()
                .enumerate()
                .map(|(i, h)| (h, i))
                .collect();

            let required_cols = [
                "Capability",
                "Status",
                "Evidence (Code)",
                "Evidence (Tests)",
                "Evidence (CI)",
            ];
            for col in required_cols {
                if !header_map.contains_key(col) {
                    errors.push(format!(
                        "{matrix_path}:{line_no}: missing required column '{col}' in table header"
                    ));
                }
            }

            header = Some(header_map);
            in_table = true;
            continue;
        }

        if !line.starts_with('|') {
            in_table = false;
            continue;
        }

        if !in_table {
            continue;
        }

        if line.starts_with("|---") || line.contains("|---|") {
            continue;
        }

        let Some(header) = header.as_ref() else {
            continue;
        };

        let row = split_md_table_row(line);

        let get = |name: &str| -> Option<&str> {
            header
                .get(name)
                .and_then(|idx| row.get(*idx))
                .map(String::as_str)
        };

        let status = get("Status").unwrap_or("").trim();
        if !status.starts_with("Implemented") {
            continue;
        }

        let capability = get("Capability").unwrap_or("<missing capability>").trim();
        let code = get("Evidence (Code)").unwrap_or("").trim();
        let tests = get("Evidence (Tests)").unwrap_or("").trim();
        let ci = get("Evidence (CI)").unwrap_or("").trim();

        let location = match current_section.as_deref() {
            Some(section) => format!("{matrix_path}:{line_no} [{section}] {capability}"),
            None => format!("{matrix_path}:{line_no} {capability}"),
        };

        if code.is_empty() {
            errors.push(format!("{location}: missing Evidence (Code)"));
        }
        if tests.is_empty() {
            errors.push(format!("{location}: missing Evidence (Tests)"));
        }
        if ci.is_empty() {
            errors.push(format!("{location}: missing Evidence (CI)"));
        }

        let legacy_plans_marker = format!("{}/{}/", "docs", "plans");
        for field in [code, tests, ci] {
            if field.contains(&legacy_plans_marker) {
                errors.push(format!(
                    "{location}: contains forbidden legacy planning reference in evidence"
                ));
            }
        }

        if !ci.contains(".github/workflows/ci.yml") {
            errors.push(format!(
                "{location}: Evidence (CI) must reference .github/workflows/ci.yml"
            ));
        }

        let tests_looks_real = tests.contains("pytest")
            || tests.contains("cargo test")
            || tests.contains("--test")
            || tests.contains("crates/")
            || tests.contains(".rs:")
            || tests.contains("test_")
            || tests.contains("parity_");
        if !tests_looks_real {
            errors.push(format!(
                "{location}: Evidence (Tests) must point at a CI-gated test (expected file path, 'cargo test', 'pytest', or a test identifier)"
            ));
        }

        let ci_looks_real = ci.contains("cargo ") || ci.contains("pytest");
        if !ci_looks_real {
            errors.push(format!(
                "{location}: Evidence (CI) must include the job command (expected 'cargo …' or 'pytest …')"
            ));
        }

        for evidence_field in [code, tests, ci] {
            for span in extract_backtick_spans(evidence_field) {
                if let Some(path) = normalize_repo_path(&span) {
                    if !Path::new(&path).exists() {
                        errors.push(format!(
                            "{location}: evidence references missing path '{path}'"
                        ));
                    }
                }
            }
        }
    }

    if !errors.is_empty() {
        println!("Parity matrix evidence errors:");
        for err in &errors {
            println!("  - {err}");
        }
        anyhow::bail!("Parity matrix check failed with {} error(s)", errors.len());
    }

    println!("Parity matrix evidence looks good!");
    Ok(())
}

fn split_md_table_row(line: &str) -> Vec<String> {
    line.trim()
        .trim_start_matches('|')
        .trim_end_matches('|')
        .split('|')
        .map(|cell| cell.trim().to_string())
        .collect()
}

fn extract_backtick_spans(text: &str) -> Vec<String> {
    let mut spans = Vec::new();
    let mut in_ticks = false;
    let mut buf = String::new();

    for ch in text.chars() {
        if ch == '`' {
            if in_ticks {
                spans.push(buf.clone());
                buf.clear();
            }
            in_ticks = !in_ticks;
            continue;
        }

        if in_ticks {
            buf.push(ch);
        }
    }

    spans
}

fn normalize_repo_path(span: &str) -> Option<String> {
    let candidate = span.trim();

    let (prefix, rest) = candidate.split_once(':').unwrap_or((candidate, ""));
    let _ = rest;

    let path = prefix.trim();
    if path.is_empty() {
        return None;
    }

    let is_repo_path = path.starts_with("crates/")
        || path.starts_with("python/")
        || path.starts_with("docs/")
        || path.starts_with(".github/");

    if !is_repo_path {
        return None;
    }

    Some(path.to_string())
}

// ============================================================================
// Integrity Verification
// ============================================================================

/// Golden schema definition for validation
#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)] // Fields used for schema validation via deserialization
struct GoldenSchema {
    name: String,
    version: u32,
    fields: Vec<GoldenField>,
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)] // Fields used for schema validation via deserialization
struct GoldenField {
    name: String,
    data_type: String,
    nullable: bool,
}

/// Expected golden schema files
const GOLDEN_SCHEMAS: &[&str] = &["namespaces", "tables", "columns", "lineage_edges"];

type CheckResult = (Vec<String>, Vec<String>);

fn run_verify_integrity(
    verbose: bool,
    dry_run: bool,
    lock_strict: bool,
    tenant: Option<String>,
    workspace: Option<String>,
    bucket: Option<String>,
) -> Result<()> {
    println!("Verifying catalog integrity...\n");

    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    // 1. Verify golden schema files exist and are valid
    println!("=== Golden Schema Files ===\n");
    for name in GOLDEN_SCHEMAS {
        print!("  {name}.schema.json... ");
        match verify_golden_schema(name) {
            Ok(schema) => {
                if verbose {
                    println!(
                        "[ok] {} fields, version {}",
                        schema.fields.len(),
                        schema.version
                    );
                } else {
                    println!("[ok]");
                }
            }
            Err(e) => {
                println!("[FAIL]");
                errors.push(format!("Golden schema '{name}': {e}"));
            }
        }
    }

    // 2. Verify ADR-006 (schema evolution policy) exists
    println!("\n=== Schema Evolution Policy ===\n");
    print!("  ADR-006 exists... ");
    let adr_path = Path::new("docs/adr/adr-006-schema-evolution.md");
    if adr_path.exists() {
        println!("[ok]");
    } else {
        println!("[FAIL]");
        errors.push("ADR-006 (schema evolution policy) not found".to_string());
    }

    // 3. Verify schema contract tests exist
    println!("\n=== Schema Contract Tests ===\n");
    print!("  schema_contracts.rs exists... ");
    let test_path = Path::new("crates/arco-catalog/tests/schema_contracts.rs");
    if test_path.exists() {
        println!("[ok]");
    } else {
        println!("[FAIL]");
        errors.push("Schema contract tests not found".to_string());
    }

    // 4. Run schema compatibility tests
    println!("\n=== Schema Compatibility Tests ===\n");
    print!("  Running cargo test schema_backward_compatible... ");
    match run_schema_tests() {
        Ok(()) => println!("[ok]"),
        Err(e) => {
            println!("[FAIL]");
            errors.push(format!("Schema compatibility tests failed: {e}"));
        }
    }

    // 5. Verify PR template has invariant checklist
    println!("\n=== PR Template ===\n");
    print!("  Invariant checklist present... ");
    match verify_pr_template() {
        Ok(()) => println!("[ok]"),
        Err(e) => {
            println!("[FAIL]");
            errors.push(format!("PR template: {e}"));
        }
    }

    // 6. Verify CatalogPaths module exists (no hardcoded paths)
    println!("\n=== Path Canonicalization ===\n");
    print!("  CatalogPaths module exists... ");
    let paths_module = Path::new("crates/arco-core/src/catalog_paths.rs");
    if paths_module.exists() {
        println!("[ok]");
    } else {
        println!("[FAIL]");
        errors.push("CatalogPaths module not found".to_string());
    }

    if let (Some(tenant), Some(workspace)) = (tenant.as_deref(), workspace.as_deref()) {
        println!("\n=== Workspace Integrity ===\n");
        match run_workspace_integrity(tenant, workspace, bucket.as_deref(), verbose, lock_strict) {
            Ok((storage_errors, storage_warnings)) => {
                errors.extend(storage_errors);
                warnings.extend(storage_warnings);
            }
            Err(e) => errors.push(format!("Workspace integrity checks failed: {e}")),
        }
    } else {
        let message = if bucket.is_some() {
            "Storage integrity checks skipped: --bucket provided without --tenant/--workspace"
        } else {
            "Storage integrity checks skipped: provide --tenant and --workspace plus ARCO_STORAGE_BUCKET or --bucket"
        };
        warnings.push(message.to_string());
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

    // Print errors
    if !errors.is_empty() {
        println!("Errors:");
        for e in &errors {
            println!("  - {e}");
        }
        println!();

        if dry_run {
            println!(
                "Dry run mode: {} error(s) found but not failing.",
                errors.len()
            );
            return Ok(());
        }

        anyhow::bail!(
            "Integrity verification failed with {} error(s).",
            errors.len()
        );
    }

    println!("All integrity checks passed!");
    Ok(())
}

fn run_workspace_integrity(
    tenant: &str,
    workspace: &str,
    bucket_override: Option<&str>,
    verbose: bool,
    lock_strict: bool,
) -> Result<CheckResult> {
    let bucket = resolve_bucket(bucket_override)?;
    println!("  Scope: tenant={tenant}, workspace={workspace}");
    println!("  Bucket: {bucket}");

    let backend = ObjectStoreBackend::from_bucket(&bucket)
        .with_context(|| format!("Failed to configure storage backend for '{bucket}'"))?;
    let storage = ScopedStorage::new(Arc::new(backend), tenant, workspace)
        .context("Failed to create scoped storage")?;

    let runtime = Runtime::new().context("Failed to create tokio runtime")?;
    runtime.block_on(verify_workspace(storage, verbose, lock_strict))
}

fn resolve_bucket(bucket_override: Option<&str>) -> Result<String> {
    if let Some(bucket) = bucket_override {
        let trimmed = bucket.trim();
        if trimmed.is_empty() {
            anyhow::bail!("--bucket cannot be empty");
        }
        return Ok(trimmed.to_string());
    }

    if let Ok(bucket) = env::var("ARCO_STORAGE_BUCKET") {
        let trimmed = bucket.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }

    anyhow::bail!(
        "ARCO_STORAGE_BUCKET is required for storage integrity checks (or pass --bucket)"
    );
}

async fn verify_workspace(
    storage: ScopedStorage,
    verbose: bool,
    lock_strict: bool,
) -> Result<CheckResult> {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    print!("  Root manifest... ");
    let root = match read_json::<RootManifest>(&storage, CatalogPaths::ROOT_MANIFEST).await {
        Ok(root) => {
            println!("[ok]");
            root
        }
        Err(e) => {
            println!("[FAIL]");
            errors.push(format!("Root manifest: {e}"));
            return Ok((errors, warnings));
        }
    };

    check_root_paths(&root, &mut warnings);

    print!("  Domain manifests... ");
    let mut domain_errors = Vec::new();
    let catalog =
        match read_json::<CatalogDomainManifest>(&storage, &root.catalog_manifest_path).await {
            Ok(manifest) => Some(manifest),
            Err(e) => {
                domain_errors.push(format!(
                    "Catalog manifest ({}): {e}",
                    root.catalog_manifest_path
                ));
                None
            }
        };
    let lineage = match read_json::<LineageManifest>(&storage, &root.lineage_manifest_path).await {
        Ok(manifest) => Some(manifest),
        Err(e) => {
            domain_errors.push(format!(
                "Lineage manifest ({}): {e}",
                root.lineage_manifest_path
            ));
            None
        }
    };
    let executions =
        match read_json::<ExecutionsManifest>(&storage, &root.executions_manifest_path).await {
            Ok(manifest) => Some(manifest),
            Err(e) => {
                domain_errors.push(format!(
                    "Executions manifest ({}): {e}",
                    root.executions_manifest_path
                ));
                None
            }
        };
    let search = match read_json::<SearchManifest>(&storage, &root.search_manifest_path).await {
        Ok(manifest) => Some(manifest),
        Err(e) => {
            domain_errors.push(format!(
                "Search manifest ({}): {e}",
                root.search_manifest_path
            ));
            None
        }
    };

    if domain_errors.is_empty() {
        println!("[ok]");
    } else {
        println!("[FAIL]");
        errors.extend(domain_errors);
    }

    print!("  Commit chain (catalog)... ");
    if let Some(catalog) = &catalog {
        if let Some(last_commit_id) = catalog.last_commit_id.as_deref() {
            let (chain_errors, commit_count) = verify_commit_chain(&storage, last_commit_id).await;
            if chain_errors.is_empty() {
                if verbose {
                    println!("[ok] {commit_count} commits");
                } else {
                    println!("[ok]");
                }
            } else {
                println!("[FAIL]");
                errors.extend(chain_errors);
            }
        } else {
            println!("[ok] (no commits)");
        }
    } else {
        println!("[SKIP]");
        warnings.push("Catalog manifest missing; skipping commit chain verification".to_string());
    }

    print!("  Catalog snapshot... ");
    if let Some(catalog) = &catalog {
        let (snap_errors, snap_warnings) = verify_catalog_snapshot(&storage, catalog).await;
        if snap_errors.is_empty() {
            println!("[ok]");
        } else {
            println!("[FAIL]");
            errors.extend(snap_errors);
        }
        warnings.extend(snap_warnings);
    } else {
        println!("[SKIP]");
        warnings.push("Catalog manifest missing; skipping catalog snapshot checks".to_string());
    }

    print!("  Lineage snapshot... ");
    if let Some(lineage) = &lineage {
        let (snap_errors, snap_warnings) = verify_lineage_snapshot(&storage, lineage).await;
        if snap_errors.is_empty() {
            println!("[ok]");
        } else {
            println!("[FAIL]");
            errors.extend(snap_errors);
        }
        warnings.extend(snap_warnings);
    } else {
        println!("[SKIP]");
        warnings.push("Lineage manifest missing; skipping lineage snapshot checks".to_string());
    }

    print!("  Executions state... ");
    if let Some(executions) = &executions {
        let (exec_errors, exec_warnings) = verify_executions_state(&storage, executions).await;
        if exec_errors.is_empty() {
            println!("[ok]");
        } else {
            println!("[FAIL]");
            errors.extend(exec_errors);
        }
        warnings.extend(exec_warnings);
    } else {
        println!("[SKIP]");
        warnings.push("Executions manifest missing; skipping executions checks".to_string());
    }

    print!("  Search state... ");
    if let Some(search) = &search {
        let (search_errors, search_warnings) = verify_search_state(search);
        if search_errors.is_empty() {
            println!("[ok]");
        } else {
            println!("[FAIL]");
            errors.extend(search_errors);
        }
        warnings.extend(search_warnings);
    } else {
        println!("[SKIP]");
        warnings.push("Search manifest missing; skipping search checks".to_string());
    }

    print!("  Lock freshness... ");
    let (lock_errors, lock_warnings) = verify_locks(&storage, verbose, lock_strict).await;
    if lock_errors.is_empty() {
        println!("[ok]");
    } else {
        println!("[FAIL]");
        errors.extend(lock_errors);
    }
    warnings.extend(lock_warnings);

    Ok((errors, warnings))
}

fn check_root_paths(root: &RootManifest, warnings: &mut Vec<String>) {
    let expected_catalog = CatalogPaths::domain_manifest(CatalogDomain::Catalog);
    if root.catalog_manifest_path != expected_catalog {
        warnings.push(format!(
            "Root manifest catalog path '{}' is not canonical (expected '{}')",
            root.catalog_manifest_path, expected_catalog
        ));
    }

    let expected_lineage = CatalogPaths::domain_manifest(CatalogDomain::Lineage);
    if root.lineage_manifest_path != expected_lineage {
        warnings.push(format!(
            "Root manifest lineage path '{}' is not canonical (expected '{}')",
            root.lineage_manifest_path, expected_lineage
        ));
    }

    let expected_executions = CatalogPaths::domain_manifest(CatalogDomain::Executions);
    if root.executions_manifest_path != expected_executions {
        warnings.push(format!(
            "Root manifest executions path '{}' is not canonical (expected '{}')",
            root.executions_manifest_path, expected_executions
        ));
    }

    let expected_search = CatalogPaths::domain_manifest(CatalogDomain::Search);
    if root.search_manifest_path != expected_search {
        warnings.push(format!(
            "Root manifest search path '{}' is not canonical (expected '{}')",
            root.search_manifest_path, expected_search
        ));
    }
}

async fn verify_catalog_snapshot(
    storage: &ScopedStorage,
    manifest: &CatalogDomainManifest,
) -> CheckResult {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    let expected_path =
        CatalogPaths::snapshot_dir(CatalogDomain::Catalog, manifest.snapshot_version);
    if manifest.snapshot_path != expected_path {
        errors.push(format!(
            "Catalog snapshot path '{}' does not match expected '{}'",
            manifest.snapshot_path, expected_path
        ));
    }

    match &manifest.snapshot {
        Some(snapshot) => {
            if snapshot.version != manifest.snapshot_version {
                errors.push(format!(
                    "Catalog snapshot version mismatch: manifest {}, snapshot {}",
                    manifest.snapshot_version, snapshot.version
                ));
            }
            if snapshot.path != manifest.snapshot_path {
                errors.push(format!(
                    "Catalog snapshot path mismatch: manifest '{}', snapshot '{}'",
                    manifest.snapshot_path, snapshot.path
                ));
            }
            let (file_errors, file_warnings) = verify_snapshot_files(storage, snapshot).await;
            errors.extend(file_errors);
            warnings.extend(file_warnings);
        }
        None => {
            if manifest.snapshot_version > 0 {
                errors.push(format!(
                    "Catalog snapshot metadata missing for version {}",
                    manifest.snapshot_version
                ));
            }
        }
    }

    (errors, warnings)
}

async fn verify_lineage_snapshot(
    storage: &ScopedStorage,
    manifest: &LineageManifest,
) -> CheckResult {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    let expected_path =
        CatalogPaths::snapshot_dir(CatalogDomain::Lineage, manifest.snapshot_version);
    if manifest.edges_path != expected_path {
        errors.push(format!(
            "Lineage snapshot path '{}' does not match expected '{}'",
            manifest.edges_path, expected_path
        ));
    }

    match &manifest.snapshot {
        Some(snapshot) => {
            if snapshot.version != manifest.snapshot_version {
                errors.push(format!(
                    "Lineage snapshot version mismatch: manifest {}, snapshot {}",
                    manifest.snapshot_version, snapshot.version
                ));
            }
            if snapshot.path != manifest.edges_path {
                errors.push(format!(
                    "Lineage snapshot path mismatch: manifest '{}', snapshot '{}'",
                    manifest.edges_path, snapshot.path
                ));
            }
            let (file_errors, file_warnings) = verify_snapshot_files(storage, snapshot).await;
            errors.extend(file_errors);
            warnings.extend(file_warnings);
        }
        None => {
            if manifest.snapshot_version > 0 {
                errors.push(format!(
                    "Lineage snapshot metadata missing for version {}",
                    manifest.snapshot_version
                ));
            }
        }
    }

    (errors, warnings)
}

async fn verify_executions_state(
    storage: &ScopedStorage,
    manifest: &ExecutionsManifest,
) -> CheckResult {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    let state_dir = CatalogPaths::state_dir(CatalogDomain::Executions);
    if !manifest.checkpoint_path.starts_with(&state_dir) {
        errors.push(format!(
            "Executions checkpoint path '{}' is not under '{}'",
            manifest.checkpoint_path, state_dir
        ));
    }

    let needs_checkpoint = manifest.watermark_version > 0 || manifest.snapshot_version > 0;
    if needs_checkpoint {
        match storage.head_raw(&manifest.checkpoint_path).await {
            Ok(Some(_)) => {}
            Ok(None) => errors.push(format!(
                "Executions checkpoint missing: {}",
                manifest.checkpoint_path
            )),
            Err(e) => errors.push(format!(
                "Failed to read executions checkpoint '{}': {e}",
                manifest.checkpoint_path
            )),
        }
    }

    match manifest.snapshot_path.as_deref() {
        Some(snapshot_path) => {
            if !snapshot_path.starts_with(&state_dir) {
                errors.push(format!(
                    "Executions snapshot path '{}' is not under '{}'",
                    snapshot_path, state_dir
                ));
            }
            if !snapshot_path.ends_with(".parquet") {
                errors.push(format!(
                    "Executions snapshot path '{}' does not end with .parquet",
                    snapshot_path
                ));
            }
            match storage.head_raw(snapshot_path).await {
                Ok(Some(_)) => {}
                Ok(None) => errors.push(format!("Executions snapshot missing: {}", snapshot_path)),
                Err(e) => errors.push(format!(
                    "Failed to read executions snapshot '{}': {e}",
                    snapshot_path
                )),
            }
            if manifest.snapshot_version == 0 {
                warnings
                    .push("Executions snapshot_path set while snapshot_version is 0".to_string());
            }
        }
        None => {
            if manifest.snapshot_version > 0 {
                errors.push(format!(
                    "Executions snapshot_version is {} but snapshot_path is None",
                    manifest.snapshot_version
                ));
            }
        }
    }

    (errors, warnings)
}

fn verify_search_state(manifest: &SearchManifest) -> CheckResult {
    let mut errors = Vec::new();
    let warnings = Vec::new();

    let expected_path =
        CatalogPaths::snapshot_dir(CatalogDomain::Search, manifest.snapshot_version);
    if manifest.base_path != expected_path {
        errors.push(format!(
            "Search base_path '{}' does not match expected '{}'",
            manifest.base_path, expected_path
        ));
    }

    (errors, warnings)
}

async fn verify_snapshot_files(storage: &ScopedStorage, snapshot: &SnapshotInfo) -> CheckResult {
    let mut errors = Vec::new();
    let warnings = Vec::new();

    if snapshot.files.is_empty() {
        errors.push(format!("Snapshot '{}' has no files", snapshot.path));
        return (errors, warnings);
    }

    let mut seen: HashSet<String> = HashSet::new();
    let mut total_rows = 0u64;
    let mut total_bytes = 0u64;

    for file in &snapshot.files {
        if file.path.trim().is_empty() {
            errors.push(format!(
                "Snapshot '{}' has a file with empty path",
                snapshot.path
            ));
            continue;
        }

        if !seen.insert(file.path.clone()) {
            errors.push(format!(
                "Snapshot '{}' has duplicate file entry '{}'",
                snapshot.path, file.path
            ));
        }

        let full_path = join_snapshot_path(&snapshot.path, &file.path);
        let bytes = match storage.get_raw(&full_path).await {
            Ok(bytes) => bytes,
            Err(e) if is_not_found(&e) => {
                errors.push(format!("Snapshot file missing: {full_path}"));
                continue;
            }
            Err(e) => {
                errors.push(format!("Failed to read snapshot file '{full_path}': {e}"));
                continue;
            }
        };

        let actual_size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if actual_size != file.byte_size {
            errors.push(format!(
                "Snapshot file size mismatch for '{}': manifest {}, actual {}",
                full_path, file.byte_size, actual_size
            ));
        }

        let actual_checksum = sha256_hex(bytes.as_ref());
        if actual_checksum != file.checksum_sha256 {
            errors.push(format!(
                "Snapshot checksum mismatch for '{}': manifest {}, actual {}",
                full_path, file.checksum_sha256, actual_checksum
            ));
        }

        total_rows = total_rows.saturating_add(file.row_count);
        total_bytes = total_bytes.saturating_add(file.byte_size);
    }

    if snapshot.total_rows != total_rows {
        errors.push(format!(
            "Snapshot '{}' total_rows mismatch: manifest {}, sum {}",
            snapshot.path, snapshot.total_rows, total_rows
        ));
    }

    if snapshot.total_bytes != total_bytes {
        errors.push(format!(
            "Snapshot '{}' total_bytes mismatch: manifest {}, sum {}",
            snapshot.path, snapshot.total_bytes, total_bytes
        ));
    }

    (errors, warnings)
}

async fn verify_commit_chain(
    storage: &ScopedStorage,
    last_commit_id: &str,
) -> (Vec<String>, usize) {
    let mut errors = Vec::new();
    let mut visited = HashSet::new();
    let mut expected_hash: Option<String> = None;
    let mut current = Some(last_commit_id.to_string());
    let mut count = 0usize;

    while let Some(commit_id) = current {
        if !visited.insert(commit_id.clone()) {
            errors.push(format!("Commit chain cycle detected at {commit_id}"));
            break;
        }

        let path = CatalogPaths::commit(CatalogDomain::Catalog, &commit_id);
        let bytes = match storage.get_raw(&path).await {
            Ok(bytes) => bytes,
            Err(e) if is_not_found(&e) => {
                errors.push(format!("Commit record missing: {path}"));
                break;
            }
            Err(e) => {
                errors.push(format!("Failed to read commit record '{path}': {e}"));
                break;
            }
        };

        let record: CommitRecord = match serde_json::from_slice(&bytes) {
            Ok(record) => record,
            Err(e) => {
                errors.push(format!("Invalid commit JSON at '{}': {e}", path));
                break;
            }
        };

        let actual_hash = record.compute_hash();
        if let Some(expected) = expected_hash.as_deref() {
            if expected != actual_hash {
                let legacy_hash = sha256_prefixed(bytes.as_ref());
                if expected == legacy_hash {
                    errors.push(format!(
                        "Commit hash mismatch for '{}': expected {}, got {} (matches legacy raw JSON hash)",
                        path, expected, actual_hash
                    ));
                } else {
                    errors.push(format!(
                        "Commit hash mismatch for '{}': expected {}, got {}",
                        path, expected, actual_hash
                    ));
                }
            }
        }

        if record.commit_id != commit_id {
            errors.push(format!(
                "Commit ID mismatch at '{}': expected {}, found {}",
                path, commit_id, record.commit_id
            ));
        }

        if record.prev_commit_id.is_none() && record.prev_commit_hash.is_some() {
            errors.push(format!(
                "Commit '{}' has prev_commit_hash but no prev_commit_id",
                commit_id
            ));
        }
        if record.prev_commit_id.is_some() && record.prev_commit_hash.is_none() {
            errors.push(format!(
                "Commit '{}' has prev_commit_id but no prev_commit_hash",
                commit_id
            ));
        }

        expected_hash = record.prev_commit_hash.clone();
        current = record.prev_commit_id.clone();
        count += 1;
    }

    (errors, count)
}

async fn verify_locks(storage: &ScopedStorage, verbose: bool, lock_strict: bool) -> CheckResult {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    for domain in CatalogDomain::all() {
        let path = CatalogPaths::domain_lock(*domain);
        let bytes = match storage.get_raw(&path).await {
            Ok(bytes) => bytes,
            Err(e) if is_not_found(&e) => continue,
            Err(e) => {
                errors.push(format!("Failed to read lock '{path}': {e}"));
                continue;
            }
        };

        let info: LockInfo = match serde_json::from_slice(&bytes) {
            Ok(info) => info,
            Err(e) => {
                errors.push(format!("Invalid lock JSON at '{path}': {e}"));
                continue;
            }
        };

        if info.is_expired() {
            // Expired locks are typically debris from crashed processes.
            // Only treat as error if expired > 1 hour ago (very stale).
            let expired_duration = Utc::now() - info.expires_at;
            let stale_threshold = chrono::Duration::hours(1);

            if expired_duration > stale_threshold {
                errors.push(format!(
                    "Lock '{}' expired {} ago (stale - consider cleanup)",
                    path,
                    format_duration(expired_duration)
                ));
            } else {
                warnings.push(format!(
                    "Lock '{}' expired {} ago (likely crashed process)",
                    path,
                    format_duration(expired_duration)
                ));
            }
        } else {
            let message = if verbose {
                format!(
                    "Lock '{}' held by {} (expires in {:?})",
                    path,
                    info.holder_id,
                    info.remaining_ttl()
                )
            } else {
                format!("Lock '{}' is active (held by {})", path, info.holder_id)
            };
            if lock_strict {
                errors.push(message);
            } else {
                warnings.push(message);
            }
        }
    }

    (errors, warnings)
}

async fn read_json<T: DeserializeOwned>(storage: &ScopedStorage, path: &str) -> Result<T> {
    let bytes = storage
        .get_raw(path)
        .await
        .with_context(|| format!("Failed to read '{path}'"))?;
    serde_json::from_slice(&bytes).with_context(|| format!("Invalid JSON at '{path}'"))
}

fn join_snapshot_path(dir: &str, file: &str) -> String {
    let file = file.trim_start_matches('/');
    if dir.ends_with('/') {
        format!("{dir}{file}")
    } else {
        format!("{dir}/{file}")
    }
}

fn is_not_found(err: &CoreError) -> bool {
    matches!(
        err,
        CoreError::NotFound(_) | CoreError::ResourceNotFound { .. }
    )
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    let hash = Sha256::digest(bytes);
    format!("sha256:{}", hex::encode(hash))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let hash = Sha256::digest(bytes);
    hex::encode(hash)
}

/// Formats a chrono Duration as a human-readable string.
fn format_duration(d: chrono::Duration) -> String {
    let hours = d.num_hours();
    let minutes = d.num_minutes() % 60;

    if hours > 0 {
        format!("{hours}h {minutes}m")
    } else if minutes > 0 {
        format!("{minutes}m")
    } else {
        let seconds = d.num_seconds() % 60;
        format!("{seconds}s")
    }
}

fn verify_golden_schema(name: &str) -> Result<GoldenSchema> {
    let path = format!("crates/arco-catalog/tests/golden_schemas/{name}.schema.json");
    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read golden schema: {path}"))?;

    let schema: GoldenSchema = serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse golden schema JSON: {path}"))?;

    // Validate schema has fields
    if schema.fields.is_empty() {
        anyhow::bail!("Schema has no fields");
    }

    Ok(schema)
}

fn run_schema_tests() -> Result<()> {
    let output = Command::new("cargo")
        .args([
            "test",
            "-p",
            "arco-catalog",
            "--test",
            "schema_contracts",
            "--",
            "--test-threads=1",
        ])
        .output()
        .context("Failed to run schema tests")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("{}", stderr.lines().take(10).collect::<Vec<_>>().join("\n"));
    }

    Ok(())
}

fn verify_pr_template() -> Result<()> {
    let path = ".github/pull_request_template.md";
    let content = std::fs::read_to_string(path).context("PR template not found")?;

    let required_sections = [
        "Invariant Checklist",
        "Architecture Invariants",
        "Two-tier consistency",
        "Schema evolution rules",
    ];

    for section in required_sections {
        if !content.contains(section) {
            anyhow::bail!("Missing section: {section}");
        }
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standalone_ai_detection_requires_word_boundaries() {
        let ai_upper: String = ['A', 'I'].into_iter().collect();
        let ai_lower: String = ['a', 'i'].into_iter().collect();

        assert!(contains_standalone_ai(&format!("{ai_upper} policy")));
        assert!(contains_standalone_ai(&format!("this is {ai_lower}.")));
        assert!(!contains_standalone_ai("said"));
        assert!(!contains_standalone_ai("rail"));
    }

    #[test]
    fn banned_term_scan_finds_vendor_tokens_case_insensitively() {
        let ai_title: String = ['A', 'I'].into_iter().collect();
        let ai_lower: String = ['a', 'i'].into_iter().collect();
        let vendor = format!("Open{ai_title}");
        let model = ["g", "Pt"].concat();
        let openai_lower = format!("open{ai_lower}");
        let gpt_lower = ["g", "pt"].concat();

        let hits = scan_text_for_banned_terms(&format!("{vendor} and {model} are forbidden."));
        assert!(hits.iter().any(|hit| hit.contains(&openai_lower)));
        assert!(hits.iter().any(|hit| hit.contains(&gpt_lower)));
    }

    #[test]
    fn banned_term_scan_avoids_substring_false_positives() {
        let tag_word = ["ta", "gp", "tr"].concat();
        let hits = scan_text_for_banned_terms(&format!("dependency name: {tag_word}"));
        assert!(hits.is_empty());
    }

    #[test]
    fn summary_link_parser_extracts_md_links() {
        let summary = "- [Intro](./intro.md)\n- [API](reference/api.md)\n";
        let links = extract_summary_links(summary);
        assert_eq!(links, vec!["./intro.md".to_string(), "reference/api.md".to_string()]);
    }

    #[test]
    fn forbidden_path_markers_include_removed_evidence_tree() {
        let markers = forbidden_path_markers();
        let catalog_evidence = format!("{}/{}/{}/", "docs", "catalog-metastore", "evidence");
        let plans = format!("{}/{}/", "docs", "plans");
        let release_evidence = format!("{}_{}/", "release", "evidence");
        assert!(
            markers.iter().any(|marker| marker == &catalog_evidence),
            "expected removed catalog evidence tree to be blocked"
        );
        assert!(
            markers.iter().any(|marker| marker == &plans),
            "expected removed planning tree to be blocked"
        );
        assert!(
            markers.iter().any(|marker| marker == &release_evidence),
            "expected removed release evidence tree to be blocked"
        );
    }
}
