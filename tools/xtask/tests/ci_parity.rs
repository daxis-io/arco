use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[test]
fn doctor_uses_repo_root_buf_yaml() {
    let harness = ToolHarness::new().expect("create fake tool harness");

    let output = harness.run_xtask(["doctor"]);

    assert!(
        output.status.success(),
        "xtask doctor should succeed with repo-root buf.yaml:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn doctor_rejects_buf_version_that_differs_from_ci() {
    let harness = ToolHarness::new_with_buf_version("1.62.1").expect("create fake tool harness");

    let output = harness.run_xtask(["doctor"]);
    let output_text = command_output_text(&output);

    assert!(
        !output.status.success(),
        "xtask doctor should fail when local buf differs from CI:\n{output_text}"
    );
    assert!(
        output_text.contains("version mismatch"),
        "expected buf version mismatch to be reported:\n{output_text}"
    );
}

#[test]
fn ci_parity_command_is_registered_and_passes_static_checks() {
    let output = Command::new(env!("CARGO_BIN_EXE_xtask"))
        .arg("ci-parity")
        .current_dir(repo_root())
        .output()
        .expect("run xtask ci-parity");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "xtask ci-parity should be a runnable static parity gate:\n{output_text}"
    );
}

#[test]
fn ci_runs_deterministic_user_acceptance_uat_gate() {
    let ci =
        fs::read_to_string(repo_root().join(".github/workflows/ci.yml")).expect("read CI workflow");

    assert!(
        ci.contains("scripts/run_user_acceptance_pipeline_uat.sh --deterministic"),
        "CI test job should run the deterministic local UAT gate"
    );
}

#[test]
fn ci_runs_proto_contract_checks() {
    let harness = ToolHarness::new().expect("create fake tool harness");

    let output = harness.run_xtask(["ci"]);

    assert!(
        output.status.success(),
        "xtask ci should succeed with fake toolchain:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let log = harness.read_log().expect("read fake tool log");
    assert!(
        log.lines().any(|line| line == "buf lint proto/"),
        "xtask ci should run buf lint like CI does; log:\n{log}"
    );
    assert!(
        log.lines().any(|line| {
            line == "buf breaking proto --against proto-baselines/post-hard-cut-v1.binpb"
        }),
        "xtask ci should run the frozen baseline breaking check; log:\n{log}"
    );
}

#[test]
fn ci_allows_documented_hard_cut_baseline_refreshes() {
    let ci =
        fs::read_to_string(repo_root().join(".github/workflows/ci.yml")).expect("read CI workflow");

    assert!(
        ci.contains("git diff --quiet FETCH_HEAD HEAD -- proto-baselines/post-hard-cut-v1.binpb"),
        "CI should identify PRs that refresh the frozen hard-cut proto baseline"
    );
    assert!(
        ci.contains("git diff --quiet FETCH_HEAD HEAD -- proto/STYLE.md"),
        "CI should require a hard-cut policy update when the proto baseline changes"
    );
    assert!(
        ci.contains(
            "The frozen baseline check above remains authoritative for this hard-cut window."
        ),
        "CI should keep the frozen baseline check authoritative during hard-cut refreshes"
    );
}

#[test]
fn cargo_deny_policy_denies_yanked_crates() {
    let deny_toml = fs::read_to_string(repo_root().join("deny.toml")).expect("read deny.toml");

    assert!(
        deny_toml.contains("yanked = \"deny\""),
        "cargo-deny advisories should fail on yanked crates"
    );
}

#[test]
fn python_ci_uses_locked_uv_resolution() {
    let ci =
        fs::read_to_string(repo_root().join(".github/workflows/ci.yml")).expect("read CI workflow");
    let security_audit =
        fs::read_to_string(repo_root().join(".github/workflows/security-audit.yml"))
            .expect("read security audit workflow");

    for (name, workflow) in [("ci.yml", ci), ("security-audit.yml", security_audit)] {
        assert!(
            workflow.contains("uv sync --locked --extra dev"),
            "{name} should install project dependencies from uv.lock"
        );
        assert!(
            !workflow.contains("pip install -e \".[dev]\""),
            "{name} should not resolve project dependencies with unconstrained pip install"
        );
    }
}

#[test]
fn dependabot_version_updates_are_rate_limited() {
    let dependabot = fs::read_to_string(repo_root().join(".github/dependabot.yml"))
        .expect("read dependabot config");
    let dependabot: serde_yaml::Value =
        serde_yaml::from_str(&dependabot).expect("parse dependabot config");

    let updates = dependabot
        .get("updates")
        .and_then(serde_yaml::Value::as_sequence)
        .expect("dependabot config should define updates");

    for update in updates {
        let ecosystem = yaml_str(update, "package-ecosystem");
        assert_ne!(
            ecosystem, "pip",
            "uv-managed Python projects should use Dependabot's uv ecosystem"
        );

        let schedule = update
            .get("schedule")
            .and_then(serde_yaml::Value::as_mapping)
            .unwrap_or_else(|| panic!("{ecosystem} update should define a schedule"));
        assert_eq!(
            map_str(schedule, "interval"),
            "monthly",
            "{ecosystem} version updates should run on the monthly maintenance train"
        );
        assert!(
            schedule.get(serde_yaml::Value::from("time")).is_some(),
            "{ecosystem} update should use an explicit run time"
        );
        assert_eq!(
            map_str(schedule, "timezone"),
            "America/Indiana/Indianapolis",
            "{ecosystem} update should use the maintainer timezone"
        );

        let open_limit = update
            .get("open-pull-requests-limit")
            .and_then(serde_yaml::Value::as_i64)
            .unwrap_or_else(|| panic!("{ecosystem} update should cap open PRs"));
        assert!(
            open_limit <= 2,
            "{ecosystem} update should keep the version-update queue small"
        );

        assert!(
            update.get("cooldown").is_some(),
            "{ecosystem} update should delay newly released versions"
        );
        assert_eq!(
            yaml_str(update, "rebase-strategy"),
            "disabled",
            "{ecosystem} update should not churn branches on base changes"
        );
    }
}

#[test]
fn release_sbom_waits_for_full_release_tag_ci_success() {
    let workflow = fs::read_to_string(repo_root().join(".github/workflows/release-sbom.yml"))
        .expect("read release SBOM workflow");

    assert!(
        workflow.contains(
            "if [ \"${run_conclusion}\" = \"success\" ] && [ \"${discipline_conclusion}\" = \"success\" ]; then"
        ),
        "release SBOM should require a fully successful release-tag CI run, not only tag discipline"
    );
    assert!(
        !workflow.contains("falling back to successful CI run conclusion"),
        "release SBOM should not bypass release-tag discipline"
    );
}

#[test]
fn workflows_pin_github_actions_to_commit_shas() {
    let mut floating_refs = Vec::new();
    let workflows_dir = repo_root().join(".github/workflows");

    for entry in fs::read_dir(&workflows_dir).expect("read workflows directory") {
        let entry = entry.expect("read workflow entry");
        let path = entry.path();
        let Some(extension) = path.extension().and_then(|extension| extension.to_str()) else {
            continue;
        };
        if extension != "yml" && extension != "yaml" {
            continue;
        }

        let workflow = fs::read_to_string(&path).expect("read workflow");
        for (line_number, line) in workflow.lines().enumerate() {
            let trimmed = line.trim();
            let Some(reference) = trimmed.strip_prefix("- uses: ").or_else(|| {
                trimmed
                    .strip_prefix("uses: ")
                    .filter(|_| !trimmed.starts_with("uses: ./"))
            }) else {
                continue;
            };
            if reference.starts_with("./") {
                continue;
            }
            let Some((_, revision)) = reference.split_once('@') else {
                continue;
            };
            let revision = revision
                .split_whitespace()
                .next()
                .expect("action reference should include a revision");
            if revision.len() != 40 || !revision.chars().all(|c| c.is_ascii_hexdigit()) {
                floating_refs.push(format!(
                    "{}:{} uses {}",
                    path.strip_prefix(repo_root()).unwrap_or(&path).display(),
                    line_number + 1,
                    reference
                ));
            }
        }
    }

    assert!(
        floating_refs.is_empty(),
        "workflow action references should be pinned to commit SHAs:\n{}",
        floating_refs.join("\n")
    );
}

struct ToolHarness {
    _tempdir: TempDir,
    bin_dir: PathBuf,
    log_path: PathBuf,
}

impl ToolHarness {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_buf_version("1.70.0")
    }

    fn new_with_buf_version(buf_version: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let tempdir = TempDir::new()?;
        let bin_dir = tempdir.path().join("bin");
        fs::create_dir(&bin_dir)?;
        let log_path = tempdir.path().join("commands.log");
        fs::write(&log_path, b"")?;

        write_fake_tool(
            &bin_dir.join("cargo"),
            &format!(
                r#"#!/bin/sh
set -eu
printf '%s\n' "cargo $*" >> "{log}"
if [ "$#" -ge 2 ] && [ "$1" = "deny" ] && [ "$2" = "--version" ]; then
  printf 'cargo-deny 0.18.9\n'
fi
"#,
                log = log_path.display()
            ),
        )?;
        write_fake_tool(
            &bin_dir.join("rustc"),
            &format!(
                r#"#!/bin/sh
set -eu
printf '%s\n' "rustc $*" >> "{log}"
if [ "$#" -ge 1 ] && [ "$1" = "--version" ]; then
  printf 'rustc 1.88.0 (fake 2025-01-01)\n'
fi
"#,
                log = log_path.display()
            ),
        )?;
        write_fake_tool(
            &bin_dir.join("buf"),
            &format!(
                r#"#!/bin/sh
set -eu
printf '%s\n' "buf $*" >> "{log}"
if [ "$#" -ge 1 ] && [ "$1" = "--version" ]; then
  printf '{buf_version}\n'
fi
"#,
                log = log_path.display(),
                buf_version = buf_version
            ),
        )?;
        write_fake_tool(
            &bin_dir.join("git"),
            &format!(
                r#"#!/bin/sh
set -eu
printf '%s\n' "git $*" >> "{log}"
if [ "$#" -ge 1 ] && [ "$1" = "ls-files" ]; then
  printf 'buf.yaml\n'
  exit 0
fi
exit 1
"#,
                log = log_path.display()
            ),
        )?;

        Ok(Self {
            _tempdir: tempdir,
            bin_dir,
            log_path,
        })
    }

    fn run_xtask<I, S>(&self, args: I) -> std::process::Output
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
        for arg in args {
            command.arg(arg.as_ref());
        }
        command
            .current_dir(repo_root())
            .env("PATH", fake_path(&self.bin_dir).expect("build fake PATH"))
            .output()
            .expect("run xtask binary")
    }

    fn read_log(&self) -> Result<String, std::io::Error> {
        fs::read_to_string(&self.log_path)
    }
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("xtask crate should live under repo root")
        .to_path_buf()
}

fn fake_path(bin_dir: &Path) -> Result<OsString, env::JoinPathsError> {
    let existing = env::var_os("PATH").unwrap_or_default();
    let mut paths = vec![bin_dir.to_path_buf()];
    paths.extend(env::split_paths(&existing));
    env::join_paths(paths)
}

fn command_output_text(output: &std::process::Output) -> String {
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn yaml_str<'a>(value: &'a serde_yaml::Value, key: &str) -> &'a str {
    value
        .get(key)
        .and_then(serde_yaml::Value::as_str)
        .unwrap_or_else(|| panic!("missing string key `{key}`"))
}

fn map_str<'a>(map: &'a serde_yaml::Mapping, key: &str) -> &'a str {
    map.get(serde_yaml::Value::from(key))
        .and_then(serde_yaml::Value::as_str)
        .unwrap_or_else(|| panic!("missing string key `{key}`"))
}

#[cfg(unix)]
fn write_fake_tool(path: &Path, contents: &str) -> Result<(), Box<dyn std::error::Error>> {
    fs::write(path, contents)?;
    let mut perms = fs::metadata(path)?.permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms)?;
    Ok(())
}
