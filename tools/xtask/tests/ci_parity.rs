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

struct ToolHarness {
    _tempdir: TempDir,
    bin_dir: PathBuf,
    log_path: PathBuf,
}

impl ToolHarness {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
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
  printf 'rustc 1.85.0 (fake 2025-01-01)\n'
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
  printf '1.47.2\n'
fi
"#,
                log = log_path.display()
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

#[cfg(unix)]
fn write_fake_tool(path: &Path, contents: &str) -> Result<(), Box<dyn std::error::Error>> {
    fs::write(path, contents)?;
    let mut perms = fs::metadata(path)?.permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms)?;
    Ok(())
}
