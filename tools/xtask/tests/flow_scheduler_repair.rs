use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[test]
fn repair_flow_scheduler_jobs_dry_run_prints_describe_and_resume_commands() {
    let output = Command::new(repo_root().join("scripts/repair-flow-scheduler-jobs.sh"))
        .arg("--env")
        .arg("dev")
        .arg("--dry-run")
        .current_dir(repo_root())
        .env("PROJECT_ID", "arco-testing-20260320")
        .output()
        .expect("run flow scheduler repair script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "flow scheduler repair dry-run should succeed:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "gcloud scheduler jobs describe arco-flow-dispatcher-run-dev --project=arco-testing-20260320 --location=us-central1 --format=value(state)"
        ),
        "dry-run should inspect dispatcher state:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "gcloud scheduler jobs resume arco-flow-sweeper-run-dev --project=arco-testing-20260320 --location=us-central1 --quiet"
        ),
        "dry-run should show the sweeper resume command:\n{output_text}"
    );
    assert!(
        !output_text.contains("terraform"),
        "scheduler repair should not invoke Terraform:\n{output_text}"
    );
}

#[test]
fn repair_flow_scheduler_jobs_dry_run_prints_target_update_commands() {
    let output = Command::new(repo_root().join("scripts/repair-flow-scheduler-jobs.sh"))
        .arg("--env")
        .arg("dev")
        .arg("--repair-targets")
        .arg("--dry-run")
        .current_dir(repo_root())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("PROJECT_NUMBER", "135245112198")
        .output()
        .expect("run flow scheduler repair script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "flow scheduler target repair dry-run should succeed:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "gcloud scheduler jobs update http arco-flow-dispatcher-run-dev --project=arco-testing-20260320 --location=us-central1 --uri=https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run --http-method=post --oidc-service-account-email=arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com --oidc-token-audience=https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app --quiet"
        ),
        "dry-run should show dispatcher target metadata repair:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "gcloud scheduler jobs update http arco-flow-sweeper-run-dev --project=arco-testing-20260320 --location=us-central1 --uri=https://arco-flow-sweeper-dev-135245112198.us-central1.run.app/run --http-method=post --oidc-service-account-email=arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com --oidc-token-audience=https://arco-flow-sweeper-dev-135245112198.us-central1.run.app --quiet"
        ),
        "dry-run should show sweeper target metadata repair:\n{output_text}"
    );
    assert!(
        !output_text.contains("terraform"),
        "scheduler target repair should not invoke Terraform:\n{output_text}"
    );
}

#[test]
fn repair_flow_scheduler_jobs_requires_owner_for_live_resume() {
    let output = Command::new(repo_root().join("scripts/repair-flow-scheduler-jobs.sh"))
        .arg("--env")
        .arg("dev")
        .current_dir(repo_root())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env_remove("ARCO_DEPLOY_OWNER")
        .output()
        .expect("run flow scheduler repair script");
    let output_text = command_output_text(&output);

    assert!(
        !output.status.success(),
        "live scheduler repair should require a deploy owner:\n{output_text}"
    );
    assert!(
        output_text.contains("ARCO_DEPLOY_OWNER is required for live Scheduler repair"),
        "failure should explain the ownership requirement:\n{output_text}"
    );
    assert!(
        !output_text.contains("gcloud scheduler jobs resume"),
        "owner guard should stop before Scheduler mutation:\n{output_text}"
    );
}

#[test]
fn repair_flow_scheduler_jobs_rejects_existing_deploy_lock() {
    let tempdir = TempDir::new().expect("create tempdir");
    let bin_dir = tempdir.path().join("bin");
    fs::create_dir(&bin_dir).expect("create bin dir");
    let log_path = tempdir.path().join("commands.log");
    let lock_dir = tempdir.path().join("deploy.lock");
    fs::create_dir(&lock_dir).expect("create active lock dir");
    fs::write(
        lock_dir.join("owner"),
        "owner=other-session\nscript=other-repair\nproject=arco-testing-20260320\n",
    )
    .expect("write lock owner");
    write_stub(
        &bin_dir,
        "gcloud",
        "printf 'gcloud %s\\n' \"$*\" >> \"$ARCO_TEST_LOG\"\n\
         exit 42\n",
    )
    .expect("write gcloud stub");

    let output = Command::new(repo_root().join("scripts/repair-flow-scheduler-jobs.sh"))
        .arg("--env")
        .arg("dev")
        .current_dir(repo_root())
        .env("PATH", path_with_bin(&bin_dir))
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_DEPLOY_LOCK_DIR", &lock_dir)
        .env("ARCO_TEST_LOG", &log_path)
        .output()
        .expect("run flow scheduler repair script");
    let output_text = command_output_text(&output);

    assert!(
        !output.status.success(),
        "live scheduler repair should reject an active deploy lock:\n{output_text}"
    );
    assert!(
        output_text.contains("Refusing live Scheduler repair while deploy lock exists"),
        "failure should explain the active deploy lock:\n{output_text}"
    );
    assert!(
        output_text.contains("owner=other-session"),
        "failure should print active lock metadata:\n{output_text}"
    );
    assert!(
        !log_path.exists(),
        "lock guard should stop before invoking gcloud"
    );
}

#[test]
fn repair_flow_scheduler_jobs_uses_inherited_deploy_lock() {
    let harness = SchedulerRepairHarness::new().expect("create scheduler harness");
    let lock_dir = harness.lock_dir();
    fs::create_dir(lock_dir).expect("create inherited lock dir");
    fs::write(
        lock_dir.join("owner"),
        "owner=uat-session\nscript=wrapper\n",
    )
    .expect("write inherited lock owner");

    let output = Command::new(repo_root().join("scripts/repair-flow-scheduler-jobs.sh"))
        .arg("--env")
        .arg("dev")
        .current_dir(repo_root())
        .env("PATH", harness.path())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_DEPLOY_LOCK_DIR", lock_dir)
        .env("ARCO_DEPLOY_LOCK_HELD", "true")
        .env("ARCO_TEST_LOG", harness.log_path())
        .output()
        .expect("run flow scheduler repair script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "scheduler repair should accept an inherited deploy lock:\n{output_text}"
    );
    let log = harness.read_log().expect("read gcloud log");
    assert!(
        log.contains("gcloud scheduler jobs resume arco-flow-sweeper-run-dev"),
        "inherited lock should allow Scheduler mutation; log:\n{log}"
    );
    assert!(
        lock_dir.join("owner").exists(),
        "inherited lock owner metadata should be left for the wrapper"
    );
}

#[test]
fn repair_flow_scheduler_jobs_resumes_only_paused_jobs() {
    let harness = SchedulerRepairHarness::new().expect("create scheduler harness");
    let output = Command::new(repo_root().join("scripts/repair-flow-scheduler-jobs.sh"))
        .arg("--env")
        .arg("dev")
        .current_dir(repo_root())
        .env("PATH", harness.path())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_DEPLOY_LOCK_DIR", harness.lock_dir())
        .env("ARCO_TEST_LOG", harness.log_path())
        .output()
        .expect("run flow scheduler repair script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "scheduler repair should resume paused jobs and skip enabled jobs:\n{output_text}"
    );
    let log = harness.read_log().expect("read gcloud log");
    assert!(
        log.contains("gcloud scheduler jobs describe arco-flow-dispatcher-run-dev"),
        "repair should inspect dispatcher state first; log:\n{log}"
    );
    assert!(
        !log.contains("gcloud scheduler jobs resume arco-flow-dispatcher-run-dev"),
        "repair should not resume an already enabled dispatcher; log:\n{log}"
    );
    assert!(
        log.contains("gcloud scheduler jobs resume arco-flow-sweeper-run-dev"),
        "repair should resume the paused sweeper; log:\n{log}"
    );
}

#[test]
fn repair_flow_scheduler_jobs_updates_only_mismatched_targets() {
    let harness = SchedulerTargetRepairHarness::new().expect("create target repair harness");
    let output = Command::new(repo_root().join("scripts/repair-flow-scheduler-jobs.sh"))
        .arg("--env")
        .arg("dev")
        .arg("--repair-targets")
        .current_dir(repo_root())
        .env("PATH", harness.path())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("PROJECT_NUMBER", "135245112198")
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_DEPLOY_LOCK_DIR", harness.lock_dir())
        .env("ARCO_TEST_LOG", harness.log_path())
        .output()
        .expect("run flow scheduler repair script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "scheduler target repair should inspect and update mismatched targets:\n{output_text}"
    );
    let log = harness.read_log().expect("read gcloud log");
    assert!(
        log.contains("gcloud scheduler jobs describe arco-flow-dispatcher-run-dev"),
        "repair should inspect dispatcher target metadata; log:\n{log}"
    );
    assert!(
        !log.contains("gcloud scheduler jobs update http arco-flow-dispatcher-run-dev"),
        "repair should skip dispatcher when target metadata is already correct; log:\n{log}"
    );
    assert!(
        log.contains("gcloud scheduler jobs update http arco-flow-sweeper-run-dev"),
        "repair should update sweeper when target metadata is mismatched; log:\n{log}"
    );
}

struct SchedulerRepairHarness {
    _tempdir: TempDir,
    bin_dir: PathBuf,
    lock_dir: PathBuf,
    log_path: PathBuf,
}

impl SchedulerRepairHarness {
    fn new() -> std::io::Result<Self> {
        let tempdir = TempDir::new()?;
        let bin_dir = tempdir.path().join("bin");
        fs::create_dir(&bin_dir)?;
        let lock_dir = tempdir.path().join("deploy.lock");
        let log_path = tempdir.path().join("commands.log");

        write_stub(
            &bin_dir,
            "gcloud",
            "printf 'gcloud %s\\n' \"$*\" >> \"$ARCO_TEST_LOG\"\n\
             if [[ \"${1:-} ${2:-} ${3:-}\" == \"scheduler jobs describe\" ]]; then\n\
               case \"${4:-}\" in\n\
                 arco-flow-dispatcher-run-dev) printf 'ENABLED\\n' ;;\n\
                 arco-flow-sweeper-run-dev) printf 'PAUSED\\n' ;;\n\
                 *) exit 1 ;;\n\
               esac\n\
               exit 0\n\
             fi\n\
             if [[ \"${1:-} ${2:-} ${3:-}\" == \"scheduler jobs resume\" ]]; then\n\
               exit 0\n\
             fi\n\
             exit 1\n",
        )?;
        write_stub(
            &bin_dir,
            "jq",
            "python3 -c 'import json, sys\n\
             args = sys.argv[1:]\n\
             doc = json.load(sys.stdin)\n\
             expr = \" \".join(args)\n\
             if \".state\" in expr:\n\
                 print(doc.get(\"state\", \"\"))\n\
             elif \".httpTarget.uri\" in expr:\n\
                 print(doc.get(\"httpTarget\", {}).get(\"uri\", \"\"))\n\
             elif \".httpTarget.oidcToken.audience\" in expr:\n\
                 print(doc.get(\"httpTarget\", {}).get(\"oidcToken\", {}).get(\"audience\", \"\"))\n\
             elif \".httpTarget.oidcToken.serviceAccountEmail\" in expr:\n\
                 print(doc.get(\"httpTarget\", {}).get(\"oidcToken\", {}).get(\"serviceAccountEmail\", \"\"))\n\
             elif \".httpTarget.httpMethod\" in expr:\n\
                 print(doc.get(\"httpTarget\", {}).get(\"httpMethod\", \"\"))\n\
             ' \"$@\"\n",
        )?;

        Ok(Self {
            _tempdir: tempdir,
            bin_dir,
            lock_dir,
            log_path,
        })
    }

    fn path(&self) -> String {
        let mut paths = vec![self.bin_dir.clone()];
        paths.extend(env::split_paths(&env::var_os("PATH").unwrap_or_default()));
        env::join_paths(paths)
            .expect("join PATH")
            .to_string_lossy()
            .into_owned()
    }

    fn log_path(&self) -> &Path {
        &self.log_path
    }

    fn lock_dir(&self) -> &Path {
        &self.lock_dir
    }

    fn read_log(&self) -> std::io::Result<String> {
        fs::read_to_string(&self.log_path)
    }
}

struct SchedulerTargetRepairHarness {
    _tempdir: TempDir,
    bin_dir: PathBuf,
    lock_dir: PathBuf,
    log_path: PathBuf,
}

impl SchedulerTargetRepairHarness {
    fn new() -> std::io::Result<Self> {
        let tempdir = TempDir::new()?;
        let bin_dir = tempdir.path().join("bin");
        fs::create_dir(&bin_dir)?;
        let lock_dir = tempdir.path().join("deploy.lock");
        let log_path = tempdir.path().join("commands.log");

        write_stub(
            &bin_dir,
            "gcloud",
            "printf 'gcloud %s\\n' \"$*\" >> \"$ARCO_TEST_LOG\"\n\
             if [[ \"${1:-} ${2:-} ${3:-}\" == \"scheduler jobs describe\" ]]; then\n\
               format=''\n\
               for arg in \"$@\"; do\n\
                 case \"$arg\" in\n\
                   --format=*) format=\"${arg#--format=}\" ;;\n\
                 esac\n\
               done\n\
               if [[ \"$format\" == 'value(state)' ]]; then\n\
                 printf 'ENABLED\\n'\n\
                 exit 0\n\
               fi\n\
               case \"${4:-}\" in\n\
                 arco-flow-dispatcher-run-dev)\n\
                   cat <<'JSON'\n\
{\"state\":\"ENABLED\",\"httpTarget\":{\"uri\":\"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run\",\"httpMethod\":\"POST\",\"oidcToken\":{\"audience\":\"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app\",\"serviceAccountEmail\":\"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com\"}}}\n\
JSON\n\
                   ;;\n\
                 arco-flow-sweeper-run-dev)\n\
                   cat <<'JSON'\n\
{\"state\":\"ENABLED\",\"httpTarget\":{\"uri\":\"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app/run\",\"httpMethod\":\"POST\",\"oidcToken\":{\"audience\":\"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app/run\",\"serviceAccountEmail\":\"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com\"}}}\n\
JSON\n\
                   ;;\n\
                 *) exit 1 ;;\n\
               esac\n\
               exit 0\n\
             fi\n\
             if [[ \"${1:-} ${2:-} ${3:-} ${4:-}\" == \"scheduler jobs update http\" ]]; then\n\
               exit 0\n\
             fi\n\
             exit 1\n",
        )?;

        Ok(Self {
            _tempdir: tempdir,
            bin_dir,
            lock_dir,
            log_path,
        })
    }

    fn path(&self) -> String {
        let mut paths = vec![self.bin_dir.clone()];
        paths.extend(env::split_paths(&env::var_os("PATH").unwrap_or_default()));
        env::join_paths(paths)
            .expect("join PATH")
            .to_string_lossy()
            .into_owned()
    }

    fn log_path(&self) -> &Path {
        &self.log_path
    }

    fn lock_dir(&self) -> &Path {
        &self.lock_dir
    }

    fn read_log(&self) -> std::io::Result<String> {
        fs::read_to_string(&self.log_path)
    }
}

fn write_stub(dir: &Path, name: &str, body: &str) -> std::io::Result<()> {
    let path = dir.join(name);
    fs::write(
        &path,
        format!("#!/usr/bin/env bash\nset -euo pipefail\n{body}"),
    )?;
    #[cfg(unix)]
    {
        let mut permissions = fs::metadata(&path)?.permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&path, permissions)?;
    }
    Ok(())
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask has repo parent")
        .parent()
        .expect("tools has repo parent")
        .to_path_buf()
}

fn path_with_bin(bin_dir: &Path) -> String {
    let mut paths = vec![bin_dir.to_path_buf()];
    paths.extend(env::split_paths(&env::var_os("PATH").unwrap_or_default()));
    env::join_paths(paths)
        .expect("join PATH")
        .to_string_lossy()
        .into_owned()
}

fn command_output_text(output: &std::process::Output) -> String {
    format!(
        "status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}
