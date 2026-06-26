use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[test]
fn refresh_cloud_run_revisions_dry_run_updates_api_and_flow_services() {
    let output = Command::new(repo_root().join("scripts/refresh-cloud-run-revisions.sh"))
        .arg("--env")
        .arg("dev")
        .arg("--dry-run")
        .current_dir(repo_root())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("API_IMAGE", "example.com/arco-api:test")
        .env("API_GIT_SHA", "test-sha")
        .env("API_CODE_VERSION", "uat-live-dev")
        .env("COMPACTOR_IMAGE", "example.com/arco-compactor:test")
        .env(
            "FLOW_COMPACTOR_IMAGE",
            "example.com/arco-flow-compactor:test",
        )
        .env(
            "FLOW_DISPATCHER_IMAGE",
            "example.com/arco-flow-dispatcher:test",
        )
        .env("FLOW_SWEEPER_IMAGE", "example.com/arco-flow-sweeper:test")
        .env(
            "FLOW_TIMER_INGEST_IMAGE",
            "example.com/arco-flow-timer-ingest:test",
        )
        .env("FLOW_WORKER_IMAGE", "example.com/arco-flow-worker:test")
        .env("FLOW_TENANT_ID", "arco-uat-tenant")
        .env("FLOW_WORKSPACE_ID", "arco-uat-workspace")
        .output()
        .expect("run revision refresh script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "revision refresh dry-run should succeed:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "gcloud run services update arco-api-dev --project=arco-testing-20260320 --region=us-central1 --image example.com/arco-api:test --update-env-vars ARCO_GIT_SHA=test-sha,ARCO_API_IMAGE=example.com/arco-api:test,ARCO_CODE_VERSION=uat-live-dev,ARCO_COMPACTOR_AUTH_MODE=gcp_id_token --quiet"
        ),
        "dry-run should update API image, provenance, and compactor auth env vars:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "gcloud run services update arco-flow-dispatcher-dev --project=arco-testing-20260320 --region=us-central1 --image example.com/arco-flow-dispatcher:test --update-env-vars ARCO_TENANT_ID=arco-uat-tenant,ARCO_WORKSPACE_ID=arco-uat-workspace --quiet"
        ),
        "dry-run should align flow controller tenant/workspace scope:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "gcloud run services update arco-compactor-dev --project=arco-testing-20260320 --region=us-central1 --image example.com/arco-compactor:test --update-env-vars ARCO_TENANT_ID=arco-uat-tenant,ARCO_WORKSPACE_ID=arco-uat-workspace --quiet"
        ),
        "dry-run should align catalog compactor tenant/workspace scope:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "gcloud run services update arco-flow-worker-dev --project=arco-testing-20260320 --region=us-central1 --image example.com/arco-flow-worker:test --update-env-vars ARCO_FLOW_TENANT_ID=arco-uat-tenant,ARCO_FLOW_WORKSPACE_ID=arco-uat-workspace --quiet"
        ),
        "dry-run should align worker tenant/workspace scope:\n{output_text}"
    );
    assert!(
        !output_text.contains("terraform"),
        "direct revision refresh should not invoke Terraform:\n{output_text}"
    );
}

#[test]
fn refresh_cloud_run_revisions_rejects_partial_flow_config() {
    let output = Command::new(repo_root().join("scripts/refresh-cloud-run-revisions.sh"))
        .arg("--env")
        .arg("dev")
        .arg("--dry-run")
        .current_dir(repo_root())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("API_IMAGE", "example.com/arco-api:test")
        .env("COMPACTOR_IMAGE", "example.com/arco-compactor:test")
        .env(
            "FLOW_COMPACTOR_IMAGE",
            "example.com/arco-flow-compactor:test",
        )
        .env(
            "FLOW_DISPATCHER_IMAGE",
            "example.com/arco-flow-dispatcher:test",
        )
        .output()
        .expect("run revision refresh script");
    let output_text = command_output_text(&output);

    assert!(
        !output.status.success(),
        "partial flow config should fail:\n{output_text}"
    );
    assert!(
        output_text.contains("Flow control-plane config is partial"),
        "failure should explain the all-or-none flow config requirement:\n{output_text}"
    );
}

#[test]
fn refresh_cloud_run_revisions_can_explicitly_set_flow_compactor_ingress() {
    let output = Command::new(repo_root().join("scripts/refresh-cloud-run-revisions.sh"))
        .arg("--env")
        .arg("dev")
        .arg("--dry-run")
        .current_dir(repo_root())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("API_IMAGE", "example.com/arco-api:test")
        .env("COMPACTOR_IMAGE", "example.com/arco-compactor:test")
        .env(
            "FLOW_COMPACTOR_IMAGE",
            "example.com/arco-flow-compactor:test",
        )
        .env("FLOW_COMPACTOR_INGRESS", "all")
        .output()
        .expect("run revision refresh script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "revision refresh dry-run should accept explicit flow compactor ingress:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "gcloud run services update arco-flow-compactor-dev --project=arco-testing-20260320 --region=us-central1 --image example.com/arco-flow-compactor:test --ingress all --quiet"
        ),
        "dry-run should show the explicit flow compactor ingress update:\n{output_text}"
    );
}

#[test]
fn refresh_cloud_run_revisions_scope_only_updates_tenant_workspace_without_images() {
    let output = Command::new(repo_root().join("scripts/refresh-cloud-run-revisions.sh"))
        .arg("--env")
        .arg("dev")
        .arg("--scope-only")
        .arg("--dry-run")
        .current_dir(repo_root())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("FLOW_TENANT_ID", "arco-uat-tenant")
        .env("FLOW_WORKSPACE_ID", "arco-uat-workspace")
        .output()
        .expect("run revision refresh script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "scope-only revision refresh dry-run should succeed without image vars:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "gcloud run services update arco-flow-dispatcher-dev --project=arco-testing-20260320 --region=us-central1 --update-env-vars ARCO_TENANT_ID=arco-uat-tenant,ARCO_WORKSPACE_ID=arco-uat-workspace --quiet"
        ),
        "scope-only dry-run should align flow dispatcher scope without changing the image:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "gcloud run services update arco-flow-worker-dev --project=arco-testing-20260320 --region=us-central1 --update-env-vars ARCO_FLOW_TENANT_ID=arco-uat-tenant,ARCO_FLOW_WORKSPACE_ID=arco-uat-workspace --quiet"
        ),
        "scope-only dry-run should align worker scope without changing the image:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "gcloud run services update arco-compactor-dev --project=arco-testing-20260320 --region=us-central1 --update-env-vars ARCO_TENANT_ID=arco-uat-tenant,ARCO_WORKSPACE_ID=arco-uat-workspace --quiet"
        ),
        "scope-only dry-run should align catalog compactor scope without changing the image:\n{output_text}"
    );
    assert!(
        !output_text.contains("--image"),
        "scope-only refresh must not update images:\n{output_text}"
    );
    assert!(
        !output_text.contains("arco-api-dev"),
        "scope-only refresh should not update the API service:\n{output_text}"
    );
}

#[test]
fn refresh_cloud_run_revisions_requires_owner_for_live_updates() {
    let output = Command::new(repo_root().join("scripts/refresh-cloud-run-revisions.sh"))
        .arg("--env")
        .arg("dev")
        .current_dir(repo_root())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("API_IMAGE", "example.com/arco-api:test")
        .env("COMPACTOR_IMAGE", "example.com/arco-compactor:test")
        .env(
            "FLOW_COMPACTOR_IMAGE",
            "example.com/arco-flow-compactor:test",
        )
        .env_remove("ARCO_DEPLOY_OWNER")
        .output()
        .expect("run revision refresh script");
    let output_text = command_output_text(&output);

    assert!(
        !output.status.success(),
        "live revision refresh should require an explicit deploy owner:\n{output_text}"
    );
    assert!(
        output_text.contains("ARCO_DEPLOY_OWNER is required for live deployment mutations"),
        "failure should explain the explicit ownership contract:\n{output_text}"
    );
    assert!(
        !output_text.contains("gcloud run services update"),
        "ownership guard should stop before any Cloud Run update:\n{output_text}"
    );
}

#[test]
fn refresh_cloud_run_revisions_rejects_existing_deploy_lock() {
    let tempdir = TempDir::new().expect("create lock tempdir");
    let lock_dir = tempdir.path().join("dev.lock");
    fs::create_dir(&lock_dir).expect("create active lock dir");
    fs::write(
        lock_dir.join("owner"),
        "owner=other-session\npid=4242\nhost=test-host\nstarted_at=2026-06-03T00:00:00Z\n",
    )
    .expect("write lock owner file");

    let output = Command::new(repo_root().join("scripts/refresh-cloud-run-revisions.sh"))
        .arg("--env")
        .arg("dev")
        .current_dir(repo_root())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("API_IMAGE", "example.com/arco-api:test")
        .env("COMPACTOR_IMAGE", "example.com/arco-compactor:test")
        .env(
            "FLOW_COMPACTOR_IMAGE",
            "example.com/arco-flow-compactor:test",
        )
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_DEPLOY_LOCK_DIR", &lock_dir)
        .output()
        .expect("run revision refresh script");
    let output_text = command_output_text(&output);

    assert!(
        !output.status.success(),
        "live revision refresh should reject an active deploy lock:\n{output_text}"
    );
    assert!(
        output_text.contains("Another live deployment appears to own"),
        "failure should explain the active deploy lock:\n{output_text}"
    );
    assert!(
        output_text.contains("owner=other-session"),
        "failure should print the recorded owner metadata:\n{output_text}"
    );
    assert!(
        !output_text.contains("gcloud run services update"),
        "lock guard should stop before any Cloud Run update:\n{output_text}"
    );
}

#[test]
fn refresh_cloud_run_revisions_rejects_conflicting_cloud_owner_label() {
    let harness = CloudRunRefreshHarness::new().expect("create refresh harness");
    let lock_dir = harness.tempdir_path().join("deploy.lock");

    let output = Command::new(repo_root().join("scripts/refresh-cloud-run-revisions.sh"))
        .arg("--env")
        .arg("dev")
        .current_dir(repo_root())
        .env("PATH", harness.path())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("API_IMAGE", "example.com/arco-api:test")
        .env("API_GIT_SHA", "ad596cf")
        .env("COMPACTOR_IMAGE", "example.com/arco-compactor:test")
        .env(
            "FLOW_COMPACTOR_IMAGE",
            "example.com/arco-flow-compactor:test",
        )
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_DEPLOY_LOCK_DIR", &lock_dir)
        .env("ARCO_TEST_LOG", harness.log_path())
        .env("ARCO_TEST_CLOUD_OWNER", "other-session")
        .output()
        .expect("run revision refresh script");
    let output_text = command_output_text(&output);

    assert!(
        !output.status.success(),
        "live revision refresh should reject a conflicting cloud owner marker:\n{output_text}"
    );
    assert!(
        output_text.contains("Cloud Run deploy owner mismatch"),
        "failure should explain the cloud-visible ownership conflict:\n{output_text}"
    );
    assert!(
        output_text.contains("other-session"),
        "failure should include the current cloud owner:\n{output_text}"
    );

    let log = harness.read_log().expect("read gcloud log");
    assert!(
        log.contains("gcloud run services describe arco-api-dev"),
        "cloud owner preflight should inspect the API service before mutation; log:\n{log}"
    );
    assert!(
        !log.contains("gcloud run services update"),
        "cloud owner guard should stop before any Cloud Run update; log:\n{log}"
    );
}

#[test]
fn refresh_cloud_run_revisions_fails_when_post_update_image_does_not_match() {
    let harness = CloudRunRefreshHarness::new().expect("create refresh harness");
    let lock_dir = harness.tempdir_path().join("deploy.lock");

    let output = Command::new(repo_root().join("scripts/refresh-cloud-run-revisions.sh"))
        .arg("--env")
        .arg("dev")
        .current_dir(repo_root())
        .env("PATH", harness.path())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("API_IMAGE", "example.com/arco-api:test")
        .env("API_GIT_SHA", "ad596cf")
        .env("COMPACTOR_IMAGE", "example.com/arco-compactor:test")
        .env(
            "FLOW_COMPACTOR_IMAGE",
            "example.com/arco-flow-compactor:test",
        )
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_DEPLOY_LOCK_DIR", &lock_dir)
        .env("ARCO_TEST_LOG", harness.log_path())
        .env("ARCO_TEST_CLOUD_OWNER", "uat-session")
        .env("ARCO_TEST_FINAL_API_IMAGE", "example.com/arco-api:other")
        .output()
        .expect("run revision refresh script");
    let output_text = command_output_text(&output);

    assert!(
        !output.status.success(),
        "live revision refresh should fail when the post-update API image is wrong:\n{output_text}"
    );
    assert!(
        output_text.contains("Cloud Run revision refresh verification failed"),
        "failure should identify the post-update verification failure:\n{output_text}"
    );
    assert!(
        output_text.contains("arco-api-dev image=example.com/arco-api:other"),
        "failure should include the observed wrong image:\n{output_text}"
    );

    let log = harness.read_log().expect("read gcloud log");
    assert!(
        log.contains("gcloud run services update arco-api-dev"),
        "the test should exercise the live update path before verification; log:\n{log}"
    );
    assert!(
        log.contains("gcloud run services describe arco-api-dev"),
        "post-update verification should re-describe the API service; log:\n{log}"
    );
}

#[test]
fn refresh_cloud_run_revisions_fails_when_service_drifts_after_immediate_verification() {
    let harness = CloudRunRefreshHarness::new().expect("create refresh harness");
    let lock_dir = harness.tempdir_path().join("deploy.lock");

    let output = Command::new(repo_root().join("scripts/refresh-cloud-run-revisions.sh"))
        .arg("--env")
        .arg("dev")
        .current_dir(repo_root())
        .env("PATH", harness.path())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("API_IMAGE", "example.com/arco-api:test")
        .env("API_GIT_SHA", "ad596cf")
        .env("COMPACTOR_IMAGE", "example.com/arco-compactor:test")
        .env(
            "FLOW_COMPACTOR_IMAGE",
            "example.com/arco-flow-compactor:test",
        )
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_DEPLOY_LOCK_DIR", &lock_dir)
        .env("ARCO_TEST_LOG", harness.log_path())
        .env("ARCO_TEST_CLOUD_OWNER", "uat-session")
        .env("ARCO_TEST_DRIFT_API_AFTER_FIRST_JSON_DESCRIBE", "1")
        .output()
        .expect("run revision refresh script");
    let output_text = command_output_text(&output);

    assert!(
        !output.status.success(),
        "live revision refresh should fail when a service drifts before the script exits:\n{output_text}"
    );
    assert!(
        output_text.contains("Cloud Run revision refresh verification failed"),
        "failure should identify the final verification failure:\n{output_text}"
    );
    assert!(
        output_text.contains("arco-api-dev image=example.com/arco-api:drifted"),
        "failure should include the drifted image:\n{output_text}"
    );

    let log = harness.read_log().expect("read gcloud log");
    let api_describes = log
        .matches("gcloud run services describe arco-api-dev")
        .count();
    assert!(
        api_describes >= 3,
        "the refresh should preflight, verify immediately, and run a final API describe; log:\n{log}"
    );
}

struct CloudRunRefreshHarness {
    tempdir: TempDir,
    bin_dir: PathBuf,
    log_path: PathBuf,
}

impl CloudRunRefreshHarness {
    fn new() -> std::io::Result<Self> {
        let tempdir = TempDir::new()?;
        let bin_dir = tempdir.path().join("bin");
        fs::create_dir(&bin_dir)?;
        let log_path = tempdir.path().join("commands.log");

        write_stub(
            &bin_dir,
            "gcloud",
            r#"printf 'gcloud %s\n' "$*" >> "$ARCO_TEST_LOG"
if [[ "${1:-} ${2:-} ${3:-}" == "run services describe" ]]; then
  format=""
  for arg in "$@"; do
    case "$arg" in
    --format=*) format="${arg#--format=}" ;;
    esac
  done
  if [[ "$format" == value\(metadata.labels.arco_deploy_owner\) ]]; then
    printf '%s\n' "${ARCO_TEST_CLOUD_OWNER:-}"
    exit 0
  fi
	  if [[ "$format" == json* ]]; then
	    service="${4:-}"
	    case "$service" in
	    arco-api-dev)
	      counter_file="${ARCO_TEST_API_JSON_DESCRIBE_COUNTER:-}"
	      if [[ -z "$counter_file" ]]; then
	        counter_file="$(dirname "$ARCO_TEST_LOG")/api-json-describe-count"
	      fi
	      count=0
	      if [[ -f "$counter_file" ]]; then
	        count="$(cat "$counter_file")"
	      fi
	      count=$((count + 1))
	      printf '%s' "$count" >"$counter_file"
	      if [[ "${ARCO_TEST_DRIFT_API_AFTER_FIRST_JSON_DESCRIBE:-}" == "1" && "$count" -gt 1 ]]; then
	        image="example.com/arco-api:drifted"
	      else
	        image="${ARCO_TEST_FINAL_API_IMAGE:-example.com/arco-api:test}"
	      fi
	      env_json="[{\"name\":\"ARCO_GIT_SHA\",\"value\":\"${API_GIT_SHA:-ad596cf}\"},{\"name\":\"ARCO_API_IMAGE\",\"value\":\"example.com/arco-api:test\"},{\"name\":\"ARCO_CODE_VERSION\",\"value\":\"\"},{\"name\":\"ARCO_COMPACTOR_AUTH_MODE\",\"value\":\"gcp_id_token\"}]"
	      ;;
    arco-compactor-dev)
      image="${ARCO_TEST_FINAL_COMPACTOR_IMAGE:-example.com/arco-compactor:test}"
      env_json='[]'
      ;;
    arco-flow-compactor-dev)
      image="${ARCO_TEST_FINAL_FLOW_COMPACTOR_IMAGE:-example.com/arco-flow-compactor:test}"
      env_json='[]'
      ;;
    *)
      exit 1
      ;;
    esac
    python3 - "$image" "$env_json" <<'PY'
import json
import sys

image = sys.argv[1]
env = json.loads(sys.argv[2])
doc = {
    "metadata": {"labels": {"arco_deploy_owner": "uat-session"}},
    "spec": {"template": {"spec": {"containers": [{"image": image, "env": env}]}}},
    "status": {
        "latestReadyRevisionName": "arco-api-dev-00001-test",
        "traffic": [{"revisionName": "arco-api-dev-00001-test", "percent": 100}],
    },
}
print(json.dumps(doc))
PY
    exit 0
  fi
fi
"#,
        )?;

        Ok(Self {
            tempdir,
            bin_dir,
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

    fn read_log(&self) -> std::io::Result<String> {
        fs::read_to_string(&self.log_path)
    }

    fn tempdir_path(&self) -> &Path {
        self.tempdir.path()
    }
}

fn write_stub(bin_dir: &Path, name: &str, body: &str) -> std::io::Result<()> {
    let path = bin_dir.join(name);
    fs::write(
        &path,
        format!("#!/usr/bin/env bash\nset -euo pipefail\n{body}"),
    )?;
    #[cfg(unix)]
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755))?;
    Ok(())
}

fn command_output_text(output: &std::process::Output) -> String {
    format!(
        "status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("xtask lives under tools/xtask")
        .to_path_buf()
}
