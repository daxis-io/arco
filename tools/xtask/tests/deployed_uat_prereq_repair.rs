use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[test]
fn repair_deployed_uat_prereqs_dry_run_chains_scope_scheduler_and_status() {
    let output = Command::new(repo_root().join("scripts/repair-deployed-uat-prereqs.sh"))
        .arg("--env")
        .arg("dev")
        .arg("--dry-run")
        .current_dir(repo_root())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("PROJECT_NUMBER", "135245112198")
        .env("FLOW_TENANT_ID", "arco-uat-tenant")
        .env("FLOW_WORKSPACE_ID", "arco-uat-workspace")
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_UAT_EXPECTED_API_CODE_VERSION", "uat-live")
        .env("ARCO_UAT_EXPECTED_API_GIT_SHA", "abc123")
        .env(
            "ARCO_UAT_EXPECTED_API_IMAGE",
            "us-central1-docker.pkg.dev/example/arco-api:uat-live",
        )
        .output()
        .expect("run deployed UAT prereq repair script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "deployed UAT prereq repair dry-run should succeed:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "gcloud run services update arco-flow-dispatcher-dev --project=arco-testing-20260320 --region=us-central1 --update-env-vars ARCO_TENANT_ID=arco-uat-tenant,ARCO_WORKSPACE_ID=arco-uat-workspace --quiet"
        ),
        "dry-run should include flow service scope repair:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "gcloud scheduler jobs update http arco-flow-dispatcher-run-dev --project=arco-testing-20260320 --location=us-central1 --uri=https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run --http-method=post --oidc-service-account-email=arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com --oidc-token-audience=https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app --quiet"
        ),
        "dry-run should include Scheduler target metadata repair:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "ARCO_DEPLOY_OWNER=uat-session ARCO_UAT_CLOUD_RUN_SERVICE=arco-api-dev PROJECT_ID=arco-testing-20260320 REGION=us-central1 ARCO_UAT_TENANT=arco-uat-tenant ARCO_UAT_WORKSPACE=arco-uat-workspace ARCO_UAT_EXPECTED_API_CODE_VERSION=uat-live ARCO_UAT_EXPECTED_API_GIT_SHA=abc123 ARCO_UAT_EXPECTED_API_IMAGE=us-central1-docker.pkg.dev/example/arco-api:uat-live ARCO_UAT_CLOUD_RUN_PORT=18080 ./scripts/run_user_acceptance_pipeline_uat.sh --require-live-deployed-ready"
        ),
        "dry-run should finish with the strict read-only status command:\n{output_text}"
    );
    assert!(
        !output_text.contains("terraform"),
        "combined repair must not invoke Terraform:\n{output_text}"
    );
    assert!(
        !output_text.contains("cargo test"),
        "combined repair dry-run must not invoke Cargo:\n{output_text}"
    );
}

#[test]
fn repair_deployed_uat_prereqs_dry_run_can_include_full_deployed_gate() {
    let output = Command::new(repo_root().join("scripts/repair-deployed-uat-prereqs.sh"))
        .arg("--env")
        .arg("dev")
        .arg("--run-live-deployed")
        .arg("--dry-run")
        .current_dir(repo_root())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("PROJECT_NUMBER", "135245112198")
        .env("FLOW_TENANT_ID", "arco-uat-tenant")
        .env("FLOW_WORKSPACE_ID", "arco-uat-workspace")
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_UAT_EXPECTED_API_CODE_VERSION", "uat-live")
        .env("ARCO_UAT_EXPECTED_API_GIT_SHA", "abc123")
        .env(
            "ARCO_UAT_EXPECTED_API_IMAGE",
            "us-central1-docker.pkg.dev/example/arco-api:uat-live",
        )
        .output()
        .expect("run deployed UAT prereq repair script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "deployed UAT full-window dry-run should succeed:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "ARCO_DEPLOY_OWNER=uat-session ARCO_UAT_CLOUD_RUN_SERVICE=arco-api-dev PROJECT_ID=arco-testing-20260320 REGION=us-central1 ARCO_UAT_TENANT=arco-uat-tenant ARCO_UAT_WORKSPACE=arco-uat-workspace ARCO_UAT_EXPECTED_API_CODE_VERSION=uat-live ARCO_UAT_EXPECTED_API_GIT_SHA=abc123 ARCO_UAT_EXPECTED_API_IMAGE=us-central1-docker.pkg.dev/example/arco-api:uat-live ARCO_UAT_CLOUD_RUN_PORT=18080 ./scripts/run_user_acceptance_pipeline_uat.sh --require-live-deployed-ready"
        ),
        "full-window dry-run should still run strict readiness first:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "ARCO_DEPLOY_OWNER=uat-session ARCO_UAT_CLOUD_RUN_SERVICE=arco-api-dev PROJECT_ID=arco-testing-20260320 REGION=us-central1 ARCO_UAT_TENANT=arco-uat-tenant ARCO_UAT_WORKSPACE=arco-uat-workspace ARCO_UAT_EXPECTED_API_CODE_VERSION=uat-live ARCO_UAT_EXPECTED_API_GIT_SHA=abc123 ARCO_UAT_EXPECTED_API_IMAGE=us-central1-docker.pkg.dev/example/arco-api:uat-live ARCO_UAT_CLOUD_RUN_PORT=18080 ./scripts/run_user_acceptance_pipeline_uat.sh --live-deployed"
        ),
        "full-window dry-run should include the evidence-producing deployed gate:\n{output_text}"
    );
}

#[test]
fn repair_deployed_uat_prereqs_dry_run_prints_selected_cloud_run_proxy_port() {
    let output = Command::new(repo_root().join("scripts/repair-deployed-uat-prereqs.sh"))
        .arg("--env")
        .arg("dev")
        .arg("--run-live-deployed")
        .arg("--dry-run")
        .current_dir(repo_root())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("PROJECT_NUMBER", "135245112198")
        .env("FLOW_TENANT_ID", "arco-uat-tenant")
        .env("FLOW_WORKSPACE_ID", "arco-uat-workspace")
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_UAT_EXPECTED_API_CODE_VERSION", "uat-live")
        .env("ARCO_UAT_EXPECTED_API_GIT_SHA", "abc123")
        .env(
            "ARCO_UAT_EXPECTED_API_IMAGE",
            "us-central1-docker.pkg.dev/example/arco-api:uat-live",
        )
        .env("ARCO_UAT_CLOUD_RUN_PORT", "18086")
        .output()
        .expect("run deployed UAT prereq repair script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "deployed UAT full-window dry-run should succeed:\n{output_text}"
    );
    assert!(
        output_text.contains("ARCO_UAT_CLOUD_RUN_PORT=18086 ./scripts/run_user_acceptance_pipeline_uat.sh --require-live-deployed-ready"),
        "strict readiness dry-run should show the selected proxy port:\n{output_text}"
    );
    assert!(
        output_text.contains("ARCO_UAT_CLOUD_RUN_PORT=18086 ./scripts/run_user_acceptance_pipeline_uat.sh --live-deployed"),
        "live gate dry-run should show the selected proxy port:\n{output_text}"
    );
}

#[test]
fn repair_deployed_uat_prereqs_run_live_deployed_fails_before_repair_when_proxy_port_is_busy() {
    let harness = DeployedPrereqRepairHarness::new(false).expect("create prereq repair harness");
    write_stub(
        &harness.bin_dir,
        "lsof",
        r#"printf 'python gcloud run services proxy arco-api-dev\n'
exit 0
"#,
    )
    .expect("write busy-port lsof stub");

    let output = Command::new(repo_root().join("scripts/repair-deployed-uat-prereqs.sh"))
        .arg("--env")
        .arg("dev")
        .arg("--run-live-deployed")
        .current_dir(repo_root())
        .env("PATH", harness.path())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("PROJECT_NUMBER", "135245112198")
        .env("FLOW_TENANT_ID", "arco-uat-tenant")
        .env("FLOW_WORKSPACE_ID", "arco-uat-workspace")
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_UAT_EXPECTED_API_CODE_VERSION", "uat-live")
        .env("ARCO_UAT_EXPECTED_API_GIT_SHA", "abc123")
        .env(
            "ARCO_UAT_EXPECTED_API_IMAGE",
            "us-central1-docker.pkg.dev/example/arco-api:uat-live",
        )
        .env("ARCO_DEPLOY_LOCK_DIR", harness.lock_dir())
        .env("ARCO_TEST_LOG", harness.log_path())
        .env("ARCO_TEST_REPAIR_MARKER", harness.repair_marker_path())
        .output()
        .expect("run deployed UAT prereq repair script");
    let output_text = command_output_text(&output);

    assert!(
        !output.status.success(),
        "busy proxy port should fail the full live window before repair:\n{output_text}"
    );
    assert!(
        output_text.contains("ARCO_UAT_CLOUD_RUN_PORT=18080 is already in use"),
        "failure should explain the local proxy port conflict:\n{output_text}"
    );
    let log = harness.read_log().unwrap_or_default();
    assert!(
        !log.contains("gcloud run services update"),
        "busy proxy port should stop before Cloud Run scope repair; log:\n{log}"
    );
    assert!(
        !log.contains("gcloud scheduler jobs update"),
        "busy proxy port should stop before Scheduler target repair; log:\n{log}"
    );
}

#[test]
fn repair_deployed_uat_prereqs_dry_run_previews_initial_and_final_status_snapshots() {
    let output = Command::new(repo_root().join("scripts/repair-deployed-uat-prereqs.sh"))
        .arg("--env")
        .arg("dev")
        .arg("--status-output-dir")
        .arg("target/uat-evidence/live-repair-status")
        .arg("--dry-run")
        .current_dir(repo_root())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("PROJECT_NUMBER", "135245112198")
        .env("FLOW_TENANT_ID", "arco-uat-tenant")
        .env("FLOW_WORKSPACE_ID", "arco-uat-workspace")
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_UAT_EXPECTED_API_CODE_VERSION", "uat-live")
        .env("ARCO_UAT_EXPECTED_API_GIT_SHA", "abc123")
        .env(
            "ARCO_UAT_EXPECTED_API_IMAGE",
            "us-central1-docker.pkg.dev/example/arco-api:uat-live",
        )
        .output()
        .expect("run deployed UAT prereq repair script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "deployed UAT prereq repair dry-run should succeed:\n{output_text}"
    );

    let initial_status = "ARCO_DEPLOY_OWNER=uat-session ARCO_UAT_CLOUD_RUN_SERVICE=arco-api-dev PROJECT_ID=arco-testing-20260320 REGION=us-central1 ARCO_UAT_TENANT=arco-uat-tenant ARCO_UAT_WORKSPACE=arco-uat-workspace ARCO_UAT_EXPECTED_API_CODE_VERSION=uat-live ARCO_UAT_EXPECTED_API_GIT_SHA=abc123 ARCO_UAT_EXPECTED_API_IMAGE=us-central1-docker.pkg.dev/example/arco-api:uat-live ARCO_UAT_CLOUD_RUN_PORT=18080 ./scripts/run_user_acceptance_pipeline_uat.sh --require-live-deployed-ready --status-output target/uat-evidence/live-repair-status/initial-status.txt";
    let scope_repair = "gcloud run services update arco-compactor-dev --project=arco-testing-20260320 --region=us-central1 --update-env-vars ARCO_TENANT_ID=arco-uat-tenant,ARCO_WORKSPACE_ID=arco-uat-workspace --quiet";
    let final_status = "ARCO_DEPLOY_OWNER=uat-session ARCO_UAT_CLOUD_RUN_SERVICE=arco-api-dev PROJECT_ID=arco-testing-20260320 REGION=us-central1 ARCO_UAT_TENANT=arco-uat-tenant ARCO_UAT_WORKSPACE=arco-uat-workspace ARCO_UAT_EXPECTED_API_CODE_VERSION=uat-live ARCO_UAT_EXPECTED_API_GIT_SHA=abc123 ARCO_UAT_EXPECTED_API_IMAGE=us-central1-docker.pkg.dev/example/arco-api:uat-live ARCO_UAT_CLOUD_RUN_PORT=18080 ./scripts/run_user_acceptance_pipeline_uat.sh --require-live-deployed-ready --status-output target/uat-evidence/live-repair-status/final-status.txt";

    let initial_pos = output_text.find(initial_status).unwrap_or_else(|| {
        panic!("dry-run should preview initial status snapshot:\n{output_text}")
    });
    let scope_pos = output_text
        .find(scope_repair)
        .unwrap_or_else(|| panic!("dry-run should preview scope repair:\n{output_text}"));
    let final_pos = output_text
        .find(final_status)
        .unwrap_or_else(|| panic!("dry-run should preview final status snapshot:\n{output_text}"));
    assert!(
        initial_pos < scope_pos,
        "initial snapshot should be previewed before repair commands:\n{output_text}"
    );
    assert!(
        scope_pos < final_pos,
        "final snapshot should be previewed after repair commands:\n{output_text}"
    );
}

#[test]
fn repair_deployed_uat_prereqs_requires_owner_for_live_repairs() {
    let output = Command::new(repo_root().join("scripts/repair-deployed-uat-prereqs.sh"))
        .arg("--env")
        .arg("dev")
        .current_dir(repo_root())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("PROJECT_NUMBER", "135245112198")
        .env("FLOW_TENANT_ID", "arco-uat-tenant")
        .env("FLOW_WORKSPACE_ID", "arco-uat-workspace")
        .env("ARCO_UAT_EXPECTED_API_CODE_VERSION", "uat-live")
        .env("ARCO_UAT_EXPECTED_API_GIT_SHA", "abc123")
        .env(
            "ARCO_UAT_EXPECTED_API_IMAGE",
            "us-central1-docker.pkg.dev/example/arco-api:uat-live",
        )
        .env_remove("ARCO_DEPLOY_OWNER")
        .output()
        .expect("run deployed UAT prereq repair script");
    let output_text = command_output_text(&output);

    assert!(
        !output.status.success(),
        "live deployed UAT prereq repair should require owner:\n{output_text}"
    );
    assert!(
        output_text
            .contains("ARCO_DEPLOY_OWNER is required for live deployed UAT prerequisite repair"),
        "failure should explain the ownership requirement:\n{output_text}"
    );
    assert!(
        !output_text.contains("gcloud"),
        "owner guard should stop before live gcloud mutations:\n{output_text}"
    );
}

#[test]
fn repair_deployed_uat_prereqs_holds_lock_across_scope_and_scheduler_repairs() {
    let harness = DeployedPrereqRepairHarness::new(false).expect("create prereq repair harness");
    let output = Command::new(repo_root().join("scripts/repair-deployed-uat-prereqs.sh"))
        .arg("--env")
        .arg("dev")
        .current_dir(repo_root())
        .env("PATH", harness.path())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("PROJECT_NUMBER", "135245112198")
        .env("FLOW_TENANT_ID", "arco-uat-tenant")
        .env("FLOW_WORKSPACE_ID", "arco-uat-workspace")
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_UAT_EXPECTED_API_CODE_VERSION", "uat-live")
        .env("ARCO_UAT_EXPECTED_API_GIT_SHA", "abc123")
        .env(
            "ARCO_UAT_EXPECTED_API_IMAGE",
            "us-central1-docker.pkg.dev/example/arco-api:uat-live",
        )
        .env("ARCO_DEPLOY_LOCK_DIR", harness.lock_dir())
        .env("ARCO_TEST_LOG", harness.log_path())
        .env("ARCO_TEST_REPAIR_MARKER", harness.repair_marker_path())
        .output()
        .expect("run deployed UAT prereq repair script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "live prereq repair should hold a lock across all repair phases:\n{output_text}"
    );
    let log = harness.read_log().expect("read gcloud log");
    assert!(
        log.contains("gcloud run services update arco-flow-dispatcher-dev"),
        "scope repair should run under the wrapper lock; log:\n{log}"
    );
    assert!(
        log.contains("gcloud scheduler jobs update http arco-flow-dispatcher-run-dev"),
        "Scheduler target repair should run under the same wrapper lock; log:\n{log}"
    );
    assert!(
        log.contains("gcloud scheduler jobs describe arco-flow-dispatcher-run-dev"),
        "final status should run after repairs; log:\n{log}"
    );
}

#[test]
fn repair_deployed_uat_prereqs_announces_repair_phase_after_initial_status_failure() {
    let harness = DeployedPrereqRepairHarness::new(false).expect("create prereq repair harness");
    let output = Command::new(repo_root().join("scripts/repair-deployed-uat-prereqs.sh"))
        .arg("--env")
        .arg("dev")
        .current_dir(repo_root())
        .env("PATH", harness.path())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("PROJECT_NUMBER", "135245112198")
        .env("FLOW_TENANT_ID", "arco-uat-tenant")
        .env("FLOW_WORKSPACE_ID", "arco-uat-workspace")
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_UAT_EXPECTED_API_CODE_VERSION", "uat-live")
        .env("ARCO_UAT_EXPECTED_API_GIT_SHA", "abc123")
        .env(
            "ARCO_UAT_EXPECTED_API_IMAGE",
            "us-central1-docker.pkg.dev/example/arco-api:uat-live",
        )
        .env("ARCO_DEPLOY_LOCK_DIR", harness.lock_dir())
        .env("ARCO_TEST_LOG", harness.log_path())
        .env("ARCO_TEST_REPAIR_MARKER", harness.repair_marker_path())
        .output()
        .expect("run deployed UAT prereq repair script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "live prereq repair should succeed after repair:\n{output_text}"
    );
    assert!(
        output_text.contains("deployed UAT prerequisites not ready; running repairs under owner uat-session for arco-testing-20260320/us-central1/dev"),
        "initial not-ready path should announce the repair phase and owner window:\n{output_text}"
    );
}

#[test]
fn repair_deployed_uat_prereqs_writes_initial_and_final_status_snapshots() {
    let harness = DeployedPrereqRepairHarness::new(false).expect("create prereq repair harness");
    let output = Command::new(repo_root().join("scripts/repair-deployed-uat-prereqs.sh"))
        .arg("--env")
        .arg("dev")
        .arg("--status-output-dir")
        .arg(harness.status_output_dir())
        .current_dir(repo_root())
        .env("PATH", harness.path())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("PROJECT_NUMBER", "135245112198")
        .env("FLOW_TENANT_ID", "arco-uat-tenant")
        .env("FLOW_WORKSPACE_ID", "arco-uat-workspace")
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_UAT_EXPECTED_API_CODE_VERSION", "uat-live")
        .env("ARCO_UAT_EXPECTED_API_GIT_SHA", "abc123")
        .env(
            "ARCO_UAT_EXPECTED_API_IMAGE",
            "us-central1-docker.pkg.dev/example/arco-api:uat-live",
        )
        .env("ARCO_DEPLOY_LOCK_DIR", harness.lock_dir())
        .env("ARCO_TEST_LOG", harness.log_path())
        .env("ARCO_TEST_REPAIR_MARKER", harness.repair_marker_path())
        .output()
        .expect("run deployed UAT prereq repair script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "live prereq repair should succeed after repair:\n{output_text}"
    );
    let initial_status = fs::read_to_string(harness.status_output_dir().join("initial-status.txt"))
        .expect("read initial status snapshot");
    let final_status = fs::read_to_string(harness.status_output_dir().join("final-status.txt"))
        .expect("read final status snapshot");
    assert!(
        initial_status
            .contains("live-deployed: not ready (flow scheduler target metadata mismatch)"),
        "initial snapshot should preserve the pre-repair blocker:\n{initial_status}"
    );
    assert!(
        final_status.contains("live-deployed: ready"),
        "final snapshot should preserve the post-repair readiness proof:\n{final_status}"
    );
}

#[test]
fn repair_deployed_uat_prereqs_stops_before_scheduler_when_cloud_owner_conflicts() {
    let harness = DeployedPrereqRepairHarness::new(false).expect("create prereq repair harness");
    let output = Command::new(repo_root().join("scripts/repair-deployed-uat-prereqs.sh"))
        .arg("--env")
        .arg("dev")
        .current_dir(repo_root())
        .env("PATH", harness.path())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("PROJECT_NUMBER", "135245112198")
        .env("FLOW_TENANT_ID", "arco-uat-tenant")
        .env("FLOW_WORKSPACE_ID", "arco-uat-workspace")
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_UAT_EXPECTED_API_CODE_VERSION", "uat-live")
        .env("ARCO_UAT_EXPECTED_API_GIT_SHA", "abc123")
        .env(
            "ARCO_UAT_EXPECTED_API_IMAGE",
            "us-central1-docker.pkg.dev/example/arco-api:uat-live",
        )
        .env("ARCO_DEPLOY_LOCK_DIR", harness.lock_dir())
        .env("ARCO_TEST_LOG", harness.log_path())
        .env("ARCO_TEST_REPAIR_MARKER", harness.repair_marker_path())
        .env("ARCO_TEST_CLOUD_OWNER", "other-session")
        .output()
        .expect("run deployed UAT prereq repair script");
    let output_text = command_output_text(&output);

    assert!(
        !output.status.success(),
        "cloud-owner conflict should fail the combined repair:\n{output_text}"
    );
    assert!(
        output_text.contains("Cloud Run deploy owner mismatch"),
        "failure should include the cloud-visible owner mismatch:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "Cloud Run scope repair failed; Scheduler repair and live deployed gate were not attempted"
        ),
        "combined wrapper should explain where the repair window stopped:\n{output_text}"
    );
    let log = harness.read_log().expect("read gcloud log");
    assert!(
        log.contains("gcloud run services describe arco-compactor-dev"),
        "scope repair should inspect Cloud Run owners before mutation; log:\n{log}"
    );
    assert!(
        !log.contains("gcloud run services update"),
        "cloud-owner guard should stop before Cloud Run updates; log:\n{log}"
    );
    assert!(
        !log.contains("gcloud scheduler jobs update"),
        "cloud-owner guard should stop before Scheduler target repair; log:\n{log}"
    );
    assert!(
        !log.contains("gcloud scheduler jobs resume"),
        "cloud-owner guard should stop before Scheduler resume; log:\n{log}"
    );
}

#[test]
fn repair_deployed_uat_prereqs_reports_partial_window_when_scheduler_repair_fails() {
    let harness = DeployedPrereqRepairHarness::new(false).expect("create prereq repair harness");
    let output = Command::new(repo_root().join("scripts/repair-deployed-uat-prereqs.sh"))
        .arg("--env")
        .arg("dev")
        .current_dir(repo_root())
        .env("PATH", harness.path())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("PROJECT_NUMBER", "135245112198")
        .env("FLOW_TENANT_ID", "arco-uat-tenant")
        .env("FLOW_WORKSPACE_ID", "arco-uat-workspace")
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_UAT_EXPECTED_API_CODE_VERSION", "uat-live")
        .env("ARCO_UAT_EXPECTED_API_GIT_SHA", "abc123")
        .env(
            "ARCO_UAT_EXPECTED_API_IMAGE",
            "us-central1-docker.pkg.dev/example/arco-api:uat-live",
        )
        .env("ARCO_DEPLOY_LOCK_DIR", harness.lock_dir())
        .env("ARCO_TEST_LOG", harness.log_path())
        .env("ARCO_TEST_REPAIR_MARKER", harness.repair_marker_path())
        .env("ARCO_TEST_SCHEDULER_REPAIR_FAIL", "1")
        .output()
        .expect("run deployed UAT prereq repair script");
    let output_text = command_output_text(&output);

    assert!(
        !output.status.success(),
        "Scheduler repair failure should fail the combined repair:\n{output_text}"
    );
    assert!(
        output_text.contains("simulated Scheduler repair failure"),
        "failure should include the delegated Scheduler repair error:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "Scheduler repair failed after Cloud Run scope repair; final readiness check and live deployed gate were not attempted"
        ),
        "combined wrapper should explain that the live window may be partially repaired:\n{output_text}"
    );
    let log = harness.read_log().expect("read gcloud log");
    assert!(
        log.contains("gcloud run services update arco-flow-dispatcher-dev"),
        "scope repair should run before the Scheduler failure; log:\n{log}"
    );
    assert!(
        log.contains("gcloud scheduler jobs update http arco-flow-dispatcher-run-dev"),
        "the test should exercise the Scheduler repair failure path; log:\n{log}"
    );
    assert!(
        !output_text.contains("live-deployed: ready"),
        "Scheduler failure should stop before the final strict readiness success path:\n{output_text}"
    );
}

#[test]
fn repair_deployed_uat_prereqs_skips_repairs_when_initial_status_is_ready() {
    let harness = DeployedPrereqRepairHarness::new(false).expect("create prereq repair harness");
    fs::write(harness.repair_marker_path(), "").expect("mark Scheduler target ready");

    let output = Command::new(repo_root().join("scripts/repair-deployed-uat-prereqs.sh"))
        .arg("--env")
        .arg("dev")
        .current_dir(repo_root())
        .env("PATH", harness.path())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("PROJECT_NUMBER", "135245112198")
        .env("FLOW_TENANT_ID", "arco-uat-tenant")
        .env("FLOW_WORKSPACE_ID", "arco-uat-workspace")
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_UAT_EXPECTED_API_CODE_VERSION", "uat-live")
        .env("ARCO_UAT_EXPECTED_API_GIT_SHA", "abc123")
        .env(
            "ARCO_UAT_EXPECTED_API_IMAGE",
            "us-central1-docker.pkg.dev/example/arco-api:uat-live",
        )
        .env("ARCO_DEPLOY_LOCK_DIR", harness.lock_dir())
        .env("ARCO_TEST_LOG", harness.log_path())
        .env("ARCO_TEST_REPAIR_MARKER", harness.repair_marker_path())
        .output()
        .expect("run deployed UAT prereq repair script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "ready deployed UAT prereq repair should skip mutations and succeed:\n{output_text}"
    );
    assert!(
        output_text.contains("deployed UAT prerequisites already ready; skipping repairs"),
        "ready path should explain why repair phases were skipped:\n{output_text}"
    );
    let log = harness.read_log().expect("read gcloud log");
    assert!(
        log.contains("gcloud scheduler jobs describe arco-flow-dispatcher-run-dev"),
        "ready path should still prove strict deployed readiness; log:\n{log}"
    );
    assert!(
        !log.contains("gcloud run services update"),
        "ready path should not create Cloud Run revisions; log:\n{log}"
    );
    assert!(
        !log.contains("gcloud scheduler jobs update"),
        "ready path should not update Scheduler targets; log:\n{log}"
    );
    assert!(
        !log.contains("gcloud scheduler jobs resume"),
        "ready path should not resume Scheduler jobs; log:\n{log}"
    );
}

#[test]
fn repair_deployed_uat_prereqs_ready_skip_still_writes_final_status_snapshot() {
    let harness = DeployedPrereqRepairHarness::new(false).expect("create prereq repair harness");
    fs::write(harness.repair_marker_path(), "").expect("mark Scheduler target ready");

    let output = Command::new(repo_root().join("scripts/repair-deployed-uat-prereqs.sh"))
        .arg("--env")
        .arg("dev")
        .arg("--status-output-dir")
        .arg(harness.status_output_dir())
        .current_dir(repo_root())
        .env("PATH", harness.path())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("PROJECT_NUMBER", "135245112198")
        .env("FLOW_TENANT_ID", "arco-uat-tenant")
        .env("FLOW_WORKSPACE_ID", "arco-uat-workspace")
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_UAT_EXPECTED_API_CODE_VERSION", "uat-live")
        .env("ARCO_UAT_EXPECTED_API_GIT_SHA", "abc123")
        .env(
            "ARCO_UAT_EXPECTED_API_IMAGE",
            "us-central1-docker.pkg.dev/example/arco-api:uat-live",
        )
        .env("ARCO_DEPLOY_LOCK_DIR", harness.lock_dir())
        .env("ARCO_TEST_LOG", harness.log_path())
        .env("ARCO_TEST_REPAIR_MARKER", harness.repair_marker_path())
        .output()
        .expect("run deployed UAT prereq repair script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "ready deployed UAT prereq repair should skip mutations and succeed:\n{output_text}"
    );
    let initial_status = fs::read_to_string(harness.status_output_dir().join("initial-status.txt"))
        .expect("read initial status snapshot");
    let final_status = fs::read_to_string(harness.status_output_dir().join("final-status.txt"))
        .expect("read final status snapshot");
    assert!(
        initial_status.contains("live-deployed: ready"),
        "initial ready snapshot should preserve readiness proof:\n{initial_status}"
    );
    assert!(
        final_status.contains("live-deployed: ready"),
        "final ready snapshot should exist even when repairs are skipped:\n{final_status}"
    );
    let log = harness.read_log().expect("read gcloud log");
    assert!(
        !log.contains("gcloud run services update"),
        "ready path should not create Cloud Run revisions; log:\n{log}"
    );
    assert!(
        !log.contains("gcloud scheduler jobs update"),
        "ready path should not update Scheduler targets; log:\n{log}"
    );
}

#[test]
fn repair_deployed_uat_prereqs_fails_when_final_status_is_not_ready() {
    let harness = DeployedPrereqRepairHarness::new(true).expect("create prereq repair harness");
    let output = Command::new(repo_root().join("scripts/repair-deployed-uat-prereqs.sh"))
        .arg("--env")
        .arg("dev")
        .current_dir(repo_root())
        .env("PATH", harness.path())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("PROJECT_NUMBER", "135245112198")
        .env("FLOW_TENANT_ID", "arco-uat-tenant")
        .env("FLOW_WORKSPACE_ID", "arco-uat-workspace")
        .env("ARCO_DEPLOY_OWNER", "uat-session")
        .env("ARCO_UAT_EXPECTED_API_CODE_VERSION", "uat-live")
        .env("ARCO_UAT_EXPECTED_API_GIT_SHA", "abc123")
        .env(
            "ARCO_UAT_EXPECTED_API_IMAGE",
            "us-central1-docker.pkg.dev/example/arco-api:uat-live",
        )
        .env("ARCO_DEPLOY_LOCK_DIR", harness.lock_dir())
        .env("ARCO_TEST_LOG", harness.log_path())
        .env("ARCO_TEST_REPAIR_MARKER", harness.repair_marker_path())
        .output()
        .expect("run deployed UAT prereq repair script");
    let output_text = command_output_text(&output);

    assert!(
        !output.status.success(),
        "live prereq repair should fail when final status remains not ready:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "deployed UAT prerequisites are still not ready after Cloud Run scope and Scheduler repair; live deployed gate was not attempted"
        ),
        "failure should explain that final status did not become ready and the evidence-producing gate was skipped:\n{output_text}"
    );
    assert!(
        output_text.contains("live-deployed: not ready"),
        "failure should include the status output:\n{output_text}"
    );
}

struct DeployedPrereqRepairHarness {
    _tempdir: TempDir,
    bin_dir: PathBuf,
    lock_dir: PathBuf,
    log_path: PathBuf,
    repair_marker_path: PathBuf,
    status_output_dir: PathBuf,
}

impl DeployedPrereqRepairHarness {
    fn new(force_not_ready: bool) -> std::io::Result<Self> {
        let tempdir = TempDir::new()?;
        let bin_dir = tempdir.path().join("bin");
        fs::create_dir(&bin_dir)?;
        let lock_dir = tempdir.path().join("deploy.lock");
        let log_path = tempdir.path().join("commands.log");
        let repair_marker_path = tempdir.path().join("scheduler-target-repaired");
        let status_output_dir = tempdir.path().join("status");

        write_stub(
            &bin_dir,
            "gcloud",
            &r#"printf 'gcloud %s\n' "$*" >> "$ARCO_TEST_LOG"
force_not_ready="__FORCE_NOT_READY__"
case "${1:-} ${2:-} ${3:-}" in
"run services describe")
  for arg in "$@"; do
    if [[ "$arg" == "--format=value(metadata.labels.arco_deploy_owner)" ]]; then
      printf '%s\n' "${ARCO_TEST_CLOUD_OWNER:-uat-session}"
      exit 0
    fi
	  done
	  cat <<'JSON'
	{"metadata":{"labels":{"arco_deploy_owner":"uat-session"}},"spec":{"template":{"spec":{"containers":[{"image":"example.com/arco:test","env":[{"name":"ARCO_TENANT_ID","value":"arco-uat-tenant"},{"name":"ARCO_WORKSPACE_ID","value":"arco-uat-workspace"},{"name":"ARCO_FLOW_TENANT_ID","value":"arco-uat-tenant"},{"name":"ARCO_FLOW_WORKSPACE_ID","value":"arco-uat-workspace"}]}]}}},"status":{"latestReadyRevisionName":"arco-test-00001","traffic":[{"revisionName":"arco-test-00001","percent":100}]}}
JSON
	  ;;
"run services update")
  [[ -d "$ARCO_DEPLOY_LOCK_DIR" ]] || { echo "missing deploy lock for Cloud Run update" >&2; exit 42; }
  ;;
"scheduler jobs describe")
  format=""
  for arg in "$@"; do
    case "$arg" in
    --format=*) format="${arg#--format=}" ;;
    esac
  done
  if [[ "$format" == "value(state)" ]]; then
    printf 'ENABLED\n'
    exit 0
  fi
  case "${4:-}" in
  arco-flow-dispatcher-run-dev)
    if [[ "$force_not_ready" != "true" && -f "$ARCO_TEST_REPAIR_MARKER" ]]; then
      cat <<'JSON'
{"state":"ENABLED","httpTarget":{"uri":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
    else
      cat <<'JSON'
{"state":"ENABLED","httpTarget":{"uri":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
    fi
    ;;
  arco-flow-sweeper-run-dev)
    cat <<'JSON'
{"state":"ENABLED","httpTarget":{"uri":"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
    ;;
  *)
    exit 1
    ;;
  esac
  ;;
"scheduler jobs resume" | "scheduler jobs update")
  [[ -d "$ARCO_DEPLOY_LOCK_DIR" ]] || { echo "missing deploy lock for Scheduler mutation" >&2; exit 43; }
  [[ "${ARCO_DEPLOY_LOCK_HELD:-}" == "true" ]] || { echo "Scheduler repair did not inherit deploy lock" >&2; exit 44; }
  if [[ "${ARCO_TEST_SCHEDULER_REPAIR_FAIL:-}" == "1" ]]; then
    echo "simulated Scheduler repair failure" >&2
    exit 45
  fi
  if [[ "${1:-} ${2:-} ${3:-} ${4:-}" == "scheduler jobs update http" ]]; then
    : >"$ARCO_TEST_REPAIR_MARKER"
  fi
  ;;
*)
  exit 1
  ;;
esac
"#
            .replace(
                "__FORCE_NOT_READY__",
                if force_not_ready { "true" } else { "false" },
            ),
        )?;
        write_stub(
            &bin_dir,
            "jq",
            r#"input="$(cat)"
JQ_INPUT="$input" JQ_EXPR="$*" python3 <<'PY'
import json
import os

doc = json.loads(os.environ["JQ_INPUT"])
expr = os.environ["JQ_EXPR"]

if ".state" in expr:
    print(doc.get("state", ""))
elif ".metadata.labels" in expr:
    print(doc.get("metadata", {}).get("labels", {}).get("arco_deploy_owner", ""))
elif ".httpTarget.uri" in expr:
    print(doc.get("httpTarget", {}).get("uri", ""))
elif ".httpTarget.oidcToken.audience" in expr:
    print(doc.get("httpTarget", {}).get("oidcToken", {}).get("audience", ""))
elif ".httpTarget.oidcToken.serviceAccountEmail" in expr:
    print(doc.get("httpTarget", {}).get("oidcToken", {}).get("serviceAccountEmail", ""))
elif ".httpTarget.httpMethod" in expr:
    print(doc.get("httpTarget", {}).get("httpMethod", ""))
elif ".spec.template.spec.containers[]?.image" in expr:
    containers = doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
    print(containers[0].get("image", "") if containers else "")
elif ".spec.template.spec.containers" in expr:
    args = expr.split()
    name = args[args.index("name") + 1]
    env = doc["spec"]["template"]["spec"]["containers"][0].get("env", [])
    for item in env:
        if item.get("name") == name:
            print(item.get("value", ""))
            break
elif ".status.latestReadyRevisionName" in expr:
    print(doc.get("status", {}).get("latestReadyRevisionName", ""))
elif ".status.traffic" in expr:
    args = expr.split()
    revision = args[args.index("revision") + 1] if "revision" in args else ""
    for item in doc.get("status", {}).get("traffic", []):
        if item.get("revisionName") == revision:
            print(item.get("percent", ""))
            break
PY
"#,
        )?;
        write_stub(
            &bin_dir,
            "curl",
            r#"cat <<'JSON'
{
  "service": "arco-api-dev",
  "packageVersion": "0.2.1",
  "codeVersion": "uat-live",
  "gitSha": "abc123",
  "image": "us-central1-docker.pkg.dev/example/arco-api:uat-live",
  "cloudRunRevision": "arco-api-dev-00001-test"
}
JSON
"#,
        )?;

        Ok(Self {
            _tempdir: tempdir,
            bin_dir,
            lock_dir,
            log_path,
            repair_marker_path,
            status_output_dir,
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

    fn lock_dir(&self) -> &Path {
        &self.lock_dir
    }

    fn log_path(&self) -> &Path {
        &self.log_path
    }

    fn repair_marker_path(&self) -> &Path {
        &self.repair_marker_path
    }

    fn status_output_dir(&self) -> &Path {
        &self.status_output_dir
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

fn command_output_text(output: &std::process::Output) -> String {
    format!(
        "status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}
