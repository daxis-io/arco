use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[test]
fn compactor_cloud_run_service_launches_serve_subcommand() {
    let cloud_run_tf = fs::read_to_string(repo_root().join("infra/terraform/cloud_run.tf"))
        .expect("read cloud_run.tf");
    let compactor_start = cloud_run_tf
        .find(r#"resource "google_cloud_run_v2_service" "compactor""#)
        .expect("find compactor service resource");
    let flow_compactor_start = cloud_run_tf
        .find(r#"resource "google_cloud_run_v2_service" "flow_compactor""#)
        .expect("find flow compactor service resource");
    let compactor_resource = &cloud_run_tf[compactor_start..flow_compactor_start];

    assert!(
        compactor_resource.contains(r#"args = ["serve"]"#),
        "compactor Cloud Run service must pass the CLI subcommand required to start its HTTP health server"
    );
    assert!(
        compactor_resource.contains("ingress = var.compactor_ingress"),
        "compactor Cloud Run ingress must stay configurable so job-to-service networking can match the environment"
    );
    assert!(
        compactor_resource.contains(r#"name  = "ARCO_TENANT_ID""#)
            && compactor_resource.contains("value = var.compactor_tenant_id"),
        "compactor Cloud Run service must keep its tenant scope env"
    );
    assert!(
        compactor_resource.contains(r#"name  = "ARCO_WORKSPACE_ID""#)
            && compactor_resource.contains("value = var.compactor_workspace_id"),
        "compactor Cloud Run service must keep its workspace scope env"
    );
}

#[test]
fn cloud_run_services_keep_task_token_env() {
    let cloud_run_tf = fs::read_to_string(repo_root().join("infra/terraform/cloud_run.tf"))
        .expect("read cloud_run.tf");

    assert!(
        cloud_run_tf.contains(r#"name  = "ARCO_TASK_TOKEN_SECRET""#)
            && cloud_run_tf.contains("value = var.task_token_secret"),
        "API Cloud Run service must keep task-token validation secret"
    );
    assert!(
        cloud_run_tf.contains(r#"name  = "ARCO_FLOW_TASK_TOKEN_SECRET""#)
            && cloud_run_tf.contains("value = var.task_token_secret"),
        "flow dispatcher/sweeper services must keep task-token minting secret"
    );
    assert!(
        cloud_run_tf.contains(r#"name  = "ARCO_FLOW_TASK_TOKEN_AUDIENCE""#)
            && cloud_run_tf.contains("value = var.task_token_audience"),
        "flow dispatcher/sweeper services must keep task-token audience"
    );
}

#[test]
fn anti_entropy_cursor_writer_uses_no_list_custom_role() {
    let iam_tf =
        fs::read_to_string(repo_root().join("infra/terraform/iam.tf")).expect("read iam.tf");
    let iam_conditions_tf =
        fs::read_to_string(repo_root().join("infra/terraform/iam_conditions.tf"))
            .expect("read iam_conditions.tf");

    assert!(
        iam_tf.contains(
            r#"resource "google_project_iam_custom_role" "storage_object_writer_no_list""#
        ) && iam_tf.contains(r#""storage.objects.create""#)
            && iam_tf.contains(r#""storage.objects.update""#)
            && iam_tf.contains(r#""storage.objects.delete""#),
        "anti-entropy cursor writes should use a no-list custom writer role"
    );
    assert!(
        iam_conditions_tf
            .contains("role   = google_project_iam_custom_role.storage_object_writer_no_list.name"),
        "anti-entropy cursor binding should use the no-list writer role"
    );
    assert!(
        !iam_conditions_tf.contains(
            "role   = \"roles/storage.objectUser\"\n  member = \"serviceAccount:${google_service_account.compactor_antientropy.email}\""
        ),
        "anti-entropy service account should not use predefined bucket-wide objectUser"
    );
}

#[test]
fn flow_worker_can_write_pipeline_data_without_bucket_list() {
    let cloud_run_tf = fs::read_to_string(repo_root().join("infra/terraform/cloud_run.tf"))
        .expect("read cloud_run.tf");
    let iam_conditions_tf =
        fs::read_to_string(repo_root().join("infra/terraform/iam_conditions.tf"))
            .expect("read iam_conditions.tf");

    let worker_start = cloud_run_tf
        .find(r#"resource "google_cloud_run_v2_service" "flow_worker""#)
        .expect("find flow worker service");
    let worker_resource = &cloud_run_tf[worker_start..];

    assert!(
        worker_resource.contains(r#"name  = "ARCO_STORAGE_BUCKET""#)
            && worker_resource.contains("value = google_storage_bucket.catalog.name"),
        "flow worker must receive the catalog bucket so live workers can write table data"
    );
    assert!(
        iam_conditions_tf.contains(
            r#"resource "google_storage_bucket_iam_member" "flow_worker_write_warehouse""#
        ) && iam_conditions_tf
            .contains("role   = google_project_iam_custom_role.storage_object_writer_no_list.name")
            && iam_conditions_tf.contains(
                "member = \"serviceAccount:${google_service_account.flow_worker[0].email}\""
            )
            && iam_conditions_tf.contains(r#"startsWith("${local.warehouse_object_prefix}")"#),
        "flow worker should get no-list write access scoped to warehouse/ objects"
    );
    assert!(
        iam_conditions_tf
            .contains(r#"resource "google_storage_bucket_iam_member" "api_write_warehouse_delta""#)
            && iam_conditions_tf
                .contains("member = \"serviceAccount:${google_service_account.api.email}\"")
            && iam_conditions_tf.contains("role   = \"roles/storage.objectCreator\"")
            && iam_conditions_tf.contains(r#"startsWith("${local.warehouse_object_prefix}")"#),
        "API should be able to create Delta log objects under warehouse/ for coordinated commits"
    );
}

#[test]
fn deploy_dry_run_accepts_existing_tfvars_file_override() {
    let harness = DeployHarness::new().expect("create deploy harness");

    let output = Command::new(repo_root().join("scripts/deploy.sh"))
        .arg("--env")
        .arg("dev")
        .arg("--dry-run")
        .arg("--tfvars-file")
        .arg("infra/terraform/environments/arco-testing-dev.tfvars")
        .current_dir(repo_root())
        .env("PATH", harness.path())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("PROJECT_NUMBER", "135245112198")
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
        .env("FLOW_SWEEPER_IMAGE", "example.com/arco-flow-sweeper:test")
        .env(
            "FLOW_TIMER_INGEST_IMAGE",
            "example.com/arco-flow-timer-ingest:test",
        )
        .env("FLOW_WORKER_IMAGE", "example.com/arco-flow-worker:test")
        .env("API_CODE_VERSION", "test-code-version")
        .env("TF_VAR_flow_tenant_id", "tenant-dev")
        .env("TF_VAR_flow_workspace_id", "workspace-dev")
        .env("ARCO_TEST_LOG", harness.log_path())
        .output()
        .expect("run deploy script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "deploy dry-run should accept a tfvars file override:\n{output_text}"
    );

    let log = harness.read_log().expect("read tool log");
    assert!(
        log.lines()
            .any(|line| line == "terraform init -lockfile=readonly"),
        "deploy dry-run should initialize Terraform with the committed lockfile; log:\n{log}"
    );
    assert!(
        !log.contains("terraform init -upgrade"),
        "deploy dry-run should not rewrite provider selections during deployment; log:\n{log}"
    );
    assert!(
        log.contains("arco-testing-dev.tfvars"),
        "terraform plans should use the overridden tfvars file; log:\n{log}"
    );
    assert!(
        !log.contains("environments/dev.tfvars"),
        "terraform plans should not fall back to the missing default dev tfvars file; log:\n{log}"
    );
    assert!(
        log.contains("-target=google_service_account.flow_worker"),
        "compactor-first Terraform plans should include the moved flow worker service account; log:\n{log}"
    );
    assert!(
        log.contains("-var=api_image=example.com/arco-api:test"),
        "deploy images must override placeholder values from tfvars; log:\n{log}"
    );
    assert!(
        log.contains("-var=api_code_version=test-code-version"),
        "deploy code version must override placeholder values from tfvars; log:\n{log}"
    );
    assert!(
        log.contains("-var=flow_dispatcher_image=example.com/arco-flow-dispatcher:test"),
        "flow service images must override blank tfvars values; log:\n{log}"
    );
    assert!(
        log.contains("-var=flow_tenant_id=tenant-dev"),
        "flow tenant scope must override blank tfvars values; log:\n{log}"
    );
}

#[test]
fn deploy_dry_run_fails_when_existing_cloud_run_service_is_missing_from_state() {
    let harness = DeployHarness::with_existing_cloud_run_service("arco-compactor-dev")
        .expect("create deploy harness");

    let output = Command::new(repo_root().join("scripts/deploy.sh"))
        .arg("--env")
        .arg("dev")
        .arg("--dry-run")
        .arg("--tfvars-file")
        .arg("infra/terraform/environments/arco-testing-dev.tfvars")
        .current_dir(repo_root())
        .env("PATH", harness.path())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("PROJECT_NUMBER", "135245112198")
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
        .env("FLOW_SWEEPER_IMAGE", "example.com/arco-flow-sweeper:test")
        .env(
            "FLOW_TIMER_INGEST_IMAGE",
            "example.com/arco-flow-timer-ingest:test",
        )
        .env("FLOW_WORKER_IMAGE", "example.com/arco-flow-worker:test")
        .env("TF_VAR_flow_tenant_id", "tenant-dev")
        .env("TF_VAR_flow_workspace_id", "workspace-dev")
        .env("ARCO_TEST_LOG", harness.log_path())
        .output()
        .expect("run deploy script");
    let output_text = command_output_text(&output);

    assert!(
        !output.status.success(),
        "deploy dry-run should fail before planning over unmanaged existing Cloud Run services:\n{output_text}"
    );
    assert!(
        output_text.contains("arco-compactor-dev exists in GCP but google_cloud_run_v2_service.compactor is not in Terraform state"),
        "failure should identify the unmanaged live resource:\n{output_text}"
    );

    let log = harness.read_log().expect("read tool log");
    assert!(
        !log.contains("terraform plan"),
        "deploy dry-run should fail before Terraform plans a create for an unmanaged live resource; log:\n{log}"
    );
}

#[test]
fn deploy_dry_run_fails_when_existing_no_list_writer_role_is_missing_from_state() {
    let harness = DeployHarness::with_existing_project_role("storageObjectWriterNoList")
        .expect("create deploy harness");

    let output = Command::new(repo_root().join("scripts/deploy.sh"))
        .arg("--env")
        .arg("dev")
        .arg("--dry-run")
        .arg("--tfvars-file")
        .arg("infra/terraform/environments/arco-testing-dev.tfvars")
        .current_dir(repo_root())
        .env("PATH", harness.path())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("PROJECT_NUMBER", "135245112198")
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
        .env("FLOW_SWEEPER_IMAGE", "example.com/arco-flow-sweeper:test")
        .env(
            "FLOW_TIMER_INGEST_IMAGE",
            "example.com/arco-flow-timer-ingest:test",
        )
        .env("FLOW_WORKER_IMAGE", "example.com/arco-flow-worker:test")
        .env("TF_VAR_flow_tenant_id", "tenant-dev")
        .env("TF_VAR_flow_workspace_id", "workspace-dev")
        .env("ARCO_TEST_LOG", harness.log_path())
        .output()
        .expect("run deploy script");
    let output_text = command_output_text(&output);

    assert!(
        !output.status.success(),
        "deploy dry-run should fail before planning over an unmanaged custom role:\n{output_text}"
    );
    assert!(
        output_text.contains("projects/arco-testing-20260320/roles/storageObjectWriterNoList exists in GCP but google_project_iam_custom_role.storage_object_writer_no_list is not in Terraform state"),
        "failure should identify the unmanaged no-list writer role:\n{output_text}"
    );

    let log = harness.read_log().expect("read tool log");
    assert!(
        !log.contains("terraform plan"),
        "deploy dry-run should fail before Terraform plans a create for an unmanaged live custom role; log:\n{log}"
    );
}

#[test]
fn deploy_validates_internal_cloud_run_services_without_local_proxy() {
    let harness = DeployHarness::with_internal_cloud_run_services().expect("create deploy harness");

    let output = Command::new(repo_root().join("scripts/deploy.sh"))
        .arg("--env")
        .arg("dev")
        .arg("--timeout")
        .arg("10")
        .arg("--tfvars-file")
        .arg("infra/terraform/environments/arco-testing-dev.tfvars")
        .current_dir(repo_root())
        .env("PATH", harness.path())
        .env("PROJECT_ID", "arco-testing-20260320")
        .env("PROJECT_NUMBER", "135245112198")
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
        .env("FLOW_SWEEPER_IMAGE", "example.com/arco-flow-sweeper:test")
        .env(
            "FLOW_TIMER_INGEST_IMAGE",
            "example.com/arco-flow-timer-ingest:test",
        )
        .env("FLOW_WORKER_IMAGE", "example.com/arco-flow-worker:test")
        .env("API_CODE_VERSION", "test-code-version")
        .env("TF_VAR_flow_tenant_id", "tenant-dev")
        .env("TF_VAR_flow_workspace_id", "workspace-dev")
        .env("API_HEALTH_TIMEOUT", "10")
        .env("FLOW_COMPACTOR_HEALTH_TIMEOUT", "10")
        .env("HEALTH_CHECK_INTERVAL", "1")
        .env("ARCO_TEST_LOG", harness.log_path())
        .output()
        .expect("run deploy script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "deploy should validate internal-only services without local proxy:\n{output_text}"
    );

    let log = harness.read_log().expect("read tool log");
    assert!(
        log.contains("gcloud logging read"),
        "internal compactor health should use Cloud Logging evidence; log:\n{log}"
    );
    assert!(
        !log.contains("gcloud run services proxy"),
        "internal-only services are not reachable through local gcloud proxy; log:\n{log}"
    );
    assert!(
        !log.contains("curl "),
        "internal-only service health should not fall back to local curl; log:\n{log}"
    );
    assert!(
        log.contains("-target=google_cloud_run_v2_service.compactor")
            && log.contains("terraform apply")
            && log.contains("-auto-approve"),
        "deploy should still run the compactor-first Terraform apply; log:\n{log}"
    );
}

struct DeployHarness {
    _tempdir: TempDir,
    bin_dir: PathBuf,
    log_path: PathBuf,
}

impl DeployHarness {
    fn new() -> std::io::Result<Self> {
        Self::with_gcloud_body(gcloud_stub_body(None))
    }

    fn with_existing_cloud_run_service(service_name: &str) -> std::io::Result<Self> {
        Self::with_gcloud_body(gcloud_stub_body(Some(service_name)))
    }

    fn with_existing_project_role(role_id: &str) -> std::io::Result<Self> {
        Self::with_gcloud_body(gcloud_stub_body_with_project_role(role_id))
    }

    fn with_internal_cloud_run_services() -> std::io::Result<Self> {
        Self::with_tool_bodies(
            &terraform_stateful_stub_body(),
            &internal_cloud_run_gcloud_stub_body(),
        )
    }

    fn with_gcloud_body(gcloud_body: String) -> std::io::Result<Self> {
        Self::with_tool_bodies(
            "printf 'terraform %s\\n' \"$*\" >> \"$ARCO_TEST_LOG\"\n",
            &gcloud_body,
        )
    }

    fn with_tool_bodies(terraform_body: &str, gcloud_body: &str) -> std::io::Result<Self> {
        let tempdir = TempDir::new()?;
        let bin_dir = tempdir.path().join("bin");
        fs::create_dir(&bin_dir)?;
        let log_path = tempdir.path().join("commands.log");

        write_stub(&bin_dir, "terraform", terraform_body)?;
        write_stub(&bin_dir, "gcloud", gcloud_body)?;
        write_stub(
            &bin_dir,
            "curl",
            "printf 'curl %s\\n' \"$*\" >> \"$ARCO_TEST_LOG\"\n",
        )?;
        write_stub(
            &bin_dir,
            "jq",
            "printf 'jq %s\\n' \"$*\" >> \"$ARCO_TEST_LOG\"\n",
        )?;

        Ok(Self {
            _tempdir: tempdir,
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
}

fn gcloud_stub_body(existing_cloud_run_service: Option<&str>) -> String {
    let existing_cloud_run_service = existing_cloud_run_service.unwrap_or("");
    format!(
        r#"printf 'gcloud %s\n' "$*" >> "$ARCO_TEST_LOG"
if [[ "${{1:-}} ${{2:-}} ${{3:-}}" == "run services describe" ]]; then
  [[ "${{4:-}}" == "{existing_cloud_run_service}" ]]
  exit $?
fi
if [[ "${{1:-}} ${{2:-}} ${{3:-}}" == "run jobs describe" ]]; then
  exit 1
fi
if [[ "${{1:-}} ${{2:-}}" == "iam service-accounts" && "${{3:-}}" == "describe" ]]; then
  exit 1
fi
if [[ "${{1:-}} ${{2:-}} ${{3:-}}" == "iam roles describe" ]]; then
  exit 1
fi
"#
    )
}

fn gcloud_stub_body_with_project_role(existing_role_id: &str) -> String {
    format!(
        r#"printf 'gcloud %s\n' "$*" >> "$ARCO_TEST_LOG"
if [[ "${{1:-}} ${{2:-}} ${{3:-}}" == "run services describe" ]]; then
  exit 1
fi
if [[ "${{1:-}} ${{2:-}} ${{3:-}}" == "run jobs describe" ]]; then
  exit 1
fi
if [[ "${{1:-}} ${{2:-}}" == "iam service-accounts" && "${{3:-}}" == "describe" ]]; then
  exit 1
fi
if [[ "${{1:-}} ${{2:-}} ${{3:-}}" == "iam roles describe" ]]; then
  [[ "${{4:-}}" == "{existing_role_id}" ]]
  exit $?
fi
"#
    )
}

fn terraform_stateful_stub_body() -> String {
    r#"printf 'terraform %s\n' "$*" >> "$ARCO_TEST_LOG"
if [[ "${1:-} ${2:-}" == "state list" ]]; then
  printf '%s\n' \
    "google_cloud_run_v2_service.api" \
    "google_cloud_run_v2_service.compactor" \
    "google_cloud_run_v2_service.flow_compactor" \
    "google_cloud_run_v2_job.compactor_antientropy" \
    "google_project_iam_custom_role.storage_object_lister" \
    "google_service_account.flow_timer_ingest[0]" \
    "google_cloud_run_v2_service.flow_dispatcher[0]" \
    "google_cloud_run_v2_service.flow_sweeper[0]" \
    "google_cloud_run_v2_service.flow_timer_ingest[0]" \
    "google_cloud_run_v2_service.flow_worker[0]"
fi
"#
    .to_string()
}

fn internal_cloud_run_gcloud_stub_body() -> String {
    r#"printf 'gcloud %s\n' "$*" >> "$ARCO_TEST_LOG"
if [[ "${1:-} ${2:-} ${3:-}" == "run services describe" ]]; then
  service="${4:-}"
  format=""
  for arg in "$@"; do
    case "$arg" in
    --format=*) format="${arg#--format=}" ;;
    esac
  done
  case "$format" in
  "value(metadata.name)")
    printf '%s\n' "$service"
    ;;
  "value(metadata.annotations.\"run.googleapis.com/ingress-status\")")
    printf '%s\n' "internal"
    ;;
  "value(status.latestCreatedRevisionName)" | "value(status.latestReadyRevisionName)")
    printf '%s\n' "${service}-00001-test"
    ;;
  *)
    printf '%s\n' "$service"
    ;;
  esac
  exit 0
fi
if [[ "${1:-} ${2:-}" == "logging read" ]]; then
  printf '%s\n' "2026-06-01T07:12:21Z"
  exit 0
fi
if [[ "${1:-} ${2:-} ${3:-}" == "run jobs describe" ]]; then
  exit 1
fi
if [[ "${1:-} ${2:-}" == "iam service-accounts" && "${3:-}" == "describe" ]]; then
  exit 1
fi
if [[ "${1:-} ${2:-} ${3:-}" == "iam roles describe" ]]; then
  exit 1
fi
"#
    .to_string()
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
