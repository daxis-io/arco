use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

#[test]
fn build_cloud_run_image_dry_run_prints_cloud_build_submit_command() {
    let output = Command::new(repo_root().join("scripts/build-cloud-run-image.sh"))
        .arg("--project")
        .arg("arco-testing-20260320")
        .arg("--bin")
        .arg("arco-api")
        .arg("--image")
        .arg("us-central1-docker.pkg.dev/arco-testing-20260320/arco/arco-api:test")
        .arg("--dry-run")
        .current_dir(repo_root())
        .output()
        .expect("run build script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "image build dry-run should succeed:\n{output_text}"
    );
    assert!(
        output_text.contains("gcloud builds submit"),
        "dry-run should show the Cloud Build command:\n{output_text}"
    );
    assert!(
        output_text.contains("infra/cloudbuild/cloud-run-image.yaml"),
        "dry-run should use the repo Cloud Build config:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "--substitutions=_BIN=arco-api,_IMAGE=us-central1-docker.pkg.dev/arco-testing-20260320/arco/arco-api:test"
        ),
        "dry-run should pass the selected binary and image tag as substitutions:\n{output_text}"
    );
}

#[test]
fn build_cloud_run_image_rejects_unknown_binary() {
    let output = Command::new(repo_root().join("scripts/build-cloud-run-image.sh"))
        .arg("--project")
        .arg("arco-testing-20260320")
        .arg("--bin")
        .arg("not-a-service")
        .arg("--image")
        .arg("us-central1-docker.pkg.dev/arco-testing-20260320/arco/not-a-service:test")
        .arg("--dry-run")
        .current_dir(repo_root())
        .output()
        .expect("run build script");
    let output_text = command_output_text(&output);

    assert!(
        !output.status.success(),
        "unknown service binary should fail:\n{output_text}"
    );
    assert!(
        output_text.contains("Unsupported --bin 'not-a-service'"),
        "failure should identify unsupported binary:\n{output_text}"
    );
}

#[test]
fn build_cloud_run_image_supports_worker_binary_once_source_exists() {
    let output = Command::new(repo_root().join("scripts/build-cloud-run-image.sh"))
        .arg("--project")
        .arg("arco-testing-20260320")
        .arg("--bin")
        .arg("arco_flow_worker")
        .arg("--image")
        .arg("us-central1-docker.pkg.dev/arco-testing-20260320/arco/arco-flow-worker:test")
        .arg("--dry-run")
        .current_dir(repo_root())
        .output()
        .expect("run build script");
    let output_text = command_output_text(&output);

    assert!(
        output.status.success(),
        "worker image build dry-run should succeed when the source is present:\n{output_text}"
    );
    assert!(
        output_text.contains(
            "--substitutions=_BIN=arco_flow_worker,_IMAGE=us-central1-docker.pkg.dev/arco-testing-20260320/arco/arco-flow-worker:test"
        ),
        "dry-run should pass the worker binary and image tag as substitutions:\n{output_text}"
    );
}

#[test]
fn cloud_run_image_dockerignore_excludes_local_build_state() {
    let dockerignore =
        std::fs::read_to_string(repo_root().join(".dockerignore")).expect("read .dockerignore");

    for ignored_path in ["target/", ".git/", ".worktrees/"] {
        assert!(
            dockerignore.lines().any(|line| line == ignored_path),
            ".dockerignore should exclude {ignored_path}"
        );
    }
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
