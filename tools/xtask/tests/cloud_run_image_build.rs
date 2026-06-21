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
