use std::fs;
use std::path::PathBuf;

#[test]
fn anti_entropy_cursor_writes_are_prefix_scoped() {
    let terraform = terraform_iam_text();
    let block = resource_block(
        &terraform,
        "google_storage_bucket_iam_member",
        "compactor_antientropy_write_cursor",
    )
    .expect("anti-entropy cursor IAM binding should exist");

    assert!(block.contains("google_project_iam_custom_role.storage_object_writer_no_list.name"));
    assert!(block.contains("condition {"));
    assert!(block.contains("startsWith(\"${local.anti_entropy_state_prefix}\")"));
    assert!(!block.contains("contains("));
}

#[test]
fn flow_timer_ingest_writes_are_prefix_scoped() {
    let terraform = terraform_iam_text();

    assert!(
        resource_block(
            &terraform,
            "google_storage_bucket_iam_member",
            "flow_timer_ingest_storage_access",
        )
        .is_none(),
        "flow timer ingest must not have bucket-wide storage.objectUser"
    );

    let ledger = resource_block(
        &terraform,
        "google_storage_bucket_iam_member",
        "flow_timer_ingest_write_ledger",
    )
    .expect("flow timer ingest ledger write binding should exist");
    assert!(ledger.contains("roles/storage.objectCreator"));
    assert!(ledger.contains("condition {"));
    assert!(ledger.contains("startsWith(\"${local.ledger_object_prefix}\")"));

    let locks = resource_block(
        &terraform,
        "google_storage_bucket_iam_member",
        "flow_timer_ingest_manage_locks",
    )
    .expect("flow timer ingest lock binding should exist");
    assert!(locks.contains("roles/storage.objectUser"));
    assert!(locks.contains("condition {"));
    assert!(locks.contains("startsWith(\"${local.locks_object_prefix}\")"));
}

#[test]
fn flow_worker_dispatch_secret_is_wired_to_producers_and_worker() {
    let terraform = terraform_text(["variables.tf", "main.tf", "iam.tf", "cloud_run.tf"]);

    assert!(terraform.contains("variable \"flow_worker_dispatch_secret_name\""));
    assert!(terraform.contains("google_secret_manager_secret\" \"flow_worker_dispatch_secret\""));
    assert!(terraform.contains("flow_controller_worker_dispatch_secret"));
    assert!(terraform.contains("flow_worker_dispatch_secret"));

    let dispatcher = resource_block(&terraform, "google_cloud_run_v2_service", "flow_dispatcher")
        .expect("flow dispatcher Cloud Run service should exist");
    assert!(dispatcher.contains("ARCO_FLOW_WORKER_DISPATCH_SECRET"));
    assert!(dispatcher.contains("secret_key_ref"));

    let sweeper = resource_block(&terraform, "google_cloud_run_v2_service", "flow_sweeper")
        .expect("flow sweeper Cloud Run service should exist");
    assert!(sweeper.contains("ARCO_FLOW_WORKER_DISPATCH_SECRET"));
    assert!(sweeper.contains("secret_key_ref"));

    let worker = resource_block(&terraform, "google_cloud_run_v2_service", "flow_worker")
        .expect("flow worker Cloud Run service should exist");
    assert!(worker.contains("ARCO_FLOW_WORKER_DISPATCH_SECRET"));
    assert!(worker.contains("secret_key_ref"));
}

fn terraform_iam_text() -> String {
    terraform_text(["iam.tf", "iam_conditions.tf"])
}

fn terraform_text<const N: usize>(files: [&str; N]) -> String {
    files
        .into_iter()
        .map(|file| {
            fs::read_to_string(repo_root().join("infra/terraform").join(file))
                .unwrap_or_else(|err| panic!("read {file}: {err}"))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn resource_block(text: &str, kind: &str, name: &str) -> Option<String> {
    let needle = format!("resource \"{kind}\" \"{name}\"");
    let start = text.find(&needle)?;
    let open = start + text[start..].find('{')?;
    let mut depth = 0usize;

    for (offset, ch) in text[open..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    let end = open + offset + ch.len_utf8();
                    return Some(text[start..end].to_string());
                }
            }
            _ => {}
        }
    }

    None
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask dir")
        .parent()
        .expect("repo root")
        .to_path_buf()
}
