use std::collections::BTreeMap;
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

#[test]
fn task_token_secret_is_wired_only_to_api_and_flow_controller() {
    let terraform = terraform_text(["variables.tf", "main.tf", "iam.tf", "cloud_run.tf"]);

    assert!(terraform.contains("variable \"task_token_secret_name\""));
    assert!(terraform.contains("google_secret_manager_secret\" \"task_token_secret\""));

    let api_access = resource_block(
        &terraform,
        "google_secret_manager_secret_iam_member",
        "api_task_token_secret",
    )
    .expect("API task-token Secret Manager grant should exist");
    assert!(api_access.contains("google_service_account.api.email"));

    let controller_access = resource_block(
        &terraform,
        "google_secret_manager_secret_iam_member",
        "flow_controller_task_token_secret",
    )
    .expect("flow controller task-token Secret Manager grant should exist");
    assert!(controller_access.contains("google_service_account.flow_controller.email"));

    assert!(!terraform.contains("variable \"task_token_secret\""));
    assert!(!terraform.contains("value = var.task_token_secret"));
}

#[test]
fn state_store_prefix_has_exactly_one_writer() {
    let terraform = terraform_iam_text();

    assert!(
        terraform.contains("state_store_object_prefix = \"state-store/\""),
        "state-store/ object prefix local should be defined"
    );

    let block = resource_block(
        &terraform,
        "google_storage_bucket_iam_member",
        "api_write_state_store",
    )
    .expect("API state-store write binding should exist");
    assert!(block.contains("roles/storage.objectUser"));
    assert!(block.contains("serviceAccount:${google_service_account.api.email}"));
    assert!(block.contains("condition {"));
    assert!(block.contains("startsWith(\"${local.state_store_object_prefix}\")"));
    assert!(!block.contains("contains("));

    // Single-writer invariant, enforced structurally rather than by counting
    // occurrences of one local's name: walk EVERY google_storage_bucket_iam_member
    // block across all of infra/terraform and assert that no block other than
    // api_write_state_store carries a condition reaching into state-store/.
    // Resolving `local.` references first means an inlined literal, a second
    // local aliased to the same value, or a binding declared in main.tf /
    // cloud_run.tf is caught just the same.
    let locals = terraform_string_locals(&terraform);
    let blocks = resource_blocks(&terraform, "google_storage_bucket_iam_member");
    assert!(
        blocks.len() >= 10,
        "expected the bucket IAM bindings to be discovered, found {}",
        blocks.len()
    );

    let mut state_store_writers = Vec::new();
    for (name, body) in &blocks {
        let Some(condition) = condition_expression(body) else {
            continue;
        };
        let resolved = resolve_terraform_locals(&condition, &locals);
        if starts_with_arguments(&resolved)
            .iter()
            .any(|argument| argument.starts_with("state-store"))
        {
            state_store_writers.push(name.clone());
        }
    }

    assert_eq!(
        state_store_writers,
        vec!["api_write_state_store".to_string()],
        "exactly one bucket IAM binding may grant authority under state-store/, found: {state_store_writers:?}"
    );
}

#[test]
fn no_other_declared_prefix_shadows_control_store_paths() {
    // The control store writes under state-store/control-mvp/...; that path must
    // not be reachable through any other prefix local declared in the terraform
    // (notably state/, whose name is a proper string prefix of "state-store"
    // only if the trailing slash is ever dropped). Both operands come from the
    // real terraform text, so deleting or editing iam_conditions.tf changes the
    // outcome of this test.
    let terraform = terraform_iam_text();
    let locals = terraform_string_locals(&terraform);

    let control_store_path = format!(
        "{}control-mvp/catalog/current.pointer.json",
        locals
            .get("state_store_object_prefix")
            .expect("state_store_object_prefix local should be declared")
    );

    let other_prefixes: Vec<(&String, &String)> = locals
        .iter()
        .filter(|(name, _)| {
            name.as_str() != "state_store_object_prefix"
                && (name.ends_with("_object_prefix") || name.ends_with("_state_prefix"))
        })
        .collect();
    assert!(
        other_prefixes.len() >= 8,
        "expected the sibling object-prefix locals to be parsed, found {}",
        other_prefixes.len()
    );

    for (name, prefix) in other_prefixes {
        assert!(
            !control_store_path.starts_with(prefix.as_str()),
            "control-store path {control_store_path} must not match the {name} ({prefix}) write condition"
        );
    }
}

#[test]
fn api_service_account_can_invoke_sync_compactors() {
    let terraform = terraform_iam_text();

    let catalog_compactor = resource_block(
        &terraform,
        "google_cloud_run_v2_service_iam_member",
        "api_compactor_invoker",
    )
    .expect("API service account should invoke catalog compactor");
    assert!(catalog_compactor.contains("name     = google_cloud_run_v2_service.compactor.name"));
    assert!(catalog_compactor.contains("role     = \"roles/run.invoker\""));
    assert!(
        catalog_compactor
            .contains("member   = \"serviceAccount:${google_service_account.api.email}\"")
    );

    let flow_compactor = resource_block(
        &terraform,
        "google_cloud_run_v2_service_iam_member",
        "api_flow_compactor_invoker",
    )
    .expect("API service account should invoke flow compactor");
    assert!(flow_compactor.contains("name     = google_cloud_run_v2_service.flow_compactor.name"));
    assert!(flow_compactor.contains("role     = \"roles/run.invoker\""));
    assert!(
        flow_compactor
            .contains("member   = \"serviceAccount:${google_service_account.api.email}\"")
    );
}

/// Every `.tf` file under infra/terraform, not just the two IAM files: a second
/// writer for a scoped prefix is just as dangerous when it is declared in
/// main.tf or cloud_run.tf.
fn terraform_iam_text() -> String {
    let dir = repo_root().join("infra/terraform");
    let mut files: Vec<PathBuf> = fs::read_dir(&dir)
        .unwrap_or_else(|err| panic!("read {}: {err}", dir.display()))
        .map(|entry| entry.expect("read terraform dir entry").path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("tf"))
        .collect();
    files.sort();
    assert!(
        files.len() >= 4,
        "expected several terraform files under {}, found {}",
        dir.display(),
        files.len()
    );

    files
        .into_iter()
        .map(|path| {
            fs::read_to_string(&path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()))
        })
        .collect::<Vec<_>>()
        .join("\n")
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

/// Collects `name = "value"` assignments (the string-valued `locals`) so
/// condition expressions can be resolved to the literal prefixes they enforce.
fn terraform_string_locals(text: &str) -> BTreeMap<String, String> {
    let mut locals = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with('#') {
            continue;
        }
        let Some((name, value)) = line.split_once('=') else {
            continue;
        };
        let name = name.trim();
        if name.is_empty()
            || !name
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        {
            continue;
        }
        let value = value.trim();
        let Some(value) = value.strip_prefix('"').and_then(|v| v.strip_suffix('"')) else {
            continue;
        };
        if value.contains('"') {
            continue;
        }
        locals.insert(name.to_string(), value.to_string());
    }
    locals
}

/// Substitutes `${local.<name>}` and bare `local.<name>` with the declared
/// string value, repeatedly, so nested locals resolve.
fn resolve_terraform_locals(text: &str, locals: &BTreeMap<String, String>) -> String {
    let mut resolved = text.to_string();
    for _ in 0..8 {
        let mut changed = false;
        for (name, value) in locals {
            for pattern in [format!("${{local.{name}}}"), format!("local.{name}")] {
                if resolved.contains(&pattern) {
                    resolved = resolved.replace(&pattern, value);
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    resolved
}

/// Returns the literal arguments of every `startsWith("...")` call in `text`.
fn starts_with_arguments(text: &str) -> Vec<String> {
    let mut arguments = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("startsWith(\"") {
        let after = &rest[start + "startsWith(\"".len()..];
        let Some(end) = after.find('"') else {
            break;
        };
        arguments.push(after[..end].to_string());
        rest = &after[end + 1..];
    }
    arguments
}

/// Extracts the `expression` heredoc from a resource block's `condition` sub-block.
fn condition_expression(block: &str) -> Option<String> {
    let start = block.find("expression")?;
    let after = &block[start..];
    let open = after.find("<<-EOT")? + "<<-EOT".len();
    let end = after[open..].find("EOT")? + open;
    Some(after[open..end].to_string())
}

/// Every `resource "<kind>" "<name>"` block in `text`, as `(name, body)`.
/// Commented-out declarations (`# resource "..."`) are skipped.
fn resource_blocks(text: &str, kind: &str) -> Vec<(String, String)> {
    let needle = format!("resource \"{kind}\" \"");
    let mut blocks = Vec::new();
    let mut search_from = 0usize;
    while let Some(found) = text[search_from..].find(&needle) {
        let start = search_from + found;
        search_from = start + needle.len();

        let line_start = text[..start].rfind('\n').map_or(0, |idx| idx + 1);
        if text[line_start..start].trim_start().starts_with('#') {
            continue;
        }

        let after = &text[start + needle.len()..];
        let Some(name_end) = after.find('"') else {
            continue;
        };
        let name = after[..name_end].to_string();
        if let Some(body) = resource_block(&text[start..], kind, &name) {
            search_from = start + body.len();
            blocks.push((name, body));
        }
    }
    blocks
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
