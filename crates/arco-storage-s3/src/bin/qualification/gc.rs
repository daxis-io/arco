//! Private authority-8 planner exercised through the existing bounded supervisor.
use super::{JOURNAL, Result, provider};
use arco_catalog::state_store::SyntheticKvEntry;
use arco_catalog::{ControlMvpMaintenanceWorker, ControlMvpStateStore, StateScope};
use arco_core::{ScopedStorage, StorageBackend, WritePrecondition};
use bytes::Bytes;
use chrono::{Duration, Utc};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, io::Write as _, sync::Arc};

const ROWS: u64 = 4096;
const ORPHANS: u64 = 600;

pub fn inventory() -> serde_json::Value {
    json!({"workload":"authority8-gc", "rows":ROWS, "orphans":ORPHANS, "roots":3, "provider":false})
}

fn check_invoice((bytes, operations): (usize, usize)) -> Result<()> {
    if bytes > 64 * 1024 * 1024 || operations > 4096 {
        return Err("GC invocation exceeded its unchanged invoice limits".into());
    }
    Ok(())
}

fn orphans(store: &ControlMvpStateStore) -> BTreeSet<String> {
    (0..ORPHANS)
        .map(|n| {
            if n % 2 == 0 {
                store.paths().state_object(&format!("000-orphan-{n:06}"))
            } else {
                format!(
                    "control/directory/v1/domains/catalog/pages/{:x}",
                    Sha256::digest(n.to_le_bytes())
                )
            }
        })
        .collect()
}

pub async fn run(backend: Arc<dyn StorageBackend>, config: &provider::Manifest) -> Result<()> {
    let workspace = format!("{}-r0", config.workspace);
    let storage = ScopedStorage::new(backend, &config.tenant, &workspace)?;
    let store = ControlMvpStateStore::new_synthetic_bounded(
        storage.clone(),
        StateScope::new(&config.tenant, &workspace, "catalog"),
    )?;
    let expected = orphans(&store);
    if config.gc_stage.as_deref() == Some("fixture") {
        store
            .install_synthetic_genesis(
                "gc-loopback",
                1,
                ROWS,
                (0..ROWS).map(|n| SyntheticKvEntry {
                    key: n.to_be_bytes().to_vec(),
                    generation: 1,
                    value: Some(vec![42; 64]),
                }),
                0,
                [],
            )
            .await?;
        for path in &expected {
            storage
                .put_raw(
                    path,
                    Bytes::from_static(b"orphan"),
                    WritePrecondition::DoesNotExist,
                )
                .await?;
        }
        let worker = ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
        let (seed, bytes, operations) = worker
            .begin_gc_mark_at_with_admission(Utc::now() + Duration::days(40))
            .await?;
        check_invoice((bytes, operations))?;
        let mut cursor = None;
        for _ in 0..16 {
            let advance = worker.advance_gc_mark(&seed, cursor.as_ref()).await?;
            check_invoice((advance.admitted_bytes, advance.charged_operations))?;
            cursor = advance.continuation;
            if cursor.is_none() {
                emit!(
                    "{}",
                    json!({"kind":"gc-completed", "stage":"fixture", "seed":seed,
                    "fixture_rows":ROWS,"orphans":ORPHANS,"provider":false})
                );
                return Ok(());
            }
        }
        return Err("GC fixture scheduling exceeded its bounded root count".into());
    }
    let seed = config.gc_seed.as_deref().ok_or("GC seed is missing")?;
    let worker = ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
    let mut plan = worker.begin_marked_gc_plan(seed, [], None).await?;
    check_invoice(plan.last_admission())?;
    let mut candidates = BTreeSet::new();
    let mut pages = 0;
    let mut ready = None;
    for invocation in 0..4096 {
        let result = if let Some(page) = ready.take() {
            Some(page)
        } else {
            worker.advance_marked_gc_plan(&mut plan).await?
        };
        check_invoice(plan.last_admission())?;
        emit!(
            "{}",
            json!({"kind":"gc-unit","invocation":invocation,
            "verified_roots":plan.verified_pages(),"admitted_bytes":plan.last_admission().0,
            "charged_operations":plan.last_admission().1,"provider":false})
        );
        if config.gc_stage.as_deref() == Some("interrupt") {
            emit!(
                "{}",
                json!({"kind":"gc-interrupted","seed":seed,"verified_roots":plan.verified_pages(),"provider":false})
            );
            std::io::stdout().flush()?;
            std::future::pending::<()>().await;
        }
        if let Some(page) = result {
            emit!(
                "{}",
                json!({"kind":"gc-page","candidates":page.candidates().len(),"provider":false})
            );
            pages += 1;
            for candidate in page.candidates() {
                if candidate.size() != 6 || !candidates.insert(candidate.path().to_owned()) {
                    return Err("GC oracle found a duplicate or unexpected candidate size".into());
                }
            }
            if page.continuation().is_none() {
                if candidates != expected || plan.verified_pages() != 3 {
                    return Err("GC candidates differ from the independent fixture oracle".into());
                }
                emit!(
                    "{}",
                    json!({"kind":"gc-completed", "stage":"sweep","seed":seed,
                    "inventory_pages":pages,"candidate_count":candidates.len(),
                    "verified_roots":plan.verified_pages(),"provider":false,"active_delete":false})
                );
                return Ok(());
            }
            ready = Some(worker.next_marked_gc_plan_page(&mut plan, []).await?);
            check_invoice(plan.last_admission())?;
        }
    }
    Err("GC rehearsal exceeded its bounded invocation count".into())
}
