//! Immutable execution intent shared by API acceptance, recovery, and dispatch.

use std::collections::BTreeMap;

use arco_core::storage::StorageBackend;
use arco_core::{ApiPaths, ScopedStorage, WritePrecondition, WriteResult};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use ulid::Ulid;

use super::events::{OrchestrationEvent, OrchestrationEventData};
pub use super::retry_policy::{RETRY_POLICY_LABEL, RIVERS_RETRY_PROFILE};
use crate::error::{Error, Result};

/// Server-owned run label binding the accepted plan's canonical bytes.
pub const ACCEPTED_PLAN_SHA256_LABEL: &str = "arco.accepted_plan_sha256";

const MAX_ACCEPTED_METADATA_BYTES: u64 = 16 * 1024 * 1024;

/// Immutable deployed manifest required by an accepted plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcceptedManifestRef {
    /// Deployed manifest identifier.
    pub manifest_id: String,
    /// SHA-256 of the exact stored manifest bytes.
    pub sha256: String,
}

/// Versioned plan containing the exact accepted events and worker payloads.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcceptedRunPlan {
    /// Descriptor format, one for legacy plans and two for a bound retry policy.
    pub version: u32,
    /// Immutable deployed manifest, absent for legacy-derived reruns.
    pub manifest: Option<AcceptedManifestRef>,
    /// Original run and plan events, in publication order.
    pub events: Vec<OrchestrationEvent>,
    /// Exact execution, code, resources and I/O declarations by task key.
    pub task_payloads: BTreeMap<String, Value>,
}

fn plan_path(plan_id: &str) -> Result<String> {
    if plan_id.is_empty()
        || !plan_id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
    {
        return Err(Error::configuration("invalid accepted plan identifier"));
    }
    Ok(format!("accepted_plans/{plan_id}.json"))
}

impl AcceptedRunPlan {
    /// Binds the pinned Rivers compatibility policy and final event order before acceptance.
    ///
    /// # Errors
    /// Rejects prebound labels, invalid event identities, and incompatible plans.
    pub fn with_rivers_retry_policy(mut self) -> Result<Self> {
        let [run_event, plan_event] = self.events.as_mut_slice() else {
            return Err(Error::configuration(
                "accepted plan must contain two events",
            ));
        };
        let OrchestrationEventData::RunTriggered { labels, .. } = &mut run_event.data else {
            return Err(Error::configuration("accepted plan lacks run event"));
        };
        let OrchestrationEventData::PlanCreated { tasks, .. } = &plan_event.data else {
            return Err(Error::configuration("accepted plan lacks task event"));
        };
        if self.version != 1
            || labels.contains_key(RETRY_POLICY_LABEL)
            || labels.contains_key(ACCEPTED_PLAN_SHA256_LABEL)
            || tasks.iter().any(|task| task.max_attempts == 0)
        {
            return Err(Error::configuration(
                "incompatible Rivers retry policy binding",
            ));
        }
        let run_event_id = run_event
            .event_id
            .parse::<Ulid>()
            .map_err(|_| Error::configuration("Rivers plan requires server event identities"))?;
        plan_event
            .event_id
            .parse::<Ulid>()
            .map_err(|_| Error::configuration("Rivers plan requires server event identities"))?;
        if plan_event.event_id <= run_event.event_id {
            let next = u128::from(run_event_id)
                .checked_add(1)
                .ok_or_else(|| Error::configuration("accepted event identity overflow"))?;
            plan_event.event_id = Ulid::from(next).to_string();
        }
        labels.insert(RETRY_POLICY_LABEL.into(), RIVERS_RETRY_PROFILE.into());
        self.version = 2;
        Ok(self)
    }

    /// Persists the plan before reserving a run key or publishing run events.
    ///
    /// # Errors
    /// Refuses noncanonical payloads, inconsistent identity, or a different existing plan.
    pub async fn persist(
        &self,
        storage: &ScopedStorage,
        run_id: &str,
        plan_id: &str,
    ) -> Result<String> {
        self.validate(storage, run_id, plan_id)?;
        let bytes = arco_core::canonical_json::to_canonical_bytes(self)
            .map_err(|e| Error::serialization(e.to_string()))?;
        if bytes.len() as u64 > MAX_ACCEPTED_METADATA_BYTES {
            return Err(Error::configuration("accepted plan exceeds 16 MiB"));
        }
        let sha256 = hex::encode(Sha256::digest(&bytes));
        let path = plan_path(plan_id)?;
        match storage
            .put_raw(
                &path,
                Bytes::from(bytes.clone()),
                WritePrecondition::DoesNotExist,
            )
            .await?
        {
            WriteResult::Success { .. } => {}
            WriteResult::PreconditionFailed { .. } => {
                if storage.get_raw(&path).await?.as_ref() != bytes.as_slice() {
                    return Err(Error::configuration("accepted plan identity conflict"));
                }
            }
        }
        Ok(sha256)
    }

    fn validate(&self, storage: &ScopedStorage, run_id: &str, plan_id: &str) -> Result<()> {
        let [run_event, plan_event] = self.events.as_slice() else {
            return Err(Error::configuration(
                "accepted plan must contain two events",
            ));
        };
        if !matches!(self.version, 1 | 2)
            || self.events.len() != 2
            || self.events.iter().any(|e| {
                e.tenant_id != storage.tenant_id() || e.workspace_id != storage.workspace_id()
            })
        {
            return Err(Error::configuration(
                "invalid accepted plan version or scope",
            ));
        }
        let OrchestrationEventData::RunTriggered {
            run_id: run,
            plan_id: plan,
            labels,
            ..
        } = &run_event.data
        else {
            return Err(Error::configuration("accepted plan lacks run event"));
        };
        let OrchestrationEventData::PlanCreated {
            run_id: planned_run,
            plan_id: planned_id,
            tasks,
        } = &plan_event.data
        else {
            return Err(Error::configuration("accepted plan lacks task event"));
        };
        if self.version == 2
            && (labels.get(RETRY_POLICY_LABEL).map(String::as_str) != Some(RIVERS_RETRY_PROFILE)
                || labels.contains_key(ACCEPTED_PLAN_SHA256_LABEL)
                || tasks.iter().any(|task| task.max_attempts == 0)
                || run_event.event_id.parse::<Ulid>().is_err()
                || plan_event.event_id.parse::<Ulid>().is_err()
                || run_event.event_id >= plan_event.event_id)
        {
            return Err(Error::configuration("unsupported accepted retry policy"));
        }
        if run != run_id
            || plan != plan_id
            || planned_run != run_id
            || planned_id != plan_id
            || tasks
                .iter()
                .any(|task| !self.task_payloads.contains_key(&task.key))
            || self.task_payloads.len() != tasks.len()
        {
            return Err(Error::configuration(
                "accepted plan identity or payload mismatch",
            ));
        }
        Ok(())
    }

    /// Returns the original events with their server-owned integrity binding.
    #[must_use]
    pub fn publication_events(&self, sha256: &str) -> Vec<OrchestrationEvent> {
        let mut events = self.events.clone();
        if let Some(OrchestrationEvent {
            data: OrchestrationEventData::RunTriggered { labels, .. },
            ..
        }) = events.first_mut()
        {
            labels.insert(ACCEPTED_PLAN_SHA256_LABEL.into(), sha256.into());
        }
        events
    }
}

/// Reads only the accepted identity, never the latest manifest.
///
/// # Errors
/// Missing, corrupt, cross-scope, or modified frozen references block recovery and dispatch.
pub async fn load_accepted_run_plan(
    storage: &ScopedStorage,
    run_id: &str,
    plan_id: &str,
    sha256: &str,
) -> Result<AcceptedRunPlan> {
    let bytes = read_metadata(storage, &plan_path(plan_id)?).await?;
    if hex::encode(Sha256::digest(&bytes)) != sha256 {
        return Err(Error::configuration("accepted plan checksum mismatch"));
    }
    let plan: AcceptedRunPlan =
        serde_json::from_slice(&bytes).map_err(|e| Error::serialization(e.to_string()))?;
    plan.validate(storage, run_id, plan_id)?;
    if let Some(manifest) = &plan.manifest {
        let bytes = read_metadata(storage, &ApiPaths::manifest_path(&manifest.manifest_id)).await?;
        if hex::encode(Sha256::digest(&bytes)) != manifest.sha256 {
            return Err(Error::configuration("accepted manifest checksum mismatch"));
        }
    }
    Ok(plan)
}

async fn read_metadata(storage: &ScopedStorage, path: &str) -> Result<Bytes> {
    let meta = storage
        .head_raw(path)
        .await?
        .ok_or_else(|| Error::configuration("accepted metadata is missing"))?;
    if meta.size == 0 || meta.size > MAX_ACCEPTED_METADATA_BYTES {
        return Err(Error::configuration(
            "accepted metadata exceeds its size limit",
        ));
    }
    let bytes = storage.get_range(path, 0..meta.size).await?;
    if bytes.len() as u64 != meta.size {
        return Err(Error::configuration("accepted metadata size mismatch"));
    }
    Ok(bytes)
}

/// Adds the frozen payload to a dispatch or repair envelope.
///
/// # Errors
/// Refuses a mismatched run or an unavailable accepted plan. Legacy runs retain their old payload.
pub async fn populate_accepted_payload(
    storage: &ScopedStorage,
    run: &super::compactor::fold::RunRow,
    envelope: &mut arco_worker_contract::WorkerDispatchEnvelope,
) -> Result<()> {
    if run.run_id != envelope.run_id
        || envelope.tenant_id != storage.tenant_id()
        || envelope.workspace_id != storage.workspace_id()
    {
        return Err(Error::dispatch("dispatch run identity mismatch"));
    }
    if let Some(sha256) = run.labels.get(ACCEPTED_PLAN_SHA256_LABEL) {
        let plan = load_accepted_run_plan(storage, &run.run_id, &run.plan_id, sha256).await?;
        envelope.payload = plan
            .task_payloads
            .get(&envelope.task_key)
            .cloned()
            .ok_or_else(|| Error::dispatch("accepted task payload is missing"))?;
    }
    Ok(())
}
