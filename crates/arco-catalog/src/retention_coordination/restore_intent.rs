//! Durable reconciliation boundary; never a capability to publish HEAD.
use super::{Result, RetainedPointerIntentPrecondition, validation};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArmedRestorePublicationIntent {
    pub(crate) intent_type: String,
    pub(crate) version: u32,
    pub(crate) domain: String,
    pub(crate) plan_sha256: String,
    pub(crate) owner_generation: u64,
    pub(crate) candidate_id: String,
    pub(crate) prepared_path: String,
    pub(crate) prepared_sha256: String,
    pub(crate) precondition: RetainedPointerIntentPrecondition,
}

impl ArmedRestorePublicationIntent {
    pub(super) fn validate(&self) -> Result<()> {
        if self.intent_type != "arco.restore.publication-intent" || self.version != 1 {
            return Err(validation(
                "unsupported restore publication intent envelope",
            ));
        }
        crate::state_store::StateScope::new("tenant", "workspace", &self.domain).validate()?;
        for digest in [&self.plan_sha256, &self.candidate_id, &self.prepared_sha256] {
            if digest.len() != 64
                || !digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(validation(
                    "restore publication intent requires raw SHA-256 hex",
                ));
            }
        }
        if self.owner_generation == 0
            || self.prepared_path
                != format!(
                    "control/v1/domains/{}/restore/v7/{}/prepared.json",
                    self.domain, self.candidate_id
                )
        {
            return Err(validation(
                "restore publication intent owner or prepared path differs",
            ));
        }
        if let RetainedPointerIntentPrecondition::MatchesVersion { version } = &self.precondition {
            if version.is_empty() || version.len() > 4096 || version.chars().any(char::is_control) {
                return Err(validation(
                    "restore publication intent requires a bounded HEAD version",
                ));
            }
        }
        Ok(())
    }
}
