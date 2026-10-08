//! Restore key policies: key prefixes a format-9 restore never restores.

use std::collections::BTreeMap;
use std::ops::Bound;

use super::StoredValue;
use super::integrity::restore_key_policy_digest;
use crate::error::{CatalogError, Result};

/// Key prefixes a Control MVP restore excludes from both sides of its diff.
///
/// A restore participant configured with a policy (see
/// [`ControlMvpRestoreParticipant::with_key_policy`](crate::state_store::ControlMvpRestoreParticipant::with_key_policy))
/// never restores a key that starts with an excluded prefix and never leaves
/// one behind:
///
/// - excluded rows are dropped from the restore source before the source scan
///   charges its row and byte budget, so they are never put;
/// - every excluded key that is live in the authority the restore replaces,
///   or in the lineage the restore candidate extends (with no current head
///   pointer, that is the source lineage), is deleted at the restore
///   sequence;
/// - every other key follows the plain restore rules.
///
/// The prefixes are canonical: sorted ascending, deduplicated, and without
/// any prefix that a shorter excluded prefix already covers, so two policies
/// that exclude the same keys have equal prefixes and an equal
/// [`sha256`](Self::sha256). Restore plan version 8 binds that digest, and a
/// plan rendered under one policy is superseded rather than applied by a
/// participant configured with another.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RestoreKeyPolicy {
    excluded_prefixes: Vec<Vec<u8>>,
}

impl RestoreKeyPolicy {
    /// The most canonical prefixes one policy may exclude.
    pub const MAX_EXCLUDED_PREFIXES: usize = 16;

    /// The policy that excludes nothing: every key follows the plain restore
    /// rules. This is the policy of
    /// [`ControlMvpRestoreParticipant::new`](crate::state_store::ControlMvpRestoreParticipant::new).
    #[must_use]
    pub const fn none() -> Self {
        Self {
            excluded_prefixes: Vec::new(),
        }
    }

    /// Builds the canonical policy excluding every key that starts with one of
    /// `prefixes`.
    ///
    /// Duplicates and prefixes covered by a shorter prefix in the same set are
    /// dropped before the count is checked.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Validation`] for an empty prefix (which would
    /// exclude every key) or for more than [`Self::MAX_EXCLUDED_PREFIXES`]
    /// canonical prefixes.
    pub fn excluding<I, P>(prefixes: I) -> Result<Self>
    where
        I: IntoIterator<Item = P>,
        P: AsRef<[u8]>,
    {
        let mut sorted = Vec::new();
        for prefix in prefixes {
            let prefix = prefix.as_ref();
            if prefix.is_empty() {
                return Err(CatalogError::Validation {
                    message: "restore key policy prefixes must not be empty".to_string(),
                });
            }
            sorted.push(prefix.to_vec());
        }
        sorted.sort();
        sorted.dedup();
        // In ascending order every key between a prefix and one of its
        // extensions also extends it, so the last kept prefix is the only one
        // that can cover the next.
        let mut excluded_prefixes: Vec<Vec<u8>> = Vec::with_capacity(sorted.len());
        for prefix in sorted {
            if excluded_prefixes
                .last()
                .is_none_or(|covering| !prefix.starts_with(covering))
            {
                excluded_prefixes.push(prefix);
            }
        }
        if excluded_prefixes.len() > Self::MAX_EXCLUDED_PREFIXES {
            return Err(CatalogError::Validation {
                message: format!(
                    "restore key policy excludes {} prefixes; at most {} are supported",
                    excluded_prefixes.len(),
                    Self::MAX_EXCLUDED_PREFIXES
                ),
            });
        }
        Ok(Self { excluded_prefixes })
    }

    /// The policy excluding every key whose first byte is `tag`.
    pub(crate) fn excluding_key_tag(tag: u8) -> Self {
        Self {
            excluded_prefixes: vec![vec![tag]],
        }
    }

    /// Returns the canonical excluded prefixes in ascending byte order.
    #[must_use]
    pub fn excluded_prefixes(&self) -> &[Vec<u8>] {
        &self.excluded_prefixes
    }

    /// Returns whether the policy excludes nothing.
    #[must_use]
    pub fn excludes_nothing(&self) -> bool {
        self.excluded_prefixes.is_empty()
    }

    /// Returns whether `key` starts with an excluded prefix.
    #[must_use]
    pub fn excludes(&self, key: &[u8]) -> bool {
        self.excluded_prefixes
            .iter()
            .any(|prefix| key.starts_with(prefix))
    }

    /// Returns the policy's canonical digest as `sha256:<hex>`.
    ///
    /// The preimage is the domain tag `arco/control-v1/restore-key-policy`
    /// and framing version 1, then the prefix count and each canonical prefix
    /// in ascending order; the tag and every prefix are prefixed with their
    /// big-endian `u64` length, the framing version is a big-endian `u32`, and
    /// the count a big-endian `u64`. The policy belongs to a restore
    /// participant rather than to an authority root, so the digest binds no
    /// scope.
    #[must_use]
    pub fn sha256(&self) -> String {
        format!(
            "sha256:{}",
            restore_key_policy_digest(&self.excluded_prefixes)
        )
    }

    /// Returns the excluded keys live (not tombstoned) in `kv`, in key order.
    pub(super) fn live_excluded_keys<'a>(
        &'a self,
        kv: &'a BTreeMap<Vec<u8>, StoredValue>,
    ) -> impl Iterator<Item = &'a Vec<u8>> + 'a {
        self.excluded_prefixes.iter().flat_map(move |prefix| {
            kv.range::<[u8], _>((Bound::Included(prefix.as_slice()), Bound::Unbounded))
                .take_while(move |(key, _)| key.starts_with(prefix))
                .filter(|(_, value)| !value.tombstone)
                .map(|(key, _)| key)
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use sha2::{Digest, Sha256};

    use super::*;

    fn stored(tombstone: bool) -> StoredValue {
        StoredValue {
            bytes: Bytes::from_static(b"value"),
            generation: 1,
            tombstone,
            expires_at_ms: None,
        }
    }

    /// Recomputes the documented preimage without the kernel's encoder.
    fn independent_digest(prefixes: &[&[u8]]) -> String {
        let tag = b"arco/control-v1/restore-key-policy";
        let mut preimage = Vec::new();
        preimage.extend_from_slice(&u64::try_from(tag.len()).unwrap().to_be_bytes());
        preimage.extend_from_slice(tag);
        preimage.extend_from_slice(&1_u32.to_be_bytes());
        preimage.extend_from_slice(&u64::try_from(prefixes.len()).unwrap().to_be_bytes());
        for prefix in prefixes {
            preimage.extend_from_slice(&u64::try_from(prefix.len()).unwrap().to_be_bytes());
            preimage.extend_from_slice(prefix);
        }
        format!("sha256:{}", hex::encode(Sha256::digest(&preimage)))
    }

    #[test]
    fn policies_are_canonical_sorted_deduplicated_and_without_covered_prefixes() {
        let policy = RestoreKeyPolicy::excluding([
            b"\x05b".as_slice(),
            b"\x03",
            b"\x05",
            b"\x03receipt",
            b"\x03",
            b"\x07a",
            b"\x07b",
        ])
        .unwrap();
        assert_eq!(
            vec![
                vec![0x03_u8],
                vec![0x05],
                b"\x07a".to_vec(),
                b"\x07b".to_vec()
            ],
            policy.excluded_prefixes()
        );
        assert_eq!(
            RestoreKeyPolicy::excluding([b"\x07b".as_slice(), b"\x07a", b"\x05", b"\x03"]).unwrap(),
            policy,
            "equal key sets build equal policies whatever the input order"
        );
        assert!(policy.excludes(b"\x03"));
        assert!(policy.excludes(b"\x03anything"));
        assert!(policy.excludes(b"\x07a1"));
        assert!(!policy.excludes(b"\x07c"));
        assert!(!policy.excludes(b"\x04"));
        assert!(!policy.excludes(b""));
        assert!(!policy.excludes_nothing());
        assert!(RestoreKeyPolicy::none().excludes_nothing());
        assert_eq!(RestoreKeyPolicy::default(), RestoreKeyPolicy::none());
        assert!(!RestoreKeyPolicy::none().excludes(b"\x03"));
        assert_eq!(
            RestoreKeyPolicy::excluding_key_tag(3),
            RestoreKeyPolicy::excluding([[3_u8]]).unwrap()
        );
    }

    #[test]
    fn policies_reject_empty_prefixes_and_more_than_sixteen_canonical_prefixes() {
        assert!(matches!(
            RestoreKeyPolicy::excluding([b"\x03".as_slice(), b""]),
            Err(CatalogError::Validation { .. })
        ));
        let sixteen = (0_u8..16).map(|byte| [byte]).collect::<Vec<_>>();
        assert_eq!(
            RestoreKeyPolicy::MAX_EXCLUDED_PREFIXES,
            RestoreKeyPolicy::excluding(sixteen.iter())
                .unwrap()
                .excluded_prefixes()
                .len()
        );
        let seventeen = (0_u8..17).map(|byte| [byte]).collect::<Vec<_>>();
        assert!(matches!(
            RestoreKeyPolicy::excluding(seventeen.iter()),
            Err(CatalogError::Validation { .. })
        ));
        // The bound applies to the canonical set: covered and duplicate
        // prefixes do not count.
        let covered = (0_u8..32).map(|byte| [3, byte]).chain([[3, 3]]);
        assert_eq!(
            vec![vec![3_u8]],
            RestoreKeyPolicy::excluding(covered.map(Vec::from).chain([vec![3]]))
                .unwrap()
                .excluded_prefixes()
        );
    }

    #[test]
    fn the_policy_digest_is_pinned_and_matches_the_documented_preimage() {
        let none = RestoreKeyPolicy::none().sha256();
        let receipts = RestoreKeyPolicy::excluding([[3_u8]]).unwrap().sha256();
        let two = RestoreKeyPolicy::excluding([b"\x09".as_slice(), b"\x03"])
            .unwrap()
            .sha256();
        assert_eq!(independent_digest(&[]), none);
        assert_eq!(independent_digest(&[b"\x03"]), receipts);
        assert_eq!(independent_digest(&[b"\x03", b"\x09"]), two);
        assert_eq!(
            "sha256:f91fb77fb64beaf1a4f9185901153a078cd922424bef1b798854e31e18a9432c", none,
            "the empty policy digest is a durable plan-8 value"
        );
        assert_eq!(
            "sha256:84a448ca23f39ec4da22620f03a4678d64594d9be4b5a244227f403a3ef549ff", receipts,
            "the receipt policy digest is a durable plan-8 value"
        );
        assert_ne!(none, receipts);
        assert_ne!(receipts, two);
    }

    #[test]
    fn live_excluded_keys_are_the_untombstoned_keys_under_each_prefix() {
        let kv = BTreeMap::from([
            (b"\x02\xff".to_vec(), stored(false)),
            (b"\x03".to_vec(), stored(false)),
            (b"\x03a".to_vec(), stored(false)),
            (b"\x03b".to_vec(), stored(true)),
            (b"\x04".to_vec(), stored(false)),
            (b"\x09z".to_vec(), stored(false)),
            (b"\x0a".to_vec(), stored(false)),
        ]);
        let policy = RestoreKeyPolicy::excluding([b"\x03".as_slice(), b"\x09"]).unwrap();
        assert_eq!(
            vec![&b"\x03".to_vec(), &b"\x03a".to_vec(), &b"\x09z".to_vec()],
            policy.live_excluded_keys(&kv).collect::<Vec<_>>()
        );
        assert_eq!(0, RestoreKeyPolicy::none().live_excluded_keys(&kv).count());
    }
}
