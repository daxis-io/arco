//! Shared repair backlog tracking helpers for automated repair loops.

use chrono::{DateTime, Utc};

/// Snapshot of the current repair backlog state.
#[derive(Debug, Clone, PartialEq)]
pub struct RepairBacklogSnapshot {
    /// Current number of repairable findings in the backlog.
    pub count: u64,
    /// Age in seconds since this backlog fingerprint was first observed.
    pub age_seconds: f64,
    /// Whether the same backlog fingerprint was observed again on update.
    pub repeated_finding: bool,
}

/// Tracks repair backlog state across repeated reconciliation checks.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct RepairBacklogEntry {
    count: u64,
    fingerprint: Option<String>,
    first_detected_at: Option<DateTime<Utc>>,
}

impl RepairBacklogEntry {
    /// Updates the tracked backlog entry and returns the current snapshot.
    #[must_use]
    pub fn update(
        &mut self,
        count: u64,
        fingerprint: Option<String>,
        now: DateTime<Utc>,
    ) -> RepairBacklogSnapshot {
        let Some(fingerprint) = fingerprint else {
            *self = Self::default();
            return RepairBacklogSnapshot {
                count: 0,
                age_seconds: 0.0,
                repeated_finding: false,
            };
        };

        let repeated_finding = matches!(
            (&self.fingerprint, self.first_detected_at),
            (Some(existing), Some(_)) if existing == &fingerprint
        );

        if !repeated_finding {
            self.first_detected_at = Some(now);
        }

        self.count = count;
        self.fingerprint = Some(fingerprint);

        let first_detected_at = self.first_detected_at.unwrap_or(now);
        RepairBacklogSnapshot {
            count,
            age_seconds: elapsed_seconds(now, first_detected_at),
            repeated_finding,
        }
    }

    /// Returns the current backlog snapshot without mutating the tracked fingerprint.
    #[must_use]
    pub fn refresh(&self, now: DateTime<Utc>) -> Option<RepairBacklogSnapshot> {
        let first_detected_at = self.first_detected_at?;
        Some(RepairBacklogSnapshot {
            count: self.count,
            age_seconds: elapsed_seconds(now, first_detected_at),
            repeated_finding: false,
        })
    }
}

fn elapsed_seconds(now: DateTime<Utc>, first_detected_at: DateTime<Utc>) -> f64 {
    (now - first_detected_at).num_seconds().max(0) as f64
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};

    use super::*;

    #[test]
    fn repair_backlog_entry_tracks_first_detection_repeat_and_refresh() {
        let now = Utc.with_ymd_and_hms(2026, 3, 31, 12, 0, 0).unwrap();
        let mut entry = RepairBacklogEntry::default();

        let first = entry.update(2, Some("issue-a|issue-b".to_string()), now);
        assert_eq!(first.count, 2);
        assert_eq!(first.age_seconds, 0.0);
        assert!(!first.repeated_finding);

        let second = entry.update(
            2,
            Some("issue-a|issue-b".to_string()),
            now + Duration::minutes(5),
        );
        assert_eq!(second.count, 2);
        assert!(second.age_seconds >= 300.0);
        assert!(second.repeated_finding);

        let refreshed = entry
            .refresh(now + Duration::minutes(10))
            .expect("active backlog snapshot");
        assert_eq!(refreshed.count, 2);
        assert!(refreshed.age_seconds >= 600.0);
        assert!(!refreshed.repeated_finding);
    }

    #[test]
    fn repair_backlog_entry_clears_when_fingerprint_is_absent() {
        let now = Utc.with_ymd_and_hms(2026, 3, 31, 12, 0, 0).unwrap();
        let mut entry = RepairBacklogEntry::default();

        let _ = entry.update(1, Some("issue-a".to_string()), now);
        let cleared = entry.update(0, None, now + Duration::minutes(1));
        assert_eq!(cleared.count, 0);
        assert_eq!(cleared.age_seconds, 0.0);
        assert!(!cleared.repeated_finding);
        assert!(entry.refresh(now + Duration::minutes(2)).is_none());
    }
}
