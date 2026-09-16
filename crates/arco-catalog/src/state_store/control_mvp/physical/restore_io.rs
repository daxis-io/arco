//! Ownership of one exact authority-8 restore physical response.
use std::sync::{Arc, Mutex};

use arco_core::storage::{BytesBackingOwnership, ClassifiedBytes, ObjectMeta};
use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccountedBytesClass {
    BackendOriginShared { held_len: usize },
    NewRequestOwned { actual_capacity: usize },
    CopiedRequestOwned { actual_capacity: usize },
}

impl AccountedBytesClass {
    const fn amount(self) -> usize {
        match self {
            Self::BackendOriginShared { held_len } => held_len,
            Self::NewRequestOwned { actual_capacity }
            | Self::CopiedRequestOwned { actual_capacity } => actual_capacity,
        }
    }

    const fn is_request_owned(self) -> bool {
        matches!(
            self,
            Self::NewRequestOwned { .. } | Self::CopiedRequestOwned { .. }
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RestoreOwnershipReport {
    request_owned_live_bytes: usize,
    // Conservative ownership peak including admitted decoder reservations.
    request_owned_peak_bytes: usize,
    working_live_bytes: usize,
    working_peak_bytes: usize,
    working_admission_failures: u64,
    working_releases: u64,
    working_underestimates: u64,
    backend_origin_shared_live_bytes: usize,
    backend_origin_shared_peak_bytes: usize,
    largest_rejected_request_owned_capacity: usize,
    largest_rejected_backend_shared_held_len: usize,
    largest_unknown_visible_len: usize,
    largest_invalid_visible_len: usize,
    unknown_responses: u64,
    invalid_responses: u64,
    over_budget_responses: u64,
    releases: u64,
    poisoned_ledger: bool,
    counter_overflow: bool,
}

impl RestoreOwnershipReport {
    fn owned_live_upper_bound(self) -> usize {
        // An unrepresentable upper bound cannot wrap into an admitted amount.
        self.request_owned_live_bytes
            .checked_add(self.working_live_bytes)
            .unwrap_or(usize::MAX)
    }
    const fn passing(self) -> bool {
        self.request_owned_live_bytes == 0
            && self.working_live_bytes == 0
            && self.working_admission_failures == 0
            && self.working_underestimates == 0
            && self.backend_origin_shared_live_bytes == 0
            && self.unknown_responses == 0
            && self.invalid_responses == 0
            && self.over_budget_responses == 0
            && !self.poisoned_ledger
            && !self.counter_overflow
    }
}

#[derive(Debug)]
struct RestoreOwnershipLedger {
    request_owned_limit: usize,
    backend_origin_shared_limit: usize,
    report: RestoreOwnershipReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnershipAdmissionError {
    Unknown,
    Invalid,
    RequestOwnedBudgetExceeded,
    BackendOriginSharedBudgetExceeded,
    Overflow,
    Poisoned,
}

impl RestoreOwnershipLedger {
    const fn new(request_owned_limit: usize, backend_origin_shared_limit: usize) -> Self {
        Self {
            request_owned_limit,
            backend_origin_shared_limit,
            report: RestoreOwnershipReport {
                request_owned_live_bytes: 0,
                request_owned_peak_bytes: 0,
                working_live_bytes: 0,
                working_peak_bytes: 0,
                working_admission_failures: 0,
                working_releases: 0,
                working_underestimates: 0,
                backend_origin_shared_live_bytes: 0,
                backend_origin_shared_peak_bytes: 0,
                largest_rejected_request_owned_capacity: 0,
                largest_rejected_backend_shared_held_len: 0,
                largest_unknown_visible_len: 0,
                largest_invalid_visible_len: 0,
                unknown_responses: 0,
                invalid_responses: 0,
                over_budget_responses: 0,
                releases: 0,
                poisoned_ledger: false,
                counter_overflow: false,
            },
        }
    }

    fn report(&self) -> RestoreOwnershipReport {
        self.report
    }

    fn increment(counter: &mut u64, overflow: &mut bool) -> Result<(), OwnershipAdmissionError> {
        let Some(next) = counter.checked_add(1) else {
            *overflow = true;
            return Err(OwnershipAdmissionError::Overflow);
        };
        *counter = next;
        Ok(())
    }

    fn observe_peak(report: &mut RestoreOwnershipReport, class: AccountedBytesClass, total: usize) {
        if class.is_request_owned() {
            report.request_owned_peak_bytes = report.request_owned_peak_bytes.max(total);
        } else {
            report.backend_origin_shared_peak_bytes =
                report.backend_origin_shared_peak_bytes.max(total);
        }
    }

    fn record_rejected(&mut self, class: AccountedBytesClass) {
        let largest = if class.is_request_owned() {
            &mut self.report.largest_rejected_request_owned_capacity
        } else {
            &mut self.report.largest_rejected_backend_shared_held_len
        };
        *largest = (*largest).max(class.amount());
    }

    fn reserve(&mut self, class: AccountedBytesClass) -> Result<(), OwnershipAdmissionError> {
        let amount = class.amount();
        let request_owned_limit = self.request_owned_limit;
        let backend_origin_shared_limit = self.backend_origin_shared_limit;
        let report = &mut self.report;
        let (current, limit) = if class.is_request_owned() {
            (report.request_owned_live_bytes, request_owned_limit)
        } else {
            (
                report.backend_origin_shared_live_bytes,
                backend_origin_shared_limit,
            )
        };
        let Some(next) = current.checked_add(amount) else {
            // Preserve the known arriving amount when the total is unrepresentable.
            report.counter_overflow = true;
            let _ = Self::increment(&mut report.invalid_responses, &mut report.counter_overflow);
            self.record_rejected(class);
            return Err(OwnershipAdmissionError::Overflow);
        };
        // The response has already arrived. Its live predecessor plus its exact
        // known class amount is the observed peak, even when it cannot remain
        // admitted under this invocation's limit.
        let Some(charged) = (if class.is_request_owned() {
            next.checked_add(report.working_live_bytes)
        } else {
            Some(next)
        }) else {
            report.counter_overflow = true;
            self.record_rejected(class);
            return Err(OwnershipAdmissionError::Overflow);
        };
        Self::observe_peak(report, class, charged);
        if charged > limit {
            Self::increment(
                &mut report.over_budget_responses,
                &mut report.counter_overflow,
            )?;
            self.record_rejected(class);
            return Err(if class.is_request_owned() {
                OwnershipAdmissionError::RequestOwnedBudgetExceeded
            } else {
                OwnershipAdmissionError::BackendOriginSharedBudgetExceeded
            });
        }
        if class.is_request_owned() {
            report.request_owned_live_bytes = next;
        } else {
            report.backend_origin_shared_live_bytes = next;
        }
        Ok(())
    }

    fn release(&mut self, class: AccountedBytesClass) {
        let report = &mut self.report;
        // This is an internal invariant: every handle was admitted once and is
        // non-Clone, so an underflow marks the final report nonpassing instead
        // of silently saturating a leak away.
        let underflow = {
            let live = if class.is_request_owned() {
                &mut report.request_owned_live_bytes
            } else {
                &mut report.backend_origin_shared_live_bytes
            };
            if *live < class.amount() {
                *live = 0;
                true
            } else {
                *live -= class.amount();
                false
            }
        };
        if underflow {
            let _ = Self::increment(&mut report.invalid_responses, &mut report.counter_overflow);
        }
        let _ = Self::increment(&mut report.releases, &mut report.counter_overflow);
    }
}

/// Drops one successful admission exactly once.
struct OwnershipHandle {
    ledger: Arc<Mutex<RestoreOwnershipLedger>>,
    class: AccountedBytesClass,
}

impl Drop for OwnershipHandle {
    fn drop(&mut self) {
        // A poisoned ledger is already nonrecoverable accounting state. The
        // production integration should convert lock poisoning into a failing
        // restore report before it can publish output; Drop must not panic.
        match self.ledger.lock() {
            Ok(mut ledger) => ledger.release(self.class),
            Err(poisoned) => {
                let mut ledger = poisoned.into_inner();
                ledger.report.poisoned_ledger = true;
                ledger.release(self.class);
            }
        }
    }
}

// A reservation precedes known decoder allocation and remains owned until all
// values it covers are dropped. It never represents a returned Bytes capacity.
struct WorkingMemory {
    ledger: Arc<Mutex<RestoreOwnershipLedger>>,
    bytes: usize,
}
impl Drop for WorkingMemory {
    fn drop(&mut self) {
        let mut ledger = match self.ledger.lock() {
            Ok(ledger) => ledger,
            Err(poisoned) => {
                let mut ledger = poisoned.into_inner();
                ledger.report.poisoned_ledger = true;
                ledger
            }
        };
        let report = &mut ledger.report;
        if let Some(next) = report.working_live_bytes.checked_sub(self.bytes) {
            report.working_live_bytes = next;
        } else {
            report.counter_overflow = true;
        }
        let _ = RestoreOwnershipLedger::increment(
            &mut report.working_releases,
            &mut report.counter_overflow,
        );
        drop(ledger);
    }
}
fn reserve_working_memory(
    ledger: Arc<Mutex<RestoreOwnershipLedger>>,
    bytes: usize,
) -> Result<WorkingMemory, OwnershipAdmissionError> {
    let mut guard = match ledger.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            poisoned.into_inner().report.poisoned_ledger = true;
            return Err(OwnershipAdmissionError::Poisoned);
        }
    };
    let limit = guard.request_owned_limit;
    let report = &mut guard.report;
    let next = report
        .working_live_bytes
        .checked_add(bytes)
        .and_then(|working| {
            working
                .checked_add(report.request_owned_live_bytes)
                .map(|total| (working, total))
        });
    let Some((working, total)) = next else {
        report.counter_overflow = true;
        return Err(OwnershipAdmissionError::Overflow);
    };
    if total > limit {
        RestoreOwnershipLedger::increment(
            &mut report.working_admission_failures,
            &mut report.counter_overflow,
        )?;
        return Err(OwnershipAdmissionError::RequestOwnedBudgetExceeded);
    }
    report.working_live_bytes = working;
    report.working_peak_bytes = report.working_peak_bytes.max(working);
    report.request_owned_peak_bytes = report.request_owned_peak_bytes.max(total);
    drop(guard);
    Ok(WorkingMemory { ledger, bytes })
}

fn checked_ownership_ledger(
    ledger: &Arc<Mutex<RestoreOwnershipLedger>>,
) -> CatalogResult<std::sync::MutexGuard<'_, RestoreOwnershipLedger>> {
    let guard = match ledger.lock() {
        Ok(ledger) => ledger,
        Err(poisoned) => {
            poisoned.into_inner().report.poisoned_ledger = true;
            return Err(ownership_failure(OwnershipAdmissionError::Poisoned));
        }
    };
    if guard.report.counter_overflow || guard.report.poisoned_ledger {
        return Err(physical_backpressure("restore ownership ledger is invalid"));
    }
    Ok(guard)
}

/// Non-Clone exact response wrapper retained across decode/carry boundaries.
pub(in super::super) struct Accounted<T> {
    value: T,
    handle: OwnershipHandle,
}
pub(in super::super) type AccountedBytes = Accounted<Bytes>;
type AccountedMeta = Accounted<ObjectMeta>;
impl AccountedBytes {
    pub(in super::super) fn as_slice(&self) -> &[u8] {
        self.value.as_ref()
    }
}
impl<T> Accounted<T> {
    pub(in super::super) fn is_owned_by(&self, io: &RestorePhysicalIo<'_>) -> bool {
        Arc::ptr_eq(&self.handle.ledger, &io.ledger)
    }

    pub(in super::super) fn value(&self) -> &T {
        &self.value
    }

    fn class(&self) -> AccountedBytesClass {
        self.handle.class
    }
}

fn classify_response(
    response: &ClassifiedBytes,
) -> Result<AccountedBytesClass, OwnershipAdmissionError> {
    let len = response.bytes.len();
    match response.ownership {
        BytesBackingOwnership::Unknown => Err(OwnershipAdmissionError::Unknown),
        BytesBackingOwnership::BackendOriginShared { held_len } if held_len == len => {
            Ok(AccountedBytesClass::BackendOriginShared { held_len })
        }
        BytesBackingOwnership::NewRequestOwned { actual_capacity } if actual_capacity >= len => {
            Ok(AccountedBytesClass::NewRequestOwned { actual_capacity })
        }
        BytesBackingOwnership::CopiedRequestOwned { actual_capacity } if actual_capacity >= len => {
            Ok(AccountedBytesClass::CopiedRequestOwned { actual_capacity })
        }
        _ => Err(OwnershipAdmissionError::Invalid),
    }
}
fn admit_classified_bytes(
    ledger: Arc<Mutex<RestoreOwnershipLedger>>,
    response: ClassifiedBytes,
) -> Result<AccountedBytes, OwnershipAdmissionError> {
    let class = classify_response(&response);
    admit_classified_response(ledger, response, class)
}
fn admit_classified_response(
    ledger: Arc<Mutex<RestoreOwnershipLedger>>,
    response: ClassifiedBytes,
    class: Result<AccountedBytesClass, OwnershipAdmissionError>,
) -> Result<AccountedBytes, OwnershipAdmissionError> {
    let visible_len = response.bytes.len();
    admit_response(ledger, response.bytes, visible_len, class)
}
fn admit_response<T>(
    ledger: Arc<Mutex<RestoreOwnershipLedger>>,
    value: T,
    len: usize,
    class: Result<AccountedBytesClass, OwnershipAdmissionError>,
) -> Result<Accounted<T>, OwnershipAdmissionError> {
    // Recover a poisoned lock only for diagnostics, never for admission.
    let (mut guard, poisoned) = match ledger.lock() {
        Ok(guard) => (guard, false),
        Err(error) => (error.into_inner(), true),
    };
    guard.report.poisoned_ledger |= poisoned;
    let report = &mut guard.report;
    let class = match class {
        Ok(class) => class,
        Err(OwnershipAdmissionError::Unknown) => {
            report.largest_unknown_visible_len = report.largest_unknown_visible_len.max(len);
            RestoreOwnershipLedger::increment(
                &mut report.unknown_responses,
                &mut report.counter_overflow,
            )?;
            return Err(OwnershipAdmissionError::Unknown);
        }
        Err(error) => {
            report.largest_invalid_visible_len = report.largest_invalid_visible_len.max(len);
            RestoreOwnershipLedger::increment(
                &mut report.invalid_responses,
                &mut report.counter_overflow,
            )?;
            return Err(error);
        }
    };
    if poisoned {
        let current = if class.is_request_owned() {
            report.owned_live_upper_bound()
        } else {
            report.backend_origin_shared_live_bytes
        };
        if let Some(total) = current.checked_add(class.amount()) {
            RestoreOwnershipLedger::observe_peak(report, class, total);
        } else {
            report.counter_overflow = true;
        }
        guard.record_rejected(class);
        return Err(OwnershipAdmissionError::Poisoned);
    }
    guard.reserve(class)?;
    drop(guard);
    Ok(Accounted {
        value,
        handle: OwnershipHandle { ledger, class },
    })
}

use crate::workspace_io_budget::catalog_error_string_capacity;
use crate::{
    error::{CatalogError, Result as CatalogResult},
    state_store::{
        StateScope,
        control_mvp::{
            ControlMvpBlock, ControlMvpSegmentLevel, ControlMvpSegmentRef, ControlMvpStateStore,
            MAX_BLOCK_BYTES, MAX_CONTROL_JSON_BYTES, MAX_SEGMENT_BYTES, MAX_SEGMENT_INDEX_BYTES,
            read_cache, valid_raw_digest,
        },
    },
    workspace_io_budget::{WorkspaceIoBudget, object_meta_owned_bytes},
};
use std::ops::Range;

const FINAL_RECEIPT_BYTES: usize = 4 * 1024 * 1024;
const FINAL_RECEIPT_PROBE_BYTES: usize = FINAL_RECEIPT_BYTES + 1;

#[derive(Clone, Copy)]
pub(in super::super) enum RestoreControlRecord {
    Selector,
    Progress(u64),
    Receipt(u64),
}

impl RestoreControlRecord {
    pub(in super::super) fn path(self, prefix: &str) -> String {
        match self {
            Self::Selector => format!("{prefix}/selector.json"),
            Self::Progress(ordinal) => format!("{prefix}/progress/{ordinal:020}.json"),
            Self::Receipt(ordinal) => format!("{prefix}/receipts/{ordinal:020}.json"),
        }
    }
}

/// Read-only fixed identities needed by native driver admission.
#[derive(Clone, Copy)]
pub(in super::super) enum RestoreGateRecord<'a> {
    Head,
    Manifest(&'a str),
    Prepared(&'a str),
}

pub(in super::super) async fn read_restore_gate_record(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    record: RestoreGateRecord<'_>,
) -> CatalogResult<Option<(AccountedBytes, AccountedMeta)>> {
    let result = async {
        let store = io.store();
        let final_stream = matches!(route, RestorePhysicalRoute::FinalMicrochunk(_));
        let cap = match record {
            RestoreGateRecord::Head => super::super::MAX_HEAD_JSON_BYTES,
            RestoreGateRecord::Manifest(_) => MAX_CONTROL_JSON_BYTES,
            RestoreGateRecord::Prepared(_) => 1024 * 1024,
        };
        let declared =
            decode_with_reservation(io, route, Some(directory_scope_reservation(store)?), || {
                if final_stream {
                    return Err(physical_backpressure(
                        "restore gate records require the control ledger",
                    ));
                }
                let path = match record {
                    RestoreGateRecord::Head => store.paths.current_pointer(),
                    RestoreGateRecord::Manifest(id) => {
                        if !super::super::integrity::valid_immutable_id(id) {
                            return Err(physical_backpressure("invalid restore gate manifest ID"));
                        }
                        store.paths.manifest_object(id)
                    }
                    RestoreGateRecord::Prepared(candidate) => {
                        if !valid_raw_digest(candidate) {
                            return Err(physical_backpressure("invalid restore gate candidate ID"));
                        }
                        format!(
                            "{}/restore/v7/{candidate}/prepared.json",
                            store.paths.base_prefix()
                        )
                    }
                };
                Ok(DeclaredPhysicalRange {
                    scope: store.scope.clone(),
                    final_stream,
                    payload: false,
                    path,
                    range: 0..(cap + 1) as u64,
                    reservation_bytes: cap + 1,
                    min_response_bytes: 1,
                    max_response_bytes: cap,
                })
            })?;
        read_stable_restore_metadata(io, route, declared, cap).await
    }
    .await;
    if result.is_err() {
        io.stop(route);
    }
    result
}

pub(in super::super) async fn read_restore_control_record(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    candidate: &str,
    record: RestoreControlRecord,
) -> CatalogResult<Option<(AccountedBytes, AccountedMeta)>> {
    let result = async {
        let store = io.store();
        let final_stream = matches!(route, RestorePhysicalRoute::FinalMicrochunk(_));
        let reservation = directory_scope_reservation(store)?;
        let declared = decode_with_reservation(io, route, Some(reservation), || {
            if final_stream {
                return Err(physical_backpressure(
                    "restore control records require the control ledger",
                ));
            }
            if !valid_raw_digest(candidate) {
                return Err(physical_backpressure("invalid restore control candidate"));
            }
            let prefix = format!("{}/restore/v7/{candidate}", store.paths.base_prefix());
            Ok(DeclaredPhysicalRange {
                scope: store.scope.clone(),
                final_stream,
                payload: false,
                path: record.path(&prefix),
                range: 0..FINAL_RECEIPT_PROBE_BYTES as u64,
                reservation_bytes: FINAL_RECEIPT_PROBE_BYTES,
                min_response_bytes: 1,
                max_response_bytes: FINAL_RECEIPT_BYTES,
            })
        })?;
        read_stable_restore_metadata(io, route, declared, FINAL_RECEIPT_BYTES).await
    }
    .await;
    if result.is_err() {
        io.stop(route);
    }
    result
}

/// One immutable ascending receipt. Only the terminal prefix reader may interpret it.
pub(in super::super) async fn read_final_restore_receipt(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    candidate: &str,
    ordinal: u64,
) -> CatalogResult<Option<(AccountedBytes, AccountedMeta)>> {
    let result = async {
        let store = io.store();
        let final_stream = matches!(route, RestorePhysicalRoute::FinalMicrochunk(_));
        let declared =
            decode_with_reservation(io, route, Some(directory_scope_reservation(store)?), || {
                DeclaredPhysicalRange::new(
                    store,
                    PhysicalObject::Receipt { candidate, ordinal },
                    final_stream,
                )
            })?;
        read_stable_restore_metadata(io, route, declared, FINAL_RECEIPT_BYTES).await
    }
    .await;
    if result.is_err() {
        io.stop(route);
    }
    result
}

async fn read_stable_restore_metadata(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    mut declared: WorkingValue<DeclaredPhysicalRange>,
    cap: usize,
) -> CatalogResult<Option<(AccountedBytes, AccountedMeta)>> {
    let Some(before) = head_optional_declared_physical(io, route, declared.value()).await? else {
        return Ok(None);
    };
    let length =
        decode_with_reservation(io, route, Some(DIRECTORY_FIXED_ALLOCATION_BYTES), || {
            let length = usize::try_from(before.value.size)
                .map_err(|_| physical_backpressure("restore control record size overflow"))?;
            admitted_physical_length(length, cap)?;
            if before.value.version.is_empty() {
                return Err(physical_backpressure(
                    "restore control record version is empty",
                ));
            }
            Ok(length)
        })?;
    declared.value.range.end = (*length.value() + 1) as u64;
    declared.value.reservation_bytes = *length.value() + 1;
    declared.value.min_response_bytes = *length.value();
    declared.value.max_response_bytes = *length.value();
    drop(length);
    let bytes = read_declared_physical_range(io, route, declared.value()).await?;
    let after = head_declared_physical(io, route, declared.value()).await?;
    let checked =
        decode_with_reservation(io, route, Some(DIRECTORY_FIXED_ALLOCATION_BYTES), || {
            if after.value.size != before.value.size || after.value.version != before.value.version
            {
                return Err(physical_backpressure(
                    "restore control record changed during read",
                ));
            }
            Ok(())
        })?;
    drop(checked);
    Ok(Some((bytes, after)))
}

pub(in super::super) async fn write_restore_control_record(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    candidate: &str,
    record: RestoreControlRecord,
    bytes: &WorkingValue<Bytes>,
    expected_version: Option<&str>,
) -> CatalogResult<Accounted<arco_core::WriteResult>> {
    let result = async {
        let store = io.store();
        let owned = bytes.is_owned_by(io);
        let final_stream = matches!(route, RestorePhysicalRoute::FinalMicrochunk(_));
        let reservation =
            directory_scope_reservation(store)?.checked_add(expected_version.map_or(0, str::len));
        let mut prepared = allocate_with_reservation(
            io,
            route,
            Some(reservation.unwrap_or(DIRECTORY_FIXED_ALLOCATION_BYTES)),
            true,
            || {
                if final_stream {
                    return Err(physical_backpressure(
                        "restore control writes require the control ledger",
                    ));
                }
                if reservation.is_none() {
                    return Err(physical_backpressure(
                        "restore control reservation overflow",
                    ));
                }
                if !valid_raw_digest(candidate) || !owned {
                    return Err(physical_backpressure(
                        "restore control identity or byte owner differs",
                    ));
                }
                admitted_physical_length(bytes.value().len(), FINAL_RECEIPT_BYTES)?;
                if expected_version.is_some_and(str::is_empty)
                    || expected_version.is_some()
                        && !matches!(record, RestoreControlRecord::Selector)
                {
                    return Err(physical_backpressure(
                        "restore control precondition is invalid",
                    ));
                }
                let prefix = format!("{}/restore/v7/{candidate}", store.paths.base_prefix());
                let probe = bytes.value().len() + 1;
                let declared = DeclaredPhysicalRange {
                    scope: store.scope.clone(),
                    final_stream,
                    payload: false,
                    path: record.path(&prefix),
                    range: 0..probe as u64,
                    reservation_bytes: probe,
                    min_response_bytes: bytes.value().len(),
                    max_response_bytes: bytes.value().len(),
                };
                let condition = expected_version.map_or(
                    arco_core::AuthorityWritePrecondition::DoesNotExist,
                    |version| {
                        arco_core::AuthorityWritePrecondition::MatchesVersion(version.to_owned())
                    },
                );
                Ok((declared, condition))
            },
        )?;
        if matches!(record, RestoreControlRecord::Selector) {
            // The reservation stays live while the cloned version moves into
            // the storage future, including cancellation of that future.
            let condition = std::mem::replace(
                &mut prepared.value.1,
                arco_core::AuthorityWritePrecondition::DoesNotExist,
            );
            put_restore_conditional(io, route, &prepared.value.0, bytes, condition).await
        } else {
            put_restore_output(io, route, &prepared.value.0, bytes).await
        }
    }
    .await;
    if result.is_err() {
        io.stop(route);
    }
    result
}

// Only these immutable physical identities can select a physical read. Control
// paths are never accepted as strings. Membership and digest validation remain
// obligations of the selected reader; this declaration is not authority.
#[derive(Clone, Copy)]
enum PhysicalObject<'a> {
    Directory {
        key: bool,
        digest: &'a [u8; 32],
        length: usize,
    },
    Descriptor {
        digest: &'a [u8; 32],
        length: usize,
    },
    Index(&'a ControlMvpSegmentRef),
    Payload {
        segment: &'a ControlMvpSegmentRef,
        block: &'a ControlMvpBlock,
    },
    Receipt {
        candidate: &'a str,
        ordinal: u64,
    },
}

struct DeclaredPhysicalRange {
    scope: StateScope,
    final_stream: bool,
    payload: bool,
    path: String,
    range: Range<u64>,
    reservation_bytes: usize,
    min_response_bytes: usize,
    max_response_bytes: usize,
}

impl DeclaredPhysicalRange {
    #[allow(
        clippy::too_many_lines,
        reason = "one exhaustive identity match keeps each path and size admission together"
    )]
    fn new(
        store: &ControlMvpStateStore,
        object: PhysicalObject<'_>,
        final_stream: bool,
    ) -> CatalogResult<Self> {
        let (path, start, length, probe, payload, bounded) = match object {
            PhysicalObject::Directory {
                key,
                digest,
                length,
            } => {
                let cap = if key { MAX_BLOCK_BYTES } else { 64 * 1024 };
                admitted_physical_length(length, cap)?;
                let kind = if key { "keys" } else { "pages" };
                (
                    format!(
                        "control/directory/v1/domains/{}/{kind}/{}",
                        store.scope.domain(),
                        hex::encode(digest)
                    ),
                    0,
                    length,
                    true,
                    false,
                    false,
                )
            }
            PhysicalObject::Descriptor { digest, length } => {
                admitted_physical_length(length, MAX_CONTROL_JSON_BYTES)?;
                (
                    format!(
                        "{}/physical/descriptors/{}.json",
                        store.paths.base_prefix(),
                        hex::encode(digest)
                    ),
                    0,
                    length,
                    false,
                    false,
                    false,
                )
            }
            PhysicalObject::Index(segment) => {
                validate_physical_segment(segment)?;
                let length = usize::try_from(segment.index_size_bytes)
                    .map_err(|_| physical_backpressure("index size overflow"))?;
                admitted_physical_length(length, MAX_SEGMENT_INDEX_BYTES)?;
                (
                    store.paths.segment_index(&segment.segment_id),
                    0,
                    length,
                    false,
                    false,
                    false,
                )
            }
            PhysicalObject::Payload { segment, block } => {
                validate_physical_segment(segment)?;
                let length = usize::try_from(block.length)
                    .map_err(|_| physical_backpressure("payload size overflow"))?;
                admitted_physical_length(
                    length,
                    if block.row_count == 1 {
                        MAX_SEGMENT_BYTES
                    } else {
                        MAX_BLOCK_BYTES
                    },
                )?;
                if block.row_count == 0
                    || block
                        .offset
                        .checked_add(block.length)
                        .is_none_or(|end| end > segment.segment_size_bytes)
                {
                    return Err(physical_backpressure("payload range exceeds its segment"));
                }
                (
                    store.paths.state_object(&segment.segment_id),
                    block.offset,
                    length,
                    false,
                    true,
                    false,
                )
            }
            PhysicalObject::Receipt { candidate, ordinal } => {
                if !final_stream || !valid_raw_digest(candidate) {
                    return Err(physical_backpressure(
                        "invalid final receipt identity or phase",
                    ));
                }
                (
                    format!(
                        "{}/restore/v7/{candidate}/receipts/{ordinal:020}.json",
                        store.paths.base_prefix()
                    ),
                    0,
                    FINAL_RECEIPT_BYTES,
                    true,
                    false,
                    true,
                )
            }
        };
        let reservation_bytes = length
            .checked_add(usize::from(probe))
            .ok_or_else(|| physical_backpressure("physical probe overflow"))?;
        let end = start
            .checked_add(
                u64::try_from(reservation_bytes)
                    .map_err(|_| physical_backpressure("physical range size overflow"))?,
            )
            .ok_or_else(|| physical_backpressure("physical range overflow"))?;
        Ok(Self {
            scope: store.scope.clone(),
            final_stream,
            payload,
            path,
            range: start..end,
            reservation_bytes,
            min_response_bytes: if bounded { 1 } else { length },
            max_response_bytes: length,
        })
    }
}

fn admitted_physical_length(length: usize, cap: usize) -> CatalogResult<()> {
    if length == 0 || length > cap {
        return Err(physical_backpressure(
            "physical object exceeds its individual size admission",
        ));
    }
    Ok(())
}
fn validate_physical_segment(segment: &ControlMvpSegmentRef) -> CatalogResult<()> {
    read_cache::validate_owner(segment)?;
    if segment.level != ControlMvpSegmentLevel::L1 {
        return Err(physical_backpressure("restore physical segment is not L1"));
    }
    Ok(())
}

#[derive(Default)]
struct RestoreReadWork {
    native: super::super::cost::NativeWork,
    range_reads: u64,
    metadata_heads: u64,
    returned_range_bytes: u64,
    write_attempts: u64,
    submitted_write_bytes: u64,
    decode_operations: u64,
    decoded_owned_allocation_bytes: u64,
    encode_operations: u64,
    encoded_owned_allocation_bytes: u64,
    hash_operations: u64,
    hashed_bytes: u64,
}

fn add_read_work(counter: &mut u64, amount: usize) -> CatalogResult<()> {
    *counter = counter
        .checked_add(
            u64::try_from(amount)
                .map_err(|_| physical_backpressure("physical read work overflow"))?,
        )
        .ok_or_else(|| physical_backpressure("physical read work overflow"))?;
    Ok(())
}

pub(in super::super) struct RestorePhysicalIo<'store> {
    store: &'store ControlMvpStateStore,
    work: RestoreReadWork,
    ledger: Arc<Mutex<RestoreOwnershipLedger>>,
    // One terminal decode failure can survive private cleanup awaits. The
    // service invoices its returned error before any subsequent service I/O.
    failed_allocation: Option<WorkingMemory>,
    failed_storage_response: Option<OwnershipHandle>,
    stopped: bool,
}
impl<'store> RestorePhysicalIo<'store> {
    #[cfg(test)]
    pub(in super::super) fn native_work_evidence(&self) -> super::super::cost::NativeWork {
        self.work.native
    }

    #[cfg(test)]
    pub(in super::super) fn live_ownership_evidence(&self) -> (usize, usize) {
        let report = self.ledger.lock().expect("ledger").report();
        (
            report.owned_live_upper_bound(),
            report.backend_origin_shared_live_bytes,
        )
    }

    #[cfg(test)]
    pub(in super::super) fn writing_evidence(&self) -> (u64, u64) {
        (self.work.write_attempts, self.work.submitted_write_bytes)
    }

    #[cfg(test)]
    pub(in super::super) fn reading_evidence(&self) -> (u64, u64, u64) {
        (
            self.work.range_reads,
            self.work.metadata_heads,
            self.work.returned_range_bytes,
        )
    }

    #[cfg(test)]
    pub(in super::super) fn hashing_evidence(&self) -> (u64, u64) {
        (self.work.hash_operations, self.work.hashed_bytes)
    }

    #[cfg(test)]
    pub(in super::super) fn encoding_evidence(&self) -> (u64, u64) {
        (
            self.work.encode_operations,
            self.work.encoded_owned_allocation_bytes,
        )
    }

    #[cfg(test)]
    pub(in super::super) fn peak_owned_evidence(&self) -> usize {
        self.ledger
            .lock()
            .expect("ledger")
            .report
            .request_owned_peak_bytes
    }

    #[cfg(test)]
    pub(in super::super) fn allocation_underestimates(&self) -> u64 {
        self.ledger
            .lock()
            .expect("ledger")
            .report
            .working_underestimates
    }

    #[cfg(test)]
    pub(in super::super) fn decoding_operations(&self) -> u64 {
        self.work.decode_operations
    }

    #[cfg(test)]
    pub(in super::super) fn allocation_evidence(&self) -> (u64, usize) {
        (
            self.work.decoded_owned_allocation_bytes,
            self.ledger
                .lock()
                .expect("ledger")
                .report
                .working_live_bytes,
        )
    }

    pub(in super::super) fn store(&self) -> &'store ControlMvpStateStore {
        self.store
    }

    pub(in super::super) fn stop_final(&mut self, totals: &mut FinalStreamTotals) {
        self.stopped = true;
        totals.stop();
    }

    pub(in super::super) fn stop(&mut self, route: &mut RestorePhysicalRoute<'_, '_>) {
        self.stopped = true;
        route.stop();
    }

    pub(in super::super) async fn directory_object(
        &mut self,
        route: &mut RestorePhysicalRoute<'_, '_>,
        key: bool,
        digest: &[u8; 32],
        length: usize,
    ) -> CatalogResult<AccountedBytes> {
        let store = self.store;
        let final_stream = matches!(route, RestorePhysicalRoute::FinalMicrochunk(_));
        let reservation = directory_scope_reservation(store).inspect_err(|_| self.stop(route))?;
        let declared = decode_with_reservation(self, route, Some(reservation), || {
            DeclaredPhysicalRange::new(
                store,
                PhysicalObject::Directory {
                    key,
                    digest,
                    length,
                },
                final_stream,
            )
        })?;
        read_declared_physical_range(self, route, &declared.value).await
    }

    pub(in super::super) fn new(
        store: &'store ControlMvpStateStore,
        request_limit: usize,
        shared_limit: usize,
    ) -> Self {
        Self {
            store,
            work: RestoreReadWork::default(),
            ledger: Arc::new(Mutex::new(RestoreOwnershipLedger::new(
                request_limit,
                shared_limit,
            ))),
            failed_allocation: None,
            failed_storage_response: None,
            stopped: false,
        }
    }
}
const ORDINARY_UNIT_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
const FINAL_MICROCHUNK_BYTES: usize = 64 * 1024 * 1024;
const FINAL_MICROCHUNK_OPERATIONS: usize = 4096;

fn physical_backpressure(message: &str) -> CatalogError {
    CatalogError::MaintenanceBackpressure {
        message: message.into(),
    }
}

fn ownership_failure(error: OwnershipAdmissionError) -> CatalogError {
    let message = match error {
        OwnershipAdmissionError::Unknown => "restore physical response ownership is unknown",
        OwnershipAdmissionError::Invalid => "restore physical response ownership is invalid",
        OwnershipAdmissionError::RequestOwnedBudgetExceeded => {
            "restore physical request-owned capacity exceeded admission"
        }
        OwnershipAdmissionError::BackendOriginSharedBudgetExceeded => {
            "restore physical backend-shared span exceeded admission"
        }
        OwnershipAdmissionError::Overflow => "restore physical ownership accounting overflowed",
        OwnershipAdmissionError::Poisoned => "restore physical ownership ledger is poisoned",
    };
    physical_backpressure(message)
}

/// Fixed ordinary-unit encoded-payload admission.  Directory pages,
/// descriptors, and indexes are *not* admitted here: their exact witnessed
/// response bytes consume `WorkspaceIoBudget` metadata admission before I/O.
pub(in super::super) struct UnitPayloadAdmission {
    input_bytes: usize,
    output_bytes: usize,
    output_blocks: usize,
}

impl UnitPayloadAdmission {
    pub(in super::super) const fn new() -> Self {
        Self {
            input_bytes: 0,
            output_bytes: 0,
            output_blocks: 0,
        }
    }

    fn reserve_output(&mut self, bytes: usize) -> CatalogResult<()> {
        let capacity = || physical_backpressure("ordinary restore packed output exceeds admission");
        let next = self.output_bytes.checked_add(bytes).ok_or_else(capacity)?;
        let count = self.output_blocks.checked_add(1).ok_or_else(capacity)?;
        if next > 32 * 1024 * 1024 || count > 32 {
            return Err(capacity());
        }
        self.output_bytes = next;
        self.output_blocks = count;
        Ok(())
    }

    fn reserve(&mut self, bytes: usize) -> CatalogResult<()> {
        let next = self
            .input_bytes
            .checked_add(bytes)
            .ok_or_else(|| physical_backpressure("ordinary restore payload admission overflow"))?;
        if next > ORDINARY_UNIT_PAYLOAD_BYTES {
            return Err(physical_backpressure(
                "ordinary restore payload exceeds its 64 MiB unit admission",
            ));
        }
        self.input_bytes = next;
        Ok(())
    }
}

/// Monotonic evidence for the one final traversal.  Its microchunk admission
/// is independent from, and cannot reset or borrow, `WorkspaceIoBudget`.
pub(in super::super) struct FinalStreamTotals {
    microchunks: u64,
    total_operations: u64,
    total_io_reservation_bytes: u64,
    total_returned_request_owned_bytes: u64,
    total_owned_allocation_bytes: u64,
    peak_request_owned_live_bytes: usize,
    peak_chunk_owned_upper_bound_bytes: usize,
    stopped: bool,
}

impl FinalStreamTotals {
    #[cfg(test)]
    pub(in super::super) fn microchunks(&self) -> u64 {
        self.microchunks
    }
    pub(in super::super) const fn new() -> Self {
        Self {
            microchunks: 0,
            total_operations: 0,
            total_io_reservation_bytes: 0,
            total_returned_request_owned_bytes: 0,
            total_owned_allocation_bytes: 0,
            peak_request_owned_live_bytes: 0,
            peak_chunk_owned_upper_bound_bytes: 0,
            stopped: false,
        }
    }

    fn record_io(&mut self, bytes: usize) -> CatalogResult<()> {
        self.total_operations = self
            .total_operations
            .checked_add(1)
            .ok_or_else(|| physical_backpressure("final stream operation total overflow"))?;
        self.total_io_reservation_bytes =
            self.total_io_reservation_bytes
                .checked_add(u64::try_from(bytes).map_err(|_| {
                    physical_backpressure("final stream range total does not fit u64")
                })?)
                .ok_or_else(|| physical_backpressure("final stream range total overflow"))?;
        Ok(())
    }

    fn record_returned_owned(&mut self, bytes: usize, live: usize) -> CatalogResult<()> {
        self.total_returned_request_owned_bytes = self
            .total_returned_request_owned_bytes
            .checked_add(u64::try_from(bytes).map_err(|_| {
                physical_backpressure("final returned-capacity total does not fit u64")
            })?)
            .ok_or_else(|| physical_backpressure("final returned-capacity total overflow"))?;
        self.peak_request_owned_live_bytes = self.peak_request_owned_live_bytes.max(live);
        Ok(())
    }

    fn stop(&mut self) {
        self.stopped = true;
    }
}

/// Admission for a final physical primitive. External carry includes owned
/// builder/cursor/decoded state outside the ownership ledger. Response handles
/// and working reservations in that ledger are never added to external carry.
/// The final driver must supply a complete carry census and enforce fixed-step
/// boundaries after authenticating terminal progress.
pub(in super::super) struct FinalMicrochunk<'a> {
    totals: &'a mut FinalStreamTotals,
    io_reservation_bytes: usize,
    returned_request_owned_bytes: usize,
    owned_allocation_bytes: usize,
    operations: usize,
    external_owned_carry_bytes: usize,
    starting_owned_carry_bytes: usize,
    ledger: Arc<Mutex<RestoreOwnershipLedger>>,
}

impl<'a> FinalMicrochunk<'a> {
    /// Finish this physical primitive without resetting its monotonic totals.
    pub(in super::super) fn finish(self) -> &'a mut FinalStreamTotals {
        self.totals
    }

    pub(in super::super) fn begin(
        totals: &'a mut FinalStreamTotals,
        external_owned_carry_bytes: usize,
        io: &mut RestorePhysicalIo<'_>,
    ) -> CatalogResult<Self> {
        let mut chunk = Self {
            totals,
            io_reservation_bytes: 0,
            returned_request_owned_bytes: 0,
            owned_allocation_bytes: 0,
            operations: 0,
            external_owned_carry_bytes,
            starting_owned_carry_bytes: external_owned_carry_bytes,
            ledger: io.ledger.clone(),
        };
        let admitted = (|| {
            chunk.totals.microchunks = chunk
                .totals
                .microchunks
                .checked_add(1)
                .ok_or_else(|| physical_backpressure("final microchunk count overflow"))?;
            let report = checked_ownership_ledger(&io.ledger)?.report();
            chunk.starting_owned_carry_bytes = external_owned_carry_bytes
                .checked_add(report.owned_live_upper_bound())
                .unwrap_or(usize::MAX);
            chunk.totals.peak_request_owned_live_bytes = chunk
                .totals
                .peak_request_owned_live_bytes
                .max(chunk.starting_owned_carry_bytes);
            chunk.totals.peak_chunk_owned_upper_bound_bytes = chunk
                .totals
                .peak_chunk_owned_upper_bound_bytes
                .max(chunk.starting_owned_carry_bytes);
            if chunk.totals.stopped
                || io.stopped
                || chunk.starting_owned_carry_bytes > FINAL_MICROCHUNK_BYTES
            {
                return Err(physical_backpressure(
                    "final stream stopped or carried ownership exceeds 64 MiB",
                ));
            }
            Ok(())
        })();
        if let Err(error) = admitted {
            io.stopped = true;
            chunk.totals.stop();
            return Err(retain_rejected_diagnostic(
                &io.ledger,
                &mut io.failed_allocation,
                &mut io.work,
                &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
                false,
                error,
            ));
        }
        Ok(chunk)
    }

    fn owned_upper_bound(&self) -> usize {
        self.starting_owned_carry_bytes
            .checked_add(self.returned_request_owned_bytes)
            .and_then(|bytes| bytes.checked_add(self.owned_allocation_bytes))
            .unwrap_or(usize::MAX)
    }

    fn validate_ledger(&self, ledger: &Arc<Mutex<RestoreOwnershipLedger>>) -> CatalogResult<()> {
        if self.totals.stopped || !Arc::ptr_eq(&self.ledger, ledger) {
            return Err(physical_backpressure(
                "final microchunk ownership ledger differs or stopped",
            ));
        }
        Ok(())
    }

    fn record_allocated_owned(&mut self, bytes: usize) {
        // Observed allocations, including a failed decode's error, remain
        // cumulative even after their live ownership guards are released.
        self.owned_allocation_bytes = self.owned_allocation_bytes.saturating_add(bytes);
        self.totals.total_owned_allocation_bytes = self
            .totals
            .total_owned_allocation_bytes
            .checked_add(u64::try_from(bytes).unwrap_or(u64::MAX))
            .unwrap_or_else(|| {
                self.totals.stopped = true;
                u64::MAX
            });
        self.totals.peak_chunk_owned_upper_bound_bytes = self
            .totals
            .peak_chunk_owned_upper_bound_bytes
            .max(self.owned_upper_bound());
        if self.owned_upper_bound() > FINAL_MICROCHUNK_BYTES {
            self.totals.stop();
        }
    }

    fn reserve_io(&mut self, bytes: usize) -> CatalogResult<()> {
        let operations = self
            .operations
            .checked_add(1)
            .ok_or_else(|| physical_backpressure("final microchunk operation overflow"))?;
        let ranges = self
            .io_reservation_bytes
            .checked_add(bytes)
            .ok_or_else(|| physical_backpressure("final microchunk range overflow"))?;
        let total = ranges
            .checked_add(self.returned_request_owned_bytes)
            .ok_or_else(|| physical_backpressure("final microchunk admission overflow"))?;
        if operations > FINAL_MICROCHUNK_OPERATIONS || total > FINAL_MICROCHUNK_BYTES {
            return Err(physical_backpressure(
                "final stream exceeds its 64 MiB or 4096-operation microchunk",
            ));
        }
        self.operations = operations;
        self.io_reservation_bytes = ranges;
        self.totals.record_io(bytes)
    }

    /// Runs immediately after classified response admission, while the
    /// non-Clone `AccountedBytes` handle is still live and before another I/O.
    /// Only request-owned actual capacity enters the 64 MiB cumulative
    /// microchunk allowance; backend-origin shared bytes remain separately
    /// live-accounted by `RestoreOwnershipLedger` as required by the contract.
    fn charge_returned_request_owned(
        &mut self,
        actual_capacity: usize,
        request_owned_live_bytes: usize,
    ) -> CatalogResult<()> {
        let returned = self
            .returned_request_owned_bytes
            .checked_add(actual_capacity)
            .ok_or_else(|| physical_backpressure("final returned-capacity overflow"))?;
        let cumulative = self
            .io_reservation_bytes
            .checked_add(returned)
            .ok_or_else(|| physical_backpressure("final microchunk admission overflow"))?;
        // The response has already arrived. Retain exact observed capacity and
        // live-peak evidence even when it makes the request nonpassing.
        self.returned_request_owned_bytes = returned;
        let combined_live = self
            .external_owned_carry_bytes
            .checked_add(request_owned_live_bytes);
        // Record a conservative unrepresentable peak and fail, never wrap.
        self.totals
            .record_returned_owned(actual_capacity, combined_live.unwrap_or(usize::MAX))?;
        self.totals.peak_chunk_owned_upper_bound_bytes = self
            .totals
            .peak_chunk_owned_upper_bound_bytes
            .max(self.owned_upper_bound());
        if cumulative > FINAL_MICROCHUNK_BYTES
            || combined_live.is_none_or(|bytes| bytes > FINAL_MICROCHUNK_BYTES)
            || self.owned_upper_bound() > FINAL_MICROCHUNK_BYTES
            || self.totals.stopped
        {
            return Err(physical_backpressure(
                "final returned response capacity exceeds microchunk admission",
            ));
        }
        Ok(())
    }
}

pub(in super::super) enum RestorePhysicalRoute<'route, 'stream> {
    OrdinaryUnit {
        workspace: &'route mut WorkspaceIoBudget,
        payload: &'route mut UnitPayloadAdmission,
    },
    FinalMicrochunk(&'route mut FinalMicrochunk<'stream>),
}
impl RestorePhysicalRoute<'_, '_> {
    fn reserve_before_io(&mut self, declared: &DeclaredPhysicalRange) -> CatalogResult<()> {
        match self {
            Self::OrdinaryUnit { workspace, payload } if !declared.final_stream => {
                workspace.charge_operations(1)?;
                if declared.payload {
                    payload.reserve(declared.reservation_bytes)
                } else {
                    workspace.reserve_bytes(declared.reservation_bytes)
                }
            }
            Self::FinalMicrochunk(chunk) if declared.final_stream => {
                chunk.reserve_io(declared.reservation_bytes)
            }
            _ => Err(physical_backpressure(
                "restore physical class does not match admission route",
            )),
        }
    }
    fn stop(&mut self) {
        if let Self::FinalMicrochunk(chunk) = self {
            chunk.totals.stop();
        }
    }
}
// A backend error is an arrived owned response, even when the operation failed.
// Keep its existing String allocation across participant cleanup without formatting
// an opaque source or copying a potentially oversized message.
fn account_physical_storage_error(
    ledger: &Arc<Mutex<RestoreOwnershipLedger>>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    retained: &mut Option<OwnershipHandle>,
    error: arco_core::Error,
) -> CatalogError {
    let (arco_core::Error::Storage {
        message,
        source: None,
    }
    | arco_core::Error::InvalidId { message }
    | arco_core::Error::TenantIsolation { message }
    | arco_core::Error::Serialization { message }
    | arco_core::Error::PreconditionFailed { message }
    | arco_core::Error::Internal { message }
    | arco_core::Error::Validation { message }
    | arco_core::Error::NotFound(message)
    | arco_core::Error::InvalidInput(message)
    | arco_core::Error::ResourceNotFound { id: message, .. }) = error
    else {
        let _unknown = admit_response(ledger.clone(), (), 0, Err(OwnershipAdmissionError::Unknown));
        return physical_backpressure("restore storage error has unknown owned source");
    };
    let owned = size_of::<arco_core::Error>()
        .checked_add(size_of::<CatalogError>())
        .and_then(|size| size.checked_add(message.capacity()))
        .unwrap_or(usize::MAX);
    let error = CatalogError::Storage { message };
    let previous_live = match ledger.lock() {
        Ok(ledger) => ledger.report().owned_live_upper_bound(),
        Err(poisoned) => poisoned.into_inner().report().owned_live_upper_bound(),
    };
    // Both invoices observe the arrived response even if either rejects it.
    match route {
        RestorePhysicalRoute::OrdinaryUnit { workspace, .. } => {
            let _ = workspace.charge_error(&error);
        }
        RestorePhysicalRoute::FinalMicrochunk(chunk) => {
            let _ = chunk.charge_returned_request_owned(
                owned,
                previous_live.checked_add(owned).unwrap_or(usize::MAX),
            );
        }
    }
    if let Ok(response) = admit_response(
        ledger.clone(),
        (),
        owned,
        Ok(AccountedBytesClass::NewRequestOwned {
            actual_capacity: owned,
        }),
    ) {
        *retained = Some(response.handle);
    }
    error
}

async fn read_declared_physical_range(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    declared: &DeclaredPhysicalRange,
) -> CatalogResult<AccountedBytes> {
    if io.stopped {
        route.stop();
        return Err(physical_backpressure(
            "restore physical I/O stopped after prior failure",
        ));
    }
    let mut attempt = RestoreReadAttempt::new(&mut io.stopped, route);
    validate_declared_read(io.store, &io.ledger, attempt.route, declared)?;
    attempt.route.reserve_before_io(declared)?;
    add_read_work(&mut io.work.range_reads, 1)?;
    let response = io
        .store
        .storage
        .get_range_with_ownership(&declared.path, declared.range.clone())
        .await
        .map_err(|error| {
            account_physical_storage_error(
                &io.ledger,
                attempt.route,
                &mut io.failed_storage_response,
                error,
            )
        })?;
    // Charge every arrived buffer, including malformed lengths and rejected
    // ownership claims, before it can be decoded or another I/O can start.
    let work_result = add_read_work(&mut io.work.returned_range_bytes, response.bytes.len());
    let class = classify_response(&response);
    let previous_live = match io.ledger.lock() {
        Ok(ledger) => ledger.report().owned_live_upper_bound(),
        Err(poisoned) => poisoned.into_inner().report().owned_live_upper_bound(),
    };
    let admitted = admit_classified_response(io.ledger.clone(), response, class);
    if let (RestorePhysicalRoute::FinalMicrochunk(chunk), Ok(class)) = (&mut attempt.route, class) {
        if class.is_request_owned() {
            // Snapshot before admission includes an allocation rejected by the
            // ledger, without charging historical peaks from released steps.
            // A concurrent handle drop can only make this bound conservative.
            let arrival_live = previous_live
                .checked_add(class.amount())
                .unwrap_or(usize::MAX);
            chunk.charge_returned_request_owned(class.amount(), arrival_live)?;
        }
    }
    work_result?;
    let bytes = admitted.map_err(ownership_failure)?;
    if !(declared.min_response_bytes..=declared.max_response_bytes)
        .contains(&bytes.as_slice().len())
    {
        return Err(physical_backpressure(
            "restore physical response differs from its declared expectation",
        ));
    }
    attempt.disarm();
    Ok(bytes)
}
fn validate_declared_read(
    store: &ControlMvpStateStore,
    ledger: &Arc<Mutex<RestoreOwnershipLedger>>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    declared: &DeclaredPhysicalRange,
) -> CatalogResult<()> {
    if store.authority_format != 8 || declared.scope != store.scope {
        return Err(physical_backpressure(
            "restore physical scope or authority format differs",
        ));
    }
    let report = checked_ownership_ledger(ledger)?.report();
    if let RestorePhysicalRoute::FinalMicrochunk(chunk) = route {
        chunk.validate_ledger(ledger)?;
        // Existing ownership and external carry are known before I/O.
        chunk.charge_returned_request_owned(0, report.owned_live_upper_bound())?;
    }
    Ok(())
}

/// Performs one conditional PUT with every arrived response charged before return.
#[allow(
    clippy::too_many_lines,
    reason = "one write admission guard retains all response owners"
)]
async fn put_restore_conditional(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    declared: &DeclaredPhysicalRange,
    bytes: &WorkingValue<Bytes>,
    condition: arco_core::AuthorityWritePrecondition,
) -> CatalogResult<Accounted<arco_core::WriteResult>> {
    if io.stopped {
        route.stop();
        return Err(physical_backpressure(
            "restore physical I/O stopped after prior failure",
        ));
    }
    let result = {
        let mut attempt = RestoreReadAttempt::new(&mut io.stopped, route);
        validate_declared_read(io.store, &io.ledger, attempt.route, declared)?;
        if !Arc::ptr_eq(&bytes.working.ledger, &io.ledger)
            || declared.range.start != 0
            || declared.min_response_bytes != bytes.value().len()
            || declared.max_response_bytes != bytes.value().len()
            || declared.reservation_bytes
                != bytes
                    .value()
                    .len()
                    .checked_add(1)
                    .ok_or_else(|| physical_backpressure("restore output probe overflow"))?
        {
            return Err(physical_backpressure(
                "restore output ownership or exact probe differs",
            ));
        }
        match &mut attempt.route {
            RestorePhysicalRoute::OrdinaryUnit { workspace, payload } if !declared.final_stream => {
                workspace.charge_operations(1)?;
                if declared.payload {
                    payload.reserve_output(bytes.value().len())?;
                } else {
                    workspace.reserve_bytes(bytes.value().len())?;
                }
            }
            RestorePhysicalRoute::FinalMicrochunk(chunk) if declared.final_stream => {
                chunk.reserve_io(bytes.value().len())?;
            }
            _ => return Err(physical_backpressure("restore output route differs")),
        }
        add_read_work(&mut io.work.write_attempts, 1)?;
        add_read_work(&mut io.work.submitted_write_bytes, bytes.value().len())?;
        let result = io
            .store
            .storage
            .put(&declared.path, bytes.value().clone(), condition)
            .await
            .map_err(|error| {
                account_physical_storage_error(
                    &io.ledger,
                    attempt.route,
                    &mut io.failed_storage_response,
                    error,
                )
            })?;
        let owned = size_of::<arco_core::WriteResult>()
            .checked_add(restore_output_version(&result).capacity())
            .ok_or_else(|| physical_backpressure("restore write response ownership overflow"))?;
        let previous_live = match io.ledger.lock() {
            Ok(ledger) => ledger.report().owned_live_upper_bound(),
            Err(poisoned) => poisoned.into_inner().report().owned_live_upper_bound(),
        };
        let invoice = match &mut attempt.route {
            RestorePhysicalRoute::OrdinaryUnit { workspace, .. } => workspace.charge_write(&result),
            RestorePhysicalRoute::FinalMicrochunk(chunk) => chunk.charge_returned_request_owned(
                owned,
                previous_live.checked_add(owned).unwrap_or(usize::MAX),
            ),
        };
        let admitted = admit_response(
            io.ledger.clone(),
            result,
            owned,
            Ok(AccountedBytesClass::NewRequestOwned {
                actual_capacity: owned,
            }),
        );
        invoice?;
        let result = admitted.map_err(ownership_failure)?;
        if restore_output_version(&result.value).is_empty() {
            return Err(physical_backpressure("restore output version is empty"));
        }
        attempt.disarm();
        result
    };
    Ok(result)
}

/// Writes one admitted immutable object; a collision is authenticated independently.
async fn put_restore_output(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    declared: &DeclaredPhysicalRange,
    bytes: &WorkingValue<Bytes>,
) -> CatalogResult<Accounted<arco_core::WriteResult>> {
    let result = put_restore_conditional(
        io,
        route,
        declared,
        bytes,
        arco_core::AuthorityWritePrecondition::DoesNotExist,
    )
    .await?;
    let verified = async {
        if matches!(
            result.value,
            arco_core::WriteResult::PreconditionFailed { .. }
        ) {
            let before = head_declared_physical(io, route, declared).await?;
            if before.value.size != bytes.value().len() as u64
                || before.value.version != *restore_output_version(&result.value)
            {
                return Err(super::invariant_violation(
                    "restore collision metadata differs",
                ));
            }
            let existing = read_declared_physical_range(io, route, declared).await?;
            let after = head_declared_physical(io, route, declared).await?;
            if after.value.size != before.value.size
                || after.value.version != before.value.version
                || existing.as_slice() != bytes.value().as_ref()
            {
                return Err(super::invariant_violation(
                    "restore immutable collision differs",
                ));
            }
        }
        Ok(result)
    }
    .await;
    if verified.is_err() {
        io.stop(route);
    }
    verified
}

fn restore_output_version(result: &arco_core::WriteResult) -> &String {
    match result {
        arco_core::WriteResult::Success { version } => version,
        arco_core::WriteResult::PreconditionFailed { current_version } => current_version,
    }
}

async fn head_declared_physical(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    declared: &DeclaredPhysicalRange,
) -> CatalogResult<AccountedMeta> {
    head_optional_declared_physical(io, route, declared)
        .await?
        .map_or_else(
            || {
                let failure = decode_with_reservation(
                    io,
                    route,
                    Some(DIRECTORY_FIXED_ALLOCATION_BYTES),
                    || -> CatalogResult<()> {
                        Err(physical_backpressure(
                            "selected restore physical object is missing",
                        ))
                    },
                );
                match failure {
                    Err(error) => Err(error),
                    Ok(_) => unreachable!("missing object closure always fails"),
                }
            },
            Ok,
        )
}

async fn head_optional_declared_physical(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    declared: &DeclaredPhysicalRange,
) -> CatalogResult<Option<AccountedMeta>> {
    if io.stopped {
        route.stop();
        return Err(physical_backpressure(
            "restore physical I/O stopped after prior failure",
        ));
    }
    let mut attempt = RestoreReadAttempt::new(&mut io.stopped, route);
    validate_declared_read(io.store, &io.ledger, attempt.route, declared)?;
    match &mut attempt.route {
        RestorePhysicalRoute::OrdinaryUnit { workspace, .. } if !declared.final_stream => {
            workspace.charge_operations(1)?;
        }
        RestorePhysicalRoute::FinalMicrochunk(chunk) if declared.final_stream => {
            chunk.reserve_io(0)?;
        }
        _ => {
            return Err(physical_backpressure(
                "restore physical class does not match admission route",
            ));
        }
    }
    add_read_work(&mut io.work.metadata_heads, 1)?;
    let meta = io
        .store
        .storage
        .head(&declared.path)
        .await
        .map_err(|error| {
            account_physical_storage_error(
                &io.ledger,
                attempt.route,
                &mut io.failed_storage_response,
                error,
            )
        })?;
    let Some(meta) = meta else {
        attempt.disarm();
        return Ok(None);
    };
    let owned = object_meta_owned_bytes(&meta)?;
    let previous_live = match io.ledger.lock() {
        Ok(ledger) => ledger.report().owned_live_upper_bound(),
        Err(poisoned) => poisoned.into_inner().report().owned_live_upper_bound(),
    };
    // Both ledgers must observe an arrived metadata allocation even if either
    // rejects it. Propagate errors only after both diagnostics are retained.
    let invoice = match &mut attempt.route {
        RestorePhysicalRoute::OrdinaryUnit { workspace, .. } => workspace.charge_head(&meta),
        RestorePhysicalRoute::FinalMicrochunk(chunk) => chunk.charge_returned_request_owned(
            owned,
            previous_live.checked_add(owned).unwrap_or(usize::MAX),
        ),
    };
    let admitted = admit_response(
        io.ledger.clone(),
        meta,
        owned,
        Ok(AccountedBytesClass::NewRequestOwned {
            actual_capacity: owned,
        }),
    );
    invoice?;
    let meta = admitted.map_err(ownership_failure)?;
    attempt.disarm();
    Ok(Some(meta))
}

/// The guard arms before validation, pre-admission, or an await.  An error or
/// cancellation drops it armed, making the invocation terminal before another
/// physical call can occur.
struct RestoreReadAttempt<'a, 'route, 'stream> {
    stopped: &'a mut bool,
    route: &'a mut RestorePhysicalRoute<'route, 'stream>,
    armed: bool,
}

impl<'a, 'route, 'stream> RestoreReadAttempt<'a, 'route, 'stream> {
    fn new(stopped: &'a mut bool, route: &'a mut RestorePhysicalRoute<'route, 'stream>) -> Self {
        Self {
            stopped,
            route,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RestoreReadAttempt<'_, '_, '_> {
    fn drop(&mut self) {
        if self.armed {
            *self.stopped = true;
            self.route.stop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ledger(request: usize, shared: usize) -> Arc<Mutex<RestoreOwnershipLedger>> {
        Arc::new(Mutex::new(RestoreOwnershipLedger::new(request, shared)))
    }

    fn response(bytes: &'static [u8], ownership: BytesBackingOwnership) -> ClassifiedBytes {
        ClassifiedBytes {
            bytes: Bytes::from_static(bytes),
            ownership,
        }
    }

    fn owned_response(payload: &[u8], requested_capacity: usize, copied: bool) -> ClassifiedBytes {
        let mut allocation = Vec::with_capacity(requested_capacity);
        allocation.extend_from_slice(payload);
        let actual_capacity = allocation.capacity();
        assert!(actual_capacity >= allocation.len());
        let ownership = if copied {
            BytesBackingOwnership::CopiedRequestOwned { actual_capacity }
        } else {
            BytesBackingOwnership::NewRequestOwned { actual_capacity }
        };
        ClassifiedBytes {
            bytes: Bytes::from(allocation),
            ownership,
        }
    }

    #[test]
    fn owned_capacity_is_charged_not_visible_length() {
        let ledger = ledger(16, 16);
        let response = owned_response(b"four", 12, false);
        let BytesBackingOwnership::NewRequestOwned { actual_capacity } = response.ownership else {
            panic!("truthful owned fixture")
        };
        assert!(actual_capacity > response.bytes.len());
        let accounted =
            admit_classified_bytes(ledger.clone(), response).expect("capacity admission");
        assert_eq!(accounted.as_slice(), b"four");
        assert!(matches!(
            accounted.class(),
            AccountedBytesClass::NewRequestOwned { actual_capacity: capacity } if capacity == actual_capacity
        ));
        let report = ledger.lock().expect("ledger").report();
        assert_eq!(report.request_owned_live_bytes, actual_capacity);
        assert_eq!(report.request_owned_peak_bytes, actual_capacity);
        drop(accounted);
        assert!(ledger.lock().expect("ledger").report().passing());
    }

    #[test]
    fn unknown_and_impossible_claims_fail_before_a_handle_exists() {
        let ledger = ledger(16, 16);
        assert!(matches!(
            admit_classified_bytes(
                ledger.clone(),
                response(b"x", BytesBackingOwnership::Unknown)
            ),
            Err(OwnershipAdmissionError::Unknown)
        ));
        assert!(matches!(
            admit_classified_bytes(
                ledger.clone(),
                response(
                    b"abcd",
                    BytesBackingOwnership::CopiedRequestOwned { actual_capacity: 3 },
                ),
            ),
            Err(OwnershipAdmissionError::Invalid)
        ));
        let report = ledger.lock().expect("ledger").report();
        assert_eq!(report.request_owned_live_bytes, 0);
        assert_eq!(report.backend_origin_shared_live_bytes, 0);
        assert_eq!(report.unknown_responses, 1);
        assert_eq!(report.invalid_responses, 1);
        assert!(!report.passing());
    }

    #[test]
    fn over_budget_response_records_known_peak_without_a_live_handle() {
        let response = owned_response(b"four", 8, false);
        let BytesBackingOwnership::NewRequestOwned { actual_capacity } = response.ownership else {
            panic!("truthful owned fixture")
        };
        let ledger = ledger(actual_capacity - 1, 16);
        assert!(matches!(
            admit_classified_bytes(ledger.clone(), response),
            Err(OwnershipAdmissionError::RequestOwnedBudgetExceeded)
        ));
        let report = ledger.lock().expect("ledger").report();
        assert_eq!(report.request_owned_live_bytes, 0);
        assert_eq!(report.request_owned_peak_bytes, actual_capacity);
        assert_eq!(report.over_budget_responses, 1);
        assert!(!report.passing());
    }

    #[test]
    fn poisoned_admission_marks_sticky_failure_without_a_handle() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        let ledger = ledger(16, 16);
        let poison_target = ledger.clone();
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _guard = poison_target.lock().expect("first lock");
            panic!("test-only poison");
        }));
        assert!(matches!(
            admit_classified_bytes(ledger.clone(), owned_response(b"four", 12, false)),
            Err(OwnershipAdmissionError::Poisoned)
        ));
        let report = match ledger.lock() {
            Ok(ledger) => ledger.report(),
            Err(poisoned) => poisoned.into_inner().report(),
        };
        assert_eq!(report.request_owned_live_bytes, 0);
        assert_eq!(report.releases, 0);
        assert!(report.poisoned_ledger);
        assert!(!report.passing());
    }

    #[test]
    fn usize_total_overflow_is_a_sticky_nonpassing_failure() {
        let mut ledger = RestoreOwnershipLedger::new(usize::MAX, 16);
        ledger.report.request_owned_live_bytes = usize::MAX;
        assert!(matches!(
            ledger.reserve(AccountedBytesClass::NewRequestOwned { actual_capacity: 1 }),
            Err(OwnershipAdmissionError::Overflow)
        ));
        assert_eq!(ledger.report.request_owned_live_bytes, usize::MAX);
        assert_eq!(ledger.report.invalid_responses, 1);
        assert!(ledger.report.counter_overflow);
        assert!(!ledger.report.passing());
    }

    #[test]
    fn poisoned_drop_marks_nonpassing_and_releases_exactly_once() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        let ledger = ledger(16, 16);
        let accounted = admit_classified_bytes(ledger.clone(), owned_response(b"four", 12, false))
            .expect("admitted response");
        let poison_target = ledger.clone();
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _guard = poison_target.lock().expect("first lock");
            panic!("test-only poison");
        }));
        drop(accounted);
        let report = match ledger.lock() {
            Ok(ledger) => ledger.report(),
            Err(poisoned) => poisoned.into_inner().report(),
        };
        assert_eq!(report.request_owned_live_bytes, 0);
        assert_eq!(report.releases, 1);
        assert!(report.poisoned_ledger);
        assert!(!report.passing());
    }

    #[test]
    fn independent_handles_release_only_their_own_exact_admission() {
        let ledger = ledger(16, 16);
        let first = admit_classified_bytes(
            ledger.clone(),
            response(
                b"one",
                BytesBackingOwnership::BackendOriginShared { held_len: 3 },
            ),
        )
        .expect("first");
        let second_response = owned_response(b"two!", 6, true);
        let BytesBackingOwnership::CopiedRequestOwned {
            actual_capacity: copied_capacity,
        } = second_response.ownership
        else {
            panic!("truthful copied fixture")
        };
        let second = admit_classified_bytes(ledger.clone(), second_response).expect("second");
        drop(first);
        let after_first = ledger.lock().expect("ledger").report();
        assert_eq!(after_first.backend_origin_shared_live_bytes, 0);
        assert_eq!(after_first.request_owned_live_bytes, copied_capacity);
        assert_eq!(after_first.releases, 1);
        drop(second);
        let final_report = ledger.lock().expect("ledger").report();
        assert_eq!(final_report.releases, 2);
        assert!(final_report.passing());
    }
    #[test]
    fn over_budget_capacity_records_the_arrived_buffer_beside_existing_live_bytes() {
        let ledger = ledger(10, 16);
        let held_response = owned_response(b"four", 4, false);
        let held = admit_classified_bytes(ledger.clone(), held_response).expect("held response");
        let rejected_response = owned_response(b"data", 8, true);
        let BytesBackingOwnership::CopiedRequestOwned {
            actual_capacity: rejected_capacity,
        } = rejected_response.ownership
        else {
            panic!("truthful copied fixture")
        };
        assert!(matches!(
            admit_classified_bytes(ledger.clone(), rejected_response),
            Err(OwnershipAdmissionError::RequestOwnedBudgetExceeded)
        ));
        let report = ledger.lock().expect("ledger").report();
        assert_eq!(report.request_owned_live_bytes, 4);
        assert_eq!(report.request_owned_peak_bytes, 12);
        assert_eq!(
            report.largest_rejected_request_owned_capacity,
            rejected_capacity
        );
        assert_eq!(report.over_budget_responses, 1);
        drop(held);
    }

    #[tokio::test]
    async fn aborting_a_future_releases_its_own_response_without_releasing_another_handle() {
        let ledger = ledger(32, 32);
        let surviving = admit_classified_bytes(
            ledger.clone(),
            response(
                b"one",
                BytesBackingOwnership::BackendOriginShared { held_len: 3 },
            ),
        )
        .expect("surviving handle");
        let cancelled_ledger = ledger.clone();
        let (admitted, observed_admission) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let held = admit_classified_bytes(cancelled_ledger, owned_response(b"four", 12, false))
                .expect("cancelled task admission");
            assert_eq!(held.as_slice(), b"four");
            admitted.send(()).expect("test observes admitted handle");
            std::future::pending::<()>().await;
        });
        observed_admission
            .await
            .expect("task admits before cancellation");
        assert_eq!(
            ledger
                .lock()
                .expect("ledger")
                .report()
                .request_owned_live_bytes,
            12,
            "the task holds its classified response across a poll"
        );
        task.abort();
        assert!(task.await.expect_err("task was cancelled").is_cancelled());
        let after_abort = ledger.lock().expect("ledger").report();
        assert_eq!(after_abort.request_owned_live_bytes, 0);
        assert_eq!(after_abort.backend_origin_shared_live_bytes, 3);
        assert_eq!(after_abort.releases, 1);
        drop(surviving);
        let final_report = ledger.lock().expect("ledger").report();
        assert_eq!(final_report.releases, 2);
        assert!(final_report.passing());
    }

    #[test]
    fn unknown_class_records_visible_evidence_without_inventing_a_capacity() {
        let ledger = ledger(16, 16);
        assert!(matches!(
            admit_classified_bytes(
                ledger.clone(),
                response(b"unknown", BytesBackingOwnership::Unknown),
            ),
            Err(OwnershipAdmissionError::Unknown)
        ));
        let report = ledger.lock().expect("ledger").report();
        assert_eq!(report.unknown_responses, 1);
        assert_eq!(report.largest_unknown_visible_len, b"unknown".len());
        assert_eq!(report.request_owned_peak_bytes, 0);
        assert_eq!(report.backend_origin_shared_peak_bytes, 0);
        assert!(!report.passing());
    }

    #[test]
    fn poisoned_admission_records_known_capacity_without_creating_a_handle() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        let ledger = ledger(16, 16);
        let poison_target = ledger.clone();
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _guard = poison_target.lock().expect("first lock");
            panic!("test-only poison");
        }));
        let rejected = owned_response(b"four", 12, false);
        let BytesBackingOwnership::NewRequestOwned {
            actual_capacity: capacity,
        } = rejected.ownership
        else {
            panic!("truthful owned fixture")
        };
        assert!(matches!(
            admit_classified_bytes(ledger.clone(), rejected),
            Err(OwnershipAdmissionError::Poisoned)
        ));
        let report = match ledger.lock() {
            Ok(ledger) => ledger.report(),
            Err(poisoned) => poisoned.into_inner().report(),
        };
        assert!(report.poisoned_ledger);
        assert_eq!(report.request_owned_live_bytes, 0);
        assert_eq!(report.releases, 0);
        assert_eq!(report.largest_rejected_request_owned_capacity, capacity);
        assert!(!report.passing());
    }
    #[test]
    fn shared_and_invalid_claims_retain_exact_rejected_evidence() {
        let shared = ledger(0, 3);
        let held = admit_classified_bytes(
            shared.clone(),
            response(
                b"abc",
                BytesBackingOwnership::BackendOriginShared { held_len: 3 },
            ),
        )
        .expect("exact shared boundary");
        assert!(matches!(
            admit_classified_bytes(
                shared.clone(),
                response(
                    b"x",
                    BytesBackingOwnership::BackendOriginShared { held_len: 1 }
                )
            ),
            Err(OwnershipAdmissionError::BackendOriginSharedBudgetExceeded)
        ));
        let observed = shared.lock().expect("ledger").report();
        assert_eq!(observed.backend_origin_shared_live_bytes, 3);
        assert_eq!(observed.backend_origin_shared_peak_bytes, 4);
        assert_eq!(observed.largest_rejected_backend_shared_held_len, 1);
        drop(held);
        assert_eq!(
            shared
                .lock()
                .expect("ledger")
                .report()
                .backend_origin_shared_live_bytes,
            0
        );
        for claim in [
            BytesBackingOwnership::BackendOriginShared { held_len: 2 },
            BytesBackingOwnership::NewRequestOwned { actual_capacity: 2 },
            BytesBackingOwnership::CopiedRequestOwned { actual_capacity: 2 },
        ] {
            let invalid = ledger(64, 64);
            assert!(matches!(
                admit_classified_bytes(invalid.clone(), response(b"abc", claim)),
                Err(OwnershipAdmissionError::Invalid)
            ));
            let report = invalid.lock().expect("ledger").report();
            assert_eq!(report.largest_invalid_visible_len, 3);
            assert_eq!(report.request_owned_live_bytes, 0);
            assert_eq!(report.backend_origin_shared_live_bytes, 0);
            assert!(!report.passing());
        }
        let mut overflow = RestoreOwnershipLedger::new(usize::MAX, 0);
        overflow.report.request_owned_live_bytes = usize::MAX;
        assert!(matches!(
            overflow.reserve(AccountedBytesClass::NewRequestOwned { actual_capacity: 7 }),
            Err(OwnershipAdmissionError::Overflow)
        ));
        assert_eq!(overflow.report.largest_rejected_request_owned_capacity, 7);
    }

    #[tokio::test]
    async fn memory_range_survives_deletion_and_decoded_arrow_rows_have_no_input_alias() {
        use super::super::super::{
            ControlMvpSegmentIndex, ControlMvpSegmentLevel, ControlMvpSegmentRow,
            PRODUCTION_SEGMENT_LIMITS, SEGMENT_RECORD_KV, StateScope, decode_block_rows,
            decode_json, encode_segment,
        };
        use arco_core::{
            AuthorityWritePrecondition, MemoryBackend, ScopedAuthorityStore, ScopedStorage,
        };
        let scoped = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
            .expect("scope");
        let authority = ScopedAuthorityStore::new(scoped.clone());
        let row = ControlMvpSegmentRow {
            record_kind: SEGMENT_RECORD_KV,
            key: b"key".to_vec(),
            value: Some(b"opaque\0payload".to_vec()),
            generation: 1,
            tombstone: false,
            logical_sequence: 1,
            logical_ordinal: 0,
            origin_sequence: None,
        };
        let (encoded, index, _) = encode_segment(
            "00000000000000000000000000000001",
            ControlMvpSegmentLevel::L1,
            1,
            &StateScope::new("tenant", "workspace", "catalog"),
            std::slice::from_ref(&row),
            PRODUCTION_SEGMENT_LIMITS,
        )
        .expect("production Arrow block");
        let index: ControlMvpSegmentIndex = decode_json(&index, "test index").expect("index");
        let block = &index.blocks[0];
        let mut inventory = Vec::with_capacity(encoded.len() + 4096);
        inventory.extend_from_slice(&encoded);
        assert!(inventory.capacity() > inventory.len());
        authority
            .put(
                "payload",
                inventory.into(),
                AuthorityWritePrecondition::DoesNotExist,
            )
            .await
            .expect("inventory");
        let response = authority
            .get_range_with_ownership("payload", block.offset..block.offset + block.length)
            .await
            .expect("physical range");
        let len = response.bytes.len();
        assert_eq!(
            response.ownership,
            BytesBackingOwnership::BackendOriginShared { held_len: len }
        );
        let invoice = ledger(0, len);
        let held = admit_classified_bytes(invoice.clone(), response).expect("shared response");
        scoped
            .delete("payload")
            .await
            .expect("remove inventory owner");
        tokio::task::yield_now().await;
        assert_eq!(
            invoice
                .lock()
                .expect("ledger")
                .report()
                .backend_origin_shared_live_bytes,
            len
        );
        let rows = decode_block_rows(held.as_slice(), block, "catalog")
            .expect("decode through accounted slice");
        drop(held);
        tokio::task::yield_now().await;
        assert_eq!(rows, [row]);
        let report = invoice.lock().expect("ledger").report();
        assert_eq!(report.releases, 1);
        assert_eq!(report.request_owned_peak_bytes, 0);
        assert!(report.passing());
    }
}

#[cfg(test)]
pub(in super::super) fn selected_read_pending_store(boundary: usize) -> ControlMvpStateStore {
    range_tests::selected_read_pending_store(boundary)
}

#[cfg(test)]
pub(in super::super) fn window_pending_store()
-> (ControlMvpStateStore, Arc<std::sync::atomic::AtomicUsize>) {
    range_tests::window_pending_store()
}

#[cfg(test)]
pub(in super::super) fn unit_publication_test_store(
    at: usize,
    after: bool,
    pending: bool,
) -> ControlMvpStateStore {
    range_tests::unit_publication_test_store(at, after, pending)
}

#[cfg(test)]
pub(in super::super) fn unit_publication_barrier_store() -> ControlMvpStateStore {
    range_tests::unit_publication_barrier_store()
}

#[cfg(test)]
mod range_tests {
    use super::*;
    use arco_core::{
        AuthorityWritePrecondition, ScopedStorage,
        storage::{ObjectMeta, StorageBackend},
    };
    use std::{
        future::{Future, pending},
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll, Waker},
        time::Duration,
    };

    enum Response {
        Shared,
        WindowPending(Arc<AtomicUsize>),
        WriteOwned(usize),
        WriteError(usize),
        WriteOpaqueError,
        WritePending,
        WritePendingAt(usize),
        PublicationFault {
            at: usize,
            after: bool,
            pending: bool,
        },
        SelectorBarrier(Arc<tokio::sync::Barrier>),
        ChangedHead(usize),
        ChangedSize(usize),
        CollisionPending(usize),
        MissingHead,
        MissingHeadAt(usize),
        HeadErrorAt(usize),
        HeadOpaqueErrorAt(usize),
        ReadError(usize),
        Unknown,
        Owned {
            capacity: usize,
            wrong_length: bool,
        },
        Pending,
    }
    #[derive(Debug)]
    struct OpaqueStorageError;
    impl std::fmt::Display for OpaqueStorageError {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            panic!("opaque storage source must never be formatted");
        }
    }
    impl std::error::Error for OpaqueStorageError {}

    struct Backend {
        inner: arco_core::MemoryBackend,
        calls: AtomicUsize,
        heads: AtomicUsize,
        puts: AtomicUsize,
        response: Response,
    }
    impl Backend {
        async fn window_boundary(&self) {
            if let Response::WindowPending(remaining) = &self.response {
                if remaining.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                    == Ok(1)
                {
                    pending::<()>().await;
                }
            }
        }
    }
    #[async_trait::async_trait]
    impl StorageBackend for Backend {
        async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
            self.inner.get(path).await
        }
        async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
            self.inner.get_range(path, range).await
        }
        async fn get_range_with_ownership(
            &self,
            path: &str,
            range: Range<u64>,
        ) -> arco_core::Result<ClassifiedBytes> {
            self.window_boundary().await;
            let ordinal = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if matches!(self.response, Response::CollisionPending(at) if at % 3 == 2 && ordinal == at / 3 + 1)
            {
                return pending().await;
            }
            match self.response {
                Response::Pending => pending().await,
                Response::Shared
                | Response::WindowPending(_)
                | Response::WriteOwned(_)
                | Response::WriteError(_)
                | Response::WriteOpaqueError
                | Response::WritePending
                | Response::WritePendingAt(_)
                | Response::PublicationFault { .. }
                | Response::SelectorBarrier(_)
                | Response::ChangedHead(_)
                | Response::ChangedSize(_)
                | Response::CollisionPending(_)
                | Response::MissingHead
                | Response::MissingHeadAt(_)
                | Response::HeadErrorAt(_)
                | Response::HeadOpaqueErrorAt(_) => {
                    self.inner.get_range_with_ownership(path, range).await
                }
                Response::ReadError(capacity) => {
                    let mut message = String::with_capacity(capacity);
                    message.push_str("unavailable record bytes");
                    Err(arco_core::Error::storage(message))
                }
                Response::Unknown => Ok(ClassifiedBytes {
                    bytes: self.inner.get_range(path, range).await?,
                    ownership: BytesBackingOwnership::Unknown,
                }),
                Response::Owned {
                    capacity,
                    wrong_length,
                } => {
                    let input = self.inner.get_range(path, range).await?;
                    let mut data = Vec::with_capacity(capacity);
                    data.extend_from_slice(&input[..input.len() - usize::from(wrong_length)]);
                    let actual_capacity = data.capacity();
                    Ok(ClassifiedBytes {
                        bytes: Bytes::from(data),
                        ownership: BytesBackingOwnership::NewRequestOwned { actual_capacity },
                    })
                }
            }
        }
        async fn put(
            &self,
            path: &str,
            data: Bytes,
            condition: arco_core::WritePrecondition,
        ) -> arco_core::Result<arco_core::WriteResult> {
            self.window_boundary().await;
            let ordinal = self.puts.fetch_add(1, Ordering::SeqCst) + 1;
            if matches!(self.response, Response::WritePendingAt(at) if at == ordinal) {
                return pending().await;
            }
            if matches!(self.response, Response::WriteOpaqueError) {
                return Err(arco_core::Error::storage_with_source(
                    "opaque",
                    OpaqueStorageError,
                ));
            }
            if let Response::WriteError(capacity) = self.response {
                let mut message = String::with_capacity(capacity);
                message.push_str("indeterminate write");
                return Err(arco_core::Error::storage(message));
            }
            if matches!(self.response, Response::WritePending) {
                return pending().await;
            }
            if let Response::SelectorBarrier(ref barrier) = self.response {
                if path.ends_with("/selector.json")
                    && matches!(condition, arco_core::WritePrecondition::MatchesVersion(_))
                {
                    barrier.wait().await;
                }
            }
            if let Response::PublicationFault {
                at,
                after: false,
                pending: wait,
            } = self.response
            {
                if at == ordinal {
                    if wait {
                        return pending().await;
                    }
                    return Err(arco_core::Error::storage("before publication write"));
                }
            }
            let mut result = self.inner.put(path, data, condition).await?;
            if let Response::PublicationFault {
                at,
                after: true,
                pending: wait,
            } = self.response
            {
                if at == ordinal {
                    if wait {
                        return pending().await;
                    }
                    return Err(arco_core::Error::storage("lost publication response"));
                }
            }
            if let Response::WriteOwned(capacity) = self.response {
                let version = match &mut result {
                    arco_core::WriteResult::Success { version } => version,
                    arco_core::WriteResult::PreconditionFailed { current_version } => {
                        current_version
                    }
                };
                let mut inflated = String::with_capacity(capacity);
                inflated.push_str(version);
                *version = inflated;
            }
            Ok(result)
        }
        async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
            self.window_boundary().await;
            let ordinal = self.heads.fetch_add(1, Ordering::SeqCst) + 1;
            if matches!(self.response, Response::HeadErrorAt(at) if ordinal == at) {
                let mut message = String::with_capacity(512 * 1024);
                message.push_str("unavailable record metadata");
                return Err(arco_core::Error::storage(message));
            }
            if matches!(self.response, Response::HeadOpaqueErrorAt(at) if ordinal == at) {
                return Err(arco_core::Error::storage_with_source(
                    "opaque",
                    OpaqueStorageError,
                ));
            }
            if matches!(self.response, Response::Pending) {
                return pending().await;
            }
            if matches!(self.response, Response::CollisionPending(at) if at % 3 == 1 && ordinal == (at / 3) * 2 + 1)
                || matches!(self.response, Response::CollisionPending(at) if at % 3 == 0 && ordinal == (at / 3) * 2)
            {
                return pending().await;
            }
            if matches!(self.response, Response::MissingHead)
                || matches!(self.response, Response::MissingHeadAt(at) if ordinal == at)
            {
                return Ok(None);
            }
            let mut meta = self.inner.head(path).await?;
            if matches!(self.response, Response::ChangedSize(at) if at == ordinal) {
                meta.as_mut().expect("seeded").size += 1;
            }
            if matches!(self.response, Response::ChangedHead(at) if at == ordinal) {
                meta.as_mut().expect("seeded").version.push_str("changed");
            }
            Ok(meta)
        }
        async fn delete(&self, path: &str) -> arco_core::Result<()> {
            self.inner.delete(path).await
        }
        async fn list(&self, path: &str) -> arco_core::Result<Vec<ObjectMeta>> {
            self.inner.list(path).await
        }
        async fn signed_url(&self, path: &str, duration: Duration) -> arco_core::Result<String> {
            self.inner.signed_url(path, duration).await
        }
    }
    #[tokio::test]
    async fn restore_control_records_cannot_borrow_the_final_stream_ledger() {
        let candidate = "ab".repeat(32);
        for write in [false, true] {
            for record in [
                RestoreControlRecord::Selector,
                RestoreControlRecord::Progress(0),
                RestoreControlRecord::Receipt(0),
            ] {
                let store = store(backend(Response::Shared), "catalog", true);
                let mut io =
                    RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
                let mut totals = FinalStreamTotals::new();
                let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
                let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
                let rejected = if write {
                    let raw = encode_with_reservation(&mut io, &mut route, 64 * 1024, || {
                        Ok(Bytes::from_static(b"{}"))
                    })
                    .expect("raw");
                    write_restore_control_record(
                        &mut io, &mut route, &candidate, record, &raw, None,
                    )
                    .await
                    .is_err()
                } else {
                    read_restore_control_record(&mut io, &mut route, &candidate, record)
                        .await
                        .is_err()
                };
                assert!(
                    rejected,
                    "generic control record cannot become terminal stream work"
                );
                assert_eq!(io.reading_evidence(), (0, 0, 0));
                assert_eq!(io.writing_evidence(), (0, 0));
            }
        }
    }

    #[tokio::test]
    async fn restore_gate_records_reject_final_stream_before_io() {
        let candidate = "ab".repeat(32);
        for record in [
            RestoreGateRecord::Head,
            RestoreGateRecord::Manifest(&candidate),
            RestoreGateRecord::Prepared(&candidate),
        ] {
            let store = store(backend(Response::Shared), "catalog", true);
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            assert!(
                read_restore_gate_record(&mut io, &mut route, record)
                    .await
                    .is_err(),
                "control cannot use stream ledger"
            );
            assert_eq!(io.reading_evidence(), (0, 0, 0));
        }
    }

    #[tokio::test]
    async fn restore_gate_records_use_scoped_stable_accounted_reads() {
        let candidate = "ab".repeat(32);
        let b = backend(Response::Shared);
        let store = store(b, "catalog", true);
        for (record, path) in [
            (RestoreGateRecord::Head, store.paths.current_pointer()),
            (
                RestoreGateRecord::Manifest(&candidate),
                store.paths.manifest_object(&candidate),
            ),
            (
                RestoreGateRecord::Prepared(&candidate),
                format!(
                    "{}/restore/v7/{candidate}/prepared.json",
                    store.paths.base_prefix()
                ),
            ),
        ] {
            store
                .retention
                .put_raw(
                    &path,
                    Bytes::from_static(b"authenticated bytes"),
                    arco_core::WritePrecondition::DoesNotExist,
                )
                .await
                .expect("fixture");
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let (raw, metadata) = read_restore_gate_record(&mut io, &mut route, record)
                .await
                .expect("gate read")
                .expect("present");
            assert_eq!(raw.as_slice(), b"authenticated bytes");
            assert!(!metadata.value().version.is_empty());
            assert_eq!(io.reading_evidence(), (1, 2, 19));
            assert_eq!(io.writing_evidence().0, 0);
            assert_eq!(io.allocation_underestimates(), 0);
            drop((raw, metadata));
            assert_eq!(io.live_ownership_evidence(), (0, 0));
        }
    }

    #[tokio::test]
    async fn restore_gate_records_reject_changed_sizes_versions_and_invalid_paths() {
        for response in [
            Response::ChangedHead(2),
            Response::ChangedSize(2),
            Response::HeadErrorAt(1),
        ] {
            let b = backend(response);
            let store = store(b.clone(), "catalog", true);
            store
                .retention
                .put_raw(
                    &store.paths.current_pointer(),
                    Bytes::from_static(b"{}"),
                    arco_core::WritePrecondition::DoesNotExist,
                )
                .await
                .expect("fixture");
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 64 * 1024 * 1024);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            assert!(
                read_restore_gate_record(&mut io, &mut route, RestoreGateRecord::Head)
                    .await
                    .is_err()
            );
            let counts = (
                b.calls.load(Ordering::SeqCst),
                b.heads.load(Ordering::SeqCst),
            );
            assert!(
                read_restore_gate_record(&mut io, &mut route, RestoreGateRecord::Head)
                    .await
                    .is_err()
            );
            assert_eq!(
                counts,
                (
                    b.calls.load(Ordering::SeqCst),
                    b.heads.load(Ordering::SeqCst)
                )
            );
            assert_eq!(io.allocation_underestimates(), 0);
        }
        for record in [
            RestoreGateRecord::Manifest("../other"),
            RestoreGateRecord::Prepared("../other"),
        ] {
            let store = store(backend(Response::Shared), "catalog", true);
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            assert!(
                read_restore_gate_record(&mut io, &mut route, record)
                    .await
                    .is_err()
            );
            assert_eq!(io.reading_evidence(), (0, 0, 0));
            assert_eq!(io.writing_evidence().0, 0);
        }
    }

    pub(super) fn selected_read_pending_store(boundary: usize) -> ControlMvpStateStore {
        assert!((1..=9).contains(&boundary));
        store(
            backend(Response::CollisionPending(boundary)),
            "catalog",
            true,
        )
    }

    pub(super) fn window_pending_store() -> (ControlMvpStateStore, Arc<AtomicUsize>) {
        let remaining = Arc::new(AtomicUsize::new(0));
        (
            store(
                backend(Response::WindowPending(remaining.clone())),
                "catalog",
                true,
            ),
            remaining,
        )
    }

    pub(super) fn unit_publication_test_store(
        at: usize,
        after: bool,
        pending: bool,
    ) -> ControlMvpStateStore {
        store(
            backend(Response::PublicationFault { at, after, pending }),
            "catalog",
            true,
        )
    }

    pub(super) fn unit_publication_barrier_store() -> ControlMvpStateStore {
        store(
            backend(Response::SelectorBarrier(Arc::new(
                tokio::sync::Barrier::new(2),
            ))),
            "catalog",
            true,
        )
    }

    fn backend(response: Response) -> Arc<Backend> {
        Arc::new(Backend {
            inner: arco_core::MemoryBackend::new(),
            calls: AtomicUsize::new(0),
            heads: AtomicUsize::new(0),
            puts: AtomicUsize::new(0),
            response,
        })
    }
    fn store(
        backend: Arc<dyn StorageBackend>,
        domain: &str,
        bounded: bool,
    ) -> ControlMvpStateStore {
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("scope");
        let scope = StateScope::new("tenant", "workspace", domain);
        if bounded {
            ControlMvpStateStore::new_synthetic_bounded(storage, scope)
        } else {
            ControlMvpStateStore::new(storage, scope)
        }
        .expect("store")
    }
    fn segment() -> ControlMvpSegmentRef {
        ControlMvpSegmentRef {
            segment_size_bytes: 7,
            index_size_bytes: 2,
            segment_id: "11".repeat(32),
            level: ControlMvpSegmentLevel::L1,
            logical_sequence: 1,
            checksum_sha256: "22".repeat(32),
            index_checksum_sha256: "33".repeat(32),
        }
    }
    fn block() -> ControlMvpBlock {
        ControlMvpBlock {
            offset: 0,
            length: 7,
            row_count: 1,
            record_kind: Some(1),
            min_key_hex: Some("61".into()),
            max_key_hex: Some("61".into()),
            min_ordinal: Some(0),
            max_ordinal: Some(0),
            checksum_sha256: "44".repeat(32),
        }
    }
    fn payload(store: &ControlMvpStateStore, final_stream: bool) -> DeclaredPhysicalRange {
        DeclaredPhysicalRange::new(
            store,
            PhysicalObject::Payload {
                segment: &segment(),
                block: &block(),
            },
            final_stream,
        )
        .expect("declaration")
    }
    async fn seed(store: &ControlMvpStateStore, path: &str, bytes: &'static [u8]) {
        store
            .storage
            .put(
                path,
                Bytes::from_static(bytes),
                AuthorityWritePrecondition::DoesNotExist,
            )
            .await
            .expect("seed");
    }
    fn error(result: CatalogResult<AccountedBytes>) -> String {
        match result {
            Ok(_) => panic!("expected reader rejection"),
            Err(e) => e.to_string(),
        }
    }

    #[tokio::test]
    async fn restore_control_records_pin_all_typed_addresses_on_both_routes() {
        {
            let final_stream = false; // Control transport is ordinary-only; rejection has separate coverage.
            for (record, suffix) in [
                (RestoreControlRecord::Selector, "selector.json"),
                (
                    RestoreControlRecord::Progress(7),
                    "progress/00000000000000000007.json",
                ),
                (
                    RestoreControlRecord::Receipt(6),
                    "receipts/00000000000000000006.json",
                ),
            ] {
                let backend = backend(Response::Shared);
                let store = store(backend.clone(), "catalog", true);
                let candidate = "a".repeat(64);
                let prefix = format!("{}/restore/v7/{candidate}", store.paths.base_prefix());
                let path = record.path(&prefix);
                assert_eq!(path, format!("{prefix}/{suffix}"));
                seed(&store, &path, b"raw record pending JCS authentication").await;
                let mut io =
                    RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut totals = FinalStreamTotals::new();
                let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
                let mut route = if final_stream {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload,
                    }
                };
                let (bytes, meta) =
                    read_restore_control_record(&mut io, &mut route, &candidate, record)
                        .await
                        .expect("native record read")
                        .expect("present");
                assert_eq!(bytes.as_slice(), b"raw record pending JCS authentication");
                assert!(!meta.value().version.is_empty());
                assert_eq!(io.work.metadata_heads, 2);
                assert_eq!(io.work.range_reads, 1);
                assert_eq!(backend.heads.load(Ordering::SeqCst), 2);
                assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
                drop(bytes);
                drop(meta);
                assert!(io.ledger.lock().expect("ledger").report().passing());
            }
        }
    }

    #[tokio::test]
    async fn restore_control_writer_uses_immutable_replay_and_explicit_selector_conflicts() {
        {
            let final_stream = false; // Control transport is ordinary-only; rejection has separate coverage.
            for record in [
                RestoreControlRecord::Selector,
                RestoreControlRecord::Progress(4),
                RestoreControlRecord::Receipt(3),
            ] {
                let backend = backend(Response::Shared);
                let store = store(backend.clone(), "catalog", true);
                let mut io =
                    RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut totals = FinalStreamTotals::new();
                let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
                let mut route = if final_stream {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload,
                    }
                };
                let bytes =
                    allocate_with_reservation(&mut io, &mut route, Some(1024), true, || {
                        Ok(Bytes::from_static(b"{}"))
                    })
                    .expect("bytes");
                let candidate = "a".repeat(64);
                let first = write_restore_control_record(
                    &mut io, &mut route, &candidate, record, &bytes, None,
                )
                .await
                .expect("create");
                assert!(matches!(
                    first.value(),
                    arco_core::WriteResult::Success { .. }
                ));
                let second = write_restore_control_record(
                    &mut io, &mut route, &candidate, record, &bytes, None,
                )
                .await
                .expect("collision");
                assert!(matches!(
                    second.value(),
                    arco_core::WriteResult::PreconditionFailed { .. }
                ));
                assert_eq!(
                    restore_output_version(first.value()),
                    restore_output_version(second.value())
                );
                let reads = usize::from(!matches!(record, RestoreControlRecord::Selector));
                assert_eq!(backend.puts.load(Ordering::SeqCst), 2);
                assert_eq!(backend.heads.load(Ordering::SeqCst), 2 * reads);
                assert_eq!(backend.calls.load(Ordering::SeqCst), reads);
                let (raw, _) = read_restore_control_record(&mut io, &mut route, &candidate, record)
                    .await
                    .expect("read")
                    .expect("present");
                assert_eq!(raw.as_slice(), b"{}");
                drop((raw, first, second, bytes));
                assert!(io.ledger.lock().expect("ledger").report().passing());
            }
        }
    }

    #[tokio::test]
    async fn restore_control_writer_selector_cas_keeps_the_exact_winner() {
        {
            let final_stream = false; // Control transport is ordinary-only; rejection has separate coverage.
            let backend = backend(Response::Shared);
            let store = store(backend.clone(), "catalog", true);
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut route = if final_stream {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            let bytes = allocate_with_reservation(&mut io, &mut route, Some(1024), true, || {
                Ok(Bytes::from_static(b"{}"))
            })
            .expect("bytes");
            let changed = allocate_with_reservation(&mut io, &mut route, Some(1024), true, || {
                Ok(Bytes::from_static(b"{\"next\":1}"))
            })
            .expect("changed");
            let candidate = "a".repeat(64);
            let first = write_restore_control_record(
                &mut io,
                &mut route,
                &candidate,
                RestoreControlRecord::Selector,
                &bytes,
                None,
            )
            .await
            .expect("create");
            let second = write_restore_control_record(
                &mut io,
                &mut route,
                &candidate,
                RestoreControlRecord::Selector,
                &changed,
                Some(restore_output_version(first.value())),
            )
            .await
            .expect("advance");
            assert!(matches!(
                second.value(),
                arco_core::WriteResult::Success { .. }
            ));
            let stale = write_restore_control_record(
                &mut io,
                &mut route,
                &candidate,
                RestoreControlRecord::Selector,
                &bytes,
                Some(restore_output_version(first.value())),
            )
            .await
            .expect("stale outcome");
            assert!(matches!(
                stale.value(),
                arco_core::WriteResult::PreconditionFailed { .. }
            ));
            assert_eq!(
                restore_output_version(second.value()),
                restore_output_version(stale.value())
            );
            assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
            assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
            let (raw, _) = read_restore_control_record(
                &mut io,
                &mut route,
                &candidate,
                RestoreControlRecord::Selector,
            )
            .await
            .expect("winner read")
            .expect("present");
            assert_eq!(raw.as_slice(), changed.value().as_ref());
            drop((raw, first, second, stale, changed, bytes));
            assert!(io.ledger.lock().expect("ledger").report().passing());
        }
    }

    #[tokio::test]
    async fn restore_control_writer_failures_retain_diagnostics_and_stop_before_recovery() {
        {
            let final_stream = false; // Control transport is ordinary-only; rejection has separate coverage.
            for response in [
                Response::WriteError(512 * 1024),
                Response::WriteOpaqueError,
                Response::WriteOwned(1024 * 1024),
            ] {
                let backend = backend(response);
                let store = store(backend.clone(), "catalog", true);
                let mut io = RestorePhysicalIo::new(&store, 1024 * 1024, FINAL_MICROCHUNK_BYTES);
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut totals = FinalStreamTotals::new();
                let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
                let mut route = if final_stream {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload,
                    }
                };
                let bytes =
                    allocate_with_reservation(&mut io, &mut route, Some(1024), true, || {
                        Ok(Bytes::from_static(b"{}"))
                    })
                    .expect("bytes");
                let candidate = "a".repeat(64);
                for _ in 0..2 {
                    assert!(
                        write_restore_control_record(
                            &mut io,
                            &mut route,
                            &candidate,
                            RestoreControlRecord::Selector,
                            &bytes,
                            None
                        )
                        .await
                        .is_err()
                    );
                    assert!(io.stopped);
                }
                assert_eq!(backend.puts.load(Ordering::SeqCst), 1);
                assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
                assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
                assert_eq!(io.work.write_attempts, 1);
                if matches!(backend.response, Response::WriteError(_)) {
                    assert!(
                        io.ledger
                            .lock()
                            .expect("ledger")
                            .report()
                            .request_owned_live_bytes
                            >= 512 * 1024
                    );
                }
                let ledger = io.ledger.clone();
                drop((bytes, io));
                assert_eq!(
                    ledger
                        .lock()
                        .expect("ledger")
                        .report()
                        .owned_live_upper_bound(),
                    0
                );
            }
        }
    }

    #[tokio::test]
    async fn restore_control_writer_cancellation_keeps_version_and_bytes_owners_until_drop() {
        {
            let final_stream = false; // Control transport is ordinary-only; rejection has separate coverage.
            let backend = backend(Response::WritePendingAt(2));
            let store = store(backend.clone(), "catalog", true);
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut route = if final_stream {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            let bytes = allocate_with_reservation(&mut io, &mut route, Some(1024), true, || {
                Ok(Bytes::from_static(b"{}"))
            })
            .expect("bytes");
            let candidate = "a".repeat(64);
            let first = write_restore_control_record(
                &mut io,
                &mut route,
                &candidate,
                RestoreControlRecord::Selector,
                &bytes,
                None,
            )
            .await
            .expect("create");
            let expected =
                allocate_with_reservation(&mut io, &mut route, Some(600 * 1024), true, || {
                    Ok("v".repeat(512 * 1024))
                })
                .expect("owned expected token");
            let ledger = io.ledger.clone();
            let live = ledger
                .lock()
                .expect("ledger")
                .report()
                .owned_live_upper_bound();
            {
                let mut future = std::pin::pin!(write_restore_control_record(
                    &mut io,
                    &mut route,
                    &candidate,
                    RestoreControlRecord::Selector,
                    &bytes,
                    Some(expected.value())
                ));
                let mut context = Context::from_waker(Waker::noop());
                assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
                assert!(
                    ledger
                        .lock()
                        .expect("ledger")
                        .report()
                        .owned_live_upper_bound()
                        > live
                );
            }
            assert!(io.stopped);
            assert_eq!(
                ledger
                    .lock()
                    .expect("ledger")
                    .report()
                    .owned_live_upper_bound(),
                live
            );
            assert!(
                write_restore_control_record(
                    &mut io,
                    &mut route,
                    &candidate,
                    RestoreControlRecord::Selector,
                    &bytes,
                    None
                )
                .await
                .is_err()
            );
            assert_eq!(backend.puts.load(Ordering::SeqCst), 2);
            assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
            assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
            drop((first, bytes, expected));
            assert!(
                ledger.lock().expect("ledger").report().working_live_bytes > 0,
                "stopped-call diagnostic retained"
            );
            drop(io);
            assert!(ledger.lock().expect("ledger").report().passing());
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // Admission matrix uses the same exact both-route zero-I/O assertions.
    async fn restore_control_writer_invalid_admission_never_reaches_storage() {
        for final_stream in [false, true] {
            for case in 0..9 {
                let backend = backend(Response::Shared);
                let store = store(backend.clone(), "catalog", true);
                let limit = if case == 8 {
                    1024 * 1024
                } else {
                    FINAL_MICROCHUNK_BYTES
                };
                let mut io = RestorePhysicalIo::new(&store, limit, FINAL_MICROCHUNK_BYTES);
                let mut foreign_io =
                    RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut totals = FinalStreamTotals::new();
                let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
                let mut route = if final_stream {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload,
                    }
                };
                let length = match case {
                    1 => 0,
                    2 => FINAL_RECEIPT_BYTES + 1,
                    _ => 2,
                };
                let bytes = if case == 6 {
                    let mut foreign_workspace = WorkspaceIoBudget::new();
                    let mut foreign_payload = UnitPayloadAdmission::new();
                    let mut foreign_route = RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut foreign_workspace,
                        payload: &mut foreign_payload,
                    };
                    allocate_with_reservation(
                        &mut foreign_io,
                        &mut foreign_route,
                        Some(1024),
                        true,
                        || Ok(Bytes::from_static(b"{}")),
                    )
                    .expect("foreign bytes")
                } else {
                    allocate_with_reservation(
                        &mut io,
                        &mut route,
                        Some(length + 1024),
                        true,
                        || {
                            let bytes = Bytes::from(vec![b'x'; length]);
                            drop(bytes.clone());
                            Ok(bytes)
                        },
                    )
                    .expect("bytes")
                };
                if case == 7 {
                    match &mut route {
                        RestorePhysicalRoute::OrdinaryUnit { workspace, .. } => {
                            workspace.charge_operations(4096).expect("exhaust");
                        }
                        RestorePhysicalRoute::FinalMicrochunk(chunk) => {
                            for _ in 0..4096 {
                                chunk.reserve_io(0).expect("exhaust");
                            }
                        }
                    }
                }
                let candidate = if case == 0 {
                    "A".repeat(64)
                } else {
                    "a".repeat(64)
                };
                let large_version = "v".repeat(2 * 1024 * 1024);
                let expected = match case {
                    3 => Some(""),
                    4 | 5 => Some("v"),
                    8 => Some(large_version.as_str()),
                    _ => None,
                };
                let record = match case {
                    4 => RestoreControlRecord::Progress(1),
                    5 => RestoreControlRecord::Receipt(0),
                    _ => RestoreControlRecord::Selector,
                };
                assert!(
                    write_restore_control_record(
                        &mut io, &mut route, &candidate, record, &bytes, expected
                    )
                    .await
                    .is_err(),
                    "case {case}"
                );
                assert!(io.stopped);
                assert!(
                    write_restore_control_record(
                        &mut io, &mut route, &candidate, record, &bytes, None
                    )
                    .await
                    .is_err()
                );
                assert_eq!(backend.puts.load(Ordering::SeqCst), 0);
                assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
                assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
            }
        }
    }

    #[tokio::test]
    async fn restore_control_writer_immutable_mismatch_is_terminal() {
        {
            let final_stream = false; // Control transport is ordinary-only; rejection has separate coverage.
            for record in [
                RestoreControlRecord::Progress(1),
                RestoreControlRecord::Receipt(0),
            ] {
                let backend = backend(Response::Shared);
                let store = store(backend.clone(), "catalog", true);
                let mut io =
                    RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut totals = FinalStreamTotals::new();
                let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
                let mut route = if final_stream {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload,
                    }
                };
                let bytes =
                    allocate_with_reservation(&mut io, &mut route, Some(1024), true, || {
                        Ok(Bytes::from_static(b"{}"))
                    })
                    .expect("bytes");
                let different =
                    allocate_with_reservation(&mut io, &mut route, Some(1024), true, || {
                        Ok(Bytes::from_static(b"[]"))
                    })
                    .expect("different bytes");
                let candidate = "a".repeat(64);
                drop(
                    write_restore_control_record(
                        &mut io, &mut route, &candidate, record, &bytes, None,
                    )
                    .await
                    .expect("create"),
                );
                assert!(
                    write_restore_control_record(
                        &mut io, &mut route, &candidate, record, &different, None
                    )
                    .await
                    .is_err()
                );
                assert!(io.stopped);
                assert!(
                    write_restore_control_record(
                        &mut io, &mut route, &candidate, record, &bytes, None
                    )
                    .await
                    .is_err()
                );
                assert_eq!(backend.puts.load(Ordering::SeqCst), 2);
                assert_eq!(backend.heads.load(Ordering::SeqCst), 2);
                assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
            }
        }
    }

    #[tokio::test]
    async fn restore_control_record_initial_absence_is_explicit_and_not_an_error() {
        let backend = backend(Response::Shared);
        let store = store(backend.clone(), "catalog", true);
        let mut io = RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        assert!(
            read_restore_control_record(
                &mut io,
                &mut route,
                &"a".repeat(64),
                RestoreControlRecord::Selector
            )
            .await
            .expect("observed absence")
            .is_none()
        );
        assert!(!io.stopped);
        assert_eq!(backend.heads.load(Ordering::SeqCst), 1);
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // Keep both routes and their failure ownership assertions together.
    async fn restore_control_record_faults_stop_before_later_io() {
        {
            let final_stream = false; // Control transport is ordinary-only; rejection has separate coverage.
            for response in [
                Response::ChangedHead(1),
                Response::ChangedHead(2),
                Response::ChangedSize(1),
                Response::ChangedSize(2),
                Response::MissingHeadAt(2),
                Response::Unknown,
                Response::Owned {
                    capacity: 1024 * 1024 + 1,
                    wrong_length: false,
                },
                Response::Owned {
                    capacity: 128,
                    wrong_length: true,
                },
                Response::ReadError(512 * 1024),
                Response::HeadErrorAt(1),
                Response::HeadErrorAt(2),
                Response::HeadOpaqueErrorAt(1),
                Response::HeadOpaqueErrorAt(2),
            ] {
                let backend = backend(response);
                let store = store(backend.clone(), "catalog", true);
                let candidate = "a".repeat(64);
                let path = RestoreControlRecord::Selector.path(&format!(
                    "{}/restore/v7/{candidate}",
                    store.paths.base_prefix()
                ));
                seed(&store, &path, b"raw record").await;
                let mut io = RestorePhysicalIo::new(&store, 1024 * 1024, FINAL_MICROCHUNK_BYTES);
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut totals = FinalStreamTotals::new();
                let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
                let mut route = if final_stream {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload,
                    }
                };
                assert!(
                    read_restore_control_record(
                        &mut io,
                        &mut route,
                        &candidate,
                        RestoreControlRecord::Selector
                    )
                    .await
                    .is_err()
                );
                assert!(io.stopped);
                let counts = (
                    backend.heads.load(Ordering::SeqCst),
                    backend.calls.load(Ordering::SeqCst),
                );
                assert!(
                    read_restore_control_record(
                        &mut io,
                        &mut route,
                        &candidate,
                        RestoreControlRecord::Selector
                    )
                    .await
                    .is_err()
                );
                assert_eq!(
                    counts,
                    (
                        backend.heads.load(Ordering::SeqCst),
                        backend.calls.load(Ordering::SeqCst)
                    )
                );
                assert_eq!(
                    io.work.metadata_heads,
                    u64::try_from(counts.0).expect("heads")
                );
                assert_eq!(
                    io.work.range_reads,
                    u64::try_from(counts.1).expect("ranges")
                );
                assert_eq!(io.work.write_attempts, 0);
                if matches!(
                    backend.response,
                    Response::ReadError(_) | Response::HeadErrorAt(_)
                ) {
                    assert!(
                        io.ledger
                            .lock()
                            .expect("ledger")
                            .report()
                            .request_owned_live_bytes
                            >= 512 * 1024
                    );
                }
                let ledger = io.ledger.clone();
                drop(io);
                assert_eq!(
                    ledger
                        .lock()
                        .expect("ledger")
                        .report()
                        .owned_live_upper_bound(),
                    0
                );
            }
        }
    }

    #[tokio::test]
    async fn restore_control_record_cancellation_releases_owners_at_each_boundary() {
        {
            let final_stream = false; // Control transport is ordinary-only; rejection has separate coverage.
            for boundary in 1..=3 {
                let backend = backend(Response::CollisionPending(boundary));
                let store = store(backend.clone(), "catalog", true);
                let candidate = "a".repeat(64);
                let path = RestoreControlRecord::Selector.path(&format!(
                    "{}/restore/v7/{candidate}",
                    store.paths.base_prefix()
                ));
                seed(&store, &path, b"raw record").await;
                let mut io =
                    RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut totals = FinalStreamTotals::new();
                let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
                let mut route = if final_stream {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload,
                    }
                };
                {
                    let mut future = Box::pin(read_restore_control_record(
                        &mut io,
                        &mut route,
                        &candidate,
                        RestoreControlRecord::Selector,
                    ));
                    assert!(
                        future
                            .as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()))
                            .is_pending()
                    );
                }
                assert!(io.stopped);
                assert!(io.ledger.lock().expect("ledger").report().passing());
                let expected = match boundary {
                    1 => (1, 0),
                    2 => (1, 1),
                    _ => (2, 1),
                };
                assert_eq!(
                    expected,
                    (
                        backend.heads.load(Ordering::SeqCst),
                        backend.calls.load(Ordering::SeqCst)
                    )
                );
                assert!(
                    read_restore_control_record(
                        &mut io,
                        &mut route,
                        &candidate,
                        RestoreControlRecord::Selector
                    )
                    .await
                    .is_err()
                );
                assert_eq!(
                    expected,
                    (
                        backend.heads.load(Ordering::SeqCst),
                        backend.calls.load(Ordering::SeqCst)
                    )
                );
            }
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // Exercise the wire cap, identity, and pre-I/O admission in one fixture.
    async fn restore_control_record_caps_identity_and_metadata_admission() {
        for length in [0, FINAL_RECEIPT_BYTES, FINAL_RECEIPT_BYTES + 1] {
            let backend = backend(Response::Shared);
            let store = store(backend.clone(), "catalog", true);
            let candidate = "a".repeat(64);
            let path = RestoreControlRecord::Progress(1).path(&format!(
                "{}/restore/v7/{candidate}",
                store.paths.base_prefix()
            ));
            store
                .storage
                .put(
                    &path,
                    Bytes::from(vec![b'x'; length]),
                    AuthorityWritePrecondition::DoesNotExist,
                )
                .await
                .expect("seed");
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let result = read_restore_control_record(
                &mut io,
                &mut route,
                &candidate,
                RestoreControlRecord::Progress(1),
            )
            .await;
            assert_eq!(result.is_ok(), length == FINAL_RECEIPT_BYTES);
            assert_eq!(
                backend.calls.load(Ordering::SeqCst),
                usize::from(length == FINAL_RECEIPT_BYTES)
            );
            if length == FINAL_RECEIPT_BYTES {
                assert_eq!(
                    workspace.test_accounting().0,
                    FINAL_RECEIPT_PROBE_BYTES + workspace.test_accounting().2
                );
            }
        }
        for invalid in [
            "../candidate",
            "A000000000000000000000000000000000000000000000000000000000000000",
            "",
        ] {
            let backend = backend(Response::Shared);
            let store = store(backend.clone(), "catalog", true);
            let mut io = RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, 0);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            assert!(
                read_restore_control_record(
                    &mut io,
                    &mut route,
                    invalid,
                    RestoreControlRecord::Selector
                )
                .await
                .is_err()
            );
            assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
        }
        let backend = backend(Response::Shared);
        let store = store(backend.clone(), "catalog", true);
        let candidate = "a".repeat(64);
        seed(
            &store,
            &RestoreControlRecord::Selector.path(&format!(
                "{}/restore/v7/{candidate}",
                store.paths.base_prefix()
            )),
            b"record",
        )
        .await;
        let mut io = RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
        let mut workspace = WorkspaceIoBudget::new();
        workspace
            .reserve_bytes(crate::workspace_io_budget::METADATA_BYTES)
            .expect("exhaust invoice");
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        assert!(
            read_restore_control_record(
                &mut io,
                &mut route,
                &candidate,
                RestoreControlRecord::Selector
            )
            .await
            .is_err()
        );
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
        assert!(io.stopped);
    }

    #[tokio::test]
    async fn restore_output_collision_cancellation_stops_at_each_read_boundary() {
        for final_stream in [false, true] {
            for boundary in 1..=3 {
                let backend = backend(Response::CollisionPending(boundary));
                let store = store(backend.clone(), "catalog", true);
                let mut io =
                    RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
                let mut totals = FinalStreamTotals::new();
                let mut chunk = FinalMicrochunk::begin(&mut totals, 4096, &mut io).expect("chunk");
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut route = if final_stream {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload,
                    }
                };
                let declared = declare_restore_output(
                    &mut io,
                    &mut route,
                    RestoreOutputObject::Index(&"81".repeat(32)),
                    2,
                )
                .expect("path");
                seed(&store, &declared.value().path, b"{}").await;
                backend.puts.store(0, Ordering::SeqCst);
                let bytes =
                    allocate_with_reservation(&mut io, &mut route, Some(1024), true, || {
                        Ok(Bytes::from_static(b"{}"))
                    })
                    .expect("bytes");
                let before = io
                    .ledger
                    .lock()
                    .expect("ledger")
                    .report()
                    .working_live_bytes;
                let mut future = Box::pin(put_restore_output(
                    &mut io,
                    &mut route,
                    declared.value(),
                    &bytes,
                ));
                assert!(
                    future
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                drop(future);
                assert!(io.stopped);
                assert!(
                    put_restore_output(&mut io, &mut route, declared.value(), &bytes)
                        .await
                        .is_err()
                );
                assert_eq!(backend.puts.load(Ordering::SeqCst), 1);
                assert_eq!(
                    backend.heads.load(Ordering::SeqCst),
                    if boundary == 3 { 2 } else { 1 }
                );
                assert_eq!(
                    backend.calls.load(Ordering::SeqCst),
                    usize::from(boundary != 1)
                );
                let report = io.ledger.lock().expect("ledger").report();
                assert_eq!(report.working_live_bytes, before);
                assert_eq!(
                    report.request_owned_live_bytes + report.backend_origin_shared_live_bytes,
                    0
                );
                drop((bytes, declared));
                assert!(io.ledger.lock().expect("ledger").report().passing());
            }
        }
    }

    #[tokio::test]
    async fn restore_output_write_error_retains_arrived_string_capacity_through_cleanup() {
        for final_stream in [false, true] {
            let backend = backend(Response::WriteError(512 * 1024));
            let store = store(backend.clone(), "catalog", true);
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let ledger = io.ledger.clone();
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 4096, &mut io).expect("chunk");
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = if final_stream {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            let declared = declare_restore_output(
                &mut io,
                &mut route,
                RestoreOutputObject::Index(&"80".repeat(32)),
                2,
            )
            .expect("path");
            let bytes = allocate_with_reservation(&mut io, &mut route, Some(1024), true, || {
                Ok(Bytes::from_static(b"{}"))
            })
            .expect("bytes");
            let Err(error) =
                put_restore_output(&mut io, &mut route, declared.value(), &bytes).await
            else {
                panic!("expected storage failure");
            };
            let CatalogError::Storage { message } = &error else {
                panic!("wrong error");
            };
            assert_eq!(message, "indeterminate write");
            assert!(message.capacity() >= 512 * 1024);
            assert!(io.stopped);
            let live = ledger
                .lock()
                .expect("ledger")
                .report()
                .request_owned_live_bytes;
            assert!(live >= message.capacity());
            tokio::task::yield_now().await;
            assert_eq!(
                ledger
                    .lock()
                    .expect("ledger")
                    .report()
                    .request_owned_live_bytes,
                live
            );
            assert_eq!(backend.puts.load(Ordering::SeqCst), 1);
            drop((error, bytes, declared));
            drop(io);
            assert!(ledger.lock().expect("ledger").report().passing());
        }
    }

    #[tokio::test]
    async fn standard_output_cannot_write_through_final_stream() {
        let store = store(backend(Response::Shared), "catalog", true);
        let mut io = RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
        let mut totals = FinalStreamTotals::new();
        let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
        let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
        let rows = decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
            Ok(owned_encode_tests::rows(3, 8, 128, Some(1)))
        })
        .expect("rows");
        assert!(
            write_standard_restore_output(&mut io, &mut route, &"77".repeat(32), 1, &rows)
                .await
                .is_err()
        );
        assert_eq!(io.writing_evidence(), (0, 0));
        assert_eq!(io.reading_evidence(), (0, 0, 0));
        assert_eq!(io.encoding_evidence(), (0, 0));
    }

    #[tokio::test]
    async fn restore_output_pipeline_publishes_readable_products_with_three_writes_and_exact_replay()
     {
        {
            let final_stream = false; // Control transport is ordinary-only; rejection has separate coverage.
            let backend = backend(Response::Shared);
            let store = store(backend.clone(), "catalog", true);
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 4096, &mut io).expect("chunk");
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = if final_stream {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            let rows = decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
                Ok(owned_encode_tests::rows(3, 8, 128, Some(1)))
            })
            .expect("owned rows");
            let id = "77".repeat(32);
            let first = write_standard_restore_output(&mut io, &mut route, &id, 1, &rows)
                .await
                .expect("write output");
            assert_eq!(
                (
                    io.work.write_attempts,
                    io.work.metadata_heads,
                    io.work.range_reads
                ),
                (3, 0, 0)
            );
            assert_eq!(backend.puts.load(Ordering::SeqCst), 3);
            assert_eq!(io.work.native.slots[15], 3);
            assert_eq!(io.work.native.slots[16], 24);
            assert!(io.work.native.slots[0] > 0);
            assert!(io.work.native.slots[1] > 0);
            assert_eq!(io.work.native.bounded.streaming_builder_inputs, 0);
            assert!(!io.work.native.overflow);
            println!(
                "native-parity output final={final_stream} {:?}",
                io.work.native
            );
            let second = write_standard_restore_output(&mut io, &mut route, &id, 1, &rows)
                .await
                .expect("replay output");
            assert_eq!(
                (
                    io.work.write_attempts,
                    io.work.metadata_heads,
                    io.work.range_reads
                ),
                (6, 6, 3)
            );
            assert_eq!(
                (
                    backend.puts.load(Ordering::SeqCst),
                    backend.heads.load(Ordering::SeqCst),
                    backend.calls.load(Ordering::SeqCst)
                ),
                (6, 6, 3)
            );
            assert_eq!(first.digest, second.digest);
            assert_eq!(first.bytes.value(), second.bytes.value());
            let descriptor = first.descriptor.value();
            let leaf = super::super::directory::Leaf {
                first: rows.value()[0].key.clone(),
                last: rows.value()[2].key.clone(),
                rows: 3,
                bytes: u32::try_from(descriptor.block.length).expect("block length"),
                digest: first.digest,
            };
            assert_eq!(
                store
                    .resolve_physical_block(super::super::Role::Kv, &leaf)
                    .await
                    .expect("independent stored physical verification"),
                *rows.value()
            );
            assert_eq!(
                io.work.submitted_write_bytes,
                2 * (descriptor.segment.segment_size_bytes
                    + descriptor.segment.index_size_bytes
                    + first.bytes.value().len() as u64)
            );
            drop((first, second, rows));
            assert!(io.ledger.lock().expect("ledger").report().passing());
        }
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "each output PUT cancellation is followed by exact restart reconciliation on both routes"
    )]
    async fn restore_output_pipeline_cancellation_at_each_put_restarts_with_exact_products() {
        {
            let final_stream = false; // Control transport is ordinary-only; rejection has separate coverage.
            for cancelled_put in 1..=3 {
                let backend = backend(Response::WritePendingAt(cancelled_put));
                let store = store(backend.clone(), "catalog", true);
                let id = "78".repeat(32);
                for restart in [false, true] {
                    let mut io = RestorePhysicalIo::new(
                        &store,
                        FINAL_MICROCHUNK_BYTES,
                        FINAL_MICROCHUNK_BYTES,
                    );
                    let ledger = io.ledger.clone();
                    let mut totals = FinalStreamTotals::new();
                    let mut chunk =
                        FinalMicrochunk::begin(&mut totals, 4096, &mut io).expect("chunk");
                    let mut workspace = WorkspaceIoBudget::new();
                    let mut payload = UnitPayloadAdmission::new();
                    let mut route = if final_stream {
                        RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                    } else {
                        RestorePhysicalRoute::OrdinaryUnit {
                            workspace: &mut workspace,
                            payload: &mut payload,
                        }
                    };
                    let rows =
                        decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
                            Ok(owned_encode_tests::rows(3, 8, 128, Some(1)))
                        })
                        .expect("owned rows");
                    if restart {
                        let output =
                            write_standard_restore_output(&mut io, &mut route, &id, 1, &rows)
                                .await
                                .expect("restart");
                        assert_eq!(
                            (
                                io.work.write_attempts,
                                io.work.metadata_heads,
                                io.work.range_reads
                            ),
                            (3, 2 * (cancelled_put as u64 - 1), cancelled_put as u64 - 1)
                        );
                        let leaf = super::super::directory::Leaf {
                            first: rows.value()[0].key.clone(),
                            last: rows.value()[2].key.clone(),
                            rows: 3,
                            bytes: u32::try_from(output.descriptor.value().block.length)
                                .expect("block length"),
                            digest: output.digest,
                        };
                        assert_eq!(
                            store
                                .resolve_physical_block(super::super::Role::Kv, &leaf)
                                .await
                                .expect("verify"),
                            *rows.value()
                        );
                    } else {
                        let mut future = Box::pin(write_standard_restore_output(
                            &mut io, &mut route, &id, 1, &rows,
                        ));
                        assert!(
                            future
                                .as_mut()
                                .poll(&mut Context::from_waker(Waker::noop()))
                                .is_pending()
                        );
                        drop(future);
                        assert!(io.stopped);
                        assert_eq!(io.work.write_attempts, cancelled_put as u64);
                        assert_eq!(
                            io.ledger
                                .lock()
                                .expect("ledger")
                                .report()
                                .working_live_bytes,
                            rows.working.bytes
                        );
                    }
                    drop(rows);
                    drop(io);
                    assert!(ledger.lock().expect("ledger").report().passing());
                }
            }
        }
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one fault matrix tests both routes and exact stopping boundary"
    )]
    async fn restore_output_write_rejects_corruption_and_arrived_capacity_before_further_io() {
        for final_stream in [false, true] {
            for (response, collision, different, heads, ranges) in [
                (Response::WriteError(2 * 1024 * 1024), false, false, 0, 0),
                (Response::WriteOpaqueError, false, false, 0, 0),
                (Response::WriteOwned(2 * 1024 * 1024), false, false, 0, 0),
                (Response::WriteOwned(2 * 1024 * 1024), true, false, 0, 0),
                (Response::Unknown, true, false, 1, 1),
                (
                    Response::Owned {
                        capacity: 2 * 1024 * 1024,
                        wrong_length: false,
                    },
                    true,
                    false,
                    1,
                    1,
                ),
                (Response::ChangedHead(1), true, false, 1, 0),
                (Response::ChangedSize(1), true, false, 1, 0),
                (Response::ChangedSize(2), true, false, 2, 1),
                (Response::ChangedHead(2), true, false, 2, 1),
                (Response::MissingHead, true, false, 1, 0),
                (Response::Shared, true, true, 2, 1),
            ] {
                let backend = backend(response);
                let store = store(backend.clone(), "catalog", true);
                let mut io = RestorePhysicalIo::new(&store, 1024 * 1024, FINAL_MICROCHUNK_BYTES);
                let declared = DeclaredPhysicalRange::new(
                    &store,
                    PhysicalObject::Directory {
                        key: false,
                        digest: &[9; 32],
                        length: 2,
                    },
                    final_stream,
                )
                .expect("declaration");
                if collision {
                    seed(
                        &store,
                        &declared.path,
                        if different { b"[]" } else { b"{}" },
                    )
                    .await;
                }
                backend.puts.store(0, Ordering::SeqCst);
                let mut totals = FinalStreamTotals::new();
                let mut chunk = FinalMicrochunk::begin(&mut totals, 4096, &mut io).expect("chunk");
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut route = if final_stream {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload,
                    }
                };
                let bytes =
                    allocate_with_reservation(&mut io, &mut route, Some(1024), true, || {
                        Ok(Bytes::from_static(b"{}"))
                    })
                    .expect("bytes");
                assert!(
                    put_restore_output(&mut io, &mut route, &declared, &bytes)
                        .await
                        .is_err()
                );
                assert!(io.stopped);
                assert!(
                    put_restore_output(&mut io, &mut route, &declared, &bytes)
                        .await
                        .is_err()
                );
                assert_eq!(
                    (
                        backend.puts.load(Ordering::SeqCst),
                        backend.heads.load(Ordering::SeqCst),
                        backend.calls.load(Ordering::SeqCst)
                    ),
                    (1, heads, ranges)
                );
                let report = io.ledger.lock().expect("ledger").report();
                if matches!(
                    backend.response,
                    Response::WriteOwned(_) | Response::WriteError(_) | Response::Owned { .. }
                ) {
                    assert!(report.largest_rejected_request_owned_capacity >= 2 * 1024 * 1024);
                }
                if matches!(
                    backend.response,
                    Response::Unknown | Response::WriteOpaqueError
                ) {
                    assert_eq!(report.unknown_responses, 1);
                }
            }
        }
    }

    #[tokio::test]
    async fn restore_output_write_cancellation_stops_both_routes_and_retains_input_owner() {
        for final_stream in [false, true] {
            let backend = backend(Response::WritePending);
            let store = store(backend.clone(), "catalog", true);
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let declared = DeclaredPhysicalRange::new(
                &store,
                PhysicalObject::Directory {
                    key: false,
                    digest: &[10; 32],
                    length: 2,
                },
                final_stream,
            )
            .expect("declaration");
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 4096, &mut io).expect("chunk");
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = if final_stream {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            let bytes = allocate_with_reservation(&mut io, &mut route, Some(1024), true, || {
                Ok(Bytes::from_static(b"{}"))
            })
            .expect("bytes");
            let before = io
                .ledger
                .lock()
                .expect("ledger")
                .report()
                .working_live_bytes;
            let mut future = Box::pin(put_restore_output(&mut io, &mut route, &declared, &bytes));
            assert!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(future);
            assert!(io.stopped);
            assert_eq!(
                io.ledger
                    .lock()
                    .expect("ledger")
                    .report()
                    .working_live_bytes,
                before
            );
            assert!(
                put_restore_output(&mut io, &mut route, &declared, &bytes)
                    .await
                    .is_err()
            );
            assert_eq!(backend.puts.load(Ordering::SeqCst), 1);
            drop(bytes);
            assert!(io.ledger.lock().expect("ledger").report().passing());
        }
    }

    #[tokio::test]
    async fn restore_output_write_exhaustion_stops_before_backend() {
        let mut observations = Vec::new();
        for final_stream in [false, true] {
            let backend = backend(Response::Shared);
            let store = store(backend.clone(), "catalog", true);
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let declared = DeclaredPhysicalRange::new(
                &store,
                PhysicalObject::Directory {
                    key: false,
                    digest: &[7; 32],
                    length: 2,
                },
                final_stream,
            )
            .expect("declaration");
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 4096, &mut io).expect("chunk");
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = if final_stream {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            let bytes = allocate_with_reservation(&mut io, &mut route, Some(1024), true, || {
                Ok(Bytes::from_static(b"{}"))
            })
            .expect("bytes");
            match &mut route {
                RestorePhysicalRoute::OrdinaryUnit { workspace, .. } => workspace
                    .charge_operations(crate::workspace_io_budget::OPERATIONS)
                    .expect("exhaust"),
                RestorePhysicalRoute::FinalMicrochunk(chunk) => {
                    chunk.operations = FINAL_MICROCHUNK_OPERATIONS;
                }
            }
            let result = put_restore_output(&mut io, &mut route, &declared, &bytes).await;
            observations.push((
                final_stream,
                result.is_err(),
                io.stopped,
                backend.puts.load(Ordering::SeqCst),
            ));
        }
        for (final_stream, rejected, stopped, puts) in observations {
            assert!(
                rejected && stopped && puts == 0,
                "write bypassed operation admission: final={final_stream} rejected={rejected} stopped={stopped} puts={puts}"
            );
        }
    }

    #[tokio::test]
    async fn restore_output_write_collision_uses_version_bracketed_classified_read() {
        for final_stream in [false, true] {
            let backend = backend(Response::Shared);
            let store = store(backend.clone(), "catalog", true);
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let declared = DeclaredPhysicalRange::new(
                &store,
                PhysicalObject::Directory {
                    key: false,
                    digest: &[8; 32],
                    length: 2,
                },
                final_stream,
            )
            .expect("declaration");
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 4096, &mut io).expect("chunk");
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = if final_stream {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            let bytes = allocate_with_reservation(&mut io, &mut route, Some(1024), true, || {
                Ok(Bytes::from_static(b"{}"))
            })
            .expect("bytes");
            let first = put_restore_output(&mut io, &mut route, &declared, &bytes)
                .await
                .expect("create");
            let second = put_restore_output(&mut io, &mut route, &declared, &bytes)
                .await
                .expect("collision");
            assert_eq!(
                restore_output_version(&first.value),
                restore_output_version(&second.value)
            );
            assert_eq!(
                (
                    backend.puts.load(Ordering::SeqCst),
                    backend.heads.load(Ordering::SeqCst),
                    backend.calls.load(Ordering::SeqCst)
                ),
                (2, 2, 1)
            );
            assert_eq!(
                (
                    io.work.write_attempts,
                    io.work.metadata_heads,
                    io.work.range_reads,
                    io.work.submitted_write_bytes
                ),
                (2, 2, 1, 4)
            );
            drop((first, second, bytes));
            assert!(io.ledger.lock().expect("ledger").report().passing());
        }
    }

    #[tokio::test]
    async fn classified_reader_shared_payload_keeps_handle_and_separate_unit_invoice() {
        let backend = backend(Response::Shared);
        let store = store(backend.clone(), "catalog", true);
        let declared = payload(&store, false);
        seed(&store, &declared.path, b"payload").await;
        let mut workspace = WorkspaceIoBudget::new();
        let mut admission = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut admission,
        };
        let mut io = RestorePhysicalIo::new(&store, 1024, 1024);
        let bytes = read_declared_physical_range(&mut io, &mut route, &declared)
            .await
            .expect("classified payload");
        assert_eq!(bytes.as_slice(), b"payload");
        assert_eq!(
            io.ledger
                .lock()
                .expect("ledger")
                .report()
                .backend_origin_shared_live_bytes,
            7
        );
        tokio::task::yield_now().await;
        drop(bytes);
        assert!(io.ledger.lock().expect("ledger").report().passing());
        assert_eq!(workspace.test_accounting(), (0, 1, 0, 0));
        assert_eq!(admission.input_bytes, 7);
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn restore_final_poisoned_ledger_stops_before_range_or_head() {
        for head in [false, true] {
            let backend = backend(Response::Shared);
            let store = store(backend.clone(), "catalog", true);
            let mut io = RestorePhysicalIo::new(&store, 1024, 1024);
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("healthy chunk");
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            let ledger = io.ledger.clone();
            let _ = std::panic::catch_unwind(|| {
                let _guard = ledger.lock().expect("healthy ledger");
                panic!("inject accounting poison");
            });
            let declared = payload(&store, true);
            if head {
                assert!(
                    head_declared_physical(&mut io, &mut route, &declared)
                        .await
                        .is_err()
                );
            } else {
                assert!(
                    read_declared_physical_range(&mut io, &mut route, &declared)
                        .await
                        .is_err()
                );
            }
            assert_eq!(
                backend.calls.load(Ordering::SeqCst),
                0,
                "poison must reject before I/O"
            );
            assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
            assert!(io.stopped);
            assert!(chunk.totals.stopped);
            assert!(
                ledger
                    .lock()
                    .expect_err("poisoned")
                    .into_inner()
                    .report()
                    .poisoned_ledger
            );
        }
    }

    #[test]
    fn restore_final_poisoned_start_records_invalid_accounting() {
        let store = store(backend(Response::Shared), "catalog", true);
        let mut io = RestorePhysicalIo::new(&store, 1024, 1024);
        let _ = std::panic::catch_unwind(|| {
            let _guard = io.ledger.lock().expect("healthy ledger");
            panic!("inject accounting poison");
        });
        let mut totals = FinalStreamTotals::new();
        assert!(FinalMicrochunk::begin(&mut totals, 0, &mut io).is_err());
        assert!(totals.stopped);
        assert!(
            io.ledger
                .lock()
                .expect_err("poisoned")
                .into_inner()
                .report()
                .poisoned_ledger
        );
    }

    #[tokio::test]
    async fn restore_physical_invalid_accounting_stops_both_routes_before_io() {
        for (head, final_stream) in [(false, false), (true, false), (false, true), (true, true)] {
            for overflow in [false, true] {
                let backend = backend(Response::Shared);
                let store = store(backend.clone(), "catalog", true);
                let mut io = RestorePhysicalIo::new(&store, 1024, 1024);
                let mut totals = FinalStreamTotals::new();
                let mut chunk =
                    FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("healthy chunk");
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload_admission = UnitPayloadAdmission::new();
                let mut route = if final_stream {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload_admission,
                    }
                };
                {
                    let mut ledger = io.ledger.lock().expect("ledger");
                    ledger.report.counter_overflow = overflow;
                    ledger.report.poisoned_ledger = !overflow;
                }
                let declared = payload(&store, final_stream);
                if head {
                    assert!(
                        head_declared_physical(&mut io, &mut route, &declared)
                            .await
                            .is_err()
                    );
                } else {
                    assert!(
                        read_declared_physical_range(&mut io, &mut route, &declared)
                            .await
                            .is_err()
                    );
                }
                assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
                assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
                assert!(io.stopped);
            }
        }
    }

    #[tokio::test]
    async fn classified_reader_short_receipt_reserves_fixed_probe_and_directory_key_is_metadata() {
        let store = store(backend(Response::Shared), "catalog", true);
        let declared = DeclaredPhysicalRange::new(
            &store,
            PhysicalObject::Receipt {
                candidate: &"aa".repeat(32),
                ordinal: 0,
            },
            true,
        )
        .expect("receipt");
        assert_eq!(declared.reservation_bytes, FINAL_RECEIPT_PROBE_BYTES);
        seed(&store, &declared.path, b"{}").await;
        let mut totals = FinalStreamTotals::new();
        {
            let mut io = RestorePhysicalIo::new(&store, 1024, 1024);
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            assert_eq!(
                read_declared_physical_range(&mut io, &mut route, &declared)
                    .await
                    .expect("short receipt")
                    .as_slice(),
                b"{}"
            );
        }
        assert_eq!(totals.total_operations, 1);
        assert_eq!(
            totals.total_io_reservation_bytes,
            FINAL_RECEIPT_PROBE_BYTES as u64
        );
        let key = DeclaredPhysicalRange::new(
            &store,
            PhysicalObject::Directory {
                key: true,
                digest: &[7; 32],
                length: 2,
            },
            false,
        )
        .expect("key");
        seed(&store, &key.path, b"ab").await;
        let mut workspace = WorkspaceIoBudget::new();
        let mut admission = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut admission,
        };
        let mut io = RestorePhysicalIo::new(&store, 1024, 1024);
        assert_eq!(
            read_declared_physical_range(&mut io, &mut route, &key)
                .await
                .expect("key")
                .as_slice(),
            b"ab"
        );
        assert_eq!(workspace.test_accounting(), (3, 1, 0, 0));
        assert_eq!(admission.input_bytes, 0);
    }

    #[tokio::test]
    async fn classified_reader_wrong_length_counts_capacity_and_stops_before_second_io() {
        let backend = backend(Response::Owned {
            capacity: 64,
            wrong_length: true,
        });
        let store = store(backend.clone(), "catalog", true);
        seed(&store, &payload(&store, true).path, b"payload").await;
        let mut totals = FinalStreamTotals::new();
        let mut io = RestorePhysicalIo::new(&store, 1024, 1024);
        {
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            assert!(
                error(
                    read_declared_physical_range(&mut io, &mut route, &payload(&store, true)).await
                )
                .contains("response differs from its declared expectation")
            );
            assert!(io.stopped);
            assert!(
                error(
                    read_declared_physical_range(&mut io, &mut route, &payload(&store, true)).await
                )
                .contains("stopped after prior failure")
            );
        }
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        assert_eq!(totals.total_returned_request_owned_bytes, 64);
        assert_eq!(totals.peak_request_owned_live_bytes, 64);
        assert!(totals.stopped);
        let report = io.ledger.lock().expect("ledger").report();
        assert_eq!(report.request_owned_live_bytes, 0);
        assert_eq!(report.releases, 1);
    }

    #[tokio::test]
    async fn classified_reader_rejected_capacity_is_retained_in_final_totals() {
        for limit in [32, usize::MAX] {
            let backend = backend(Response::Owned {
                capacity: FINAL_MICROCHUNK_BYTES,
                wrong_length: false,
            });
            let store = store(backend.clone(), "catalog", true);
            seed(&store, &payload(&store, true).path, b"payload").await;
            let mut totals = FinalStreamTotals::new();
            let mut io = RestorePhysicalIo::new(&store, limit, 1024);
            {
                let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
                let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
                assert!(
                    error(
                        read_declared_physical_range(&mut io, &mut route, &payload(&store, true))
                            .await
                    )
                    .contains("capacity exceeds microchunk admission")
                );
            }
            assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                totals.total_returned_request_owned_bytes,
                FINAL_MICROCHUNK_BYTES as u64
            );
            assert_eq!(totals.peak_request_owned_live_bytes, FINAL_MICROCHUNK_BYTES);
            assert!(totals.stopped);
            assert_eq!(
                io.ledger
                    .lock()
                    .expect("ledger")
                    .report()
                    .request_owned_live_bytes,
                0
            );
        }
    }

    #[tokio::test]
    async fn classified_reader_ledger_rejection_below_microchunk_keeps_arrival_diagnostics() {
        let backend = backend(Response::Owned {
            capacity: 64,
            wrong_length: false,
        });
        let store = store(backend.clone(), "catalog", true);
        seed(&store, &payload(&store, true).path, b"payload").await;
        let mut io = RestorePhysicalIo::new(&store, 32, 1024);
        let mut totals = FinalStreamTotals::new();
        {
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            assert!(
                error(
                    read_declared_physical_range(&mut io, &mut route, &payload(&store, true)).await
                )
                .contains("request-owned capacity exceeded admission")
            );
            assert!(io.stopped);
            assert!(
                read_declared_physical_range(&mut io, &mut route, &payload(&store, true))
                    .await
                    .is_err()
            );
        }
        let report = io.ledger.lock().expect("ledger").report();
        assert_eq!(report.largest_rejected_request_owned_capacity, 64);
        assert_eq!(report.over_budget_responses, 1);
        assert_eq!(report.request_owned_live_bytes, 0);
        assert_eq!(report.releases, 0);
        assert_eq!(totals.total_returned_request_owned_bytes, 64);
        assert_eq!(totals.peak_request_owned_live_bytes, 64);
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn classified_reader_rejects_foreign_scope_and_legacy_authority_before_io() {
        for (domain, bounded) in [("other", true), ("catalog", false)] {
            let backend = backend(Response::Shared);
            let selected = store(backend.clone(), "catalog", true);
            let foreign = store(backend.clone(), domain, bounded);
            let declared = payload(&foreign, true);
            seed(&foreign, &declared.path, b"payload").await;
            let pinned = if bounded { &selected } else { &foreign };
            let mut io = RestorePhysicalIo::new(pinned, 1024, 1024);
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            assert!(
                error(read_declared_physical_range(&mut io, &mut route, &declared).await)
                    .contains("scope or authority format differs")
            );
            assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
            assert!(io.stopped);
        }
    }

    #[tokio::test]
    async fn classified_reader_unknown_backend_error_and_cancellation_are_sticky() {
        for (response, exists) in [
            (Response::Unknown, true),
            (Response::Shared, false),
            (Response::Pending, false),
        ] {
            let is_pending = matches!(response, Response::Pending);
            let backend = backend(response);
            let store = store(backend.clone(), "catalog", true);
            if exists {
                seed(&store, &payload(&store, true).path, b"payload").await;
            }
            let mut totals = FinalStreamTotals::new();
            let mut io = RestorePhysicalIo::new(&store, 1024, 1024);
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            if is_pending {
                let declared = payload(&store, true);
                let mut future =
                    Box::pin(read_declared_physical_range(&mut io, &mut route, &declared));
                assert!(matches!(
                    future
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop())),
                    Poll::Pending
                ));
                drop(future);
            } else {
                assert!(
                    read_declared_physical_range(&mut io, &mut route, &payload(&store, true))
                        .await
                        .is_err()
                );
            }
            assert!(io.stopped);
            assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
            assert!(
                error(
                    read_declared_physical_range(&mut io, &mut route, &payload(&store, true)).await
                )
                .contains("stopped after prior failure")
            );
            assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
            assert!(chunk.totals.stopped);
            assert_eq!(chunk.totals.total_operations, 1);
            assert_eq!(chunk.totals.total_io_reservation_bytes, 7);
        }
    }

    #[tokio::test]
    async fn classified_reader_rejects_known_carry_plus_retained_response_before_io() {
        let mib = 1024 * 1024;
        let backend = backend(Response::Owned {
            capacity: 20 * mib,
            wrong_length: false,
        });
        let store = store(backend.clone(), "catalog", true);
        seed(&store, &payload(&store, true).path, b"payload").await;
        let mut io = RestorePhysicalIo::new(&store, 64 * mib, 1024);
        let mut totals = FinalStreamTotals::new();
        let retained = {
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("first chunk");
            read_declared_physical_range(
                &mut io,
                &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
                &payload(&store, true),
            )
            .await
            .expect("first response")
        };
        let error = FinalMicrochunk::begin(&mut totals, 50 * mib, &mut io)
            .err()
            .expect("excess carry");
        let diagnostic = catalog_error_string_capacity(&error).expect("diagnostic capacity");
        assert_eq!(
            backend.calls.load(Ordering::SeqCst),
            1,
            "known 70 MiB combined carry must reject before another read"
        );
        assert_eq!(totals.total_operations, 1);
        assert_eq!(totals.peak_request_owned_live_bytes, 70 * mib + diagnostic);
        drop(retained);
        assert_eq!(io.live_ownership_evidence(), (diagnostic, 0));
        assert_eq!(
            io.ledger
                .lock()
                .expect("ledger")
                .report()
                .request_owned_live_bytes,
            0
        );
    }
    #[tokio::test]
    async fn physical_head_retains_actual_metadata_ownership_on_the_declared_route() {
        for final_stream in [false, true] {
            let backend = backend(Response::Shared);
            let store = store(backend.clone(), "catalog", true);
            let declared = payload(&store, final_stream);
            seed(&store, &declared.path, b"payload").await;
            let mut workspace = WorkspaceIoBudget::new();
            let mut admission = UnitPayloadAdmission::new();
            let mut totals = FinalStreamTotals::new();
            let mut io = RestorePhysicalIo::new(&store, 1024 * 1024, 1024);
            let owned;
            {
                let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
                let mut route = if final_stream {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut admission,
                    }
                };
                let meta = head_declared_physical(&mut io, &mut route, &declared)
                    .await
                    .expect("physical metadata");
                owned = size_of::<ObjectMeta>()
                    + meta.value.path.capacity()
                    + meta.value.version.capacity()
                    + meta.value.etag.as_ref().map_or(0, String::capacity);
                assert_eq!(meta.value.size, 7);
                assert!(!meta.value.version.is_empty());
                assert_eq!(
                    io.ledger
                        .lock()
                        .expect("ledger")
                        .report()
                        .request_owned_live_bytes,
                    owned
                );
                tokio::task::yield_now().await;
                drop(meta);
                assert!(io.ledger.lock().expect("ledger").report().passing());
            }
            assert_eq!(backend.heads.load(Ordering::SeqCst), 1);
            assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
            assert_eq!(admission.input_bytes, 0);
            if final_stream {
                assert_eq!(totals.total_operations, 1);
                assert_eq!(totals.total_io_reservation_bytes, 0);
                assert_eq!(totals.total_returned_request_owned_bytes, owned as u64);
                assert_eq!(workspace.test_accounting(), (0, 0, 0, 0));
            } else {
                assert_eq!(workspace.test_accounting(), (owned, 1, owned, 0));
                assert_eq!(totals.total_operations, 0);
            }
        }
    }

    #[tokio::test]
    async fn physical_head_missing_and_cancelled_are_counted_and_sticky() {
        for response in [Response::Shared, Response::Pending] {
            let pending_head = matches!(response, Response::Pending);
            let backend = backend(response);
            let store = store(backend.clone(), "catalog", true);
            let declared = payload(&store, true);
            let mut io = RestorePhysicalIo::new(&store, 1024, 1024);
            let mut totals = FinalStreamTotals::new();
            {
                let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
                let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
                if pending_head {
                    let mut future =
                        Box::pin(head_declared_physical(&mut io, &mut route, &declared));
                    assert!(matches!(
                        future
                            .as_mut()
                            .poll(&mut Context::from_waker(Waker::noop())),
                        Poll::Pending
                    ));
                    drop(future);
                } else {
                    assert!(
                        head_declared_physical(&mut io, &mut route, &declared)
                            .await
                            .is_err()
                    );
                }
                assert!(io.stopped);
                assert!(
                    head_declared_physical(&mut io, &mut route, &declared)
                        .await
                        .is_err()
                );
            }
            assert!(totals.stopped);
            assert_eq!(totals.total_operations, 1);
            assert_eq!(backend.heads.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn physical_read_work_counts_heads_ranges_and_unknown_returned_bytes() {
        for response in [Response::Shared, Response::Unknown] {
            let unknown = matches!(response, Response::Unknown);
            let backend = backend(response);
            let store = store(backend.clone(), "catalog", true);
            let declared = payload(&store, true);
            seed(&store, &declared.path, b"payload").await;
            let mut io = RestorePhysicalIo::new(&store, 1024 * 1024, 1024);
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            drop(
                head_declared_physical(&mut io, &mut route, &declared)
                    .await
                    .expect("HEAD"),
            );
            let result = read_declared_physical_range(&mut io, &mut route, &declared).await;
            assert_eq!(result.is_err(), unknown);
            drop(result);
            assert_eq!(
                io.work.metadata_heads,
                backend.heads.load(Ordering::SeqCst) as u64
            );
            assert_eq!(
                io.work.range_reads,
                backend.calls.load(Ordering::SeqCst) as u64
            );
            assert_eq!(
                io.work.returned_range_bytes, 7,
                "unknown ownership still counts arrived physical bytes"
            );
        }
    }

    #[tokio::test]
    async fn physical_head_rejection_retains_both_invoices_before_stopping() {
        for exhausted_workspace in [false, true] {
            let backend = backend(Response::Shared);
            let store = store(backend.clone(), "catalog", true);
            let declared = payload(&store, false);
            seed(&store, &declared.path, b"payload").await;
            let mut workspace = WorkspaceIoBudget::new();
            if exhausted_workspace {
                workspace
                    .reserve_bytes(crate::workspace_io_budget::METADATA_BYTES)
                    .expect("fill");
            }
            let mut admission = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut admission,
            };
            let mut io = RestorePhysicalIo::new(
                &store,
                if exhausted_workspace { usize::MAX } else { 1 },
                1024,
            );
            assert!(
                head_declared_physical(&mut io, &mut route, &declared)
                    .await
                    .is_err()
            );
            assert!(io.stopped);
            assert!(
                head_declared_physical(&mut io, &mut route, &declared)
                    .await
                    .is_err()
            );
            assert_eq!(backend.heads.load(Ordering::SeqCst), 1);
            let report = io.ledger.lock().expect("ledger").report();
            let owned = workspace.test_accounting().2;
            assert!(owned > size_of::<ObjectMeta>());
            assert_eq!(report.request_owned_live_bytes, 0);
            assert_eq!(report.request_owned_peak_bytes, owned);
            if exhausted_workspace {
                assert_eq!(report.releases, 1);
            } else {
                assert_eq!(report.over_budget_responses, 1);
                assert_eq!(report.largest_rejected_request_owned_capacity, owned);
            }
        }
    }

    #[tokio::test]
    async fn physical_read_work_overflow_still_records_arrived_buffer_ownership() {
        let backend = backend(Response::Owned {
            capacity: 64,
            wrong_length: false,
        });
        let store = store(backend.clone(), "catalog", true);
        seed(&store, &payload(&store, true).path, b"payload").await;
        let mut io = RestorePhysicalIo::new(&store, 1024, 1024);
        io.work.returned_range_bytes = u64::MAX;
        let mut totals = FinalStreamTotals::new();
        let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
        let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
        assert!(
            error(read_declared_physical_range(&mut io, &mut route, &payload(&store, true)).await)
                .contains("physical read work overflow")
        );
        assert!(
            read_declared_physical_range(&mut io, &mut route, &payload(&store, true))
                .await
                .is_err()
        );
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        let report = io.ledger.lock().expect("ledger").report();
        assert_eq!(report.request_owned_peak_bytes, 64);
        assert_eq!(report.request_owned_live_bytes, 0);
        assert_eq!(report.releases, 1);
        assert_eq!(chunk.totals.total_returned_request_owned_bytes, 64);
        assert!(chunk.totals.stopped);
    }

    #[tokio::test]
    async fn restore_native_directory_cancellation_stops_both_routes_and_releases_ownership() {
        for final_stream in [false, true] {
            let backend = backend(Response::Pending);
            let store = store(backend.clone(), "catalog", true);
            let directory =
                super::super::directory::Directory::new(store.retention.clone(), &store.scope)
                    .expect("directory");
            let root = directory.empty_root_reference().expect("root");
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = if final_stream {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            let mut future = Box::pin(super::super::directory::restore::first_after(
                &mut io, &mut route, &root, None,
            ));
            assert!(matches!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
            drop(future);
            assert!(io.stopped);
            assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                io.ledger
                    .lock()
                    .expect("ledger")
                    .report()
                    .working_live_bytes,
                0
            );
            assert!(
                super::super::directory::restore::first_after(&mut io, &mut route, &root, None)
                    .await
                    .is_err()
            );
            assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
            if final_stream {
                assert!(chunk.totals.stopped);
            }
        }
    }

    #[test]
    fn classified_reader_declarations_enforce_object_caps_and_closed_identity_paths() {
        let store = store(backend(Response::Shared), "catalog", true);
        for (key, cap) in [(false, 64 * 1024), (true, MAX_BLOCK_BYTES)] {
            for length in [0, cap + 1] {
                assert!(
                    DeclaredPhysicalRange::new(
                        &store,
                        PhysicalObject::Directory {
                            key,
                            digest: &[0; 32],
                            length
                        },
                        true
                    )
                    .is_err()
                );
            }
        }
        assert!(
            DeclaredPhysicalRange::new(
                &store,
                PhysicalObject::Descriptor {
                    digest: &[0; 32],
                    length: MAX_CONTROL_JSON_BYTES + 1
                },
                true
            )
            .is_err()
        );
        for candidate in [
            store.paths.current_pointer(),
            "aa/../../HEAD".into(),
            "AA".repeat(32),
        ] {
            assert!(
                DeclaredPhysicalRange::new(
                    &store,
                    PhysicalObject::Receipt {
                        candidate: &candidate,
                        ordinal: 0
                    },
                    true
                )
                .is_err()
            );
        }
        let mut segment = segment();
        segment.segment_id = store.paths.current_pointer();
        assert!(DeclaredPhysicalRange::new(&store, PhysicalObject::Index(&segment), true).is_err());
        segment = self::segment();
        segment.segment_size_bytes = MAX_SEGMENT_BYTES as u64;
        let mut block = block();
        block.row_count = 2;
        block.length = MAX_BLOCK_BYTES as u64 + 1;
        assert!(
            DeclaredPhysicalRange::new(
                &store,
                PhysicalObject::Payload {
                    segment: &segment,
                    block: &block
                },
                true
            )
            .is_err()
        );
        block.row_count = 1;
        block.length = MAX_SEGMENT_BYTES as u64;
        assert!(
            DeclaredPhysicalRange::new(
                &store,
                PhysicalObject::Payload {
                    segment: &segment,
                    block: &block
                },
                true
            )
            .is_ok()
        );
        block.offset = 1;
        assert!(
            DeclaredPhysicalRange::new(
                &store,
                PhysicalObject::Payload {
                    segment: &segment,
                    block: &block
                },
                true
            )
            .is_err()
        );
    }
}

#[cfg(test)]
mod final_carry_tests {
    use super::*;

    #[test]
    fn final_chunk_combines_nonresponse_carry_with_all_live_responses() {
        let mib = 1024 * 1024;
        let store = ControlMvpStateStore::new_synthetic_bounded(
            arco_core::ScopedStorage::new(
                Arc::new(arco_core::MemoryBackend::new()),
                "tenant",
                "workspace",
            )
            .expect("scope"),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store");
        let mut io = RestorePhysicalIo::new(&store, 64 * mib, 0);
        let mut totals = FinalStreamTotals::new();
        {
            let mut chunk =
                FinalMicrochunk::begin(&mut totals, 50 * mib, &mut io).expect("carry fits");
            chunk.reserve_io(7).expect("first range");
            chunk
                .charge_returned_request_owned(10 * mib, 10 * mib)
                .expect("60 MiB total live fits");
            chunk.reserve_io(7).expect("second range");
            assert!(
                chunk
                    .charge_returned_request_owned(10 * mib, 20 * mib)
                    .is_err(),
                "50 MiB nonresponse carry plus two live 10 MiB responses exceeds 64 MiB"
            );
        }
        assert_eq!(totals.peak_request_owned_live_bytes, 70 * mib);
        assert_eq!(totals.total_returned_request_owned_bytes, (20 * mib) as u64);
    }
}

#[cfg(test)]
mod working_memory_tests {
    use super::*;
    fn owned_response() -> ClassifiedBytes {
        let mut value = Vec::with_capacity(16);
        value.extend_from_slice(b"small");
        let actual_capacity = value.capacity();
        assert_eq!(actual_capacity, 16, "fixture capacity");
        ClassifiedBytes {
            bytes: Bytes::from(value),
            ownership: BytesBackingOwnership::NewRequestOwned { actual_capacity },
        }
    }

    #[test]
    fn working_memory_and_arrived_responses_share_one_owned_limit() {
        let ledger = Arc::new(Mutex::new(RestoreOwnershipLedger::new(64, 64)));
        let working = reserve_working_memory(ledger.clone(), 40).expect("working reservation");
        let first =
            admit_classified_bytes(ledger.clone(), owned_response()).expect("combined 56 fits");
        let rejected = admit_classified_bytes(ledger.clone(), owned_response());
        assert!(matches!(
            rejected,
            Err(OwnershipAdmissionError::RequestOwnedBudgetExceeded)
        ));
        let report = ledger.lock().expect("ledger").report();
        assert_eq!(report.request_owned_live_bytes, 16);
        assert_eq!(report.working_live_bytes, 40);
        assert_eq!(report.request_owned_peak_bytes, 72);
        assert_eq!(report.largest_rejected_request_owned_capacity, 16);
        drop(first);
        drop(working);
        let report = ledger.lock().expect("ledger").report();
        assert_eq!(report.request_owned_live_bytes, 0);
        assert_eq!(report.working_live_bytes, 0);
        assert_eq!(report.working_releases, 1);
    }

    #[test]
    fn working_memory_rejection_precedes_allocation_and_preserves_existing_owner() {
        let ledger = Arc::new(Mutex::new(RestoreOwnershipLedger::new(64, 64)));
        let working = reserve_working_memory(ledger.clone(), 40).expect("first reservation");
        assert!(matches!(
            reserve_working_memory(ledger.clone(), 32),
            Err(OwnershipAdmissionError::RequestOwnedBudgetExceeded)
        ));
        let report = ledger.lock().expect("ledger").report();
        assert_eq!(report.working_live_bytes, 40);
        assert_eq!(report.working_admission_failures, 1);
        assert_eq!(
            report.request_owned_peak_bytes, 40,
            "no rejected scratch allocation occurred"
        );
        drop(working);
        assert_eq!(
            ledger.lock().expect("ledger").report().working_live_bytes,
            0
        );
    }

    #[tokio::test]
    async fn cancelled_working_memory_releases_exactly_once() {
        let ledger = Arc::new(Mutex::new(RestoreOwnershipLedger::new(64, 64)));
        let working = reserve_working_memory(ledger.clone(), 40).expect("reservation");
        let (ready, waiting) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _working = working;
            let _ = ready.send(());
            std::future::pending::<()>().await;
        });
        waiting.await.expect("task owns reservation");
        assert_eq!(
            ledger.lock().expect("ledger").report().working_live_bytes,
            40
        );
        task.abort();
        assert!(task.await.expect_err("cancelled").is_cancelled());
        let report = ledger.lock().expect("ledger").report();
        assert_eq!(report.working_releases, 1);
        assert!(report.passing());
    }
}

fn validate_restore_arrow_schema(schema: arrow::ipc::Schema<'_>) -> super::Result<()> {
    // Validate borrowed FlatBuffer fields before Arrow constructs owned names,
    // types, metadata maps, or nested children. No owned schema is needed here.
    let fields = schema
        .fields()
        .ok_or_else(|| super::invariant_violation("restore Arrow schema fields are absent"))?;
    if schema.endianness() != arrow::ipc::Endianness::Little
        || schema.custom_metadata().is_some()
        || schema
            .features()
            .is_some_and(|features| !features.is_empty())
        || fields.len() != 8
    {
        return Err(super::invariant_violation(
            "unsupported restore Arrow schema shape",
        ));
    }
    let expected = [
        ("record_kind", false, 8),
        ("key", false, 0),
        ("value", true, 0),
        ("generation", false, 64),
        ("tombstone", false, -1),
        ("logical_sequence", false, 64),
        ("logical_ordinal", false, 64),
        ("origin_sequence", true, 64),
    ];
    for (field, (name, nullable, width)) in fields.iter().zip(expected) {
        let type_matches = match width {
            0 => field.type_as_binary().is_some(),
            -1 => field.type_as_bool().is_some(),
            width => field
                .type_as_int()
                .is_some_and(|int| int.bitWidth() == width && !int.is_signed()),
        };
        if field.name() != Some(name)
            || field.nullable() != nullable
            || !type_matches
            || field.dictionary().is_some()
            || field.custom_metadata().is_some()
            || field
                .children()
                .is_some_and(|children| !children.is_empty())
        {
            return Err(super::invariant_violation(
                "unsupported restore Arrow field shape",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod raw_schema_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn encoded_schema(schema: &Schema) -> Vec<u8> {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let schema =
            arrow::ipc::convert::IpcSchemaEncoder::new().schema_to_fb_offset(&mut builder, schema);
        let footer = arrow::ipc::Footer::create(
            &mut builder,
            &arrow::ipc::FooterArgs {
                version: arrow::ipc::MetadataVersion::V5,
                schema: Some(schema),
                ..Default::default()
            },
        );
        builder.finish(footer, None);
        builder.finished_data().to_vec()
    }

    #[test]
    fn restore_raw_schema_accepts_production_shape_without_allocation() {
        let bytes = encoded_schema(&super::super::super::control_mvp_segment_schema());
        let footer = arrow::ipc::root_as_footer(&bytes).expect("verified footer");
        let schema = footer.schema().expect("schema");
        let allocations = allocation_counter::measure(|| {
            validate_restore_arrow_schema(schema).expect("production schema");
        });
        assert_eq!(allocations.count_total, 0);
    }

    #[test]
    fn restore_raw_schema_rejects_each_owned_schema_shape_change() {
        let production = super::super::super::control_mvp_segment_schema();
        let mut mutations = vec![Schema::empty()];
        for index in 0..production.fields().len() {
            let original = production.field(index);
            for replacement in [
                original.clone().with_name("untrusted"),
                original.clone().with_nullable(!original.is_nullable()),
                original.clone().with_data_type(DataType::Utf8),
                original
                    .clone()
                    .with_metadata(std::collections::HashMap::from([(
                        "untrusted".into(),
                        "metadata".into(),
                    )])),
            ] {
                let mut fields: Vec<Field> = production
                    .fields()
                    .iter()
                    .map(|field| field.as_ref().clone())
                    .collect();
                fields[index] = replacement;
                mutations.push(Schema::new(fields));
            }
        }
        mutations.push(production.with_metadata(std::collections::HashMap::from([(
            "untrusted".into(),
            "metadata".into(),
        )])));
        for (index, schema) in mutations.iter().enumerate() {
            let bytes = encoded_schema(schema);
            let footer = arrow::ipc::root_as_footer(&bytes).expect("valid flatbuffer");
            assert!(
                validate_restore_arrow_schema(footer.schema().expect("schema")).is_err(),
                "mutation {index} must fail before owned schema conversion"
            );
        }
    }
}

fn preflight_restore_arrow_segment(
    bytes: &[u8],
) -> super::Result<super::super::ArrowSegmentPreflight> {
    if bytes.get(..8) != Some(b"ARROW1\0\0".as_slice()) {
        return Err(super::invariant_violation(
            "restore Arrow file prefix is invalid",
        ));
    }
    let (footer, _) = super::super::verified_arrow_footer(bytes)?;
    let schema = footer
        .schema()
        .ok_or_else(|| super::invariant_violation("restore Arrow footer schema is absent"))?;
    validate_restore_arrow_schema(schema)?;
    let preflight = super::super::preflight_arrow_segment(bytes)?;
    let blocks = footer
        .recordBatches()
        .ok_or_else(|| super::invariant_violation("restore Arrow footer blocks are absent"))?;
    for block in blocks {
        let offset = usize::try_from(block.offset())
            .map_err(|_| super::invariant_violation("restore Arrow message offset is invalid"))?;
        let span = usize::try_from(block.metaDataLength())
            .map_err(|_| super::invariant_violation("restore Arrow message length is invalid"))?;
        validate_restore_arrow_message(super::super::preflight_ipc_message(bytes, offset, span)?)?;
    }
    Ok(preflight)
}

fn validate_restore_arrow_message(message: arrow::ipc::Message<'_>) -> super::Result<()> {
    if message.version() != arrow::ipc::MetadataVersion::V5 || message.custom_metadata().is_some() {
        return Err(super::invariant_violation(
            "restore Arrow message version or metadata is unsupported",
        ));
    }
    let batch = message
        .header_as_record_batch()
        .ok_or_else(|| super::invariant_violation("restore Arrow message is not a record batch"))?;
    let nodes = batch
        .nodes()
        .ok_or_else(|| super::invariant_violation("restore Arrow nodes are absent"))?;
    let buffers = batch
        .buffers()
        .ok_or_else(|| super::invariant_violation("restore Arrow buffers are absent"))?;
    // Six fixed-width primitive fields use two buffers each; two Binary
    // fields use three each. This schema has no variadic buffer collections.
    if batch.variadicBufferCounts().is_some()
        || nodes.len() != 8
        || buffers.len() != 18
        || nodes
            .iter()
            .enumerate()
            .any(|(index, node)| !matches!(index, 2 | 7) && node.null_count() != 0)
    {
        return Err(super::invariant_violation(
            "restore Arrow fixed buffer or nullability grammar differs",
        ));
    }
    validate_restore_buffer_spans(batch, message.bodyLength())
}

fn validate_restore_buffer_spans(
    batch: arrow::ipc::RecordBatch<'_>,
    body_length: i64,
) -> super::Result<()> {
    let invalid =
        || super::invariant_violation("restore Arrow buffer span or minimum length is invalid");
    let rows = u64::try_from(batch.length()).map_err(|_| invalid())?;
    if rows == 0 || rows > super::super::MAX_SEGMENT_ROWS as u64 {
        return Err(invalid());
    }
    let body = u64::try_from(body_length).map_err(|_| invalid())?;
    let bitmap = rows.div_ceil(8);
    let offsets = rows
        .checked_add(1)
        .and_then(|n| n.checked_mul(4))
        .ok_or_else(invalid)?;
    let integers = rows.checked_mul(8).ok_or_else(invalid)?;
    // Exact schema roles: validity/value for primitive fields, and
    // validity/offsets/data for Binary. Body offsets themselves may be unordered.
    let mut minima = [
        0, rows, 0, offsets, 0, 0, offsets, 0, 0, integers, 0, bitmap, 0, integers, 0, integers, 0,
        integers,
    ];
    let nodes = batch.nodes().ok_or_else(invalid)?;
    let buffers = batch.buffers().ok_or_else(invalid)?;
    for (index, node) in [0_usize, 2, 5, 8, 10, 12, 14, 16].into_iter().zip(nodes) {
        if node.length() != batch.length()
            || node.null_count() < 0
            || node.null_count() > batch.length()
        {
            return Err(invalid());
        }
        if node.null_count() > 0 || buffers.get(index).length() != 0 {
            *minima.get_mut(index).ok_or_else(invalid)? = bitmap;
        }
    }
    for (index, (buffer, minimum)) in buffers.iter().zip(minima).enumerate() {
        let start = u64::try_from(buffer.offset()).map_err(|_| invalid())?;
        let length = u64::try_from(buffer.length()).map_err(|_| invalid())?;
        let end = start.checked_add(length).ok_or_else(invalid)?;
        if length < minimum || end > body {
            return Err(invalid());
        }
        if length == 0 {
            continue;
        }
        // Eighteen descriptors bound this pairwise check; no collection or sort.
        for previous in buffers
            .iter()
            .take(index)
            .filter(|previous| previous.length() > 0)
        {
            let previous_start = u64::try_from(previous.offset()).map_err(|_| invalid())?;
            let previous_length = u64::try_from(previous.length()).map_err(|_| invalid())?;
            let previous_end = previous_start
                .checked_add(previous_length)
                .ok_or_else(invalid)?;
            if start < previous_end && previous_start < end {
                return Err(super::invariant_violation(
                    "restore Arrow positive buffer spans overlap",
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod restore_preflight_tests {
    use super::super::super::{
        ControlMvpSegmentRow, SEGMENT_RECORD_KV, control_mvp_segment_schema, encode_arrow_block,
    };
    use super::*;

    #[test]
    #[ignore = "Rust 1.88 64-bit decoder qualification; requires separate pinned-source evidence"]
    fn restore_decoder_fixed_layout_qualification() {
        use arrow::{
            array::{ArrayRef, BinaryArray, BooleanArray, UInt8Array, UInt64Array},
            buffer::Buffer,
            datatypes::{Field, FieldRef, Fields, Schema},
            ipc::Block,
            record_batch::RecordBatch,
        };
        use std::{alloc::Layout, mem::size_of, sync::atomic::AtomicUsize};
        fn array<T>(count: usize) -> Layout {
            Layout::array::<T>(count).expect("fixed array layout")
        }
        fn arc(value: Layout) -> usize {
            array::<AtomicUsize>(2)
                .extend(value)
                .expect("Arc inner layout")
                .0
                .pad_to_align()
                .size()
        }
        assert_eq!(size_of::<usize>(), 8);
        assert_eq!(size_of::<AtomicUsize>(), 8);
        assert_eq!(size_of::<FieldRef>(), 8);
        assert_eq!(size_of::<Fields>(), 16);
        assert!(size_of::<flatbuffers::ErrorTraceDetail>() <= 32);
        let row = size_of::<ControlMvpSegmentRow>();
        assert!(row <= 96);
        let schema = control_mvp_segment_schema();
        assert_eq!(schema.fields().len(), 8);
        let names: usize = schema.fields().iter().map(|field| field.name().len()).sum();
        // Four owned schemas, with two additional 4 -> 8 Field Vec growth steps.
        let schemas = 4
            * (array::<Field>(8).size()
                + 2 * names
                + 8 * arc(Layout::new::<Field>())
                + array::<FieldRef>(8).size()
                + arc(array::<FieldRef>(8)))
            + 2 * array::<Field>(4).size();
        // Pinned Arrow private Bytes is five words; Arc adds two counters.
        let private_owner = arc(array::<usize>(5));
        assert_eq!(private_owner, 56);
        let reader = arc(Layout::new::<Schema>())
            + array::<Block>(4).size()
            + array::<RecordBatch>(4).size()
            + array::<ArrayRef>(4).size()
            + array::<ArrayRef>(8).size()
            + arc(Layout::new::<UInt8Array>())
            + 4 * arc(Layout::new::<UInt64Array>())
            + 2 * arc(Layout::new::<BinaryArray>())
            + arc(Layout::new::<BooleanArray>())
            + 8 * array::<Buffer>(4).size()
            + 7 * private_owner
            + array::<u64>(1).size();
        let rendered = (192 * (96 + 256 + 2 * 20) + 1024 + 63) & !63;
        let error_live = 2 * rendered
            + 2 * (rendered + 256)
            + array::<flatbuffers::ErrorTraceDetail>(256).size()
            + 4 * 1024
            + 64 * 1024;
        let error_cumulative = 4 * rendered
            + 4 * (rendered + 256)
            + array::<flatbuffers::ErrorTraceDetail>(508).size()
            + 8 * 1024
            + 64 * 1024;
        assert!(schemas + reader <= 512 * 1024);
        assert!(schemas + reader + error_live <= 1024 * 1024);
        assert!(schemas + reader + error_cumulative <= 2 * 1024 * 1024);
        eprintln!(
            "restore decoder layouts: row={row} schemas={schemas} reader={reader} error_live={error_live} error_cumulative={error_cumulative}"
        );
        assert_eq!(
            preflight_restore_arrow_segment(&production_block())
                .expect("production grammar")
                .row_count,
            1
        );
    }

    fn production_block() -> Vec<u8> {
        encode_arrow_block(&[ControlMvpSegmentRow {
            record_kind: SEGMENT_RECORD_KV,
            key: b"key".to_vec(),
            value: Some(b"value".to_vec()),
            generation: 1,
            tombstone: false,
            logical_sequence: 1,
            logical_ordinal: 0,
            origin_sequence: None,
        }])
        .expect("production block")
    }

    #[test]
    fn restore_reader_rejects_interior_binary_offsets_before_row_allocation() {
        let rows: Vec<_> = (0_u8..3)
            .map(|ordinal| ControlMvpSegmentRow {
                record_kind: SEGMENT_RECORD_KV,
                key: vec![b'a' + ordinal],
                value: Some(vec![ordinal; 5]),
                generation: 1,
                tombstone: false,
                logical_sequence: 1,
                logical_ordinal: u64::from(ordinal),
                origin_sequence: None,
            })
            .collect();
        for (index, offsets) in [(3, [0_i32, 2, 1, 3]), (6, [0, 10, 5, 15])] {
            let mut bytes = encode_arrow_block(&rows).expect("production block");
            let (footer, _) = super::super::super::verified_arrow_footer(&bytes).expect("footer");
            let block = footer.recordBatches().expect("batches").get(0);
            let offset = usize::try_from(block.offset()).expect("offset");
            let span = usize::try_from(block.metaDataLength()).expect("span");
            let message =
                super::super::super::preflight_ipc_message(&bytes, offset, span).expect("message");
            let buffer = message
                .header_as_record_batch()
                .expect("batch")
                .buffers()
                .expect("buffers")
                .get(index);
            let start = offset + span + usize::try_from(buffer.offset()).expect("buffer offset");
            for (slot, value) in bytes
                .get_mut(start..start + 16)
                .expect("offset bytes")
                .chunks_exact_mut(4)
                .zip(offsets)
            {
                slot.copy_from_slice(&value.to_le_bytes());
            }
            preflight_restore_arrow_segment(&bytes).expect("whole spans and grammar remain valid");
            let block = super::super::super::block_metadata(0, &bytes, &rows);
            #[cfg(feature = "test-utils")]
            let _ = super::super::super::cost::take();
            let error = super::super::super::decode_block_rows_with_preflight(
                &bytes,
                &block,
                "catalog",
                preflight_restore_arrow_segment,
            )
            .expect_err("Arrow validates all offsets");
            assert!(!error.to_string().contains("panicked"), "{error}");
            #[cfg(feature = "test-utils")]
            assert!(
                super::super::super::cost::take()
                    .values()
                    .all(|counts| counts.get(30) == Some(&0)),
                "row vector was never allocated"
            );
        }
    }

    fn replace_footer_schema(bytes: &[u8], schema: &arrow::datatypes::Schema) -> Vec<u8> {
        let trailer_start = bytes.len() - 10;
        let trailer = bytes[trailer_start..].try_into().expect("trailer");
        let footer_len = arrow::ipc::reader::read_footer_length(trailer).expect("footer length");
        let footer_start = trailer_start - footer_len;
        let footer =
            arrow::ipc::root_as_footer(&bytes[footer_start..trailer_start]).expect("footer");
        let block = *footer.recordBatches().expect("batches").get(0);
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let schema =
            arrow::ipc::convert::IpcSchemaEncoder::new().schema_to_fb_offset(&mut builder, schema);
        let blocks = builder.create_vector(&[block]);
        let footer = arrow::ipc::Footer::create(
            &mut builder,
            &arrow::ipc::FooterArgs {
                version: arrow::ipc::MetadataVersion::V5,
                schema: Some(schema),
                recordBatches: Some(blocks),
                ..Default::default()
            },
        );
        builder.finish(footer, None);
        let footer = builder.finished_data();
        let mut result = bytes[..footer_start].to_vec();
        result.extend_from_slice(footer);
        result.extend_from_slice(&u32::try_from(footer.len()).expect("length").to_le_bytes());
        result.extend_from_slice(b"ARROW1");
        result
    }

    fn replace_production_buffer(
        bytes: &[u8],
        index: usize,
        replacement: impl FnOnce(arrow::ipc::Buffer, arrow::ipc::Buffer) -> arrow::ipc::Buffer,
    ) -> Vec<u8> {
        let (footer, _) = super::super::super::verified_arrow_footer(bytes).expect("footer");
        let block = footer.recordBatches().expect("batches").get(0);
        let message = super::super::super::preflight_ipc_message(
            bytes,
            usize::try_from(block.offset()).expect("offset"),
            usize::try_from(block.metaDataLength()).expect("span"),
        )
        .expect("message");
        let buffers = message
            .header_as_record_batch()
            .expect("batch")
            .buffers()
            .expect("buffers");
        let offset = buffers.bytes().as_ptr().addr() - bytes.as_ptr().addr() + index * 16;
        let replacement = replacement(*buffers.get(index), *buffers.get(13));
        let mut changed = bytes.to_vec();
        changed[offset..offset + 16].copy_from_slice(&replacement.0);
        changed
    }

    #[test]
    fn restore_preflight_rejects_overlapping_positive_buffer_spans() {
        let bytes = production_block();
        let changed = replace_production_buffer(&bytes, 9, |_, other| other);
        // This is a structurally in-range alias of the generation and sequence
        // value buffers. The generic codec's preflight permits it.
        super::super::super::preflight_arrow_segment(&changed).expect("generic in-range shape");
        assert!(
            preflight_restore_arrow_segment(&changed).is_err(),
            "restore admission must reject aliased positive buffer spans before Arrow copies"
        );
    }

    #[test]
    fn restore_preflight_rejects_short_values_offsets_and_required_validity_buffers() {
        let bytes = production_block();
        for (index, length) in [
            (1, 0),
            (3, 4),
            (6, 4),
            (9, 7),
            (11, 0),
            (13, 7),
            (15, 7),
            (16, 0),
            (17, 7),
        ] {
            let changed = replace_production_buffer(&bytes, index, |old, _| {
                arrow::ipc::Buffer::new(old.offset(), length)
            });
            super::super::super::preflight_arrow_segment(&changed).expect("generic in-range shape");
            assert!(
                preflight_restore_arrow_segment(&changed).is_err(),
                "buffer {index} is shorter than its role requires"
            );
        }
    }

    #[test]
    fn restore_preflight_rejects_large_field_name_before_owned_schema_conversion() {
        let bytes = production_block();
        let preflight = preflight_restore_arrow_segment(&bytes).expect("production preflight");
        assert_eq!(preflight.row_count, 1);
        let mut fields: Vec<arrow::datatypes::Field> = control_mvp_segment_schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        fields[0] = fields[0].clone().with_name("x".repeat(64 * 1024));
        let malformed = replace_footer_schema(&bytes, &arrow::datatypes::Schema::new(fields));
        let mut result = None;
        let allocations = allocation_counter::measure(|| {
            result = Some(preflight_restore_arrow_segment(&malformed));
        });
        assert!(result.expect("result").is_err());
        assert!(
            allocations.bytes_total < 4096,
            "untrusted 64 KiB field name must not be copied: {allocations:?}"
        );
    }

    fn record_message(
        custom: bool,
        variadic: bool,
        buffer_count: usize,
        version: arrow::ipc::MetadataVersion,
        null_count: i64,
    ) -> Vec<u8> {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let mut raw_nodes = [arrow::ipc::FieldNode::new(1, 0); 8];
        raw_nodes[0] = arrow::ipc::FieldNode::new(1, null_count);
        let nodes = builder.create_vector(&raw_nodes);
        let lengths = [0, 1, 0, 8, 0, 0, 8, 0, 0, 8, 0, 1, 0, 8, 0, 8, 0, 8];
        let raw_buffers: Vec<_> = (0..buffer_count)
            .map(|index| {
                arrow::ipc::Buffer::new(
                    i64::try_from(index * 64).expect("offset"),
                    lengths.get(index).copied().unwrap_or(0),
                )
            })
            .collect();
        let buffers = builder.create_vector(&raw_buffers);
        let counts = variadic.then(|| builder.create_vector::<i64>(&[]));
        let batch = arrow::ipc::RecordBatch::create(
            &mut builder,
            &arrow::ipc::RecordBatchArgs {
                length: 1,
                nodes: Some(nodes),
                buffers: Some(buffers),
                variadicBufferCounts: counts,
                ..Default::default()
            },
        );
        let metadata = custom.then(|| {
            builder.create_vector::<flatbuffers::WIPOffset<arrow::ipc::KeyValue<'_>>>(&[])
        });
        let message = arrow::ipc::Message::create(
            &mut builder,
            &arrow::ipc::MessageArgs {
                bodyLength: 18 * 64,
                version,
                header_type: arrow::ipc::MessageHeader::RecordBatch,
                header: Some(batch.as_union_value()),
                custom_metadata: metadata,
            },
        );
        builder.finish(message, None);
        builder.finished_data().to_vec()
    }

    #[test]
    fn restore_preflight_rejects_record_message_metadata() {
        let production = record_message(false, false, 18, arrow::ipc::MetadataVersion::V5, 0);
        validate_restore_arrow_message(arrow::ipc::root_as_message(&production).expect("message"))
            .expect("supported record message shape");
        let bytes = record_message(true, false, 18, arrow::ipc::MetadataVersion::V5, 0);
        let message = arrow::ipc::root_as_message(&bytes).expect("message");
        assert!(
            validate_restore_arrow_message(message).is_err(),
            "even empty custom metadata is outside production message shape"
        );
    }

    #[test]
    fn restore_preflight_rejects_variadic_and_extra_buffer_metadata() {
        for (variadic, count) in [(true, 18), (false, 19), (false, 17)] {
            let bytes = record_message(false, variadic, count, arrow::ipc::MetadataVersion::V5, 0);
            let message = arrow::ipc::root_as_message(&bytes).expect("message");
            assert!(
                validate_restore_arrow_message(message).is_err(),
                "variadic={variadic}, buffers={count} is outside fixed primitive grammar"
            );
        }
    }
    #[test]
    fn restore_preflight_rejects_record_message_version_before_body_decode() {
        let bytes = record_message(false, false, 18, arrow::ipc::MetadataVersion::V4, 0);
        let message = arrow::ipc::root_as_message(&bytes).expect("message");
        assert!(
            validate_restore_arrow_message(message).is_err(),
            "V4 message must not reach the V5 decoder"
        );
    }

    #[test]
    fn restore_preflight_rejects_null_count_for_required_field() {
        let bytes = record_message(false, false, 18, arrow::ipc::MetadataVersion::V5, 1);
        let message = arrow::ipc::root_as_message(&bytes).expect("message");
        assert!(
            validate_restore_arrow_message(message).is_err(),
            "required record_kind field cannot have nulls"
        );
    }

    #[test]
    fn restore_preflight_requires_complete_file_magic() {
        let bytes = production_block();
        assert_eq!(&bytes[..8], b"ARROW1\0\0", "production file prefix");
        for index in [0, 6, 7] {
            let mut malformed = bytes.clone();
            malformed[index] ^= 1;
            assert!(
                preflight_restore_arrow_segment(&malformed).is_err(),
                "invalid prefix byte {index}"
            );
        }
    }
}

impl WorkingMemory {
    fn retain_allocation_bound(&mut self, bytes: usize) -> Result<(), OwnershipAdmissionError> {
        let mut ledger = match self.ledger.lock() {
            Ok(ledger) => ledger,
            Err(poisoned) => {
                poisoned.into_inner().report.poisoned_ledger = true;
                return Err(OwnershipAdmissionError::Poisoned);
            }
        };
        let report = &mut ledger.report;
        let observed_working = report
            .working_live_bytes
            .checked_sub(self.bytes)
            .and_then(|working| working.checked_add(bytes));
        let Some((observed_working, peak)) = observed_working.and_then(|working| {
            working
                .checked_add(report.request_owned_live_bytes)
                .map(|peak| (working, peak))
        }) else {
            report.counter_overflow = true;
            return Err(OwnershipAdmissionError::Overflow);
        };
        report.request_owned_peak_bytes = report.request_owned_peak_bytes.max(peak);
        report.working_peak_bytes = report.working_peak_bytes.max(observed_working);
        let underestimated = bytes > self.bytes;
        // The allocation has arrived even if it exceeds the reservation.
        // Retain that live charge so failed decoding cannot release it early.
        report.working_live_bytes = observed_working;
        self.bytes = bytes;
        if underestimated {
            RestoreOwnershipLedger::increment(
                &mut report.working_underestimates,
                &mut report.counter_overflow,
            )?;
            return Err(OwnershipAdmissionError::RequestOwnedBudgetExceeded);
        }
        drop(ledger);
        Ok(())
    }
}

// The output drops before its conservative allocation reservation.
pub(in super::super) struct WorkingValue<T> {
    value: T,
    working: WorkingMemory,
}

// Prevent moving the value out and releasing its reservation independently.
// Field drop order still releases the value before its working-memory guard.
impl<T> WorkingValue<T> {
    pub(in super::super) fn is_owned_by(&self, io: &RestorePhysicalIo<'_>) -> bool {
        Arc::ptr_eq(&self.working.ledger, &io.ledger)
    }

    pub(in super::super) fn value(&self) -> &T {
        &self.value
    }
}

impl<T> Drop for WorkingValue<T> {
    fn drop(&mut self) {}
}

pub(in super::super) fn new_directory_builder(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
) -> CatalogResult<WorkingValue<super::super::directory::restore::NativeBuilder>> {
    use super::super::directory::{Directory, restore::NativeBuilder};
    let store = io.store;
    let reservation = directory_scope_reservation(store)?
        .checked_add(NativeBuilder::reservation()?)
        .ok_or_else(|| physical_backpressure("native builder reservation overflow"))?;
    let final_stream = matches!(route, RestorePhysicalRoute::FinalMicrochunk(_));
    decode_with_reservation(io, route, Some(reservation), || {
        if store.authority_format != 8 || !final_stream {
            return Err(physical_backpressure(
                "native builder requires final authority-8 stream",
            ));
        }
        Ok(NativeBuilder::new(Directory::new(
            store.retention.clone(),
            &store.scope,
        )?))
    })
}

impl WorkingValue<super::super::directory::restore::NativeBuilder> {
    pub(in super::super) async fn push_directory_leaf(
        &mut self,
        io: &mut RestorePhysicalIo<'_>,
        route: &mut RestorePhysicalRoute<'_, '_>,
        leaf: &WorkingValue<super::super::directory::Leaf>,
    ) -> CatalogResult<()> {
        let owned = self.is_owned_by(io) && leaf.is_owned_by(io);
        let final_stream = matches!(route, RestorePhysicalRoute::FinalMicrochunk(_));
        let admitted =
            decode_with_reservation(io, route, Some(DIRECTORY_FIXED_ALLOCATION_BYTES), || {
                if !owned || !final_stream {
                    return Err(physical_backpressure(
                        "native builder owner or phase differs",
                    ));
                }
                Ok(())
            })?;
        drop(admitted);
        self.value.push(io, route, leaf.value()).await
    }

    pub(in super::super) async fn finish_directory(
        mut self,
        io: &mut RestorePhysicalIo<'_>,
        route: &mut RestorePhysicalRoute<'_, '_>,
    ) -> CatalogResult<WorkingValue<super::super::directory::Root>> {
        let owned = self.is_owned_by(io);
        let final_stream = matches!(route, RestorePhysicalRoute::FinalMicrochunk(_));
        let admitted =
            decode_with_reservation(io, route, Some(DIRECTORY_FIXED_ALLOCATION_BYTES), || {
                if !owned || !final_stream {
                    return Err(physical_backpressure(
                        "native builder owner or phase differs",
                    ));
                }
                Ok(())
            })?;
        drop(admitted);
        self.value.finish(io, route).await
    }
}

/// Closed directory namespace only; all bodies originate in admitted builder codecs.
pub(in super::super) async fn write_directory_output(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    key: bool,
    digest: &[u8; 32],
    raw: &[u8],
) -> CatalogResult<()> {
    let store = io.store;
    let final_stream = matches!(route, RestorePhysicalRoute::FinalMicrochunk(_));
    let declared = encode_with_reservation(io, route, directory_scope_reservation(store)?, || {
        if !final_stream {
            return Err(physical_backpressure(
                "native directory output requires final stream",
            ));
        }
        DeclaredPhysicalRange::new(
            store,
            PhysicalObject::Directory {
                key,
                digest,
                length: raw.len(),
            },
            true,
        )
    })?;
    let bytes = encode_directory_output(io, route, raw)?;
    put_restore_output(io, route, declared.value(), &bytes).await?;
    Ok(())
}

#[allow(
    clippy::redundant_clone,
    reason = "promote exact-capacity Bytes backing inside the measured allocation guard"
)]
fn encode_directory_output(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    raw: &[u8],
) -> CatalogResult<WorkingValue<Bytes>> {
    encode_with_reservation(
        io,
        route,
        raw.len()
            .checked_add(DIRECTORY_FIXED_ALLOCATION_BYTES)
            .ok_or_else(|| physical_backpressure("directory encoding reservation overflow"))?,
        || {
            let bytes = Bytes::copy_from_slice(raw);
            Ok(bytes.clone())
        },
    )
}

// Frozen native-directory contract; includes the fixed cost-counter nodes,
// scope clones, path formatting and fixed validation errors, but no I/O response.
pub(in super::super) const DIRECTORY_FIXED_ALLOCATION_BYTES: usize = 16 * 1024;

pub(in super::super) fn directory_scope_reservation(
    store: &ControlMvpStateStore,
) -> CatalogResult<usize> {
    if usize::BITS != 64 {
        return Err(physical_backpressure(
            "unqualified directory allocation target",
        ));
    }
    [
        store.scope.tenant_id(),
        store
            .scope
            .workspace_id()
            .ok_or_else(|| physical_backpressure("restore directory requires workspace root"))?,
        store.scope.domain(),
        store.retention.tenant_id(),
        store.retention.scope().workspace_id().unwrap_or_default(),
        store.retention.scope().metastore_id().unwrap_or_default(),
        store
            .retention
            .scope()
            .workspace_id()
            .ok_or_else(|| physical_backpressure("restore directory requires workspace storage"))?,
    ]
    .into_iter()
    .try_fold(DIRECTORY_FIXED_ALLOCATION_BYTES, |total, part| {
        part.len()
            .checked_mul(32)
            .and_then(|bytes| total.checked_add(bytes))
            .ok_or_else(|| physical_backpressure("directory scope allocation overflow"))
    })
}

pub(in super::super) fn decode_owned<T>(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    decode: impl FnOnce() -> CatalogResult<T>,
) -> CatalogResult<WorkingValue<T>> {
    decode_with_reservation(io, route, None, decode)
}

pub(in super::super) fn decode_with_reservation<T>(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    reservation: Option<usize>,
    decode: impl FnOnce() -> CatalogResult<T>,
) -> CatalogResult<WorkingValue<T>> {
    allocate_with_reservation(io, route, reservation, false, decode)
}

pub(in super::super) fn encode_with_reservation<T>(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    reservation: usize,
    encode: impl FnOnce() -> CatalogResult<T>,
) -> CatalogResult<WorkingValue<T>> {
    allocate_with_reservation(io, route, Some(reservation), true, encode)
}

/// The closed unit codec calls this with an infallible hash after its fallible
/// canonicalization. Preserve completed hash work even if retention then fails.
pub(in super::super) fn hash_with_reservation(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    reservation: usize,
    input_bytes: usize,
    hash: impl FnOnce() -> CatalogResult<String>,
) -> CatalogResult<WorkingValue<String>> {
    let next = io.work.hash_operations.checked_add(1).zip(
        u64::try_from(input_bytes)
            .ok()
            .and_then(|bytes| io.work.hashed_bytes.checked_add(bytes)),
    );
    let mut completed = None;
    let result = encode_with_reservation(io, route, reservation, || {
        let next = next.ok_or_else(|| physical_backpressure("restore hash work overflow"))?;
        let digest = hash()?;
        completed = Some(next);
        Ok(digest)
    });
    if let Some((operations, bytes)) = completed {
        io.work.hash_operations = operations;
        io.work.hashed_bytes = bytes;
    }
    if result.is_err() {
        io.stop(route);
    }
    result
}

#[cfg(test)]
mod unit_hash_tests {
    use super::*;

    #[tokio::test]
    async fn admitted_unit_hash_counter_overflow_and_posthash_failure() {
        let storage = arco_core::ScopedStorage::new(
            Arc::new(arco_core::MemoryBackend::new()),
            "tenant",
            "workspace",
        )
        .expect("scope");
        let store = ControlMvpStateStore::new_synthetic_bounded(
            storage,
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store");
        for case in 0..3 {
            let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
            let initial = match case {
                0 => (u64::MAX, 0),
                1 => (0, u64::MAX),
                _ => (0, 0),
            };
            io.work.hash_operations = initial.0;
            io.work.hashed_bytes = initial.1;
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let mut ran = false;
            let result = hash_with_reservation(
                &mut io,
                &mut route,
                if case == 2 { 8 } else { 64 * 1024 },
                3,
                || {
                    ran = true;
                    Ok(super::super::super::prefixed_sha256(b"abc"))
                },
            );
            assert!(result.is_err());
            assert_eq!(ran, case == 2);
            assert_eq!(
                io.hashing_evidence(),
                if case == 2 { (1, 3) } else { initial }
            );
            assert!(
                hash_with_reservation(&mut io, &mut route, 64 * 1024, 3, || panic!("stopped hash"))
                    .is_err()
            );
        }
    }
}

// A refused decoder did not underestimate its working reservation. Its known
// new static diagnostic still needs an owner, including after admission stops.
fn retain_rejected_diagnostic(
    ledger: &Arc<Mutex<RestoreOwnershipLedger>>,
    owner: &mut Option<WorkingMemory>,
    work: &mut RestoreReadWork,
    route: &mut RestorePhysicalRoute<'_, '_>,
    encoding: bool,
    error: CatalogError,
) -> CatalogError {
    let bytes = catalog_error_string_capacity(&error).unwrap_or(usize::MAX);
    let allocated = if encoding {
        &mut work.encoded_owned_allocation_bytes
    } else {
        &mut work.decoded_owned_allocation_bytes
    };
    let next = allocated.checked_add(u64::try_from(bytes).unwrap_or(u64::MAX));
    *allocated = next.unwrap_or(u64::MAX);
    let mut guard = match ledger.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            guard.report.poisoned_ledger = true;
            guard
        }
    };
    let report = &mut guard.report;
    let working = report.working_live_bytes.checked_add(bytes);
    let total_live = working.and_then(|n| n.checked_add(report.request_owned_live_bytes));
    report.counter_overflow |= next.is_none() || working.is_none() || total_live.is_none();
    report.working_live_bytes = working.unwrap_or(usize::MAX);
    report.working_peak_bytes = report.working_peak_bytes.max(report.working_live_bytes);
    report.request_owned_peak_bytes = report
        .request_owned_peak_bytes
        .max(total_live.unwrap_or(usize::MAX));
    if let Some(owner) = owner {
        let sum = owner.bytes.checked_add(bytes);
        report.counter_overflow |= sum.is_none();
        owner.bytes = sum.unwrap_or(usize::MAX);
    } else {
        *owner = Some(WorkingMemory {
            ledger: ledger.clone(),
            bytes,
        });
    }
    let live = total_live.unwrap_or(usize::MAX);
    drop(guard);
    if let RestorePhysicalRoute::FinalMicrochunk(chunk) = route {
        chunk.record_allocated_owned(bytes);
        let live = live.saturating_add(chunk.external_owned_carry_bytes);
        chunk.totals.peak_request_owned_live_bytes =
            chunk.totals.peak_request_owned_live_bytes.max(live);
        chunk.totals.peak_chunk_owned_upper_bound_bytes =
            chunk.totals.peak_chunk_owned_upper_bound_bytes.max(live);
    }
    error
}

#[cfg(any(test, feature = "test-utils"))]
#[allow(clippy::too_many_lines)]
fn allocate_with_reservation<T>(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    reservation: Option<usize>,
    encoding: bool,
    decode: impl FnOnce() -> CatalogResult<T>,
) -> CatalogResult<WorkingValue<T>> {
    if io.stopped || io.store.authority_format != 8 {
        io.stopped = true;
        route.stop();
        let error = physical_backpressure("restore decoder authority is unavailable");
        return Err(retain_rejected_diagnostic(
            &io.ledger,
            &mut io.failed_allocation,
            &mut io.work,
            route,
            encoding,
            error,
        ));
    }
    let mut attempt = RestoreReadAttempt::new(&mut io.stopped, route);
    let external = match &attempt.route {
        RestorePhysicalRoute::OrdinaryUnit { .. } => 0,
        RestorePhysicalRoute::FinalMicrochunk(chunk) => chunk.external_owned_carry_bytes,
    };
    let (mut remaining, previous_owned) = {
        let ledger = checked_ownership_ledger(&io.ledger)?;
        let previous_owned = ledger.report.owned_live_upper_bound();
        let remaining = ledger
            .request_owned_limit
            .checked_sub(previous_owned)
            .and_then(|remaining| remaining.checked_sub(external))
            .ok_or_else(|| {
                physical_backpressure("restore decoder carried ownership exceeds admission")
            })?;
        drop(ledger);
        (remaining, previous_owned)
    };
    if let RestorePhysicalRoute::FinalMicrochunk(chunk) = &mut attempt.route {
        chunk.validate_ledger(&io.ledger)?;
        remaining = remaining.min(
            FINAL_MICROCHUNK_BYTES
                .checked_sub(chunk.owned_upper_bound())
                .ok_or_else(|| {
                    physical_backpressure("final decoder cumulative admission exhausted")
                })?,
        );
    }
    let admitted = reservation.unwrap_or(remaining);
    if admitted > remaining {
        let mut ledger = checked_ownership_ledger(&io.ledger)?;
        let report = &mut ledger.report;
        RestoreOwnershipLedger::increment(
            &mut report.working_admission_failures,
            &mut report.counter_overflow,
        )
        .map_err(ownership_failure)?;
        drop(ledger);
        let error =
            physical_backpressure("restore decoder reservation exceeds remaining admission");
        return Err(retain_rejected_diagnostic(
            &io.ledger,
            &mut io.failed_allocation,
            &mut io.work,
            attempt.route,
            encoding,
            error,
        ));
    }

    let mut working =
        reserve_working_memory(io.ledger.clone(), admitted).map_err(ownership_failure)?;
    let (operations, allocated_bytes) = if encoding {
        (
            &mut io.work.encode_operations,
            &mut io.work.encoded_owned_allocation_bytes,
        )
    } else {
        (
            &mut io.work.decode_operations,
            &mut io.work.decoded_owned_allocation_bytes,
        )
    };
    add_read_work(operations, 1)?;
    // A supplied reservation precedes the decoder; measurement only detects
    // overruns and retains output ownership. None is observational evidence
    // for callers whose a-priori allocation proof is still incomplete.
    // Nested measurements count each fresh allocation once. All owned outputs
    // here must be newly constructed from borrowed input.
    let mut decoded = None;
    let mut native = super::super::cost::NativeWork::default();
    let allocations = allocation_counter::measure(|| {
        let capture = super::super::cost::NativeCapture::begin();
        decoded = Some(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(decode))
                .unwrap_or_else(|_| Err(super::invariant_violation("restore decoder panicked"))),
        );
        native = capture.finish();
    });
    // Preserve every slot even if subsequent ownership bookkeeping fails.
    io.work.native.merge(native);
    if io.work.native.overflow {
        if let Ok(mut ledger) = io.ledger.lock() {
            ledger.report.counter_overflow = true;
        }
    }
    let checked = (|| {
        let bytes = usize::try_from(allocations.bytes_total)
            .map_err(|_| physical_backpressure("restore decoder allocation count overflow"))?;
        let work = add_read_work(allocated_bytes, bytes);
        let retained = working.retain_allocation_bound(bytes);
        if let RestorePhysicalRoute::FinalMicrochunk(chunk) = &mut attempt.route {
            // Include an observed underestimate even when the ownership ledger
            // rejected it. A concurrent release can only make this conservative.
            let observed_live = previous_owned.checked_add(bytes).unwrap_or(usize::MAX);
            chunk.record_allocated_owned(bytes);
            chunk.charge_returned_request_owned(0, observed_live)?;
        }
        work?;
        retained.map_err(ownership_failure)?;
        if io.work.native.overflow {
            return Err(physical_backpressure("restore native work overflow"));
        }
        Ok(())
    })();
    let result = match checked {
        Ok(()) => {
            decoded.ok_or_else(|| physical_backpressure("restore decoder did not execute"))?
        }
        Err(error) => {
            // Bookkeeping failures create one String after the measured decode.
            // Keep the original allocation bound while dropping decoded values,
            // then invoice the replacement's exact capacity once.
            drop(decoded);
            let extra = catalog_error_string_capacity(&error).unwrap_or(usize::MAX);
            let total = working.bytes.saturating_add(extra);
            let extra = u64::try_from(extra).unwrap_or(u64::MAX);
            let counter = allocated_bytes.checked_add(extra);
            *allocated_bytes = counter.unwrap_or(u64::MAX);
            if counter.is_none() {
                if let Ok(mut ledger) = io.ledger.lock() {
                    ledger.report.counter_overflow = true;
                }
            }
            let _ = working.retain_allocation_bound(total);
            if let RestorePhysicalRoute::FinalMicrochunk(chunk) = &mut attempt.route {
                chunk.record_allocated_owned(usize::try_from(extra).unwrap_or(usize::MAX));
                let live = previous_owned
                    .saturating_add(total)
                    .saturating_add(chunk.external_owned_carry_bytes);
                chunk.totals.peak_request_owned_live_bytes =
                    chunk.totals.peak_request_owned_live_bytes.max(live);
            }
            Err(error)
        }
    };
    match result {
        Ok(value) => {
            attempt.disarm();
            Ok(WorkingValue { value, working })
        }
        Err(error) => {
            io.failed_allocation = Some(working);
            Err(error)
        }
    }
}

#[cfg(not(any(test, feature = "test-utils")))]
fn allocate_with_reservation<T>(
    _io: &mut RestorePhysicalIo<'_>,
    _route: &mut RestorePhysicalRoute<'_, '_>,
    _reservation: Option<usize>,
    _encoding: bool,
    _decode: impl FnOnce() -> CatalogResult<T>,
) -> CatalogResult<WorkingValue<T>> {
    Err(physical_backpressure(
        "synthetic restore allocation observation is unavailable",
    ))
}

#[derive(Clone, Copy)]
enum RestoreOutputObject<'a> {
    Segment(&'a str),
    Index(&'a str),
    Descriptor(&'a [u8; 32]),
}

fn declare_restore_output(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    object: RestoreOutputObject<'_>,
    length: usize,
) -> CatalogResult<WorkingValue<DeclaredPhysicalRange>> {
    let result = (|| {
        let (cap, payload) = match object {
            RestoreOutputObject::Segment(id) | RestoreOutputObject::Index(id)
                if !valid_raw_digest(id) =>
            {
                return Err(physical_backpressure(
                    "restore output identity is not a frozen digest",
                ));
            }
            RestoreOutputObject::Segment(_) => (MAX_BLOCK_BYTES, true),
            RestoreOutputObject::Index(_) => (MAX_SEGMENT_INDEX_BYTES, false),
            RestoreOutputObject::Descriptor(_) => (MAX_CONTROL_JSON_BYTES, false),
        };
        admitted_physical_length(length, cap)?;
        let probe = length
            .checked_add(1)
            .ok_or_else(|| physical_backpressure("restore output probe overflow"))?;
        let reservation = directory_scope_reservation(io.store)?;
        let store = io.store;
        let final_stream = matches!(route, RestorePhysicalRoute::FinalMicrochunk(_));
        allocate_with_reservation(io, route, Some(reservation), true, || {
            let path = match object {
                RestoreOutputObject::Segment(id) => store.paths.state_object(id),
                RestoreOutputObject::Index(id) => store.paths.segment_index(id),
                RestoreOutputObject::Descriptor(digest) => format!(
                    "{}/physical/descriptors/{}.json",
                    store.paths.base_prefix(),
                    hex::encode(digest)
                ),
            };
            Ok(DeclaredPhysicalRange {
                scope: store.scope.clone(),
                final_stream,
                payload,
                path,
                range: 0..probe as u64,
                reservation_bytes: probe,
                min_response_bytes: length,
                max_response_bytes: length,
            })
        })
    })();
    if result.is_err() {
        io.stop(route);
    }
    result
}

/// Unit output artifacts alone do not establish receipt coverage or readable authority.
pub(in super::super) struct StandardRestoreOutput {
    pub(in super::super) descriptor: WorkingValue<super::Descriptor>,
    pub(in super::super) bytes: WorkingValue<Bytes>,
    pub(in super::super) digest: [u8; 32],
}

/// Validate the exact standard Arrow/index encoding without any storage effect.
/// The caller preflights every chosen partition before writing the first one.
pub(in super::super) fn preflight_standard_restore_output(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    output_id: &str,
    logical_sequence: u64,
    rows: &WorkingValue<Vec<super::ControlMvpSegmentRow>>,
) -> CatalogResult<()> {
    let owned = rows.is_owned_by(io);
    let result = (|| {
        drop(decode_with_reservation(io, route, Some(64 * 1024), || {
            if !owned || !valid_raw_digest(output_id) {
                return Err(physical_backpressure(
                    "restore output preflight owner or identity differs",
                ));
            }
            Ok(())
        })?);
        let segment = encode_standard_restore_output(io, route, rows.value())?;
        let index = build_restore_output_index(
            io,
            route,
            output_id,
            logical_sequence,
            rows.value(),
            segment.value(),
        )?;
        drop(encode_restore_output_metadata(
            io,
            route,
            RestoreOutputMetadata::Index(index.value()),
        )?);
        Ok(())
    })();
    if result.is_err() {
        io.stop(route);
    }
    result
}

/// The unit codec supplies its frozen `output_id`; only exact encoder products
/// reach the private model constructors and immutable writer below.
pub(in super::super) async fn write_standard_restore_output(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    output_id: &str,
    logical_sequence: u64,
    rows: &WorkingValue<Vec<super::ControlMvpSegmentRow>>,
) -> CatalogResult<StandardRestoreOutput> {
    let result = async {
        let ordinary = matches!(route, RestorePhysicalRoute::OrdinaryUnit { .. });
        let owned = rows.is_owned_by(io);
        drop(decode_with_reservation(
            io,
            route,
            Some(DIRECTORY_FIXED_ALLOCATION_BYTES),
            || {
                if !ordinary || !owned || !valid_raw_digest(output_id) {
                    return Err(physical_backpressure(
                        "restore output requires ordinary owned rows and a frozen identity",
                    ));
                }
                Ok(())
            },
        )?);
        let segment = encode_standard_restore_output(io, route, rows.value())?;
        let index = build_restore_output_index(
            io,
            route,
            output_id,
            logical_sequence,
            rows.value(),
            segment.value(),
        )?;
        let index_bytes =
            encode_restore_output_metadata(io, route, RestoreOutputMetadata::Index(index.value()))?;
        let segment_path = declare_restore_output(
            io,
            route,
            RestoreOutputObject::Segment(output_id),
            segment.value().len(),
        )?;
        let index_path = declare_restore_output(
            io,
            route,
            RestoreOutputObject::Index(output_id),
            index_bytes.value().len(),
        )?;
        let segment_version = put_restore_output(io, route, segment_path.value(), &segment).await?;
        let index_version = put_restore_output(io, route, index_path.value(), &index_bytes).await?;
        let descriptor = build_restore_output_descriptor(
            io,
            route,
            index.value(),
            index_bytes.value(),
            restore_output_version(&segment_version.value),
            restore_output_version(&index_version.value),
        )?;
        let bytes = encode_restore_output_metadata(
            io,
            route,
            RestoreOutputMetadata::Descriptor(descriptor.value()),
        )?;
        let digest = allocate_with_reservation(
            io,
            route,
            Some(DIRECTORY_FIXED_ALLOCATION_BYTES),
            true,
            || {
                let hash = super::super::sha256_hex(bytes.value());
                let mut digest = [0; 32];
                hex::decode_to_slice(hash, &mut digest)
                    .map_err(|_| super::invariant_violation("restore output digest is invalid"))?;
                Ok(digest)
            },
        )?;
        let descriptor_path = declare_restore_output(
            io,
            route,
            RestoreOutputObject::Descriptor(digest.value()),
            bytes.value().len(),
        )?;
        let _descriptor_version =
            put_restore_output(io, route, descriptor_path.value(), &bytes).await?;
        Ok(StandardRestoreOutput {
            descriptor,
            bytes,
            digest: *digest.value(),
        })
    }
    .await;
    if result.is_err() {
        io.stop(route);
    }
    result
}

fn build_restore_output_index(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    segment_id: &str,
    logical_sequence: u64,
    rows: &[super::ControlMvpSegmentRow],
    bytes: &[u8],
) -> CatalogResult<WorkingValue<super::ControlMvpSegmentIndex>> {
    let result = (|| {
        let capacity = || physical_backpressure("restore output index model exceeds admission");
        let (envelope, _) = standard_output_reservation(rows)?;
        if size_of::<super::ControlMvpSegmentIndex>() > 512
            || size_of::<ControlMvpBlock>() > 192
            || !super::super::integrity::valid_immutable_id(segment_id)
            || bytes.is_empty()
            || bytes.len() > envelope
            || rows
                .iter()
                .any(|row| row.record_kind != super::super::SEGMENT_RECORD_KV)
            || rows.windows(2).any(|pair| match pair {
                [first, last] => {
                    first.key >= last.key
                        || first.logical_ordinal.checked_add(1) != Some(last.logical_ordinal)
                }
                _ => true,
            })
        {
            return Err(capacity());
        }
        let endpoints = rows
            .first()
            .ok_or_else(capacity)?
            .key
            .len()
            .checked_add(rows.last().ok_or_else(capacity)?.key.len())
            .ok_or_else(capacity)?;
        let bloom = rows
            .len()
            .checked_mul(10)
            .and_then(|bits| bits.checked_add(7))
            .ok_or_else(capacity)?
            / 8;
        let store = io.store;
        let reservation = [
            64 * 1024,
            rows.len().checked_mul(64).ok_or_else(capacity)?,
            bloom.checked_mul(3).ok_or_else(capacity)?,
            endpoints.checked_mul(5).ok_or_else(capacity)?,
            store.scope.tenant_id().len(),
            store.scope.workspace_id().ok_or_else(capacity)?.len(),
            store.scope.domain().len(),
            segment_id.len(),
            128,
        ]
        .into_iter()
        .try_fold(0_usize, |sum, term| {
            sum.checked_add(term).ok_or_else(capacity)
        })?;
        allocate_with_reservation(io, route, Some(reservation), true, || {
            read_cache::validate_rows_for_sequence(
                ControlMvpSegmentLevel::L1,
                logical_sequence,
                rows,
            )?;
            let block = super::super::block_metadata(0, bytes, rows);
            super::super::build_segment_index(
                segment_id,
                ControlMvpSegmentLevel::L1,
                logical_sequence,
                &store.scope,
                rows,
                bytes,
                super::super::sha256_hex(bytes),
                vec![block],
            )
        })
    })();
    if result.is_err() {
        io.stop(route);
    }
    result
}

fn build_restore_output_descriptor(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    index: &super::ControlMvpSegmentIndex,
    index_bytes: &[u8],
    segment_version: &str,
    index_version: &str,
) -> CatalogResult<WorkingValue<super::Descriptor>> {
    let result = (|| {
        let capacity =
            || physical_backpressure("restore output descriptor model exceeds admission");
        let [block] = index.blocks.as_slice() else {
            return Err(capacity());
        };
        if usize::BITS != 64
            || size_of::<super::Descriptor>() > 520
            || size_of::<super::ControlMvpSegmentIndex>() > 512
            || size_of::<ControlMvpBlock>() > 192
            || index.scope != io.store.scope
            || index.level != ControlMvpSegmentLevel::L1
            || block.record_kind != Some(super::super::SEGMENT_RECORD_KV)
            || block.row_count == 0
            || block.row_count != index.row_count
            || block.offset != 0
            || block.length != index.segment_size_bytes
            || index.segment_size_bytes > MAX_BLOCK_BYTES as u64
            || !valid_raw_digest(&index.segment_checksum_sha256)
            || !valid_raw_digest(&block.checksum_sha256)
            || !super::super::integrity::valid_immutable_id(&index.segment_id)
            || segment_version.is_empty()
            || index_version.is_empty()
        {
            return Err(capacity());
        }
        admitted_physical_length(index_bytes.len(), MAX_SEGMENT_INDEX_BYTES)?;
        let first = block.min_key_hex.as_ref().ok_or_else(capacity)?;
        let last = block.max_key_hex.as_ref().ok_or_else(capacity)?;
        let reservation = [
            64 * 1024,
            index.scope.tenant_id().len(),
            index.scope.workspace_id().ok_or_else(capacity)?.len(),
            index.scope.domain().len(),
            index.segment_id.len(),
            segment_version.len(),
            index_version.len(),
            first.len(),
            last.len(),
            192,
        ]
        .into_iter()
        .try_fold(0_usize, |sum, term| {
            sum.checked_add(term).ok_or_else(capacity)
        })?;
        allocate_with_reservation(io, route, Some(reservation), true, || {
            Ok(super::Descriptor {
                encoding_version: 1,
                scope: index.scope.clone(),
                role: super::Role::Kv,
                segment: ControlMvpSegmentRef {
                    segment_size_bytes: index.segment_size_bytes,
                    index_size_bytes: u64::try_from(index_bytes.len()).map_err(|_| {
                        physical_backpressure("restore output index length overflow")
                    })?,
                    segment_id: index.segment_id.clone(),
                    level: index.level,
                    logical_sequence: index.logical_sequence,
                    checksum_sha256: index.segment_checksum_sha256.clone(),
                    index_checksum_sha256: super::super::sha256_hex(index_bytes),
                },
                segment_version: segment_version.to_owned(),
                index_version: index_version.to_owned(),
                block: block.clone(),
            })
        })
    })();
    if result.is_err() {
        io.stop(route);
    }
    result
}

#[derive(Clone, Copy)]
enum RestoreOutputMetadata<'a> {
    Index(&'a super::ControlMvpSegmentIndex),
    Descriptor(&'a super::Descriptor),
}

#[allow(
    clippy::redundant_clone,
    reason = "promote exact-capacity Bytes backing inside the measured allocation guard"
)]
fn encode_restore_output_metadata(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    metadata: RestoreOutputMetadata<'_>,
) -> CatalogResult<WorkingValue<Bytes>> {
    let result = (|| {
        if usize::BITS != 64 {
            return Err(physical_backpressure(
                "restore output target is unsupported",
            ));
        }
        let cap = match &metadata {
            RestoreOutputMetadata::Index(_)
                if size_of::<super::ControlMvpSegmentIndex>() <= 512
                    && size_of::<ControlMvpBlock>() <= 192 =>
            {
                MAX_SEGMENT_INDEX_BYTES
            }
            RestoreOutputMetadata::Descriptor(_) if size_of::<super::Descriptor>() <= 520 => {
                MAX_CONTROL_JSON_BYTES
            }
            _ => {
                return Err(physical_backpressure(
                    "restore output metadata layout is unsupported",
                ));
            }
        };
        let reservation = cap
            .checked_add(64 * 1024)
            .ok_or_else(|| physical_backpressure("restore output metadata allocation overflow"))?;
        allocate_with_reservation(io, route, Some(reservation), true, || {
            let mut encoded = vec![0; cap];
            let length = {
                // The slice writer cannot grow even when escaped fields exceed cap.
                let mut writer = std::io::Cursor::new(encoded.as_mut_slice());
                match metadata {
                    RestoreOutputMetadata::Index(value) => {
                        serde_json::to_writer(&mut writer, value)
                    }
                    RestoreOutputMetadata::Descriptor(value) => {
                        serde_json::to_writer(&mut writer, value)
                    }
                }
                .map_err(|_| {
                    physical_backpressure("restore output metadata exceeds its encoded cap")
                })?;
                usize::try_from(writer.position())
                    .map_err(|_| physical_backpressure("restore output metadata length overflow"))?
            };
            encoded.truncate(length);
            let bytes = Bytes::from(encoded);
            // Promote exact-capacity backing while its header allocation is
            // measured, so later immutable-write handle clones do not allocate.
            Ok(bytes.clone())
        })
    })();
    if result.is_err() {
        io.stop(route);
    }
    result
}

#[cfg(test)]
mod output_metadata_tests {
    use super::*;

    #[tokio::test]
    async fn restore_output_model_rejects_invalid_l1_generations_payloads_and_ordinals() {
        let (fixture, descriptor, _) = super::super::tests::fixture().await;
        let raw = fixture
            .retention
            .get_raw(&fixture.paths.state_object(&descriptor.segment.segment_id))
            .await
            .expect("segment");
        let original =
            super::super::decode_block_rows(&raw, &descriptor.block, descriptor.scope.domain())
                .expect("rows");
        let store = ControlMvpStateStore::new_synthetic_bounded(
            fixture.retention.clone(),
            fixture.scope.clone(),
        )
        .expect("store");
        let mut observations = Vec::new();
        for case in 0..8 {
            let mut rows = vec![original[0].clone(), original[0].clone()];
            rows[1].key = b"next".to_vec();
            rows[1].logical_ordinal = 1;
            match case {
                0 => rows[0].generation = 0,
                1 => rows[0].generation = 3,
                2 => rows[0].logical_sequence = 3,
                3 => rows[0].origin_sequence = Some(1),
                4 => rows[0].tombstone = true,
                5 => rows[0].value = None,
                6 => rows[1].logical_ordinal = 2,
                _ => {
                    rows[0].logical_ordinal = u64::MAX;
                    rows[1].logical_ordinal = 0;
                }
            }
            {
                let final_stream = false; // Control transport is ordinary-only; rejection has separate coverage.
                let mut io =
                    RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
                let mut totals = FinalStreamTotals::new();
                let mut chunk = FinalMicrochunk::begin(&mut totals, 4096, &mut io).expect("chunk");
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut route = if final_stream {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload,
                    }
                };
                let owned = decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
                    Ok(rows.clone())
                })
                .expect("input owner");
                let result =
                    write_standard_restore_output(&mut io, &mut route, &"79".repeat(32), 2, &owned)
                        .await;
                assert!(
                    (1..=2).contains(&io.work.encode_operations),
                    "must reach semantic validation before any metadata publication"
                );
                observations.push((case, final_stream, result.is_err(), io.stopped));
                assert_eq!(
                    io.work.write_attempts + io.work.metadata_heads + io.work.range_reads,
                    0
                );
            }
        }
        for (case, final_stream, rejected, stopped) in observations {
            assert!(
                rejected && stopped,
                "invalid output accepted: case={case} final={final_stream}"
            );
        }
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "one matrix binds production parity and live model ownership on both routes"
    )]
    async fn restore_output_models_match_production_with_dense_rows_endpoints_and_versions() {
        let (fixture, existing, _) = super::super::tests::fixture().await;
        let store =
            ControlMvpStateStore::new_synthetic_bounded(fixture.retention.clone(), fixture.scope)
                .expect("store");
        for (count, key_len, value_len) in [
            (1_usize, 8, 200_000),
            (4096, 8, 0),
            (1, 50_000, 0),
            (1, 80_000, 0),
        ] {
            let rows = (0..count)
                .map(|i| {
                    let mut key = vec![b'k'; key_len];
                    key[..8].copy_from_slice(&(i as u64).to_be_bytes());
                    super::super::ControlMvpSegmentRow {
                        record_kind: 0,
                        key,
                        value: (i % 2 == 0).then(|| vec![0xff; value_len]),
                        generation: 1,
                        tombstone: i % 2 != 0,
                        logical_sequence: 2,
                        logical_ordinal: i as u64,
                        origin_sequence: None,
                    }
                })
                .collect::<Vec<_>>();
            let raw = super::super::super::encode_arrow_block(&rows).expect("production Arrow");
            let block = super::super::super::block_metadata(0, &raw, &rows);
            let expected = super::super::super::build_segment_index(
                &existing.segment.segment_id,
                ControlMvpSegmentLevel::L1,
                2,
                &store.scope,
                &rows,
                &raw,
                super::super::super::sha256_hex(&raw),
                vec![block],
            )
            .expect("production index");
            let segment_version = "v".repeat(200_000);
            let index_version = "w".repeat(300_000);
            for final_stream in [false, true] {
                let mut io =
                    RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
                let ledger = io.ledger.clone();
                let mut totals = FinalStreamTotals::new();
                let mut chunk = FinalMicrochunk::begin(&mut totals, 4096, &mut io).expect("chunk");
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut route = if final_stream {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload,
                    }
                };
                let index = build_restore_output_index(
                    &mut io,
                    &mut route,
                    &existing.segment.segment_id,
                    2,
                    &rows,
                    &raw,
                )
                .expect("index");
                assert_eq!(index.value(), &expected);
                let expected_json =
                    super::super::super::encode_json(&expected, "index").expect("production JSON");
                let encoded = encode_restore_output_metadata(
                    &mut io,
                    &mut route,
                    RestoreOutputMetadata::Index(index.value()),
                );
                if expected_json.len() > MAX_SEGMENT_INDEX_BYTES {
                    assert!(matches!(
                        encoded,
                        Err(CatalogError::MaintenanceBackpressure { .. })
                    ));
                    assert!(io.stopped);
                    drop(index);
                    drop(io);
                    assert!(ledger.lock().expect("ledger").report().passing());
                    continue;
                }
                let encoded = encoded.expect("index JSON");
                assert_eq!(encoded.value().as_ref(), expected_json);
                let descriptor = build_restore_output_descriptor(
                    &mut io,
                    &mut route,
                    index.value(),
                    encoded.value(),
                    &segment_version,
                    &index_version,
                )
                .expect("descriptor");
                assert_eq!(descriptor.value().block, expected.blocks[0]);
                assert_eq!(descriptor.value().scope, expected.scope);
                assert_eq!(descriptor.value().segment_version, segment_version);
                assert_eq!(descriptor.value().index_version, index_version);
                assert_eq!(
                    descriptor.value().segment.checksum_sha256,
                    expected.segment_checksum_sha256
                );
                assert_eq!(
                    descriptor.value().segment.index_checksum_sha256,
                    super::super::super::sha256_hex(encoded.value())
                );
                super::super::super::validate_segment_index_identity(
                    index.value(),
                    &descriptor.value().segment,
                    &store.scope,
                )
                .expect("production identity");
                read_cache::validate_rows(&descriptor.value().segment, &rows)
                    .expect("production row semantics");
                assert_eq!(
                    super::super::decode_block_rows(
                        &raw,
                        &descriptor.value().block,
                        descriptor.value().scope.domain(),
                    )
                    .expect("decode"),
                    rows
                );
                let descriptor_bytes = encode_restore_output_metadata(
                    &mut io,
                    &mut route,
                    RestoreOutputMetadata::Descriptor(descriptor.value()),
                )
                .expect("descriptor JSON");
                assert_eq!(
                    descriptor_bytes.value().as_ref(),
                    super::super::super::encode_json(descriptor.value(), "descriptor")
                        .expect("production descriptor")
                );
                println!(
                    "restore output models: rows={count} key_len={key_len} final={final_stream} index_owned={} descriptor_owned={} all_allocations={}",
                    index.working.bytes,
                    descriptor.working.bytes,
                    io.work.encoded_owned_allocation_bytes
                );
                assert_eq!(io.work.encode_operations, 4);
                assert_eq!(
                    io.work.decode_operations + io.work.metadata_heads + io.work.range_reads,
                    0
                );
                drop((descriptor_bytes, descriptor, encoded, index));
                drop(io);
                assert!(ledger.lock().expect("ledger").report().passing());
            }
        }
    }

    #[tokio::test]
    async fn restore_output_descriptor_model_reserves_before_construction() {
        let (fixture, descriptor, _) = super::super::tests::fixture().await;
        let raw = fixture
            .retention
            .get_raw(&fixture.paths.segment_index(&descriptor.segment.segment_id))
            .await
            .expect("index");
        let index: super::super::ControlMvpSegmentIndex =
            super::super::decode_json(&raw, "fixture index").expect("index");
        let store = ControlMvpStateStore::new_synthetic_bounded(
            fixture.retention.clone(),
            fixture.scope.clone(),
        )
        .expect("store");
        let mut observations = Vec::new();
        for final_stream in [false, true] {
            let allowance = 16 * 1024;
            let mut io = RestorePhysicalIo::new(
                &store,
                if final_stream {
                    FINAL_MICROCHUNK_BYTES
                } else {
                    allowance
                },
                FINAL_MICROCHUNK_BYTES,
            );
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(
                &mut totals,
                if final_stream {
                    FINAL_MICROCHUNK_BYTES - allowance
                } else {
                    0
                },
                &mut io,
            )
            .expect("chunk");
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = if final_stream {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            let result = build_restore_output_descriptor(
                &mut io,
                &mut route,
                &index,
                &raw,
                &descriptor.segment_version,
                &descriptor.index_version,
            );
            observations.push((
                final_stream,
                matches!(result, Err(CatalogError::MaintenanceBackpressure { .. })),
                io.work.encode_operations,
                io.work.decode_operations,
                io.stopped,
            ));
            assert_eq!(io.work.metadata_heads + io.work.range_reads, 0);
        }
        for (final_stream, rejected, encodes, decodes, stopped) in observations {
            assert!(
                rejected && stopped && encodes == 0 && decodes == 0,
                "descriptor model reservation missing: final={final_stream} rejected={rejected} encodes={encodes} decodes={decodes} stopped={stopped}"
            );
        }
    }

    #[tokio::test]
    async fn restore_output_index_model_reserves_before_construction() {
        let (fixture, descriptor, _) = super::super::tests::fixture().await;
        let raw = fixture
            .retention
            .get_raw(&fixture.paths.state_object(&descriptor.segment.segment_id))
            .await
            .expect("segment");
        let rows =
            super::super::decode_block_rows(&raw, &descriptor.block, descriptor.scope.domain())
                .expect("rows");
        let store = ControlMvpStateStore::new_synthetic_bounded(
            fixture.retention.clone(),
            fixture.scope.clone(),
        )
        .expect("store");
        let mut observations = Vec::new();
        for final_stream in [false, true] {
            let allowance = 16 * 1024;
            let mut io = RestorePhysicalIo::new(
                &store,
                if final_stream {
                    FINAL_MICROCHUNK_BYTES
                } else {
                    allowance
                },
                FINAL_MICROCHUNK_BYTES,
            );
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(
                &mut totals,
                if final_stream {
                    FINAL_MICROCHUNK_BYTES - allowance
                } else {
                    0
                },
                &mut io,
            )
            .expect("chunk");
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = if final_stream {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            let result = build_restore_output_index(
                &mut io,
                &mut route,
                &descriptor.segment.segment_id,
                descriptor.segment.logical_sequence,
                &rows,
                &raw,
            );
            observations.push((
                final_stream,
                matches!(result, Err(CatalogError::MaintenanceBackpressure { .. })),
                io.work.encode_operations,
                io.work.decode_operations,
                io.stopped,
            ));
            assert_eq!(io.work.metadata_heads + io.work.range_reads, 0);
        }
        for (final_stream, rejected, encodes, decodes, stopped) in observations {
            assert!(
                rejected && stopped && encodes == 0 && decodes == 0,
                "index model reservation missing: final={final_stream} rejected={rejected} encodes={encodes} decodes={decodes} stopped={stopped}"
            );
        }
    }

    #[tokio::test]
    async fn restore_output_metadata_owner_survives_await_and_releases_on_cancellation() {
        let (fixture, descriptor, _) = super::super::tests::fixture().await;
        let raw = fixture
            .retention
            .get_raw(&fixture.paths.segment_index(&descriptor.segment.segment_id))
            .await
            .expect("index");
        let index: super::super::ControlMvpSegmentIndex =
            super::super::decode_json(&raw, "fixture index").expect("index");
        let store = ControlMvpStateStore::new_synthetic_bounded(
            fixture.retention.clone(),
            fixture.scope.clone(),
        )
        .expect("store");
        assert!(size_of::<super::super::Descriptor>() <= 520);
        assert!(size_of::<super::super::ControlMvpSegmentIndex>() <= 512);
        assert!(size_of::<ControlMvpBlock>() <= 192);
        for is_index in [false, true] {
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let ledger = io.ledger.clone();
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let metadata = if is_index {
                RestoreOutputMetadata::Index(&index)
            } else {
                RestoreOutputMetadata::Descriptor(&descriptor)
            };
            let output =
                encode_restore_output_metadata(&mut io, &mut route, metadata).expect("encode");
            let owned = output.working.bytes;
            let (started, entered) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move {
                started.send(()).expect("signal");
                std::future::pending::<()>().await;
                drop(output);
            });
            entered.await.expect("entered");
            assert_eq!(
                ledger.lock().expect("ledger").report().working_live_bytes,
                owned
            );
            task.abort();
            assert!(task.await.expect_err("cancelled").is_cancelled());
            drop(io);
            assert!(ledger.lock().expect("ledger").report().passing());
        }
    }

    #[tokio::test]
    async fn restore_output_metadata_matches_production_and_retains_backing_capacity() {
        let (fixture, mut descriptor, _) = super::super::tests::fixture().await;
        let index_raw = fixture
            .retention
            .get_raw(&fixture.paths.segment_index(&descriptor.segment.segment_id))
            .await
            .expect("index");
        let mut index: super::super::ControlMvpSegmentIndex =
            super::super::decode_json(&index_raw, "fixture index").expect("index");
        let store = ControlMvpStateStore::new_synthetic_bounded(
            fixture.retention.clone(),
            fixture.scope.clone(),
        )
        .expect("store");
        for shape in 0..3 {
            if shape == 1 {
                descriptor.segment_version = "opaque \0\n\"\\🙂".into();
                index.scope.domain = "domain \0\n\"\\🙂".into();
            } else if shape == 2 {
                descriptor.segment_version.clear();
                index.scope.domain.clear();
                let descriptor_len = super::super::super::encode_json(&descriptor, "fixture")
                    .expect("encode")
                    .len();
                let index_len = super::super::super::encode_json(&index, "fixture")
                    .expect("encode")
                    .len();
                descriptor.segment_version = "v".repeat(MAX_CONTROL_JSON_BYTES - descriptor_len);
                index.scope.domain = "d".repeat(MAX_SEGMENT_INDEX_BYTES - index_len);
            }
            for final_stream in [false, true] {
                for is_index in [false, true] {
                    let cap = if is_index {
                        MAX_SEGMENT_INDEX_BYTES
                    } else {
                        MAX_CONTROL_JSON_BYTES
                    };
                    let expected = if is_index {
                        super::super::super::encode_json(&index, "fixture")
                    } else {
                        super::super::super::encode_json(&descriptor, "fixture")
                    }
                    .expect("production");
                    if shape == 2 {
                        assert_eq!(expected.len(), cap);
                    }
                    let mut io = RestorePhysicalIo::new(
                        &store,
                        FINAL_MICROCHUNK_BYTES,
                        FINAL_MICROCHUNK_BYTES,
                    );
                    let ledger = io.ledger.clone();
                    let mut totals = FinalStreamTotals::new();
                    let mut chunk =
                        FinalMicrochunk::begin(&mut totals, 4096, &mut io).expect("carry");
                    let mut workspace = WorkspaceIoBudget::new();
                    let mut payload = UnitPayloadAdmission::new();
                    let mut route = if final_stream {
                        RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                    } else {
                        RestorePhysicalRoute::OrdinaryUnit {
                            workspace: &mut workspace,
                            payload: &mut payload,
                        }
                    };
                    let metadata = if is_index {
                        RestoreOutputMetadata::Index(&index)
                    } else {
                        RestoreOutputMetadata::Descriptor(&descriptor)
                    };
                    let output = encode_restore_output_metadata(&mut io, &mut route, metadata)
                        .expect("bounded encoding");
                    assert_eq!(output.value, expected);
                    assert!(output.working.bytes >= cap);
                    assert!(output.working.bytes <= cap + 64 * 1024);
                    assert_eq!(io.work.encode_operations, 1);
                    assert_eq!(io.work.decode_operations, 0);
                    let promotion = allocation_counter::measure(|| {
                        let handle = output.value.clone();
                        drop(handle);
                    });
                    assert_eq!(
                        promotion.bytes_total, 0,
                        "later handles already share admitted backing"
                    );
                    println!(
                        "restore output metadata: index={is_index} shape={shape} final={final_stream} encoded={} backing={cap} allocated={}",
                        output.value.len(),
                        io.work.encoded_owned_allocation_bytes
                    );
                    drop(output);
                    drop(io);
                    assert!(ledger.lock().expect("ledger").report().passing());
                }
            }
        }
    }

    #[tokio::test]
    async fn restore_output_metadata_reserves_before_encoding_and_enforces_cap() {
        let (fixture, mut descriptor, _) = super::super::tests::fixture().await;
        let index_raw = fixture
            .retention
            .get_raw(&fixture.paths.segment_index(&descriptor.segment.segment_id))
            .await
            .expect("index");
        let mut index: super::super::ControlMvpSegmentIndex =
            super::super::decode_json(&index_raw, "fixture index").expect("index");
        let store = ControlMvpStateStore::new_synthetic_bounded(
            fixture.retention.clone(),
            fixture.scope.clone(),
        )
        .expect("store");
        let mut observations = Vec::new();
        for oversized in [false, true] {
            if oversized {
                descriptor.segment_version = "\n".repeat(MAX_CONTROL_JSON_BYTES);
                index.implementation = "\n".repeat(MAX_SEGMENT_INDEX_BYTES);
            }
            for final_stream in [false, true] {
                for is_index in [false, true] {
                    let allowance = if oversized {
                        FINAL_MICROCHUNK_BYTES
                    } else {
                        64 * 1024
                    };
                    let mut io = RestorePhysicalIo::new(
                        &store,
                        if final_stream {
                            FINAL_MICROCHUNK_BYTES
                        } else {
                            allowance
                        },
                        FINAL_MICROCHUNK_BYTES,
                    );
                    let mut totals = FinalStreamTotals::new();
                    let mut chunk = FinalMicrochunk::begin(
                        &mut totals,
                        if final_stream {
                            FINAL_MICROCHUNK_BYTES - allowance
                        } else {
                            0
                        },
                        &mut io,
                    )
                    .expect("chunk");
                    let mut workspace = WorkspaceIoBudget::new();
                    let mut payload = UnitPayloadAdmission::new();
                    let mut route = if final_stream {
                        RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                    } else {
                        RestorePhysicalRoute::OrdinaryUnit {
                            workspace: &mut workspace,
                            payload: &mut payload,
                        }
                    };
                    let metadata = if is_index {
                        RestoreOutputMetadata::Index(&index)
                    } else {
                        RestoreOutputMetadata::Descriptor(&descriptor)
                    };
                    let result = encode_restore_output_metadata(&mut io, &mut route, metadata);
                    observations.push((
                        oversized,
                        final_stream,
                        is_index,
                        matches!(result, Err(CatalogError::MaintenanceBackpressure { .. })),
                        io.work.encode_operations,
                        io.work.decode_operations,
                        io.stopped,
                    ));
                    assert_eq!(io.work.metadata_heads + io.work.range_reads, 0);
                }
            }
        }
        for (oversized, final_stream, is_index, rejected, encodes, decodes, stopped) in observations
        {
            assert!(
                rejected && stopped && decodes == 0 && encodes == u64::from(oversized),
                "output metadata admission missing: oversized={oversized} final={final_stream} index={is_index} rejected={rejected} encodes={encodes} decodes={decodes} stopped={stopped}"
            );
        }
    }
}

/// One existing Arrow encoder call, admitted before any builders are created.
#[allow(
    clippy::redundant_clone,
    reason = "promote exact-capacity Bytes backing inside the measured allocation guard"
)]
fn encode_standard_restore_output(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    rows: &[super::ControlMvpSegmentRow],
) -> CatalogResult<WorkingValue<Bytes>> {
    let result = (|| {
        let (envelope, reservation) = standard_output_reservation(rows)?;
        allocate_with_reservation(io, route, Some(reservation), true, || {
            let bytes = super::super::encode_arrow_block(rows)?;
            if bytes.len() > envelope {
                return Err(super::invariant_violation(
                    "restore output exceeded its encoded envelope",
                ));
            }
            let bytes = Bytes::from(bytes);
            Ok(bytes.clone())
        })
    })();
    if result.is_err() {
        io.stop(route);
    }
    result
}

pub(in super::super) fn standard_output_reservation(
    rows: &[super::ControlMvpSegmentRow],
) -> CatalogResult<(usize, usize)> {
    let capacity = || {
        physical_backpressure("standard restore output exceeds its allocation or encoded admission")
    };
    let row_count = rows.len();
    // Bound the metadata-only scan before examining any row, including invalid
    // caller slices. The file envelope rejects more tightly below.
    if usize::BITS != 64 || row_count == 0 || row_count > MAX_BLOCK_BYTES / 41 {
        return Err(capacity());
    }
    let bitmap_bytes = row_count.checked_add(7).ok_or_else(capacity)? / 8;
    let aligned_bitmap_bytes = bitmap_bytes
        .checked_add(63)
        .and_then(|bitmap_bytes| (bitmap_bytes / 64).checked_mul(64))
        .ok_or_else(capacity)?;
    let (key_bytes, value_bytes) =
        rows.iter()
            .try_fold((0_usize, 0_usize), |(key_bytes, value_bytes), row| {
                Ok::<_, CatalogError>((
                    key_bytes.checked_add(row.key.len()).ok_or_else(capacity)?,
                    value_bytes
                        .checked_add(row.value.as_ref().map_or(0, Vec::len))
                        .ok_or_else(capacity)?,
                ))
            })?;
    let sum = |terms: &[usize]| {
        terms
            .iter()
            .try_fold(0_usize, |a, b| a.checked_add(*b).ok_or_else(capacity))
    };
    let envelope = sum(&[
        key_bytes,
        value_bytes,
        row_count.checked_mul(41).ok_or_else(capacity)?,
        8,
        bitmap_bytes.checked_mul(9).ok_or_else(capacity)?,
        18 * 63,
        49_376,
    ])?;
    if envelope > MAX_BLOCK_BYTES {
        return Err(capacity());
    }
    let reservation = sum(&[
        3 * 1024 * 1024 + 64 * 1024,
        key_bytes.checked_mul(8).ok_or_else(capacity)?,
        value_bytes.checked_mul(8).ok_or_else(capacity)?,
        row_count.checked_mul(328).ok_or_else(capacity)?,
        bitmap_bytes.checked_mul(48).ok_or_else(capacity)?,
        aligned_bitmap_bytes.checked_mul(8).ok_or_else(capacity)?,
    ])?;
    Ok((envelope, reservation))
}

#[cfg(test)]
mod owned_encode_tests {
    use super::super::super::{
        ControlMvpSegmentRow, block_metadata, decode_block_rows, encode_arrow_block,
        preflight_ipc_message, verified_arrow_footer,
    };
    use super::*;

    fn store() -> ControlMvpStateStore {
        ControlMvpStateStore::new_synthetic_bounded(
            arco_core::ScopedStorage::new(
                Arc::new(arco_core::MemoryBackend::new()),
                "tenant",
                "workspace",
            )
            .expect("scope"),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("synthetic store")
    }

    pub(super) fn rows(
        n: usize,
        key_bytes: usize,
        value_bytes: usize,
        null_at: Option<usize>,
    ) -> Vec<ControlMvpSegmentRow> {
        (0..n)
            .map(|i| {
                let mut key = vec![b'k'; key_bytes];
                key[..8].copy_from_slice(&(i as u64).to_be_bytes());
                ControlMvpSegmentRow {
                    record_kind: 0,
                    key,
                    value: (null_at != Some(i)).then(|| vec![b'v'; value_bytes]),
                    generation: 1,
                    tombstone: null_at == Some(i),
                    logical_sequence: 1,
                    logical_ordinal: i as u64,
                    origin_sequence: None,
                }
            })
            .collect()
    }

    #[test]
    fn restore_output_arrow_handle_clone_is_allocation_free_and_keeps_charge() {
        let store = store();
        for final_stream in [false, true] {
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 4096, &mut io).expect("chunk");
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = if final_stream {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            let output =
                encode_standard_restore_output(&mut io, &mut route, &rows(32, 8, 32, None))
                    .expect("encode");
            let before = io
                .ledger
                .lock()
                .expect("ledger")
                .report()
                .working_live_bytes;
            let mut cloned = None;
            let allocations = allocation_counter::measure(|| {
                cloned = Some(output.value().clone());
            });
            assert_eq!(
                allocations.bytes_total, 0,
                "output handle allocated beyond its guard"
            );
            assert_eq!(cloned.as_ref().expect("clone"), output.value());
            assert_eq!(
                io.ledger
                    .lock()
                    .expect("ledger")
                    .report()
                    .working_live_bytes,
                before
            );
            drop(cloned);
            drop(output);
            assert!(io.ledger.lock().expect("ledger").report().passing());
        }
    }

    fn check_framing(bytes: &[u8]) {
        let (footer, footer_start) = verified_arrow_footer(bytes).expect("footer");
        assert_eq!(footer.version(), arrow::ipc::MetadataVersion::V5);
        assert!(bytes.len() - 10 - footer_start <= 16 * 1024);
        assert!(footer.custom_metadata().is_none());
        assert!(footer.dictionaries().is_none_or(|blocks| blocks.is_empty()));
        let batches = footer.recordBatches().expect("batches");
        assert_eq!(batches.len(), 1);
        let block = batches.get(0);
        assert!(block.metaDataLength() <= 16 * 1024);
        let offset = usize::try_from(block.offset()).expect("offset");
        let message = preflight_ipc_message(
            bytes,
            offset,
            usize::try_from(block.metaDataLength()).expect("metadata length"),
        )
        .expect("batch");
        validate_restore_arrow_message(message).expect("fixed batch grammar");
        assert!(
            message
                .header_as_record_batch()
                .expect("batch")
                .compression()
                .is_none()
        );
        let schema = preflight_ipc_message(bytes, 64, offset - 64).expect("schema message");
        assert_eq!(schema.version(), arrow::ipc::MetadataVersion::V5);
        assert!(schema.custom_metadata().is_none());
        validate_restore_arrow_schema(schema.header_as_schema().expect("schema"))
            .expect("fixed schema");
        assert!(offset - 64 <= 16 * 1024);
        preflight_restore_arrow_segment(bytes).expect("production restore grammar");
        println!(
            "restore output framing: schema={} batch={} footer={}",
            offset - 64,
            block.metaDataLength(),
            bytes.len() - 10 - footer_start
        );
    }

    #[test]
    fn restore_standard_output_matches_production_for_boundaries_and_nulls() {
        let store = store();
        // Codec-layout fixtures exercise both nullable columns; these raw rows
        // do not establish directory membership or authenticated outbox authority.
        let origin_pattern = |null_index| {
            let mut input = rows(9, 8, 128, None);
            for (i, row) in input.iter_mut().enumerate() {
                row.record_kind = 1;
                row.generation = 0;
                row.origin_sequence = (i != null_index).then_some(1);
            }
            input
        };
        for input in [
            rows(1, 8, 211_568, None),
            rows(1, 200_000, 0, None),
            rows(4096, 8, 0, None),
            rows(9, 8, 128, Some(0)),
            rows(9, 8, 128, Some(8)),
            origin_pattern(0),
            origin_pattern(8),
        ] {
            let (envelope, admitted) = standard_output_reservation(&input).expect("admission");
            let mut io = RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, 0);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let output = encode_standard_restore_output(
                &mut io,
                &mut RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                },
                &input,
            )
            .expect("encode");
            assert!(output.value.len() <= envelope);
            assert!(
                output.working.bytes
                    >= encode_arrow_block(&input)
                        .expect("production backing")
                        .capacity()
            );
            assert!(output.working.bytes <= admitted);
            assert_eq!(io.work.encode_operations, 1);
            assert_eq!(io.work.decode_operations, 0);
            assert_eq!(
                output.value,
                encode_arrow_block(&input).expect("independent production encoding")
            );
            let block = block_metadata(0, &output.value, &input);
            assert_eq!(
                decode_block_rows(&output.value, &block, "catalog").expect("decoded rows"),
                input
            );
            check_framing(&output.value);
            println!(
                "restore output qualification: rows={} keys={} values={} value_nulls={} origin_nulls={} envelope={} admitted={} encoded={} capacity={} allocations={}",
                input.len(),
                input.iter().map(|row| row.key.len()).sum::<usize>(),
                input
                    .iter()
                    .map(|row| row.value.as_ref().map_or(0, Vec::len))
                    .sum::<usize>(),
                input.iter().filter(|row| row.value.is_none()).count(),
                input
                    .iter()
                    .filter(|row| row.origin_sequence.is_none())
                    .count(),
                envelope,
                admitted,
                output.value.len(),
                encode_arrow_block(&input)
                    .expect("production backing")
                    .capacity(),
                io.work.encoded_owned_allocation_bytes
            );
            drop(output);
            assert!(io.ledger.lock().expect("ledger").report().passing());
        }
        let exact = rows(1, 8, 211_568, None);
        assert_eq!(
            standard_output_reservation(&exact).expect("exact"),
            (262_144, 4_904_760)
        );
        assert!(standard_output_reservation(&rows(1, 8, 211_569, None)).is_err());
        assert!(standard_output_reservation(&[]).is_err());
    }

    #[test]
    fn restore_final_output_retains_cumulative_work_and_starting_carry() {
        let store = store();
        let input = rows(1, 8, 211_568, None);
        let mut io = RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, 0);
        let mut totals = FinalStreamTotals::new();
        let mut chunk =
            FinalMicrochunk::begin(&mut totals, 60 * 1024 * 1024, &mut io).expect("carry");
        assert!(
            encode_standard_restore_output(
                &mut io,
                &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
                &input
            )
            .is_err()
        );
        assert_eq!(io.work.encode_operations, 0);
        drop(chunk);
        assert!(totals.stopped);

        let mut io = RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, 0);
        let mut totals = FinalStreamTotals::new();
        let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
        let first = encode_standard_restore_output(
            &mut io,
            &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
            &input,
        )
        .expect("first");
        let first_allocated = io.work.encoded_owned_allocation_bytes;
        drop(first);
        let second = encode_standard_restore_output(
            &mut io,
            &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
            &input,
        )
        .expect("second");
        drop(second);
        assert_eq!(
            chunk.totals.total_owned_allocation_bytes,
            io.work.encoded_owned_allocation_bytes
        );
        assert!(chunk.totals.total_owned_allocation_bytes > first_allocated);
        assert!(io.ledger.lock().expect("ledger").report().passing());
    }

    #[test]
    fn restore_encoder_overrun_keeps_failed_allocation_and_mixed_final_totals() {
        let store = store();
        let input = rows(1, 8, 128, None);
        let mut io = RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, 0);
        let mut totals = FinalStreamTotals::new();
        let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
        let decoded = decode_owned(
            &mut io,
            &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
            || Ok(vec![1_u8; 1024]),
        )
        .expect("decoded carry");
        let output = encode_standard_restore_output(
            &mut io,
            &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
            &input,
        )
        .expect("encode");
        let total = io.work.decoded_owned_allocation_bytes + io.work.encoded_owned_allocation_bytes;
        assert_eq!(chunk.totals.total_owned_allocation_bytes, total);
        drop(output);
        drop(decoded);
        assert_eq!(chunk.totals.total_owned_allocation_bytes, total);
        assert!(io.ledger.lock().expect("ledger").report().passing());
        let before = io.work.encoded_owned_allocation_bytes;
        let failure = allocate_with_reservation(
            &mut io,
            &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
            Some(64),
            true,
            || Ok(vec![2_u8; 1024]),
        );
        assert!(matches!(
            failure,
            Err(CatalogError::MaintenanceBackpressure { .. })
        ));
        assert!(io.stopped);
        assert_eq!(io.work.encode_operations, 2);
        assert_eq!(io.work.decode_operations, 1);
        let error_capacity = match &failure {
            Err(error) => catalog_error_string_capacity(error).expect("known error"),
            Ok(_) => unreachable!("forced overrun must fail"),
        };
        assert_eq!(
            io.work.encoded_owned_allocation_bytes - before,
            1024 + error_capacity as u64
        );
        assert_eq!(
            chunk.totals.total_owned_allocation_bytes,
            io.work.decoded_owned_allocation_bytes + io.work.encoded_owned_allocation_bytes
        );
        assert!(
            io.failed_allocation
                .as_ref()
                .expect("failed encoder guard")
                .bytes
                >= 1024
        );
        let ledger = io.ledger.clone();
        drop(failure);
        drop(io);
        let report = ledger.lock().expect("ledger").report();
        assert_eq!(report.working_live_bytes, 0);
        // The overrun and its replacement error each expand the failed guard.
        assert_eq!(report.working_underestimates, 2);
        assert!(!report.passing());
    }

    #[test]
    fn restore_output_reservation_precedes_encoder_and_rejects_large_rows() {
        let store = store();
        let mut incorrectly_admitted = Vec::new();
        for (limit, value_bytes) in [(1024 * 1024, 1), (FINAL_MICROCHUNK_BYTES, MAX_BLOCK_BYTES)] {
            let rows = vec![ControlMvpSegmentRow {
                record_kind: 0,
                key: b"key".to_vec(),
                value: Some(vec![b'v'; value_bytes]),
                generation: 1,
                tombstone: false,
                logical_sequence: 1,
                logical_ordinal: 0,
                origin_sequence: None,
            }];
            let mut io = RestorePhysicalIo::new(&store, limit, FINAL_MICROCHUNK_BYTES);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let result = encode_standard_restore_output(&mut io, &mut route, &rows);
            if result.is_ok() {
                incorrectly_admitted.push((limit, value_bytes));
                continue;
            }
            assert!(matches!(
                result,
                Err(CatalogError::MaintenanceBackpressure { .. })
            ));
            assert_eq!(io.work.decode_operations, 0);
            assert_eq!(io.work.encode_operations, 0);
            assert_eq!(io.work.range_reads, 0);
            assert_eq!(io.work.metadata_heads, 0);
            assert!(io.stopped);
        }
        assert!(
            incorrectly_admitted.is_empty(),
            "output encoding lacked pre-admission: {incorrectly_admitted:?}"
        );
    }
}

#[cfg(test)]
mod owned_decode_tests {
    use super::*;

    #[test]
    fn restore_final_decode_cumulative_work_cannot_be_released_or_omit_starting_carry() {
        let store = ControlMvpStateStore::new_synthetic_bounded(
            arco_core::ScopedStorage::new(
                Arc::new(arco_core::MemoryBackend::new()),
                "tenant",
                "workspace",
            )
            .expect("scope"),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("synthetic store");
        let mib = 1024 * 1024;
        for (external, carried, first, second) in [(0, 0, 40, 40), (50, 0, 8, 8), (0, 20, 1, 50)] {
            let mut io = RestorePhysicalIo::new(&store, 64 * mib, 0);
            let carry = reserve_working_memory(io.ledger.clone(), carried * mib).expect("carry");
            let mut totals = FinalStreamTotals::new();
            {
                let mut chunk =
                    FinalMicrochunk::begin(&mut totals, external * mib, &mut io).expect("chunk");
                let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
                let first = decode_owned(&mut io, &mut route, || Ok(vec![1_u8; first * mib]))
                    .expect("first decode fits");
                drop(first);
                drop(carry);
                let second = decode_owned(&mut io, &mut route, || Ok(vec![2_u8; second * mib]));
                assert!(
                    second.is_err(),
                    "freed allocations and initial carry still count"
                );
                assert!(io.stopped);
                assert!(
                    decode_owned(&mut io, &mut route, || -> CatalogResult<()> {
                        panic!("stopped decoder must not execute");
                    })
                    .is_err()
                );
            }
            assert!(totals.stopped);
            assert_eq!(
                totals.total_owned_allocation_bytes,
                io.work.decoded_owned_allocation_bytes
            );
            assert!(
                totals.peak_chunk_owned_upper_bound_bytes
                    >= (external + carried + first + second) * mib
            );
            let ledger = io.ledger.clone();
            drop(io);
            assert_eq!(
                ledger.lock().expect("ledger").report().working_live_bytes,
                0
            );
        }
    }

    #[test]
    fn restore_decoder_retains_its_allocation_bound_without_releasing_other_owners() {
        let ledger = Arc::new(Mutex::new(RestoreOwnershipLedger::new(64, 0)));
        let mut first = reserve_working_memory(ledger.clone(), 40).expect("first");
        let second = reserve_working_memory(ledger.clone(), 24).expect("second");
        first.retain_allocation_bound(16).expect("shrink");
        assert_eq!(
            ledger.lock().expect("ledger").report().working_live_bytes,
            40
        );
        drop(first);
        assert_eq!(
            ledger.lock().expect("ledger").report().working_live_bytes,
            24
        );
        drop(second);
        assert!(ledger.lock().expect("ledger").report().passing());
    }

    #[test]
    fn restore_decoder_underestimate_records_arrived_memory_and_is_nonpassing() {
        let ledger = Arc::new(Mutex::new(RestoreOwnershipLedger::new(64, 0)));
        let mut working = reserve_working_memory(ledger.clone(), 40).expect("working");
        assert!(working.retain_allocation_bound(72).is_err());
        let report = ledger.lock().expect("ledger").report();
        assert_eq!(report.working_underestimates, 1);
        assert_eq!(report.request_owned_peak_bytes, 72);
        drop(working);
        let report = ledger.lock().expect("ledger").report();
        assert_eq!(report.working_live_bytes, 0);
        assert!(!report.passing());
    }

    #[tokio::test]
    async fn restore_decoded_value_keeps_its_actual_fresh_allocations_across_await() {
        use arco_core::{MemoryBackend, ScopedStorage};
        let store = ControlMvpStateStore::new_synthetic_bounded(
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
                .expect("scope"),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("synthetic store");
        let mut io = RestorePhysicalIo::new(&store, 1024, 1024);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let value = decode_owned(&mut io, &mut route, || {
            let mut bytes = Vec::with_capacity(64);
            bytes.extend_from_slice(b"owned output");
            Ok(bytes)
        })
        .expect("owned decoder");
        tokio::task::yield_now().await;
        assert_eq!(value.value, b"owned output");
        assert_eq!(value.working.bytes, 64);
        assert_eq!(
            io.ledger
                .lock()
                .expect("ledger")
                .report()
                .working_live_bytes,
            64
        );
        drop(value);
        assert!(io.ledger.lock().expect("ledger").report().passing());
    }
}

#[cfg(test)]
mod decode_error_ownership_tests {
    use super::*;

    #[test]
    fn rejected_reservation_final_carry_accounts_exactly_one_diagnostic() {
        let store = ControlMvpStateStore::new_synthetic_bounded(
            arco_core::ScopedStorage::new(
                Arc::new(arco_core::MemoryBackend::new()),
                "tenant",
                "workspace",
            )
            .expect("scope"),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store");
        let mut io = RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
        let mut totals = FinalStreamTotals::new();
        let mut chunk = FinalMicrochunk::begin(&mut totals, FINAL_MICROCHUNK_BYTES, &mut io)
            .expect("exact carry");
        let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
        let mut result = None;
        let measured = allocation_counter::measure(|| {
            result = Some(decode_with_reservation(
                &mut io,
                &mut route,
                Some(1),
                || -> CatalogResult<()> { panic!("refused closure") },
            ));
        });
        assert!(result.expect("called").is_err());
        assert_eq!(io.allocation_evidence().0, measured.bytes_total);
        assert_eq!(io.allocation_evidence().1 as u64, measured.bytes_total);
    }

    #[tokio::test]
    async fn restore_decode_zero_remaining_keeps_replacement_error_charged() {
        use arco_core::{MemoryBackend, ScopedStorage};
        let store = ControlMvpStateStore::new_synthetic_bounded(
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
                .expect("scope"),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("store");
        for limit in [0, 8] {
            let mut io = RestorePhysicalIo::new(&store, limit, 1024);
            // Lazy mutex allocation is invocation infrastructure, outside the
            // decoder invoice. Retain its separate measured setup evidence.
            let setup =
                allocation_counter::measure(|| drop(io.ledger.lock().expect("ledger setup")));
            eprintln!("decoder test ledger setup bytes={}", setup.bytes_total);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let mut result = None;
            let measured = allocation_counter::measure(|| {
                result = Some(decode_owned(&mut io, &mut route, || {
                    let mut bytes = Vec::with_capacity(16);
                    bytes.push(1_u8);
                    Ok(bytes)
                }));
            });
            let Some(Err(error)) = result else {
                panic!("expected allocation backpressure");
            };
            let CatalogError::MaintenanceBackpressure { ref message } = error else {
                panic!("unexpected error {error}");
            };
            tokio::task::yield_now().await;
            assert_eq!(io.work.decoded_owned_allocation_bytes, measured.bytes_total);
            assert!(
                io.failed_allocation.as_ref().expect("failure owner").bytes >= message.capacity()
            );
            let ledger = io.ledger.clone();
            let report = ledger.lock().expect("ledger").report();
            assert!(report.working_live_bytes >= message.capacity());
            assert!(report.working_underestimates > 0);
            drop(error);
            drop(io);
            let report = ledger.lock().expect("ledger").report();
            assert_eq!(report.working_live_bytes, 0);
            assert!(!report.passing());
        }
    }

    #[tokio::test]
    async fn restore_decode_error_keeps_owned_message_across_cleanup_await() {
        use arco_core::{MemoryBackend, ScopedStorage};
        let store = ControlMvpStateStore::new_synthetic_bounded(
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
                .expect("scope"),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("synthetic store");
        let mut io = RestorePhysicalIo::new(&store, 16 * 1024, 1024);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let Err(failure) = decode_owned(&mut io, &mut route, || {
            let mut message = String::with_capacity(4096);
            message.push_str("terminal decode failure");
            Err::<(), _>(CatalogError::InvariantViolation { message })
        }) else {
            panic!("decoder unexpectedly succeeded");
        };
        assert!(io.stopped);
        tokio::task::yield_now().await;
        assert!(
            io.ledger
                .lock()
                .expect("ledger")
                .report()
                .working_live_bytes
                >= 4096
        );
        drop(failure);
        let ledger = io.ledger.clone();
        drop(io);
        assert!(ledger.lock().expect("ledger").report().passing());
    }

    #[test]
    fn restore_decode_panic_error_is_inside_measured_allocation_scope() {
        use arco_core::{MemoryBackend, ScopedStorage};
        let store = ControlMvpStateStore::new_synthetic_bounded(
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
                .expect("scope"),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("synthetic store");
        let mut io = RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, 1024);
        let setup = allocation_counter::measure(|| drop(io.ledger.lock().expect("ledger setup")));
        eprintln!("decoder test ledger setup bytes={}", setup.bytes_total);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let mut failure = None;
        let outer = allocation_counter::measure(|| {
            failure = Some(decode_owned(
                &mut io,
                &mut route,
                || -> CatalogResult<()> {
                    panic!("synthetic decoder panic");
                },
            ));
        });
        assert!(matches!(failure, Some(Err(_))));
        assert!(io.stopped);
        assert_eq!(outer.bytes_total, io.work.decoded_owned_allocation_bytes);
        drop(failure);
    }
}

fn payload_decode_reservation(descriptor: &super::Descriptor) -> CatalogResult<usize> {
    let invalid = || physical_backpressure("invalid or unqualified restore payload reservation");
    let bytes = usize::try_from(descriptor.block.length).map_err(|_| invalid())?;
    let rows = usize::try_from(descriptor.block.row_count).map_err(|_| invalid())?;
    let row_size = size_of::<super::ControlMvpSegmentRow>();
    if size_of::<usize>() != 8
        || row_size > 96
        || bytes == 0
        || bytes > MAX_SEGMENT_BYTES
        || rows == 0
        || rows > super::super::MAX_SEGMENT_ROWS
        || (rows > 1 && bytes > MAX_BLOCK_BYTES)
    {
        return Err(invalid());
    }
    // Frozen source/target derivation: see the payload-reservation amendment.
    // Singleton endpoints may encode the same key twice. Active-ID JSON has
    // its own P-dependent and per-row terms; KV values remain opaque bytes.
    let mut reserved = bytes
        .checked_mul(if rows == 1 { 7 } else { 5 })
        .and_then(|bytes| bytes.checked_add(rows.checked_mul(row_size)?))
        .and_then(|bytes| bytes.checked_add(8 * 1024 * 1024 + 378))
        .ok_or_else(invalid)?;
    if descriptor.role == super::Role::ActiveId {
        reserved = reserved
            .checked_add(bytes.checked_mul(16).ok_or_else(invalid)?)
            .and_then(|bytes| bytes.checked_add(rows.checked_mul(1024)?))
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(invalid)?;
    }
    Ok(reserved)
}

fn declare_restore_physical(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    object: PhysicalObject<'_>,
) -> CatalogResult<WorkingValue<DeclaredPhysicalRange>> {
    let store = io.store;
    let final_stream = matches!(route, RestorePhysicalRoute::FinalMicrochunk(_));
    decode_with_reservation(io, route, Some(directory_scope_reservation(store)?), || {
        DeclaredPhysicalRange::new(store, object, final_stream)
    })
}

async fn read_restore_payload(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    descriptor: &super::Descriptor,
) -> CatalogResult<WorkingValue<Vec<super::ControlMvpSegmentRow>>> {
    // The enclosing leaf reader must establish descriptor/index/root membership.
    // This primitive authenticates the selected payload and its two
    // version fences; it does not mint retained or readable authority.
    let store = io.store;
    let result = async {
        drop(decode_with_reservation(io, route, Some(64 * 1024), || {
            if descriptor.encoding_version != 1
                || descriptor.scope != store.scope
                || descriptor.segment_version.is_empty()
            {
                return Err(super::invariant_violation(
                    "restore payload descriptor identity differs",
                ));
            }
            Ok(())
        })?);
        let selected = declare_restore_physical(
            io,
            route,
            PhysicalObject::Payload {
                segment: &descriptor.segment,
                block: &descriptor.block,
            },
        )?;
        let before = head_declared_physical(io, route, selected.value()).await?;
        drop(decode_with_reservation(io, route, Some(64 * 1024), || {
            validate_restore_payload_version(&before.value, descriptor)
        })?);
        drop(before);
        let encoded = read_declared_physical_range(io, route, selected.value()).await?;
        let reservation = payload_decode_reservation(descriptor)?;
        let rows = decode_with_reservation(io, route, Some(reservation), || {
            let rows = super::super::decode_block_rows_with_preflight(
                encoded.as_slice(),
                &descriptor.block,
                store.scope.domain(),
                preflight_restore_arrow_segment,
            )?;
            read_cache::validate_rows(&descriptor.segment, &rows)?;
            if descriptor.role == super::Role::Kv {
                if rows
                    .iter()
                    .any(|row| row.record_kind != super::SEGMENT_RECORD_KV)
                    || rows.windows(2).any(|pair| match pair {
                        [first, second] => {
                            first.logical_ordinal.checked_add(1) != Some(second.logical_ordinal)
                        }
                        _ => true,
                    })
                {
                    return Err(super::invariant_violation(
                        "restore physical KV kind or ordinals differ",
                    ));
                }
            } else {
                for row in &rows {
                    super::super::bounded::validate_outbox_row(descriptor.role, row, &store.scope)?;
                }
            }
            Ok(rows)
        })?;
        // The production decoder copies every returned key/value. The owned
        // rows remain charged while the encoded response is released.
        drop(encoded);

        let after = head_declared_physical(io, route, selected.value()).await?;
        drop(decode_with_reservation(io, route, Some(64 * 1024), || {
            validate_restore_payload_version(&after.value, descriptor)
        })?);
        drop(after);
        Ok(rows)
    }
    .await;
    if result.is_err() {
        io.stopped = true;
        route.stop();
    }
    result
}

fn validate_restore_payload_version(
    meta: &ObjectMeta,
    descriptor: &super::Descriptor,
) -> CatalogResult<()> {
    if meta.version.is_empty()
        || meta.version != descriptor.segment_version
        || meta.size != descriptor.segment.segment_size_bytes
    {
        return Err(super::invariant_violation(
            "restore payload object version or size changed",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod native_payload_tests {
    use super::*;

    #[tokio::test]
    async fn restore_payload_rejects_insufficient_proven_reservation_before_decoder() {
        for (final_stream, limit, carry) in [(false, 1, 0), (true, 1, 0), (true, 64, 60)] {
            let (fixture, descriptor, _) = super::super::tests::fixture().await;
            let store = ControlMvpStateStore::new_synthetic_bounded(
                fixture.retention.clone(),
                fixture.scope.clone(),
            )
            .expect("store");
            let mut io = RestorePhysicalIo::new(&store, limit * 1024 * 1024, 1024 * 1024);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut totals = FinalStreamTotals::new();
            let mut chunk =
                FinalMicrochunk::begin(&mut totals, carry * 1024 * 1024, &mut io).expect("chunk");
            let mut route = if final_stream {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            let result = read_restore_payload(&mut io, &mut route, &descriptor).await;
            assert!(
                matches!(result, Err(CatalogError::MaintenanceBackpressure { .. })),
                "observed small allocations cannot replace the proven admission"
            );
            assert_eq!(
                io.work.decode_operations, 3,
                "identity, declaration, and version guards run before decoder admission"
            );
            assert_eq!(
                io.work.range_reads, 1,
                "classified input is separately charged"
            );
            assert_eq!(
                io.work.metadata_heads, 1,
                "failed admission cannot reach the trailing HEAD"
            );
            assert!(io.stopped);
            let report = io.ledger.lock().expect("ledger").report();
            assert_eq!(report.working_admission_failures, 1);
            assert_eq!(report.working_underestimates, 0);
            let Err(CatalogError::MaintenanceBackpressure { ref message }) = result else {
                panic!("expected rejection")
            };
            assert_eq!(report.working_live_bytes, message.capacity());
            assert!(report.request_owned_peak_bytes <= limit * 1024 * 1024);
        }
    }

    #[tokio::test]
    async fn restore_payload_reservation_matches_role_vectors_and_rejects_invalid_shapes() {
        let (_, mut descriptor, _) = super::super::tests::fixture().await;
        for (role, rows, bytes, expected) in [
            (super::super::Role::Kv, 1, 4096, 8_417_754),
            (super::super::Role::Kv, 8, 4096, 8_410_234),
            (super::super::Role::ActiveId, 1, 4096, 8_488_410),
            (super::super::Role::ActiveId, 8, 4096, 8_488_058),
            (super::super::Role::DeliveryOrder, 8, 4096, 8_410_234),
            (super::super::Role::ActiveId, 32_768, 262_144, 50_598_266),
            (super::super::Role::ActiveId, 1, 67_108_864, 1_551_898_074),
        ] {
            descriptor.role = role;
            descriptor.block.row_count = rows;
            descriptor.block.length = bytes;
            assert_eq!(
                payload_decode_reservation(&descriptor).expect("reservation"),
                expected
            );
        }
        for (rows, bytes) in [
            (0, 4096),
            (1, 0),
            (2, 262_145),
            (1, 67_108_865),
            (1_000_001, 4096),
            (u64::MAX, u64::MAX),
        ] {
            descriptor.block.row_count = rows;
            descriptor.block.length = bytes;
            assert!(payload_decode_reservation(&descriptor).is_err());
        }
    }

    #[tokio::test]
    async fn restore_payload_outbox_roles_stay_within_admitted_allocation() {
        use super::super::super::{
            ControlMvpSegmentRow, ProjectionIntentV2, SEGMENT_RECORD_OUTBOX,
        };
        for role in [
            super::super::Role::ActiveId,
            super::super::Role::DeliveryOrder,
        ] {
            let rows = (0..8).map(|ordinal| {
                let id = format!("intent-{ordinal:02}");
                let intent = ProjectionIntentV2::new(&id, "catalog",
                    StateScope::new("tenant", "workspace", "catalog"), 2, "ab".repeat(32), ordinal, [0, 255, b'"', b'\\'])
                    .expect("V2 intent");
                let payload = serde_json::to_vec(&intent).expect("V2 encoding");
                // Exact active-record wire order, independently assembled for
                // this payload-decoder fixture; it confers no source authority.
                let active = format!(r#"{{"recordId":"{id}","originSequence":2,"ordinal":{ordinal},"sourceDescriptorSha256":"{}","payload":{}}}"#,
                    "ab".repeat(32), serde_json::to_string(&payload).expect("byte array"));
                let (key, value) = if role == super::super::Role::ActiveId {
                    (id.into_bytes(), active.into_bytes())
                } else {
                    (format!("{:020}/{ordinal:020}/{id}", 2).into_bytes(), "ab".repeat(32).into_bytes())
                };
                ControlMvpSegmentRow { record_kind: SEGMENT_RECORD_OUTBOX, key, value: Some(value),
                    generation: 0, tombstone: false, logical_sequence: 2, logical_ordinal: ordinal, origin_sequence: Some(2) }
            }).collect();
            let (fixture, mut descriptor, _) = super::super::tests::fixture_with_rows(rows).await;
            descriptor.role = role;
            assert_eq!(descriptor.block.row_count, 8);
            let store = ControlMvpStateStore::new_synthetic_bounded(
                fixture.retention.clone(),
                fixture.scope.clone(),
            )
            .expect("store");
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let mut totals = FinalStreamTotals::new();
            {
                let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
                let result = read_restore_payload(
                    &mut io,
                    &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
                    &descriptor,
                )
                .await
                .expect("authenticated payload");
                assert_eq!(result.value().len(), 8);
                assert!(
                    result.working.bytes
                        <= payload_decode_reservation(&descriptor).expect("reservation")
                );
            }
            assert_eq!(io.work.native.slots[20], 1);
            assert_eq!(io.work.native.slots[21], descriptor.block.length);
            assert_eq!(io.work.native.bounded.decoded_rows, 8);
            for slot in [24, 25, 30, 31] {
                assert!(io.work.native.slots[slot] > 0);
            }
            assert_eq!(io.work.native.slots[22..24], [0, 0]);
            if role == super::super::Role::ActiveId {
                assert!(io.work.native.slots[32] > 0);
                assert!(io.work.native.slots[33] > 0);
            }
            assert!(!io.work.native.overflow);
            println!("native-parity outbox role={role:?} {:?}", io.work.native);
            assert!(!totals.stopped);
            assert!(io.ledger.lock().expect("ledger").report().passing());
        }
    }

    #[tokio::test]
    async fn restore_native_payload_uses_classified_range_version_fences_and_owned_decode() {
        for final_stream in [false, true] {
            let (fixture, descriptor, _) = super::super::tests::fixture().await;
            let store = ControlMvpStateStore::new_synthetic_bounded(
                fixture.retention.clone(),
                fixture.scope.clone(),
            )
            .expect("synthetic store");
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut route = if final_stream {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            let rows = read_restore_payload(&mut io, &mut route, &descriptor)
                .await
                .expect("native payload");
            tokio::task::yield_now().await;
            assert_eq!(rows.value.len(), 1);
            assert_eq!(rows.value[0].key, b"key");
            assert_eq!(rows.value[0].value, Some(vec![42; 1024]));
            assert_eq!(io.work.range_reads, 1);
            assert_eq!(io.work.metadata_heads, 2);
            assert_eq!(io.work.decode_operations, 5);
            assert!(rows.working.bytes >= 1024);
            let report = io.ledger.lock().expect("ledger").report();
            assert_eq!(
                report.backend_origin_shared_live_bytes, 0,
                "rows must not alias encoded bytes"
            );
            assert_eq!(report.working_live_bytes, rows.working.bytes);
            drop(rows);
            assert!(io.ledger.lock().expect("ledger").report().passing());
        }
    }
}

fn descriptor_decode_reservation(
    length: usize,
    leaf: &super::directory::Leaf,
) -> CatalogResult<usize> {
    admitted_physical_length(length, MAX_CONTROL_JSON_BYTES)?;
    let capacity = || physical_backpressure("restore descriptor allocation bound is unsupported");
    if usize::BITS != 64 || size_of::<super::Descriptor>() > 520 {
        return Err(capacity());
    }
    leaf.first
        .len()
        .checked_add(leaf.last.len())
        .and_then(|endpoints| endpoints.checked_mul(2))
        .and_then(|endpoints| length.checked_mul(60)?.checked_add(endpoints))
        .and_then(|variable| variable.checked_add(512 * 1024 + 4 * 1024))
        .ok_or_else(capacity)
}

fn index_decode_reservation(length: usize) -> CatalogResult<usize> {
    admitted_physical_length(length, MAX_SEGMENT_INDEX_BYTES)?;
    let capacity = || physical_backpressure("restore index allocation bound is unsupported");
    if usize::BITS != 64
        || size_of::<super::ControlMvpSegmentIndex>() > 512
        || size_of::<ControlMvpBlock>() > 192
    {
        return Err(capacity());
    }
    length
        .checked_mul(80)
        .and_then(|variable| variable.checked_add(8 * 1024 * 1024 + 128 * 1024))
        .ok_or_else(capacity)
}

/// Physical authentication only; membership and receipt coverage remain the caller's proof.
pub(in super::super) struct FinalDescriptor {
    descriptor: WorkingValue<super::Descriptor>,
    raw_bytes: u64,
}

impl FinalDescriptor {
    pub(in super::super) fn value(&self) -> &super::Descriptor {
        self.descriptor.value()
    }

    pub(in super::super) fn raw_bytes(&self) -> u64 {
        self.raw_bytes
    }
}

pub(in super::super) async fn read_final_descriptor(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    leaf: &WorkingValue<super::directory::Leaf>,
) -> CatalogResult<FinalDescriptor> {
    let admissible =
        leaf.is_owned_by(io) && matches!(route, RestorePhysicalRoute::FinalMicrochunk(_));
    drop(decode_with_reservation(io, route, Some(64 * 1024), || {
        if !admissible {
            return Err(physical_backpressure(
                "final descriptor owner or phase differs",
            ));
        }
        Ok(())
    })?);
    let (descriptor, raw_bytes) =
        read_restore_descriptor_sized(io, route, super::Role::Kv, leaf.value()).await?;
    Ok(FinalDescriptor {
        descriptor,
        raw_bytes,
    })
}

pub(in super::super) async fn read_final_payload(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    descriptor: &FinalDescriptor,
) -> CatalogResult<WorkingValue<Vec<super::ControlMvpSegmentRow>>> {
    let admissible = descriptor.descriptor.is_owned_by(io)
        && matches!(route, RestorePhysicalRoute::FinalMicrochunk(_));
    drop(decode_with_reservation(io, route, Some(64 * 1024), || {
        if !admissible {
            return Err(physical_backpressure(
                "final payload owner or phase differs",
            ));
        }
        Ok(())
    })?);
    read_restore_payload(io, route, descriptor.value()).await
}

/// The caller authenticates membership in its pinned role root. Keep the exact
/// descriptor beside decoded rows so the merge can bind its physical sequence.
pub(in super::super) async fn read_restore_leaf(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    role: super::Role,
    leaf: &super::directory::Leaf,
) -> CatalogResult<(
    WorkingValue<super::Descriptor>,
    WorkingValue<Vec<super::ControlMvpSegmentRow>>,
)> {
    let descriptor = read_restore_descriptor(io, route, role, leaf).await?;
    let rows = read_restore_payload(io, route, descriptor.value()).await?;
    Ok((descriptor, rows))
}

/// Return the authenticated raw descriptor size for exact unit witnesses.
pub(in super::super) async fn read_restore_leaf_sized(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    role: super::Role,
    leaf: &super::directory::Leaf,
) -> CatalogResult<(
    WorkingValue<super::Descriptor>,
    WorkingValue<Vec<super::ControlMvpSegmentRow>>,
    u64,
)> {
    let (descriptor, bytes) = read_restore_descriptor_sized(io, route, role, leaf).await?;
    let rows = read_restore_payload(io, route, descriptor.value()).await?;
    Ok((descriptor, rows, bytes))
}

async fn read_restore_descriptor(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    role: super::Role,
    leaf: &super::directory::Leaf,
) -> CatalogResult<WorkingValue<super::Descriptor>> {
    Ok(read_restore_descriptor_sized(io, route, role, leaf)
        .await?
        .0)
}

async fn read_restore_descriptor_sized(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    role: super::Role,
    leaf: &super::directory::Leaf,
) -> CatalogResult<(WorkingValue<super::Descriptor>, u64)> {
    let store = io.store;
    let result = async {
        let announced = declare_restore_physical(
            io,
            route,
            PhysicalObject::Descriptor {
                digest: &leaf.digest,
                length: MAX_CONTROL_JSON_BYTES,
            },
        )?;
        let before = head_declared_physical(io, route, announced.value()).await?;
        let length = decode_with_reservation(io, route, Some(64 * 1024), || {
            let length = usize::try_from(before.value.size)
                .map_err(|_| physical_backpressure("restore descriptor length overflow"))?;
            admitted_physical_length(length, MAX_CONTROL_JSON_BYTES)?;
            if before.value.version.is_empty() {
                return Err(super::invariant_violation(
                    "restore descriptor version is absent",
                ));
            }
            Ok(length)
        })?;
        let length = *length.value();
        drop(announced);
        let selected = declare_restore_physical(
            io,
            route,
            PhysicalObject::Descriptor {
                digest: &leaf.digest,
                length,
            },
        )?;
        let bytes = read_declared_physical_range(io, route, selected.value()).await?;
        let reservation =
            descriptor_decode_reservation(length, leaf).inspect_err(|_| io.stop(route))?;
        let descriptor = decode_with_reservation(io, route, Some(reservation), || {
            super::validate_raw_checksum(
                bytes.as_slice(),
                Some(&hex::encode(leaf.digest)),
                "restore physical descriptor",
            )?;
            let descriptor: super::Descriptor =
                super::decode_json(bytes.as_slice(), "restore physical descriptor")?;
            validate_restore_descriptor_binding(store, role, leaf, &descriptor)?;
            Ok(descriptor)
        })?;
        drop(bytes);

        let after = head_declared_physical(io, route, selected.value()).await?;
        drop(decode_with_reservation(io, route, Some(64 * 1024), || {
            if after.value.size != before.value.size || after.value.version != before.value.version
            {
                return Err(super::invariant_violation(
                    "restore descriptor version or size changed",
                ));
            }
            Ok(())
        })?);
        drop(before);
        drop(after);
        verify_restore_index(io, route, &descriptor.value).await?;
        Ok((descriptor, length as u64))
    }
    .await;
    if result.is_err() {
        io.stopped = true;
        route.stop();
    }
    result
}

fn validate_restore_descriptor_binding(
    store: &ControlMvpStateStore,
    role: super::Role,
    leaf: &super::directory::Leaf,
    descriptor: &super::Descriptor,
) -> CatalogResult<()> {
    if descriptor.encoding_version != 1
        || descriptor.scope != store.scope
        || descriptor.role != role
        || descriptor.segment.level != ControlMvpSegmentLevel::L1
        || descriptor.segment_version.is_empty()
        || descriptor.index_version.is_empty()
        || descriptor.block.row_count == 0
        || descriptor.block.row_count != leaf.rows
        || descriptor.block.length != u64::from(leaf.bytes)
        || super::block_key_bounds(&descriptor.block)?.as_ref()
            != Some(&(leaf.first.clone(), leaf.last.clone()))
    {
        return Err(super::invariant_violation(
            "restore descriptor differs from its owning leaf",
        ));
    }
    read_cache::validate_owner(&descriptor.segment)
}

async fn verify_restore_index(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    descriptor: &super::Descriptor,
) -> CatalogResult<()> {
    let store = io.store;
    let selected = declare_restore_physical(io, route, PhysicalObject::Index(&descriptor.segment))?;
    let before = head_declared_physical(io, route, selected.value()).await?;
    drop(decode_with_reservation(io, route, Some(64 * 1024), || {
        validate_restore_index_version(&before.value, descriptor)
    })?);
    drop(before);
    let bytes = read_declared_physical_range(io, route, selected.value()).await?;
    let reservation =
        index_decode_reservation(bytes.as_slice().len()).inspect_err(|_| io.stop(route))?;
    let verified = decode_with_reservation(io, route, Some(reservation), || {
        super::validate_raw_checksum(
            bytes.as_slice(),
            Some(&descriptor.segment.index_checksum_sha256),
            "restore physical index",
        )?;
        super::validate_version_header(
            bytes.as_slice(),
            super::SEGMENT_FORMAT_VERSION,
            "restore physical index",
        )?;
        let index: super::ControlMvpSegmentIndex =
            super::decode_json(bytes.as_slice(), "restore physical index")?;
        super::validate_segment_index_identity(&index, &descriptor.segment, &store.scope)?;
        super::validate_segment_index_key_metadata(&index)?;
        if index
            .blocks
            .iter()
            .filter(|block| **block == descriptor.block)
            .count()
            != 1
        {
            return Err(super::invariant_violation(
                "restore descriptor block is absent from authenticated index",
            ));
        }
        Ok(())
    })?;
    drop(verified);
    drop(bytes);

    let after = head_declared_physical(io, route, selected.value()).await?;
    drop(decode_with_reservation(io, route, Some(64 * 1024), || {
        validate_restore_index_version(&after.value, descriptor)
    })?);
    drop(after);
    Ok(())
}

fn validate_restore_index_version(
    meta: &ObjectMeta,
    descriptor: &super::Descriptor,
) -> CatalogResult<()> {
    if meta.version.is_empty()
        || meta.version != descriptor.index_version
        || meta.size != descriptor.segment.index_size_bytes
    {
        return Err(super::invariant_violation(
            "restore index version or size changed",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod native_descriptor_tests {
    use super::*;
    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "the source-bound matrix and its real I/O assertions stay together"
    )]
    async fn restore_metadata_descriptor_hostile_and_maximum_inputs_stay_reserved() {
        use sha2::Digest as _;
        let (fixture, descriptor, leaf) = super::super::tests::fixture().await;
        let store = ControlMvpStateStore::new_synthetic_bounded(
            fixture.retention.clone(),
            fixture.scope.clone(),
        )
        .expect("store");
        let mut cases = Vec::new();
        for raw in [
            "{",
            "{}",
            "false",
            "{\"unknown\":0}",
            "{\"encoding_version\":false}",
        ] {
            cases.push((Bytes::copy_from_slice(raw.as_bytes()), leaf.clone(), false));
        }
        for length in [16, 4096, 65_520, 262_000, MAX_CONTROL_JSON_BYTES - 64] {
            let raw = format!(
                "{{\"encoding_version\":\"\\n{}\"}}",
                "\u{7f}".repeat(length)
            );
            cases.push((Bytes::from(raw), leaf.clone(), false));
        }
        for numeric in [
            "9".repeat(100_000),
            format!("0.{}1", "0".repeat(100_000)),
            format!("1.{}e-1", "23456789".repeat(100_000)),
        ] {
            let raw = format!("{{\"encoding_version\":{numeric}}}");
            cases.push((Bytes::from(raw), leaf.clone(), false));
        }
        for spelling in ["\\n\u{7f}", "\\u007f"] {
            let raw = format!(
                "{{\"encoding_version\":\"{}\"}}",
                spelling.repeat((MAX_CONTROL_JSON_BYTES - 4096) / spelling.len())
            );
            cases.push((Bytes::from(raw), leaf.clone(), false));
        }
        let mut maximum = descriptor.clone();
        let baseline = super::super::super::encode_json(&maximum, "test descriptor")
            .expect("encode")
            .len();
        maximum.segment_version =
            "v".repeat(MAX_CONTROL_JSON_BYTES - baseline + maximum.segment_version.len());
        let raw = super::super::super::encode_json(&maximum, "test descriptor").expect("encode");
        assert_eq!(raw.len(), MAX_CONTROL_JSON_BYTES);
        cases.push((raw, leaf.clone(), true));
        let mut wrong_leaf = leaf.clone();
        wrong_leaf.first = vec![b'a'; MAX_BLOCK_BYTES];
        wrong_leaf.last = vec![b'z'; MAX_BLOCK_BYTES];
        cases.push((
            super::super::super::encode_json(&descriptor, "test descriptor").expect("encode"),
            wrong_leaf,
            false,
        ));
        for final_stream in [false, true] {
            for (raw, original_leaf, valid) in &cases {
                let mut selected = original_leaf.clone();
                selected.digest = sha2::Sha256::digest(raw).into();
                let path = format!(
                    "{}/physical/descriptors/{}.json",
                    fixture.paths.base_prefix(),
                    hex::encode(selected.digest)
                );
                fixture
                    .retention
                    .put_raw(&path, raw.clone(), arco_core::WritePrecondition::None)
                    .await
                    .expect("fixture put");
                let reservation =
                    descriptor_decode_reservation(raw.len(), &selected).expect("reservation");
                let mut io =
                    RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
                let ledger = io.ledger.clone();
                let mut totals = FinalStreamTotals::new();
                let mut chunk = FinalMicrochunk::begin(&mut totals, 1024, &mut io).expect("carry");
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut route = if final_stream {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload,
                    }
                };
                let result =
                    read_restore_descriptor(&mut io, &mut route, super::super::Role::Kv, &selected)
                        .await;
                assert_eq!(
                    result.is_ok(),
                    *valid,
                    "P={} final={final_stream}",
                    raw.len()
                );
                assert!(
                    usize::try_from(io.work.decoded_owned_allocation_bytes).expect("count")
                        <= reservation
                );
                if raw.len() <= 64 {
                    assert!(
                        io.work.decoded_owned_allocation_bytes < 512 * 1024,
                        "fixed allocation qualification"
                    );
                }
                assert_eq!(io.work.decode_operations, if *valid { 9 } else { 4 });
                assert_eq!(io.work.metadata_heads, if *valid { 4 } else { 1 });
                println!(
                    "restore metadata descriptor: P={} F={} final={final_stream} valid={valid} reserved={reservation} allocated={}",
                    raw.len(),
                    selected.first.len() + selected.last.len(),
                    io.work.decoded_owned_allocation_bytes
                );
                drop(result);
                drop(io);
                assert!(ledger.lock().expect("ledger").report().passing());
            }
        }
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "the source-bound matrix and its real I/O assertions stay together"
    )]
    async fn restore_metadata_index_omitted_fields_offsets_errors_and_nesting_stay_reserved() {
        let (fixture, original, _) = super::super::tests::fixture().await;
        let store = ControlMvpStateStore::new_synthetic_bounded(
            fixture.retention.clone(),
            fixture.scope.clone(),
        )
        .expect("store");
        let path = fixture.paths.segment_index(&original.segment.segment_id);
        let raw = fixture
            .retention
            .get_raw(&path)
            .await
            .expect("fixture index");
        let template: serde_json::Value = serde_json::from_slice(&raw).expect("fixture JSON");
        let mut cases = vec![(raw, true)];
        let minimal = r#"{"offset":0,"length":0,"rowCount":0,"checksumSha256":""}"#;
        assert_eq!(minimal.len(), 56);
        let mut value = template.clone();
        value["blocks"] =
            serde_json::Value::Array(vec![serde_json::from_str(minimal).expect("minimal"); 8192]);
        cases.push((
            Bytes::from(serde_json::to_vec(&value).expect("blocks")),
            false,
        ));
        value = template.clone();
        value["recordBatchOffsets"] = serde_json::Value::Array(vec![serde_json::json!(0); 250_000]);
        cases.push((
            Bytes::from(serde_json::to_vec(&value).expect("offsets")),
            false,
        ));
        for length in [16, 4096, 65_520, 262_000, MAX_SEGMENT_INDEX_BYTES - 4096] {
            value = template.clone();
            value["rowCount"] = serde_json::json!("REPLACE_INPUT");
            let raw = serde_json::to_string(&value)
                .expect("template")
                .replace("REPLACE_INPUT", &format!("\\n{}", "\u{7f}".repeat(length)));
            cases.push((Bytes::from(raw), false));
        }
        for numeric in [
            "9".repeat(100_000),
            format!("0.{}1", "0".repeat(100_000)),
            format!("1.{}e-1", "23456789".repeat(60_000)),
        ] {
            value = template.clone();
            value["rowCount"] = serde_json::json!("REPLACE_NUMBER");
            let raw = serde_json::to_string(&value)
                .expect("template")
                .replace("\"REPLACE_NUMBER\"", &numeric);
            cases.push((Bytes::from(raw), false));
        }
        for spelling in ["\\n\u{7f}", "\\u007f"] {
            value = template.clone();
            value["rowCount"] = serde_json::json!("REPLACE_ESCAPES");
            let raw = serde_json::to_string(&value).expect("template").replace(
                "REPLACE_ESCAPES",
                &spelling.repeat((MAX_SEGMENT_INDEX_BYTES - 4096) / spelling.len()),
            );
            cases.push((Bytes::from(raw), false));
        }
        value = template;
        value["ignoredNested"] = serde_json::json!("REPLACE_NESTING");
        let raw = serde_json::to_string(&value).expect("template").replace(
            "\"REPLACE_NESTING\"",
            &format!("{}0{}", "[".repeat(100_000), "]".repeat(100_000)),
        );
        cases.push((Bytes::from(raw), true));
        for final_stream in [false, true] {
            for (raw, valid) in &cases {
                assert!(raw.len() <= MAX_SEGMENT_INDEX_BYTES);
                let mut descriptor = original.clone();
                descriptor.segment.index_size_bytes = u64::try_from(raw.len()).expect("length");
                descriptor.segment.index_checksum_sha256 = super::super::super::sha256_hex(raw);
                fixture
                    .retention
                    .put_raw(&path, raw.clone(), arco_core::WritePrecondition::None)
                    .await
                    .expect("fixture put");
                descriptor.index_version = fixture
                    .retention
                    .head_raw(&path)
                    .await
                    .expect("HEAD")
                    .expect("exists")
                    .version;
                let reservation = index_decode_reservation(raw.len()).expect("reservation");
                let mut io =
                    RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
                let ledger = io.ledger.clone();
                let mut totals = FinalStreamTotals::new();
                let mut chunk = FinalMicrochunk::begin(&mut totals, 1024, &mut io).expect("carry");
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut route = if final_stream {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload,
                    }
                };
                let result = verify_restore_index(&mut io, &mut route, &descriptor).await;
                assert_eq!(
                    result.is_ok(),
                    *valid,
                    "Q={} final={final_stream}",
                    raw.len()
                );
                assert_eq!(io.work.decode_operations, if *valid { 4 } else { 3 });
                assert_eq!(io.work.metadata_heads, if *valid { 2 } else { 1 });
                assert!(
                    usize::try_from(io.work.decoded_owned_allocation_bytes).expect("count")
                        <= reservation
                );
                println!(
                    "restore metadata index: Q={} final={final_stream} valid={valid} reserved={reservation} allocated={}",
                    raw.len(),
                    io.work.decoded_owned_allocation_bytes
                );
                drop(result);
                drop(io);
                assert!(ledger.lock().expect("ledger").report().passing());
            }
        }
    }

    #[test]
    fn restore_metadata_layout_and_checked_bounds() {
        let descriptor = size_of::<super::super::Descriptor>();
        let index = size_of::<super::super::ControlMvpSegmentIndex>();
        let block = size_of::<ControlMvpBlock>();
        println!(
            "restore metadata layouts: usize={} descriptor={descriptor} index={index} block={block}",
            usize::BITS
        );
        assert_eq!(usize::BITS, 64);
        assert!(descriptor <= 520 && index <= 512 && block <= 192);
        assert!(index_decode_reservation(0).is_err());
        assert!(index_decode_reservation(MAX_SEGMENT_INDEX_BYTES + 1).is_err());
        assert!(index_decode_reservation(usize::MAX).is_err());
    }

    #[tokio::test]
    async fn restore_metadata_reserves_before_decode_and_trailing_head() {
        let mut observations = Vec::new();
        for final_stream in [false, true] {
            for index in [false, true] {
                let (fixture, descriptor, leaf) = super::super::tests::fixture().await;
                let store = ControlMvpStateStore::new_synthetic_bounded(
                    fixture.retention.clone(),
                    fixture.scope.clone(),
                )
                .expect("store");
                let allowance = if index { 1024 * 1024 } else { 512 * 1024 };
                let mut io = RestorePhysicalIo::new(
                    &store,
                    if final_stream {
                        FINAL_MICROCHUNK_BYTES
                    } else {
                        allowance
                    },
                    FINAL_MICROCHUNK_BYTES,
                );
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut totals = FinalStreamTotals::new();
                let mut chunk = FinalMicrochunk::begin(
                    &mut totals,
                    if final_stream {
                        FINAL_MICROCHUNK_BYTES - allowance
                    } else {
                        0
                    },
                    &mut io,
                )
                .expect("chunk");
                let mut route = if final_stream {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload,
                    }
                };
                let result = if index {
                    verify_restore_index(&mut io, &mut route, &descriptor).await
                } else {
                    read_restore_descriptor(&mut io, &mut route, super::super::Role::Kv, &leaf)
                        .await
                        .map(drop)
                };
                observations.push((
                    final_stream,
                    index,
                    matches!(result, Err(CatalogError::MaintenanceBackpressure { .. })),
                    io.work.metadata_heads,
                    io.work.range_reads,
                    io.work.decode_operations,
                    io.stopped,
                ));
            }
        }
        for (final_stream, index, rejected, heads, reads, decodes, stopped) in observations {
            assert!(
                rejected
                    && stopped
                    && heads == 1
                    && reads == 1
                    && decodes == if index { 2 } else { 3 },
                "metadata admission missing: final={final_stream} index={index} rejected={rejected} heads={heads} reads={reads} decodes={decodes} stopped={stopped}"
            );
        }
    }

    #[tokio::test]
    async fn restore_native_descriptor_rejects_forgery_and_incomplete_index_before_payload() {
        for mutation in 0..6 {
            let (fixture, mut descriptor, _) = super::super::tests::fixture().await;
            let index_path = fixture.paths.segment_index(&descriptor.segment.segment_id);
            match mutation {
                0 => descriptor.role = super::super::Role::ActiveId,
                1 => descriptor.scope.domain = "other".into(),
                2 => descriptor.index_version.push_str("changed"),
                3 | 4 => {
                    let bytes = fixture.retention.get_raw(&index_path).await.expect("index");
                    let mut index: super::super::ControlMvpSegmentIndex =
                        super::super::decode_json(&bytes, "test index").expect("decode index");
                    if mutation == 3 {
                        index.blocks.clear();
                    } else {
                        index.blocks.push(index.blocks[0].clone());
                    }
                    let bytes =
                        super::super::super::encode_json(&index, "test index").expect("encode");
                    descriptor.segment.index_size_bytes = u64::try_from(bytes.len()).expect("size");
                    descriptor.segment.index_checksum_sha256 =
                        super::super::super::sha256_hex(&bytes);
                    fixture
                        .retention
                        .put_raw(&index_path, bytes, arco_core::WritePrecondition::None)
                        .await
                        .expect("replace index");
                    descriptor.index_version = fixture
                        .retention
                        .head_raw(&index_path)
                        .await
                        .expect("head")
                        .expect("index exists")
                        .version;
                }
                5 => descriptor.segment.index_checksum_sha256 = "00".repeat(32),
                _ => unreachable!(),
            }
            let leaf = super::super::tests::persist(&fixture, &descriptor).await;
            let store = ControlMvpStateStore::new_synthetic_bounded(
                fixture.retention.clone(),
                fixture.scope.clone(),
            )
            .expect("store");
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let Err(error) =
                read_restore_descriptor(&mut io, &mut route, super::super::Role::Kv, &leaf).await
            else {
                panic!("accepted forgery {mutation}");
            };
            assert!(
                matches!(error, CatalogError::InvariantViolation { .. }),
                "wrong failure for {mutation}: {error}"
            );
            assert!(io.stopped);
            assert!(io.work.native.slots[0] > 0);
            assert!(io.work.native.slots[13] > 0);
            assert_eq!(io.work.native.slots[20], 0);
            assert_eq!(io.work.native.slots[22..24], [0, 0]);
            assert!(!io.work.native.overflow);
            let expected_reads = if mutation < 3 { 1 } else { 2 };
            assert_eq!(io.work.range_reads, expected_reads, "mutation {mutation}");
            assert_eq!(
                io.work.decode_operations,
                match mutation {
                    0 | 1 => 4,
                    2 => 7,
                    _ => 8,
                },
                "mutation {mutation}"
            );
            assert_eq!(
                io.ledger
                    .lock()
                    .expect("ledger")
                    .report()
                    .backend_origin_shared_live_bytes,
                0
            );
        }
    }

    #[tokio::test]
    async fn restore_native_descriptor_authenticates_index_before_payload_read() {
        for final_stream in [false, true] {
            let (fixture, _, leaf) = super::super::tests::fixture().await;
            let store = ControlMvpStateStore::new_synthetic_bounded(
                fixture.retention.clone(),
                fixture.scope.clone(),
            )
            .expect("synthetic store");
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut totals = FinalStreamTotals::new();
            let descriptor;
            {
                let mut chunk =
                    FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("descriptor chunk");
                let mut route = if final_stream {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload,
                    }
                };
                descriptor =
                    read_restore_descriptor(&mut io, &mut route, super::super::Role::Kv, &leaf)
                        .await
                        .expect("authenticated descriptor and index");
            }
            assert!(io.work.native.slots[0] > 0);
            assert!(io.work.native.slots[1] > 0);
            assert!(io.work.native.slots[13] > 0);
            assert!(io.work.native.slots[14] > 0);
            assert!(io.work.native.slots[32] > 0);
            assert!(io.work.native.slots[33] > 0);
            assert_eq!(io.work.native.slots[22..24], [0, 0]);
            println!(
                "native-parity descriptor final={final_stream} {:?}",
                io.work.native
            );
            assert_eq!(io.work.range_reads, 2);
            assert_eq!(io.work.metadata_heads, 4);
            assert_eq!(io.work.decode_operations, 9);
            assert_eq!(
                io.ledger
                    .lock()
                    .expect("ledger")
                    .report()
                    .working_live_bytes,
                descriptor.working.bytes
            );
            {
                let mut chunk =
                    FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("payload chunk");
                let mut route = if final_stream {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload,
                    }
                };
                let rows = read_restore_payload(&mut io, &mut route, &descriptor.value)
                    .await
                    .expect("payload");
                assert_eq!(rows.value[0].key, b"key");
                assert_eq!(io.work.native.slots[20], 1);
                assert_eq!(io.work.native.slots[21], descriptor.value.block.length);
                for slot in [24, 25, 30, 31] {
                    assert!(io.work.native.slots[slot] > 0);
                }
                assert_eq!(io.work.native.bounded.decoded_rows, 1);
                assert_eq!(io.work.native.slots[22..24], [0, 0]);
                assert!(!io.work.native.overflow);
                println!("native-parity kv final={final_stream} {:?}", io.work.native);
                assert_eq!(io.work.range_reads, 3);
                assert_eq!(io.work.metadata_heads, 6);
                assert_eq!(io.work.decode_operations, 14);
            }
            drop(descriptor);
            assert!(io.ledger.lock().expect("ledger").report().passing());
        }
    }
}

#[cfg(test)]
mod native_directory_tests {
    use super::super::directory::{Directory, Leaf, restore};
    use super::*;

    #[tokio::test]
    async fn native_builder_empty_root_matches_legacy_and_releases_frontier() {
        let (store, _) = native_counter_fixture().await;
        let directory = Directory::new(store.retention.clone(), &store.scope).expect("directory");
        let expected = directory.builder().finish().await.expect("legacy empty");
        let mut io = RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
        let mut totals = FinalStreamTotals::new();
        let builder = {
            let mut chunk =
                FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("constructor chunk");
            new_directory_builder(
                &mut io,
                &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
            )
            .expect("admitted native builder")
        };
        let root = {
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("finish chunk");
            builder
                .finish_directory(
                    &mut io,
                    &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
                )
                .await
                .expect("native empty root")
        };
        assert_eq!(root.value(), &expected);
        assert_eq!(io.writing_evidence().0, 1);
        drop(root);
        assert!(io.ledger.lock().expect("ledger").report().passing());
    }

    #[tokio::test]
    async fn native_builder_output_first_share_is_already_invoiced() {
        let (store, _) = native_counter_fixture().await;
        let mut io = RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
        let mut totals = FinalStreamTotals::new();
        let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
        let bytes = encode_directory_output(
            &mut io,
            &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
            b"directory body",
        )
        .expect("encoded");
        let promotion = allocation_counter::measure(|| {
            let clone = bytes.value().clone();
            std::hint::black_box(&clone);
        });
        assert_eq!(
            promotion.bytes_total, 0,
            "first PUT clone must allocate nothing outside encoder"
        );
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "keep exact object parity and cross-chunk ownership evidence in one scenario"
    )]
    async fn native_builder_stream_matches_legacy_across_page_boundaries() {
        for collision in [false, true] {
            for (count, key_length) in [
                (1, 0),
                (127, 8),
                (128, 64),
                (129, 65),
                (513, 8),
                (16_385, 8),
                (1_025, 65),
                (33, 131_072),
                (2, 262_144),
            ] {
                let (store, _) = native_counter_fixture().await;
                let directory =
                    Directory::new(store.retention.clone(), &store.scope).expect("directory");
                let leaf = |index: u64| {
                    let mut key = vec![0; key_length];
                    if !key.is_empty() {
                        key[..8].copy_from_slice(&index.to_be_bytes());
                    }
                    Leaf {
                        first: key.clone(),
                        last: key,
                        rows: 1,
                        bytes: 100,
                        digest: [7; 32],
                    }
                };
                let mut legacy = directory.builder();
                for index in 0..count {
                    legacy.push(leaf(index)).await.expect("legacy push");
                }
                let expected = legacy.finish().await.expect("legacy root");
                let (fresh, _) = range_tests::window_pending_store();
                let native_store = if collision { &store } else { &fresh };
                let mut io = RestorePhysicalIo::new(
                    native_store,
                    FINAL_MICROCHUNK_BYTES,
                    FINAL_MICROCHUNK_BYTES,
                );
                let mut totals = FinalStreamTotals::new();
                let mut builder = {
                    let mut chunk =
                        FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("constructor");
                    new_directory_builder(
                        &mut io,
                        &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
                    )
                    .expect("builder")
                };
                let retained = io
                    .ledger
                    .lock()
                    .expect("ledger")
                    .report()
                    .working_live_bytes;
                assert!(retained >= 256 * 1024);
                for index in 0..count {
                    let mut chunk =
                        FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("push chunk");
                    let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
                    let input = decode_with_reservation(
                        &mut io,
                        &mut route,
                        Some(16 * 1024 + 2 * key_length),
                        || Ok(leaf(index)),
                    )
                    .expect("leaf");
                    builder
                        .push_directory_leaf(&mut io, &mut route, &input)
                        .await
                        .expect("native push");
                    drop(input);
                    assert_eq!(
                        io.ledger
                            .lock()
                            .expect("ledger")
                            .report()
                            .working_live_bytes,
                        retained
                    );
                }
                let root = {
                    let mut chunk =
                        FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("finish");
                    builder
                        .finish_directory(
                            &mut io,
                            &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
                        )
                        .await
                        .expect("native root")
                };
                assert_eq!(
                    root.value(),
                    &expected,
                    "count={count} key_length={key_length}"
                );
                assert_eq!(io.work.native.bounded.streaming_builder_inputs, count);
                assert!(io.writing_evidence().0 > 0);
                drop(root);
                assert!(io.ledger.lock().expect("ledger").report().passing());
                if count == 1_025 && collision {
                    assert!(totals.total_operations > 4096);
                }
                let expected_paths = store
                    .retention
                    .list("control/directory/v1")
                    .await
                    .expect("oracle inventory");
                let actual_paths = native_store
                    .retention
                    .list("control/directory/v1")
                    .await
                    .expect("native inventory");
                let expected_paths: std::collections::BTreeSet<_> = expected_paths
                    .iter()
                    .map(arco_core::scoped_storage::ScopedPath::as_str)
                    .collect();
                let actual_paths: std::collections::BTreeSet<_> = actual_paths
                    .iter()
                    .map(arco_core::scoped_storage::ScopedPath::as_str)
                    .collect();
                assert_eq!(actual_paths, expected_paths);
                for path in expected_paths {
                    assert_eq!(
                        store.retention.get_raw(path).await.expect("oracle bytes"),
                        native_store
                            .retention
                            .get_raw(path)
                            .await
                            .expect("native bytes")
                    );
                }
                println!(
                    "builder fixture leaves={count} key_bytes={key_length} collision={collision} operations={} peak_chunk_owned={} retained_frontier={retained}",
                    totals.total_operations, totals.peak_chunk_owned_upper_bound_bytes
                );
            }
        }
    }

    #[tokio::test]
    async fn native_builder_constructor_admission_is_exact_and_phase_closed() {
        use super::super::super::directory::restore::NativeBuilder;
        let (store, _) = native_counter_fixture().await;
        let reservation = directory_scope_reservation(&store).expect("scope")
            + NativeBuilder::reservation().expect("frontier");
        for limit in [0, reservation - 1, reservation] {
            let mut io = RestorePhysicalIo::new(&store, limit, FINAL_MICROCHUNK_BYTES);
            let ledger = io.ledger.clone();
            let mut totals = FinalStreamTotals::new();
            {
                let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
                let result = new_directory_builder(
                    &mut io,
                    &mut RestorePhysicalRoute::FinalMicrochunk(&mut chunk),
                );
                assert_eq!(result.is_ok(), limit == reservation);
                assert_eq!(io.reading_evidence(), (0, 0, 0));
                assert_eq!(io.writing_evidence(), (0, 0));
                if result.is_ok() {
                    let retained = ledger.lock().expect("ledger").report().working_live_bytes;
                    assert!(retained >= NativeBuilder::reservation().expect("frontier"));
                    assert!(retained <= reservation);
                }
                drop(result);
            }
            drop(io);
            let report = ledger.lock().expect("ledger").report();
            assert_eq!(report.owned_live_upper_bound(), 0);
            assert_eq!(report.working_underestimates, 0);
        }
        let mut io = RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        assert!(
            new_directory_builder(
                &mut io,
                &mut RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload
                }
            )
            .is_err()
        );
        assert_eq!(io.writing_evidence(), (0, 0));
    }

    #[tokio::test]
    async fn native_builder_rejects_bad_leaves_before_output() {
        for mutation in 0..7 {
            let (store, _) = native_counter_fixture().await;
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            let mut builder = new_directory_builder(&mut io, &mut route).expect("builder");
            let input = decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
                let mut leaf = Leaf {
                    first: b"a".to_vec(),
                    last: b"b".to_vec(),
                    rows: 1,
                    bytes: 100,
                    digest: [7; 32],
                };
                match mutation {
                    0 => leaf.first = b"z".to_vec(),
                    1 => leaf.rows = 0,
                    2 => leaf.bytes = 0,
                    3 => leaf.bytes = 64 * 1024 * 1024 + 1,
                    4 => {
                        leaf.rows = 2;
                        leaf.bytes = 256 * 1024 + 1;
                    }
                    5 => leaf.last = vec![255; 256 * 1024 + 1],
                    _ => {}
                }
                Ok(leaf)
            })
            .expect("input");
            if mutation == 6 {
                builder
                    .push_directory_leaf(&mut io, &mut route, &input)
                    .await
                    .expect("first leaf");
            }
            assert!(
                builder
                    .push_directory_leaf(&mut io, &mut route, &input)
                    .await
                    .is_err()
            );
            assert_eq!(io.writing_evidence(), (0, 0));
            assert!(io.stopped);
            assert_eq!(io.allocation_underestimates(), 0);
        }
    }

    #[tokio::test]
    async fn native_builder_cancellation_at_each_immutable_boundary_stops_retry() {
        use std::future::Future;
        use std::sync::atomic::Ordering;
        use std::task::{Context, Poll, Waker};
        for collision in [false, true] {
            for boundary in 1..=if collision { 8 } else { 2 } {
                let (store, remaining) = range_tests::window_pending_store();
                let directory =
                    Directory::new(store.retention.clone(), &store.scope).expect("directory");
                let fixture = Leaf {
                    first: vec![1; 65],
                    last: vec![2; 65],
                    rows: 1,
                    bytes: 100,
                    digest: [7; 32],
                };
                if collision {
                    let mut legacy = directory.builder();
                    legacy.push(fixture.clone()).await.expect("seed collision");
                    legacy.finish().await.expect("legacy root");
                }
                let mut io =
                    RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
                let ledger = io.ledger.clone();
                let mut totals = FinalStreamTotals::new();
                {
                    let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
                    let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
                    let mut builder = new_directory_builder(&mut io, &mut route).expect("builder");
                    let input =
                        decode_with_reservation(&mut io, &mut route, Some(16 * 1024), || {
                            Ok(fixture.clone())
                        })
                        .expect("input");
                    remaining.store(boundary, Ordering::SeqCst);
                    let mut future =
                        Box::pin(builder.push_directory_leaf(&mut io, &mut route, &input));
                    assert!(matches!(
                        future
                            .as_mut()
                            .poll(&mut Context::from_waker(Waker::noop())),
                        Poll::Pending
                    ));
                    drop(future);
                    assert!(io.stopped);
                    let before = (io.reading_evidence(), io.writing_evidence());
                    assert!(
                        builder
                            .push_directory_leaf(&mut io, &mut route, &input)
                            .await
                            .is_err()
                    );
                    assert_eq!((io.reading_evidence(), io.writing_evidence()), before);
                    assert!(
                        ledger.lock().expect("ledger").report().working_live_bytes >= 256 * 1024
                    );
                }
                drop(io);
                let report = ledger.lock().expect("ledger").report();
                assert_eq!(report.owned_live_upper_bound(), 0);
                assert_eq!(report.backend_origin_shared_live_bytes, 0);
                assert_eq!(report.working_underestimates, 0);
            }
        }
    }

    #[tokio::test]
    async fn native_builder_empty_finish_faults_release_frontier() {
        use std::future::Future;
        use std::task::{Context, Poll, Waker};
        for after in [false, true] {
            for pending in [false, true] {
                let store = range_tests::unit_publication_test_store(1, after, pending);
                let mut io =
                    RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
                let ledger = io.ledger.clone();
                let mut totals = FinalStreamTotals::new();
                {
                    let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
                    let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
                    let builder = new_directory_builder(&mut io, &mut route).expect("builder");
                    let mut future = Box::pin(builder.finish_directory(&mut io, &mut route));
                    let result = future
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()));
                    match result {
                        Poll::Pending => assert!(pending),
                        Poll::Ready(result) => {
                            assert!(!pending);
                            assert!(result.is_err());
                        }
                    }
                    drop(future);
                    assert!(io.stopped);
                    assert_eq!(io.writing_evidence().0, 1);
                }
                drop(io);
                let report = ledger.lock().expect("ledger").report();
                assert_eq!(report.owned_live_upper_bound(), 0);
                assert_eq!(report.working_underestimates, 0);
            }
        }
    }

    #[tokio::test]
    async fn native_builder_push_and_finish_reject_foreign_owner_or_phase() {
        for operation in 0..4 {
            let (store, _) = native_counter_fixture().await;
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let mut foreign =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let mut totals = FinalStreamTotals::new();
            let (mut builder, input) = {
                let mut chunk =
                    FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("constructor");
                let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
                let builder = new_directory_builder(&mut io, &mut route).expect("builder");
                let input = decode_with_reservation(&mut io, &mut route, Some(16 * 1024), || {
                    Ok(Leaf {
                        first: vec![1],
                        last: vec![2],
                        rows: 1,
                        bytes: 100,
                        digest: [7; 32],
                    })
                })
                .expect("input");
                (builder, input)
            };
            let target = if operation < 2 { &mut foreign } else { &mut io };
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, target).expect("attempt");
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = if operation < 2 {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            if operation % 2 == 0 {
                assert!(
                    builder
                        .push_directory_leaf(target, &mut route, &input)
                        .await
                        .is_err()
                );
            } else {
                assert!(builder.finish_directory(target, &mut route).await.is_err());
            }
            assert_eq!(target.reading_evidence(), (0, 0, 0));
            assert_eq!(target.writing_evidence(), (0, 0));
            assert!(target.stopped);
        }
    }

    #[tokio::test]
    async fn native_builder_depth_eight_and_overflow_match_simulated_legacy_frontiers() {
        for overflow in [false, true] {
            let (store, _) = native_counter_fixture().await;
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            let mut builder = new_directory_builder(&mut io, &mut route).expect("builder");
            let mut oracle =
                decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
                    builder.value.seed_depth_frontier(overflow)
                })
                .expect("simulated frontier");
            if overflow {
                let leaf = decode_with_reservation(&mut io, &mut route, Some(16 * 1024), || {
                    Ok(Leaf {
                        first: 1024_u64.to_be_bytes().to_vec(),
                        last: 1024_u64.to_be_bytes().to_vec(),
                        rows: 1,
                        bytes: 100,
                        digest: [7; 32],
                    })
                })
                .expect("last leaf");
                assert!(oracle.value.push(leaf.value().clone()).await.is_err());
                assert!(
                    builder
                        .push_directory_leaf(&mut io, &mut route, &leaf)
                        .await
                        .is_err()
                );
                assert!(io.stopped);
            } else {
                // Legacy finish consumes its owned model, so construct its independent fixture outside the native ledger.
                drop(oracle);
                let directory =
                    Directory::new(store.retention.clone(), &store.scope).expect("directory");
                let mut fixture = restore::NativeBuilder::new(directory);
                let expected = fixture
                    .seed_depth_frontier(false)
                    .expect("oracle")
                    .finish()
                    .await
                    .expect("depth eight oracle");
                let root = builder
                    .finish_directory(&mut io, &mut route)
                    .await
                    .expect("depth eight native");
                assert_eq!(root.value().depth(), 8);
                assert_eq!(root.value(), &expected);
                drop(root);
                assert!(io.ledger.lock().expect("ledger").report().passing());
            }
        }
    }

    #[tokio::test]
    async fn native_builder_corrupt_key_or_page_collision_fails_closed() {
        for key in [false, true] {
            for wrong_size in [false, true] {
                let (store, _) = range_tests::window_pending_store();
                let directory =
                    Directory::new(store.retention.clone(), &store.scope).expect("directory");
                let fixture = Leaf {
                    first: vec![1; 65],
                    last: vec![2; 65],
                    rows: 1,
                    bytes: 100,
                    digest: [7; 32],
                };
                let mut legacy = directory.builder();
                if key {
                    legacy.push(fixture.clone()).await.expect("seed keys");
                }
                legacy.finish().await.expect("seed page");
                let paths = store
                    .retention
                    .list(if key {
                        "control/directory/v1/domains/catalog/keys"
                    } else {
                        "control/directory/v1/domains/catalog/pages"
                    })
                    .await
                    .expect("fixture paths");
                let path = paths.first().expect("object").as_str();
                let mut bytes = store
                    .retention
                    .get_raw(path)
                    .await
                    .expect("fixture body")
                    .to_vec();
                if wrong_size {
                    bytes.pop();
                } else {
                    bytes[0] ^= 1;
                }
                store
                    .retention
                    .put_raw(path, Bytes::from(bytes), arco_core::WritePrecondition::None)
                    .await
                    .expect("corrupt fixture");
                let mut io =
                    RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
                let mut totals = FinalStreamTotals::new();
                let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
                let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
                let mut builder = new_directory_builder(&mut io, &mut route).expect("builder");
                if key {
                    let input =
                        decode_with_reservation(&mut io, &mut route, Some(16 * 1024), || {
                            Ok(fixture.clone())
                        })
                        .expect("input");
                    assert!(
                        builder
                            .push_directory_leaf(&mut io, &mut route, &input)
                            .await
                            .is_err()
                    );
                } else {
                    assert!(builder.finish_directory(&mut io, &mut route).await.is_err());
                }
                assert!(io.stopped);
                assert_eq!(io.allocation_underestimates(), 0);
            }
        }
    }

    #[tokio::test]
    async fn final_receipt_transport_reads_stable_ordinal_without_opening_control_route() {
        let (store, _) = range_tests::window_pending_store();
        let candidate = "88".repeat(32);
        let path = format!(
            "{}/restore/v7/{candidate}/receipts/{:020}.json",
            store.paths.base_prefix(),
            0
        );
        store
            .retention
            .put_raw(
                &path,
                Bytes::from_static(b"{}"),
                arco_core::WritePrecondition::DoesNotExist,
            )
            .await
            .expect("fixture receipt");
        let mut io = RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
        let mut totals = FinalStreamTotals::new();
        let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("receipt chunk");
        let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
        let (raw, metadata) = read_final_restore_receipt(&mut io, &mut route, &candidate, 0)
            .await
            .expect("receipt transport")
            .expect("receipt exists");
        assert_eq!(raw.as_slice(), b"{}");
        assert!(!metadata.value.version.is_empty());
        assert_eq!(io.reading_evidence().0, 1);
        assert_eq!(io.reading_evidence().1, 2);
        drop((raw, metadata));
        let prior = io.reading_evidence();
        assert!(
            read_restore_control_record(
                &mut io,
                &mut route,
                &candidate,
                RestoreControlRecord::Receipt(0)
            )
            .await
            .is_err()
        );
        assert_eq!(io.reading_evidence(), prior);
    }

    async fn native_counter_fixture() -> (ControlMvpStateStore, Vec<u8>) {
        let (fixture, _, _) = super::super::tests::fixture().await;
        let store =
            ControlMvpStateStore::new_synthetic_bounded(fixture.retention.clone(), fixture.scope)
                .expect("store");
        let directory = Directory::new(store.retention.clone(), &store.scope).expect("directory");
        let root = directory
            .empty_root_reference()
            .expect("empty root")
            .encode();
        (store, root)
    }

    #[cfg(feature = "test-utils")]
    #[tokio::test]
    async fn native_counter_capture_does_not_retain_global_maps() {
        let (store, raw) = native_counter_fixture().await;
        let mut io = RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        drop(super::super::super::cost::take());
        drop(super::super::super::cost::take_bounded_work());
        let decoded = decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
            Directory::new(store.retention.clone(), &store.scope)?.decode_root(&raw)
        })
        .expect("real directory constructor and root codec");
        drop(decoded);
        assert!(
            super::super::super::cost::take().is_empty(),
            "native closure retained a global WORK map"
        );
        assert!(
            super::super::super::cost::take_bounded_work().is_empty(),
            "native closure retained a global BOUNDED_WORK map"
        );
    }

    #[tokio::test]
    async fn native_counter_capture_records_directory_in_every_enabled_build() {
        let (store, raw) = native_counter_fixture().await;
        let mut io = RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let decoded = decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
            Directory::new(store.retention.clone(), &store.scope)?.decode_root(&raw)
        })
        .expect("real codec");
        drop(decoded);
        assert_eq!(
            io.work.native.slots[0], 1,
            "scope SHA must be captured in default and test-utils builds"
        );
        assert_eq!(
            io.work.native.slots[1],
            u64::try_from(
                b"arco.directory.scope.v1\0".len()
                    + 24
                    + store.scope.tenant_id().len()
                    + store.scope.workspace_id().len()
                    + store.scope.domain().len()
            )
            .expect("scope work")
        );
        assert_eq!(io.work.native.bounded.directory_references, 1);
        assert!(!io.work.native.overflow);
    }

    #[tokio::test]
    async fn native_counter_capture_keeps_failed_work_and_stops_route() {
        use super::super::super::cost;
        let (store, _) = native_counter_fixture().await;
        for case in 0..6 {
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            if case == 4 {
                io.work.native.slots[8] = u64::MAX;
            }
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            #[cfg(feature = "test-utils")]
            {
                cost::take();
                cost::take_bounded_work();
            }
            let result: CatalogResult<WorkingValue<Vec<u8>>> = decode_with_reservation(
                &mut io,
                &mut route,
                Some(if case == 2 { 8 } else { 64 * 1024 }),
                || {
                    cost::record(8, 1);
                    cost::bounded_work(cost::BoundedWork {
                        decoded_rows: 3,
                        ..Default::default()
                    });
                    match case {
                        0 => Err(super::super::invariant_violation("recorded error")),
                        1 => cost::allocated(24, || {
                            let value = std::hint::black_box(vec![0_u8; 64]);
                            std::hint::black_box(&value);
                            panic!("recorded nested allocation panic")
                        }),
                        2 => Ok(vec![0_u8; 1024]),
                        3 => {
                            cost::record(8, usize::MAX);
                            Ok(Vec::new())
                        }
                        4 => Ok(Vec::new()),
                        _ => {
                            cost::record(36, 1);
                            Ok(Vec::new())
                        }
                    }
                },
            );
            assert!(result.is_err(), "case {case}");
            assert!(io.stopped);
            assert_eq!(
                io.work.native.slots[8],
                if matches!(case, 3 | 4) { u64::MAX } else { 1 }
            );
            assert_eq!(io.work.native.bounded.decoded_rows, 3);
            assert_eq!(io.work.native.overflow, case >= 3);
            assert!(!cost::native_capture_active());
            if case == 1 {
                assert!(io.work.native.slots[24] > 0);
                assert!(io.work.native.slots[25] >= 64);
            }
            let report = io.ledger.lock().expect("ledger").report();
            assert_eq!(report.counter_overflow, case >= 3);
            assert!(
                report.working_live_bytes > 0,
                "failed output/error remains charged"
            );
            // The rejected allocation and the replacement bookkeeping error
            // are separately retained by the existing terminal-error path.
            assert_eq!(
                report.working_underestimates,
                match case {
                    0 | 1 => 0,
                    2 => 2,
                    _ => 1,
                }
            );
            assert!(
                decode_with_reservation(
                    &mut io,
                    &mut route,
                    Some(64 * 1024),
                    || -> CatalogResult<()> { panic!("stopped decoder") }
                )
                .is_err()
            );
            #[cfg(feature = "test-utils")]
            {
                assert!(cost::take().is_empty());
                assert!(cost::take_bounded_work().is_empty());
            }
            // A fresh owner still captures correctly after a nested panic.
            let next = cost::NativeCapture::begin();
            cost::record(9, 7);
            assert_eq!(next.finish().slots[9], 7);
        }
    }

    #[tokio::test]
    async fn native_counter_capture_survives_final_chunk_and_value_release() {
        use super::super::super::cost;
        let (store, _) = native_counter_fixture().await;
        let mut io = RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
        let mut totals = FinalStreamTotals::new();
        for expected in 1..=2 {
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
            drop(
                decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || {
                    cost::record(8, 1);
                    Ok(vec![0_u8; 32])
                })
                .expect("decode"),
            );
            assert_eq!(io.work.native.slots[8], expected);
        }
        assert_eq!(
            io.ledger
                .lock()
                .expect("ledger")
                .report()
                .working_live_bytes,
            0
        );
    }

    #[tokio::test]
    async fn restore_directory_reservation_rejects_before_constructor_and_io() {
        let (fixture, _, _) = super::super::tests::fixture().await;
        let store = ControlMvpStateStore::new_synthetic_bounded(
            fixture.retention.clone(),
            fixture.scope.clone(),
        )
        .expect("store");
        let directory = Directory::new(store.retention.clone(), &store.scope).expect("directory");
        let root = directory.empty_root().await.expect("persist empty");
        for (final_stream, kib, reads, decodes) in [
            (false, 8, 0, 0),
            (true, 8, 0, 0),
            (false, 24, 1, 2),
            (true, 24, 1, 2),
        ] {
            let mut io = RestorePhysicalIo::new(&store, kib * 1024, FINAL_MICROCHUNK_BYTES);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut route = if final_stream {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            let result = restore::first_after(&mut io, &mut route, &root, None).await;
            assert!(
                matches!(result, Err(CatalogError::MaintenanceBackpressure { .. })),
                "a small observed empty directory must not replace its a-priori reservation"
            );
            assert_eq!(io.work.decode_operations, decodes);
            assert_eq!(io.work.range_reads, reads);
            assert_eq!(io.work.metadata_heads, 0);
            assert!(io.stopped);
            assert_eq!(
                io.ledger
                    .lock()
                    .expect("ledger")
                    .report
                    .working_admission_failures,
                1
            );
        }
    }

    #[tokio::test]
    async fn restore_directory_scope_reservation_includes_long_path_errors() {
        let (fixture, _, _) = super::super::tests::fixture().await;
        for domain in ["x".repeat(32 * 1024), "%".repeat(32 * 1024)] {
            let mut store = ControlMvpStateStore::new_synthetic_bounded(
                fixture.retention.clone(),
                fixture.scope.clone(),
            )
            .expect("store");
            let directory =
                Directory::new(store.retention.clone(), &store.scope).expect("directory");
            let root = directory.empty_root_reference().expect("root");
            store.scope =
                StateScope::new(store.scope.tenant_id(), store.scope.workspace_id(), domain);
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let result = restore::first_after(&mut io, &mut route, &root, None).await;
            assert!(matches!(
                result,
                Err(CatalogError::Validation { .. } | CatalogError::InvariantViolation { .. })
            ));
            assert_eq!(io.work.decode_operations, 1);
            assert_eq!(io.work.range_reads, 0);
            assert!(io.stopped);
            let ledger = io.ledger.clone();
            assert_eq!(
                ledger.lock().expect("ledger").report.working_underestimates,
                0
            );
            drop(result);
            drop(io);
            assert!(ledger.lock().expect("ledger").report().passing());
        }
    }

    fn leaf(n: u64) -> Leaf {
        Leaf {
            first: (n * 4).to_be_bytes().to_vec(),
            last: (n * 4 + 1).to_be_bytes().to_vec(),
            rows: 2,
            bytes: 100,
            digest: [1; 32],
        }
    }

    #[tokio::test]
    async fn restore_native_directory_pins_paths_and_insertion_gaps_on_both_routes() {
        let (fixture, _, _) = super::super::tests::fixture().await;
        let store = ControlMvpStateStore::new_synthetic_bounded(
            fixture.retention.clone(),
            fixture.scope.clone(),
        )
        .expect("store");
        let directory = Directory::new(store.retention.clone(), &store.scope).expect("directory");
        let mut builder = directory.builder();
        for n in 0..513 {
            builder.push(leaf(n)).await.expect("leaf");
        }
        let root = builder.finish().await.expect("root");
        for final_stream in [false, true] {
            for n in [0_u64, 127, 128, 512, 513] {
                let after = n.checked_sub(1).map(|p| (p * 4 + 2).to_be_bytes());
                let mut io =
                    RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
                let mut workspace = WorkspaceIoBudget::new();
                let mut payload = UnitPayloadAdmission::new();
                let mut totals = FinalStreamTotals::new();
                let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
                let mut route = if final_stream {
                    RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
                } else {
                    RestorePhysicalRoute::OrdinaryUnit {
                        workspace: &mut workspace,
                        payload: &mut payload,
                    }
                };
                let result = restore::first_after(
                    &mut io,
                    &mut route,
                    &root,
                    after.as_ref().map(<[u8; 8]>::as_slice),
                )
                .await
                .expect("authenticated path");
                if n < 513 {
                    let selected = result.value.as_ref().expect("leaf exists");
                    assert_eq!(selected.leaf, leaf(n));
                    assert_eq!(selected.path.len(), usize::from(root.depth()));
                    assert_eq!(selected.path[0].1, u32::try_from(n / 128).expect("index"));
                    assert_eq!(selected.path[1].1, u32::try_from(n % 128).expect("index"));
                    assert_eq!(io.work.range_reads, u64::from(root.depth()));
                } else {
                    assert!(result.value.is_none());
                    assert_eq!(io.work.range_reads, 1);
                }
                assert!(io.work.native.slots[0] > 0);
                assert!(io.work.native.slots[1] > 0);
                assert_eq!(
                    io.work.native.bounded.directory_references,
                    match n {
                        0..512 => 133,
                        512 => 6,
                        _ => 5,
                    }
                );
                assert_eq!(io.work.native.bounded.streaming_builder_inputs, 0);
                assert!(!io.work.native.overflow);
                println!(
                    "native-parity directory final={final_stream} n={n} {:?}",
                    io.work.native
                );
                assert_eq!(io.work.metadata_heads, 0);
                assert_eq!(payload.input_bytes, 0);
                drop(result);
                assert!(io.ledger.lock().expect("ledger").report().passing());
            }
        }
    }

    #[tokio::test]
    async fn restore_native_directory_rejects_unselected_overlapping_sibling() {
        let (fixture, _, _) = super::super::tests::fixture().await;
        let store = ControlMvpStateStore::new_synthetic_bounded(
            fixture.retention.clone(),
            fixture.scope.clone(),
        )
        .expect("store");
        let directory = Directory::new(store.retention.clone(), &store.scope).expect("directory");
        let root = restore::overlapping_root(&directory).await;
        for after in [None, Some(b"z".as_slice())] {
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let Err(error) = restore::first_after(&mut io, &mut route, &root, after).await else {
                panic!("accepted overlapping excluded sibling")
            };
            assert!(
                matches!(error, CatalogError::InvariantViolation { .. }),
                "{error}"
            );
            assert!(io.stopped);
            assert_eq!(io.work.range_reads, 1);
            assert!(
                restore::first_after(&mut io, &mut route, &root, None)
                    .await
                    .is_err()
            );
            assert_eq!(
                io.work.range_reads, 1,
                "terminal failure must prevent later reads"
            );
        }
    }

    #[tokio::test]
    async fn restore_native_directory_empty_requires_bytes_and_metadata_admission() {
        let (fixture, _, _) = super::super::tests::fixture().await;
        let store = ControlMvpStateStore::new_synthetic_bounded(
            fixture.retention.clone(),
            fixture.scope.clone(),
        )
        .expect("store");
        let directory = Directory::new(store.retention.clone(), &store.scope).expect("directory");
        let root = directory.empty_root_reference().expect("empty reference");
        for phase in 0..3 {
            if phase == 1 {
                assert_eq!(directory.empty_root().await.expect("persist empty"), root);
            }
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let mut workspace = WorkspaceIoBudget::new();
            if phase == 2 {
                workspace
                    .reserve_bytes(crate::workspace_io_budget::METADATA_BYTES)
                    .expect("fill control admission");
            }
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let result = restore::first_after(&mut io, &mut route, &root, None).await;
            match phase {
                0 => {
                    assert!(
                        result.is_err(),
                        "missing empty page cannot establish an empty source"
                    );
                    assert!(io.stopped);
                    assert_eq!(io.work.range_reads, 1);
                }
                1 => {
                    assert!(result.expect("authenticated empty page").value.is_none());
                    assert!(!io.stopped);
                    assert_eq!(io.work.range_reads, 1);
                }
                2 => {
                    assert!(result.is_err());
                    assert!(io.stopped);
                    assert_eq!(
                        io.work.range_reads, 0,
                        "control admission must precede physical I/O"
                    );
                }
                _ => unreachable!(),
            }
        }
    }

    #[tokio::test]
    async fn restore_native_directory_checks_long_fences_and_changed_warm_objects() {
        for mutation in 0..4 {
            let (fixture, _, _) = super::super::tests::fixture().await;
            let store = ControlMvpStateStore::new_synthetic_bounded(
                fixture.retention.clone(),
                fixture.scope.clone(),
            )
            .expect("store");
            let directory =
                Directory::new(store.retention.clone(), &store.scope).expect("directory");
            let expected = Leaf {
                first: vec![1; MAX_BLOCK_BYTES],
                last: vec![2; MAX_BLOCK_BYTES],
                ..leaf(0)
            };
            let mut builder = directory.builder();
            builder.push(expected.clone()).await.expect("long leaf");
            let root = builder.finish().await.expect("root");
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut route = RestorePhysicalRoute::OrdinaryUnit {
                workspace: &mut workspace,
                payload: &mut payload,
            };
            let first = restore::first_after(&mut io, &mut route, &root, None)
                .await
                .expect("warm valid directory");
            let selected = first.value.as_ref().expect("leaf");
            assert_eq!(selected.leaf, expected);
            assert_eq!(
                io.work.range_reads, 5,
                "one page, both complete fences, both selected fences"
            );
            let page_path = format!(
                "control/directory/v1/domains/{}/pages/{}",
                store.scope.domain(),
                hex::encode(selected.path[0].0)
            );
            let bytes = store.retention.get_raw(&page_path).await.expect("page");
            drop(first);
            assert!(io.ledger.lock().expect("ledger").report().passing());
            if mutation == 0 {
                continue;
            }
            let (path, replacement) = match mutation {
                1 => {
                    // directory-v1 fixed header (43) + child depth/length/rows (13)
                    let key_path = format!(
                        "control/directory/v1/domains/{}/keys/{}",
                        store.scope.domain(),
                        hex::encode(&bytes[56..88])
                    );
                    (key_path, Bytes::from(vec![3; MAX_BLOCK_BYTES]))
                }
                2 => {
                    let mut changed = bytes.to_vec();
                    changed[0] ^= 1;
                    (page_path, Bytes::from(changed))
                }
                3 => {
                    let mut changed = bytes.to_vec();
                    changed.push(0);
                    (page_path, Bytes::from(changed))
                }
                _ => unreachable!(),
            };
            store
                .retention
                .put_raw(&path, replacement, arco_core::WritePrecondition::None)
                .await
                .expect("corrupt warm object");
            let Err(error) = restore::first_after(&mut io, &mut route, &root, None).await else {
                panic!("accepted corrupt directory object {mutation}")
            };
            assert!(matches!(
                error,
                CatalogError::InvariantViolation { .. }
                    | CatalogError::MaintenanceBackpressure { .. }
            ));
            assert!(io.stopped);
            assert_eq!(io.work.range_reads, if mutation == 1 { 7 } else { 6 });
        }
    }

    #[tokio::test]
    async fn restore_native_directory_selected_leaf_reaches_real_authenticated_payload() {
        let (fixture, _, leaf) = super::super::tests::fixture().await;
        let store = ControlMvpStateStore::new_synthetic_bounded(
            fixture.retention.clone(),
            fixture.scope.clone(),
        )
        .expect("store");
        let directory = Directory::new(store.retention.clone(), &store.scope).expect("directory");
        let mut builder = directory.builder();
        builder.push(leaf).await.expect("physical leaf");
        let root = builder.finish().await.expect("root");
        let mut io = RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
        let mut workspace = WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        let position = restore::first_after(&mut io, &mut route, &root, None)
            .await
            .expect("membership");
        let descriptor = read_restore_descriptor(
            &mut io,
            &mut route,
            super::super::Role::Kv,
            &position.value.as_ref().expect("leaf").leaf,
        )
        .await
        .expect("descriptor and index");
        let rows = read_restore_payload(&mut io, &mut route, &descriptor.value)
            .await
            .expect("physical rows");
        assert_eq!(rows.value[0].key, b"key");
        assert_eq!(io.work.range_reads, 4);
        assert_eq!(io.work.metadata_heads, 6);
        drop(rows);
        drop(descriptor);
        drop(position);
        assert!(io.ledger.lock().expect("ledger").report().passing());
    }

    #[tokio::test]
    async fn restore_native_directory_dense_path_preserves_distinct_admission_limits() {
        let (fixture, _, _) = super::super::tests::fixture().await;
        let store = ControlMvpStateStore::new_synthetic_bounded(
            fixture.retention.clone(),
            fixture.scope.clone(),
        )
        .expect("store");
        let directory = Directory::new(store.retention.clone(), &store.scope).expect("directory");
        let root = restore::dense_eight_level_path(&directory).await;
        for final_stream in [false, true] {
            let mut io =
                RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
            let mut workspace = WorkspaceIoBudget::new();
            let mut payload = UnitPayloadAdmission::new();
            let mut totals = FinalStreamTotals::new();
            let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).expect("chunk");
            let mut route = if final_stream {
                RestorePhysicalRoute::FinalMicrochunk(&mut chunk)
            } else {
                RestorePhysicalRoute::OrdinaryUnit {
                    workspace: &mut workspace,
                    payload: &mut payload,
                }
            };
            let result = restore::first_after(&mut io, &mut route, &root, None).await;
            if final_stream {
                let result = result.expect("declared final path fits its separate limit");
                let selected = result.value.as_ref().expect("selected leaf");
                assert_eq!(selected.path.len(), 8);
                assert!(selected.path.iter().all(|(_, index)| *index == 0));
                assert_eq!(io.work.range_reads, 2058);
                assert_eq!(chunk.totals.total_operations, 2058);
                assert_eq!(chunk.totals.total_io_reservation_bytes, 33_586_282);
                drop(result);
                assert!(io.ledger.lock().expect("ledger").report().passing());
            } else {
                let Err(error) = result else {
                    panic!("ordinary path bypassed its metadata ceiling")
                };
                assert!(matches!(
                    error,
                    CatalogError::MaintenanceBackpressure { .. }
                ));
                assert!(io.stopped);
                assert_eq!(
                    io.work.range_reads, 2056,
                    "stop before first additional selected-fence read"
                );
                assert_eq!(
                    workspace.test_accounting().1,
                    2057,
                    "failed admission remains charged"
                );
            }
        }
    }
}

#[cfg(test)]
mod final_descriptor_ownership_tests {
    use super::*;

    #[test]
    fn final_chunk_admission_failure_retains_diagnostic_and_stops_owner() {
        let (store, _) = window_pending_store();
        let mut io = RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
        let mut totals = FinalStreamTotals::new();
        let error = FinalMicrochunk::begin(&mut totals, usize::MAX, &mut io)
            .err()
            .unwrap();
        assert_eq!(
            io.live_ownership_evidence(),
            (catalog_error_string_capacity(&error).unwrap(), 0),
            "the returned admission diagnostic remains owned until the invocation drops"
        );
        assert!(io.stopped);
        assert!(totals.stopped);
        assert_eq!(io.allocation_underestimates(), 0);
    }

    #[tokio::test]
    async fn final_descriptor_declaration_is_owned_across_pending_head() {
        use std::sync::atomic::Ordering;
        let (_, _, leaf) = super::super::tests::fixture().await;
        let (store, remaining) = window_pending_store();
        let mut io = RestorePhysicalIo::new(&store, FINAL_MICROCHUNK_BYTES, FINAL_MICROCHUNK_BYTES);
        let ledger = io.ledger.clone();
        let mut totals = FinalStreamTotals::new();
        let mut chunk = FinalMicrochunk::begin(&mut totals, 0, &mut io).unwrap();
        let mut route = RestorePhysicalRoute::FinalMicrochunk(&mut chunk);
        let leaf =
            decode_with_reservation(&mut io, &mut route, Some(64 * 1024), || Ok(leaf.clone()))
                .unwrap();
        let baseline = ledger.lock().unwrap().report().working_live_bytes;
        remaining.store(1, Ordering::SeqCst);
        let mut pending = Box::pin(read_final_descriptor(&mut io, &mut route, &leaf));
        assert!(matches!(
            futures::poll!(pending.as_mut()),
            std::task::Poll::Pending
        ));
        let carried = ledger.lock().unwrap().report().working_live_bytes;
        assert!(
            carried > baseline,
            "descriptor declaration must retain its owner across the HEAD await: baseline={baseline}, carried={carried}"
        );
        drop(pending);
        assert_eq!(ledger.lock().unwrap().report().working_live_bytes, baseline);
        drop(leaf);
        assert_eq!(io.live_ownership_evidence(), (0, 0));
        assert_eq!(io.allocation_underestimates(), 0);
    }
}
