//! Authenticated immutable read ownership. No authority decision is cached.
// Lock scopes cover complete directory/ledger ownership transitions.
#![allow(clippy::significant_drop_tightening)]
use super::cost;
use super::{
    Bytes, CONTROL_MVP_FORMAT_VERSION, CatalogError, ControlMvpBlock, ControlMvpScopeDoc,
    ControlMvpSegmentIndex, ControlMvpSegmentLevel, ControlMvpSegmentRef, ControlMvpSegmentRow,
    ControlMvpStateStore, ControlMvpTxObject, ControlMvpTxRef, MAX_SEGMENT_BYTES,
    MAX_SEGMENT_INDEX_BYTES, MAX_SEGMENT_ROWS, MAX_TRANSACTION_JSON_BYTES, Result,
    SEGMENT_FORMAT_VERSION, SEGMENT_RECORD_KV, SEGMENT_RECORD_OUTBOX, SEGMENT_RECORD_OUTBOX_TRIM,
    Serialize, StateScope, StateStoreBindingIdentity, decode_segment_rows, fmt, integrity,
    invariant_violation, segment_serialization_error, valid_raw_digest,
    validate_segment_index_identity, validation_failed,
};
use futures::FutureExt;
use futures::future::{BoxFuture, Shared, WeakShared};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

#[cfg(test)]
mod tests;

const MIB: usize = 1024 * 1024;
const RECORD: usize = 4096;
const PARTICIPANT: usize = 512;
const ADMIN: usize = 8192;
const ALLOCATION: usize = 64;
const MAX_ENTRY: usize = 8 * MIB;
const MAX_RESERVATIONS: usize = 64 * MIB;
const MAX_LOADS: usize = 8;

/// Byte capacities include reservations and evicted entries with live leases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ControlMvpReadCacheConfig {
    /// Authenticated directories, transaction metadata and complete certificates.
    pub metadata_bytes: usize,
    /// Validated owned decoded block rows.
    pub decoded_bytes: usize,
}
impl Default for ControlMvpReadCacheConfig {
    fn default() -> Self {
        Self {
            metadata_bytes: 32 * MIB,
            decoded_bytes: 128 * MIB,
        }
    }
}

/// Current and peak ownership in one independently bounded pool.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ControlMvpReadCachePoolStatistics {
    /// Configured capacity.
    pub capacity_bytes: usize,
    /// Fixed handle administration charged to this pool.
    pub administration_bytes: usize,
    /// Retained discoverable entries.
    pub resident_bytes: usize,
    /// In-flight reservations.
    pub reserved_bytes: usize,
    /// Nonresident entries still leased, including declined admissions.
    pub live_evicted_bytes: usize,
    /// Active participant bookkeeping.
    pub participant_bytes: usize,
    /// Records including reservations and evicted leases.
    pub live_records: usize,
    /// Maximum sum of all ownership categories.
    pub high_water_bytes: usize,
    /// Maximum simultaneous records.
    pub high_water_records: usize,
    /// Peak discoverable ownership.
    pub high_water_resident_bytes: usize,
    /// Peak reservations in this pool.
    pub high_water_reserved_bytes: usize,
    /// Peak evicted ownership still held by leases.
    pub high_water_live_evicted_bytes: usize,
    /// Peak participant bookkeeping in this pool.
    pub high_water_participant_bytes: usize,
}
impl ControlMvpReadCachePoolStatistics {
    fn used(&self) -> usize {
        self.administration_bytes
            + self.resident_bytes
            + self.reserved_bytes
            + self.live_evicted_bytes
            + self.participant_bytes
    }
    fn observe(&mut self) {
        self.high_water_bytes = self.high_water_bytes.max(self.used());
        self.high_water_records = self.high_water_records.max(self.live_records);
        self.high_water_resident_bytes = self.high_water_resident_bytes.max(self.resident_bytes);
        self.high_water_reserved_bytes = self.high_water_reserved_bytes.max(self.reserved_bytes);
        self.high_water_live_evicted_bytes = self
            .high_water_live_evicted_bytes
            .max(self.live_evicted_bytes);
        self.high_water_participant_bytes = self
            .high_water_participant_bytes
            .max(self.participant_bytes);
    }
}

/// Aggregate counters for one shared cache identity.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ControlMvpReadCacheStatistics {
    /// Metadata ownership.
    pub metadata: ControlMvpReadCachePoolStatistics,
    /// Decoded ownership.
    pub decoded: ControlMvpReadCachePoolStatistics,
    /// Eligible caller demands, including fallbacks.
    pub demands: u64,
    /// Resident substitutions.
    pub hits: u64,
    /// Newly admitted loads.
    pub loads: u64,
    /// Callers joining existing work.
    pub coalesced: u64,
    /// Direct fallbacks and completed entries declined for residency.
    pub fallbacks: u64,
    /// FIFO evictions.
    pub evictions: u64,
    /// Loaded results declined for residency.
    pub declined: u64,
    /// Failed shared loads.
    pub failures: u64,
    /// Reservation underestimates; any nonzero count fails qualification.
    pub underestimates: u64,
    /// Current loads.
    pub active_loads: usize,
    /// Current load participants.
    pub participants: usize,
    /// Peak loads.
    pub high_water_loads: usize,
    /// Peak aggregate participants.
    pub high_water_participants: usize,
    /// Peak aggregate reservation bytes.
    pub high_water_reservations: usize,
    /// Peak participants attached to any single load.
    pub high_water_participants_per_load: usize,
}

/// Opaque process-local read cache. Clones share ownership and backend identity.
#[derive(Clone)]
pub struct ControlMvpReadCache(Arc<Inner>);
impl fmt::Debug for ControlMvpReadCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlMvpReadCache")
            .finish_non_exhaustive()
    }
}
struct Inner {
    binding: StateStoreBindingIdentity,
    physical: arco_core::AuthorityScope,
    scope: StateScope,
    ledger: Arc<Mutex<ControlMvpReadCacheStatistics>>,
    directory: Mutex<Directory>,
}
#[derive(Default)]
struct Directory {
    entries: BTreeMap<Arc<String>, Arc<Entry>>,
    fifo: BTreeMap<u64, Arc<String>>,
    flights: [Option<Flight>; MAX_LOADS],
    generation: u64,
}
struct Flight {
    key: Arc<String>,
    generation: u64,
    future: WeakShared<LoadFuture>,
    participants: Arc<AtomicUsize>,
}
type LoadFuture = BoxFuture<'static, std::result::Result<Arc<Entry>, Arc<CatalogError>>>;
type Load = Shared<LoadFuture>;
#[derive(Clone, Copy, PartialEq, Eq)]
enum Pool {
    Metadata,
    Decoded,
}
impl Pool {
    fn stats(
        self,
        ledger: &mut ControlMvpReadCacheStatistics,
    ) -> &mut ControlMvpReadCachePoolStatistics {
        match self {
            Self::Metadata => &mut ledger.metadata,
            Self::Decoded => &mut ledger.decoded,
        }
    }
    const fn records(self) -> usize {
        match self {
            Self::Metadata => 1024,
            Self::Decoded => 4096,
        }
    }
}
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Only loaders in this module construct cache payloads, after validation.
enum Value {
    Directory(Bytes, ControlMvpSegmentIndex),
    Transaction(ControlMvpTxObject),
    Block(Vec<ControlMvpSegmentRow>),
    Certificate(Vec<Arc<Entry>>),
    Unavailable,
}
impl Value {
    fn charge(&self) -> usize {
        match self {
            Self::Certificate(blocks) => heap(blocks.capacity() * size_of::<Arc<Entry>>()),
            Self::Unavailable => 0,
            Self::Directory(bytes, index) => bytes.len() + ALLOCATION + index_charge(index),
            Self::Transaction(tx) => tx_charge(tx),
            Self::Block(rows) => {
                heap(rows.capacity() * size_of::<ControlMvpSegmentRow>())
                    + rows
                        .iter()
                        .map(|r| {
                            heap(r.key.capacity())
                                + r.value.as_ref().map_or(0, |v| heap(v.capacity()))
                        })
                        .sum::<usize>()
            }
        }
    }
}
fn heap(bytes: usize) -> usize {
    if bytes == 0 { 0 } else { bytes + ALLOCATION }
}
fn key_charge(key: &String) -> usize {
    heap(key.capacity()) + heap(size_of::<String>() + 2 * size_of::<usize>())
}
fn string(s: &String) -> usize {
    heap(s.capacity())
}
fn optional(s: Option<&String>) -> usize {
    s.map_or(0, string)
}
fn scope_charge(s: &ControlMvpScopeDoc) -> usize {
    string(&s.tenant_id) + string(&s.workspace_id) + string(&s.domain)
}
fn reference_charge(r: &ControlMvpSegmentRef) -> usize {
    string(&r.segment_id) + string(&r.checksum_sha256) + string(&r.index_checksum_sha256)
}
fn block_charge(b: &ControlMvpBlock) -> usize {
    optional(b.min_key_hex.as_ref()) + optional(b.max_key_hex.as_ref()) + string(&b.checksum_sha256)
}
fn index_charge(i: &ControlMvpSegmentIndex) -> usize {
    heap(i.blocks.capacity() * size_of::<ControlMvpBlock>())
        + i.blocks.iter().map(block_charge).sum::<usize>()
        + heap(i.record_batch_offsets.capacity() * size_of::<u64>())
        + string(&i.implementation)
        + scope_charge(&i.scope)
        + string(&i.segment_id)
        + optional(i.min_key_hex.as_ref())
        + optional(i.max_key_hex.as_ref())
        + optional(i.min_key_utf8.as_ref())
        + optional(i.max_key_utf8.as_ref())
        + string(&i.bloom_bits_hex)
        + string(&i.segment_checksum_sha256)
}
fn tx_charge(t: &ControlMvpTxObject) -> usize {
    string(&t.history.preceding_root)
        + string(&t.history.mutation_sha256)
        + string(&t.history.resulting_root)
        + string(&t.implementation)
        + scope_charge(&t.scope)
        + string(&t.tx_id)
        + optional(t.base_manifest_id.as_ref())
        + optional(t.request_id.as_ref())
        + reference_charge(&t.l0_segment)
}
struct Entry {
    key: Arc<String>,
    value: Value,
    charge: usize,
    pool: Pool,
    resident: AtomicBool,
    ledger: Arc<Mutex<ControlMvpReadCacheStatistics>>,
}
impl Drop for Entry {
    fn drop(&mut self) {
        let mut l = lock(&self.ledger);
        let p = self.pool.stats(&mut l);
        if self.resident.load(Ordering::Relaxed) {
            p.resident_bytes -= self.charge;
        } else {
            p.live_evicted_bytes -= self.charge;
        }
        p.live_records -= 1;
    }
}
struct Request {
    key: Arc<String>,
    pool: Pool,
    reservation: usize,
}
impl Request {
    #[cfg(test)]
    fn test(key: &str, pool: Pool, reservation: usize) -> Self {
        Self {
            key: Arc::new(key.to_string()),
            pool,
            reservation,
        }
    }
}
struct Reservation {
    bytes: usize,
    pool: Pool,
    ledger: Arc<Mutex<ControlMvpReadCacheStatistics>>,
    transferred: bool,
}
impl Reservation {
    fn finish(mut self, key: Arc<String>, value: Value, eligible: bool) -> Result<Arc<Entry>> {
        // Entry, lookup table, FIFO and flight share one owning key allocation.
        let charge = RECORD + key_charge(&key) + value.charge();
        let mut l = lock(&self.ledger);
        if charge > self.bytes {
            l.underestimates += 1;
            return Err(invariant_violation(
                "read cache reservation underestimated ownership",
            ));
        }
        let resident = eligible && charge <= MAX_ENTRY;
        if !resident {
            l.declined += 1;
            l.fallbacks += 1;
        }
        let p = self.pool.stats(&mut l);
        p.reserved_bytes -= self.bytes;
        // Completion returns a charged lease; insertion marks residency separately.
        p.live_evicted_bytes += charge;
        p.observe();
        self.transferred = true;
        Ok(Arc::new(Entry {
            key,
            value,
            charge,
            pool: self.pool,
            resident: AtomicBool::new(false),
            ledger: self.ledger.clone(),
        }))
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.transferred {
            let mut l = lock(&self.ledger);
            let p = self.pool.stats(&mut l);
            p.reserved_bytes -= self.bytes;
            p.live_records -= 1;
        }
    }
}
struct Participant {
    ledger: Arc<Mutex<ControlMvpReadCacheStatistics>>,
    pool: Pool,
    count: Arc<AtomicUsize>,
}
impl Participant {
    fn new(
        ledger: &Arc<Mutex<ControlMvpReadCacheStatistics>>,
        pool: Pool,
        count: &Arc<AtomicUsize>,
    ) -> Option<Self> {
        let mut l = lock(ledger);
        if count.load(Ordering::Relaxed) >= 32 || l.participants >= 256 {
            return None;
        }
        let p = pool.stats(&mut l);
        if p.used().checked_add(PARTICIPANT)? > p.capacity_bytes {
            return None;
        }
        p.participant_bytes += PARTICIPANT;
        p.observe();
        l.participants += 1;
        l.high_water_participants = l.high_water_participants.max(l.participants);
        let participants = count.fetch_add(1, Ordering::Relaxed) + 1;
        l.high_water_participants_per_load = l.high_water_participants_per_load.max(participants);
        Some(Self {
            ledger: ledger.clone(),
            pool,
            count: count.clone(),
        })
    }
}
impl Drop for Participant {
    fn drop(&mut self) {
        let mut l = lock(&self.ledger);
        l.participants -= 1;
        self.pool.stats(&mut l).participant_bytes -= PARTICIPANT;
        self.count.fetch_sub(1, Ordering::Relaxed);
    }
}
enum Selected {
    Hit(Arc<Entry>),
    Flight(Load, Participant),
    Fallback,
}

struct Cleanup {
    inner: Weak<Inner>,
    generation: u64,
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            let mut d = lock(&inner.directory);
            for slot in &mut d.flights {
                if slot
                    .as_ref()
                    .is_some_and(|f| f.generation == self.generation)
                {
                    *slot = None;
                }
            }
            lock(&inner.ledger).active_loads -= 1;
        }
    }
}

impl ControlMvpReadCache {
    pub(super) fn new(
        store: &ControlMvpStateStore,
        config: ControlMvpReadCacheConfig,
    ) -> Option<Self> {
        let identity_bytes = 2 * heap(store.scope.tenant_id().len())
            + 2 * heap(store.scope.workspace_id().len())
            + heap(store.scope.domain().len());
        let administration = ADMIN.saturating_add(identity_bytes);
        if config.metadata_bytes < administration || config.decoded_bytes == 0 {
            return None;
        }
        let mut stats = ControlMvpReadCacheStatistics::default();
        stats.metadata.capacity_bytes = config.metadata_bytes;
        stats.metadata.administration_bytes = administration;
        stats.metadata.observe();
        stats.decoded.capacity_bytes = config.decoded_bytes;
        Some(Self(Arc::new(Inner {
            binding: store.binding_identity.clone(),
            physical: store.retention.scope().clone(),
            scope: store.scope.clone(),
            ledger: Arc::new(Mutex::new(stats)),
            directory: Mutex::new(Directory::default()),
        })))
    }
    /// Returns an atomic snapshot of aggregate ownership and event counters.
    #[must_use]
    pub fn statistics(&self) -> ControlMvpReadCacheStatistics {
        lock(&self.0.ledger).clone()
    }
    fn fallback(&self) {
        lock(&self.0.ledger).fallbacks += 1;
    }
    fn reserve_locked(&self, request: &Request, d: &mut Directory) -> Option<Reservation> {
        if (request.pool == Pool::Metadata && request.reservation > 16 * MIB)
            || request.reservation > MAX_RESERVATIONS
        {
            return None;
        }
        loop {
            let mut l = lock(&self.0.ledger);
            if l.metadata.reserved_bytes + l.decoded.reserved_bytes + request.reservation
                > MAX_RESERVATIONS
            {
                return None;
            }
            let p = request.pool.stats(&mut l);
            if request.reservation + PARTICIPANT > p.capacity_bytes {
                return None;
            }
            if p.live_records < request.pool.records()
                && p.used() + request.reservation + PARTICIPANT <= p.capacity_bytes
            {
                p.reserved_bytes += request.reservation;
                p.live_records += 1;
                p.observe();
                l.high_water_reservations = l
                    .high_water_reservations
                    .max(l.metadata.reserved_bytes + l.decoded.reserved_bytes);
                return Some(Reservation {
                    bytes: request.reservation,
                    pool: request.pool,
                    ledger: self.0.ledger.clone(),
                    transferred: false,
                });
            }
            drop(l);
            if !self.evict_locked(request.pool, d) {
                return None;
            }
        }
    }
    #[cfg(test)]
    fn reserve(&self, request: &Request) -> Option<Reservation> {
        self.reserve_locked(request, &mut lock(&self.0.directory))
    }
    fn remove_locked(&self, key: &Arc<String>, d: &mut Directory) {
        d.fifo.retain(|_, queued| queued.as_str() != key.as_str());
        if let Some(entry) = d.entries.remove(key) {
            let mut l = lock(&self.0.ledger);
            let p = entry.pool.stats(&mut l);
            p.resident_bytes -= entry.charge;
            p.live_evicted_bytes += entry.charge;
            p.observe();
            entry.resident.store(false, Ordering::Relaxed);
            l.evictions += 1;
            drop(l);
            drop(entry);
        }
    }
    fn evict_locked(&self, pool: Pool, d: &mut Directory) -> bool {
        // ponytail: at most 5,120 FIFO records; separate pool queues if eviction scans become material.
        let selected = d
            .fifo
            .values()
            .filter_map(|key| d.entries.get(key))
            .find(|entry| entry.pool == pool)
            .cloned();
        let Some(selected) = selected else {
            return false;
        };
        self.remove_locked(&selected.key, d);
        // Hold entry leases, rather than detached key allocations, while removing
        // certificates so their complete ownership remains charged throughout.
        let certificates = d
            .entries
            .values()
            .filter(|entry| match &entry.value {
                Value::Certificate(blocks) => blocks
                    .iter()
                    .any(|block| !d.entries.contains_key(&block.key)),
                _ => false,
            })
            .cloned()
            .collect::<Vec<_>>();
        for entry in certificates {
            self.remove_locked(&entry.key, d);
        }
        true
    }
    #[cfg(test)]
    fn evict(&self, pool: Pool) {
        self.evict_locked(pool, &mut lock(&self.0.directory));
    }
    fn insert(&self, entry: Arc<Entry>) -> Arc<Entry> {
        let mut d = lock(&self.0.directory);
        if let Some(existing) = d.entries.get(&entry.key) {
            let mut l = lock(&self.0.ledger);
            l.declined += 1;
            l.fallbacks += 1;
            return existing.clone();
        }
        let Some(generation) = d.generation.checked_add(1) else {
            let mut l = lock(&self.0.ledger);
            l.declined += 1;
            l.fallbacks += 1;
            return entry;
        };
        d.generation = generation;
        let mut l = lock(&self.0.ledger);
        let p = entry.pool.stats(&mut l);
        p.live_evicted_bytes -= entry.charge;
        p.resident_bytes += entry.charge;
        p.observe();
        entry.resident.store(true, Ordering::Relaxed);
        d.fifo.insert(generation, entry.key.clone());
        d.entries.insert(entry.key.clone(), entry.clone());
        entry
    }

    /// Admission is synchronous; the returned future owns only the selected lease
    /// or participant. Loader captures do not inflate every caller's future.
    fn load<F, Fut>(
        &self,
        request: Request,
        loader: F,
    ) -> impl Future<Output = Result<Option<Arc<Entry>>>> + Send
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(Value, bool)>> + Send + 'static,
    {
        let selected = self.select(request, loader);
        async move {
            match selected? {
                Selected::Hit(entry) => Ok(Some(entry)),
                Selected::Fallback => Ok(None),
                Selected::Flight(future, _participant) => {
                    future.await.map(Some).map_err(|error| copy_error(&error))
                }
            }
        }
    }

    /// Every caller independently observes object lifetime before calling this.
    // Slot indices come from the fixed-size arrays; one lock publishes one flight.
    #[allow(clippy::too_many_lines, clippy::indexing_slicing)]
    fn select<F, Fut>(&self, mut request: Request, loader: F) -> Result<Selected>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(Value, bool)>> + Send + 'static,
    {
        request.reservation = request.reservation.saturating_add(heap(size_of::<Fut>()));
        let mut deferred: [Option<Load>; MAX_LOADS + 1] = std::array::from_fn(|_| None);
        let selected: Option<(Load, Participant)> = {
            let mut d = lock(&self.0.directory);
            if let Some(entry) = d.entries.get(&request.key) {
                let complete = match &entry.value {
                    Value::Certificate(blocks) => blocks.iter().all(|block| {
                        d.entries
                            .get(&block.key)
                            .is_some_and(|resident| Arc::ptr_eq(resident, block))
                    }),
                    _ => true,
                };
                if complete {
                    lock(&self.0.ledger).hits += 1;
                    return Ok(Selected::Hit(entry.clone()));
                }
                self.remove_locked(&request.key, &mut d);
            }
            let mut joined = None;
            for (index, slot) in d.flights.iter_mut().enumerate() {
                if let Some(f) = slot {
                    if let Some(shared) = f.future.upgrade() {
                        if f.key == request.key {
                            joined = Some((shared, f.participants.clone()));
                            break;
                        }
                        deferred[index] = Some(shared);
                    } else {
                        *slot = None;
                    }
                }
            }
            if let Some((shared, count)) = joined {
                if let Some(participant) = Participant::new(&self.0.ledger, request.pool, &count) {
                    lock(&self.0.ledger).coalesced += 1;
                    Some((shared, participant))
                } else {
                    deferred[MAX_LOADS] = Some(shared);
                    None
                }
            } else if let Some(slot) = d.flights.iter().position(Option::is_none) {
                if lock(&self.0.ledger).active_loads >= MAX_LOADS {
                    None
                } else if let Some(reservation) = self.reserve_locked(&request, &mut d) {
                    let count = Arc::new(AtomicUsize::new(0));
                    if let Some(participant) =
                        Participant::new(&self.0.ledger, request.pool, &count)
                    {
                        d.generation = d.generation.checked_add(1).ok_or_else(|| {
                            invariant_violation("cache flight generation exhausted")
                        })?;
                        let generation = d.generation;
                        let cleanup = Cleanup {
                            inner: Arc::downgrade(&self.0),
                            generation,
                        };
                        let cache = self.clone();
                        let key = request.key.clone();
                        let future = loader();
                        #[cfg(feature = "test-utils")]
                        let phase = cost::current();
                        let future = async move {
                            let _cleanup = cleanup;
                            #[cfg(feature = "test-utils")]
                            let future = cost::phase(phase, future);
                            let result = future.await.and_then(|(value, eligible)| {
                                let entry = reservation.finish(key, value, eligible)?;
                                if eligible && entry.charge <= MAX_ENTRY {
                                    cache.insert(entry.clone());
                                }
                                Ok(entry)
                            });
                            if result.is_err() {
                                lock(&cache.0.ledger).failures += 1;
                            }
                            result.map_err(Arc::new)
                        }
                        .boxed()
                        .shared();
                        d.flights[slot] = Some(Flight {
                            key: request.key,
                            generation,
                            future: future
                                .downgrade()
                                .ok_or_else(|| invariant_violation("new shared load missing"))?,
                            participants: count,
                        });
                        let mut l = lock(&self.0.ledger);
                        l.loads += 1;
                        l.active_loads += 1;
                        l.high_water_loads = l.high_water_loads.max(l.active_loads);
                        Some((future, participant))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };
        drop(deferred);
        if let Some((future, participant)) = selected {
            Ok(Selected::Flight(future, participant))
        } else {
            self.fallback();
            Ok(Selected::Fallback)
        }
    }
}

// Private fan-out preserves every typed variant and field without changing the public error contract.
fn copy_error(error: &CatalogError) -> CatalogError {
    cost::allocated(28, || copy_error_direct(error))
}
fn copy_error_direct(error: &CatalogError) -> CatalogError {
    match error {
        CatalogError::Storage { message } => CatalogError::Storage {
            message: message.clone(),
        },
        CatalogError::Serialization { message } => CatalogError::Serialization {
            message: message.clone(),
        },
        CatalogError::Parquet { message } => CatalogError::Parquet {
            message: message.clone(),
        },
        CatalogError::Validation { message } => CatalogError::Validation {
            message: message.clone(),
        },
        CatalogError::AlreadyExists { entity, name } => CatalogError::AlreadyExists {
            entity: entity.clone(),
            name: name.clone(),
        },
        CatalogError::NotFound { entity, name } => CatalogError::NotFound {
            entity: entity.clone(),
            name: name.clone(),
        },
        CatalogError::PreconditionFailed { message } => CatalogError::PreconditionFailed {
            message: message.clone(),
        },
        CatalogError::CasFailed { message } => CatalogError::CasFailed {
            message: message.clone(),
        },
        CatalogError::StaleWriterEpoch { message } => CatalogError::StaleWriterEpoch {
            message: message.clone(),
        },
        CatalogError::AmbiguousAuthorityOutcome { message } => {
            CatalogError::AmbiguousAuthorityOutcome {
                message: message.clone(),
            }
        }
        CatalogError::MaintenanceBackpressure { message } => {
            CatalogError::MaintenanceBackpressure {
                message: message.clone(),
            }
        }
        CatalogError::UnsupportedAuthorityFormat { message } => {
            CatalogError::UnsupportedAuthorityFormat {
                message: message.clone(),
            }
        }
        CatalogError::RequestFailed {
            http_status,
            message,
        } => CatalogError::RequestFailed {
            http_status: *http_status,
            message: message.clone(),
        },
        CatalogError::InvariantViolation { message } => CatalogError::InvariantViolation {
            message: message.clone(),
        },
        CatalogError::UnsupportedOperation { message } => CatalogError::UnsupportedOperation {
            message: message.clone(),
        },
    }
}

impl ControlMvpStateStore {
    /// Replaces the read cache with an empty cache using these byte capacities.
    /// Zero capacity disables shared caches.
    ///
    /// # Errors
    /// Rejects nonzero capacities that cannot fund the handle administration and
    /// retained scope identity. Entry admission failures use counted direct reads.
    pub fn with_read_cache_config(mut self, config: ControlMvpReadCacheConfig) -> Result<Self> {
        self.read_cache = ControlMvpReadCache::new(&self, config);
        if config.metadata_bytes != 0 && config.decoded_bytes != 0 && self.read_cache.is_none() {
            return Err(validation_failed(
                "read cache metadata capacity cannot fund administration",
            ));
        }
        Ok(self)
    }
    /// Returns the shared cache handle, if enabled.
    #[must_use]
    pub fn read_cache(&self) -> Option<ControlMvpReadCache> {
        self.read_cache.clone()
    }
    /// Attaches a cache from the identical backend, typed root and state scope.
    ///
    /// # Errors
    /// Returns a validation error for incompatible backend or scope identity.
    pub fn with_read_cache(mut self, cache: ControlMvpReadCache) -> Result<Self> {
        if cache.0.binding != self.binding_identity
            || cache.0.physical != *self.retention.scope()
            || cache.0.scope != self.scope
        {
            return Err(validation_failed("read cache backend or scope mismatch"));
        }
        self.read_cache = Some(cache);
        Ok(self)
    }
    /// Uses the original direct read path, without cache HEAD probes or bookkeeping.
    #[must_use]
    pub fn without_read_cache(mut self) -> Self {
        self.read_cache = None;
        self
    }

    async fn cache_version(
        &self,
        cache: &ControlMvpReadCache,
        path: &str,
        size: u64,
    ) -> Option<String> {
        match self.storage.head(path).await {
            Ok(Some(meta)) if meta.size == size && !meta.version.is_empty() => {
                // Backend strings may have arbitrary spare capacity. Only the
                // normalized ownership can be retained by a cache-owned loader.
                Some(meta.version.into_boxed_str().into())
            }
            _ => {
                cache.fallback();
                None
            }
        }
    }
    async fn cache_version_matches(&self, path: &str, size: u64, version: &str) -> bool {
        matches!(self.storage.head(path).await, Ok(Some(meta)) if meta.size == size && meta.version == version && !meta.version.is_empty())
    }
    fn cache_request<T: Serialize>(
        &self,
        class: &str,
        path: &str,
        owner: &T,
        version: &str,
        pool: Pool,
        bound: usize,
    ) -> Result<Request> {
        // Ordinary reference keys fit here; larger authenticated keys grow normally.
        let mut encoded = Vec::with_capacity(1024);
        serde_json::to_writer(
            &mut encoded,
            &(
                class,
                path,
                owner,
                version,
                CONTROL_MVP_FORMAT_VERSION,
                SEGMENT_FORMAT_VERSION,
                self.cache_namespace,
            ),
        )
        .map_err(|e| segment_serialization_error("read cache key", e))?;
        let key = Arc::new(
            String::from_utf8(encoded)
                .map_err(|e| segment_serialization_error("read cache key UTF-8", e))?,
        );
        // The cache-owned future retains an uncached store clone plus owner/path
        // copies until completion. Charge those captures as well as the final key.
        let captures = 8_usize.saturating_mul(
            heap(self.scope.tenant_id().len())
                + heap(self.scope.workspace_id().len())
                + heap(self.scope.domain().len()),
        );
        let reservation = bound
            .saturating_add(RECORD)
            // One key plus owner/descriptor and three paths (outer, direct loader,
            // scoped I/O). Formatting may retain twice a path's logical length;
            // four further key charges cover those independently owned captures.
            .saturating_add(key_charge(&key).saturating_mul(5))
            // The normalized version also lives in the loader until HEAD-after.
            .saturating_add(heap(version.len()))
            .saturating_add(captures);
        Ok(Request {
            key,
            pool,
            reservation,
        })
    }
    pub(super) async fn cached_directory(
        &self,
        reference: &ControlMvpSegmentRef,
    ) -> Result<(Bytes, ControlMvpSegmentIndex)> {
        let Some(cache) = &self.read_cache else {
            return self.load_segment_index_direct(reference).await;
        };
        lock(&cache.0.ledger).demands += 1;
        validate_owner(reference)?;
        let path = self.paths.segment_index(&reference.segment_id);
        let Some(version) = self
            .cache_version(cache, &path, reference.index_size_bytes)
            .await
        else {
            return self.load_segment_index_direct(reference).await;
        };
        let request = self.cache_request(
            "directory",
            &path,
            reference,
            &version,
            Pool::Metadata,
            (bounded_size(reference.index_size_bytes)).saturating_mul(16),
        )?;
        let store = self.clone().without_read_cache();
        let owner = reference.clone();
        let loaded = cache
            .load(request, move || async move {
                let (bytes, index) = store.load_segment_index_direct(&owner).await?;
                let encoded = cost::allocated(34, || Bytes::copy_from_slice(&bytes));
                let eligible = store
                    .cache_version_matches(&path, owner.index_size_bytes, &version)
                    .await;
                Ok((Value::Directory(encoded, index), eligible))
            })
            .await?;
        if let Some(entry) = loaded {
            if let Value::Directory(bytes, index) = &entry.value {
                // Request-owned copies may outlive the lease and are not cache ownership.
                return Ok(cost::allocated(26, || {
                    (Bytes::copy_from_slice(bytes), index.clone())
                }));
            }
            return Err(invariant_violation("read cache directory type mismatch"));
        }
        self.load_segment_index_direct(reference).await
    }
    pub(super) async fn cached_transaction(
        &self,
        reference: &ControlMvpTxRef,
    ) -> Result<ControlMvpTxObject> {
        let Some(cache) = &self.read_cache else {
            return self.load_tx_metadata_direct(reference).await;
        };
        lock(&cache.0.ledger).demands += 1;
        if reference.size_bytes == 0
            || reference.size_bytes > MAX_TRANSACTION_JSON_BYTES as u64
            || !integrity::valid_immutable_id(&reference.tx_id)
            || !valid_raw_digest(&reference.checksum_sha256)
        {
            return Err(invariant_violation("invalid cache transaction owner"));
        }
        reference
            .history
            .validate(&ControlMvpScopeDoc::from(&self.scope), reference.sequence)?;
        let path = self.paths.tx_object(&reference.tx_id);
        let Some(version) = self.cache_version(cache, &path, reference.size_bytes).await else {
            return self.load_tx_metadata_direct(reference).await;
        };
        let request = self.cache_request(
            "transaction",
            &path,
            reference,
            &version,
            Pool::Metadata,
            (bounded_size(reference.size_bytes)).saturating_mul(16),
        )?;
        let store = self.clone().without_read_cache();
        let owner = reference.clone();
        let loaded = cache
            .load(request, move || async move {
                let tx = store.load_tx_metadata_direct(&owner).await?;
                let eligible = store
                    .cache_version_matches(&path, owner.size_bytes, &version)
                    .await;
                Ok((Value::Transaction(tx), eligible))
            })
            .await?;
        if let Some(entry) = loaded {
            if let Value::Transaction(tx) = &entry.value {
                return Ok(cost::allocated(26, || tx.clone()));
            }
            return Err(invariant_violation("read cache transaction type mismatch"));
        }
        self.load_tx_metadata_direct(reference).await
    }
    pub(super) async fn cached_block(
        &self,
        reference: &ControlMvpSegmentRef,
        block: &ControlMvpBlock,
    ) -> Result<Vec<ControlMvpSegmentRow>> {
        let Some(cache) = &self.read_cache else {
            return self.load_block_direct(reference, block).await;
        };
        lock(&cache.0.ledger).demands += 1;
        validate_owner(reference)?;
        if block.row_count > MAX_SEGMENT_ROWS as u64
            || block.length == 0
            || block
                .offset
                .checked_add(block.length)
                .is_none_or(|end| end > reference.segment_size_bytes)
            || !valid_raw_digest(&block.checksum_sha256)
        {
            return Err(invariant_violation("invalid cache block owner"));
        }
        let path = segment_path(self, reference);
        let Some(version) = self
            .cache_version(cache, &path, reference.segment_size_bytes)
            .await
        else {
            return self.load_block_direct(reference, block).await;
        };
        let request = self.cache_request(
            "block",
            &path,
            &(reference, block),
            &version,
            Pool::Decoded,
            (bounded_size(block.length))
                .saturating_mul(3)
                .saturating_add(
                    (bounded_size(block.row_count))
                        .saturating_mul(2 * size_of::<ControlMvpSegmentRow>() + 2 * ALLOCATION),
                ),
        )?;
        let store = self.clone().without_read_cache();
        let owner = reference.clone();
        let descriptor = block.clone();
        let loaded = cache
            .load(request, move || async move {
                let rows = store.load_block_direct(&owner, &descriptor).await?;
                validate_rows(&owner, &rows)?;
                let eligible = store
                    .cache_version_matches(&path, owner.segment_size_bytes, &version)
                    .await;
                Ok((Value::Block(rows), eligible))
            })
            .await?;
        if let Some(entry) = loaded {
            if let Value::Block(rows) = &entry.value {
                return Ok(cost::allocated(26, || rows.clone()));
            }
            return Err(invariant_violation("read cache block type mismatch"));
        }
        self.load_block_direct(reference, block).await
    }
}
fn segment_path(store: &ControlMvpStateStore, reference: &ControlMvpSegmentRef) -> String {
    match reference.level {
        ControlMvpSegmentLevel::L0 => store.paths.l0_segment_object(&reference.segment_id),
        ControlMvpSegmentLevel::L1 => store.paths.state_object(&reference.segment_id),
    }
}
fn validate_owner(reference: &ControlMvpSegmentRef) -> Result<()> {
    if !integrity::valid_immutable_id(&reference.segment_id)
        || reference.segment_size_bytes == 0
        || reference.segment_size_bytes > MAX_SEGMENT_BYTES as u64
        || reference.index_size_bytes == 0
        || reference.index_size_bytes > MAX_SEGMENT_INDEX_BYTES as u64
        || !valid_raw_digest(&reference.checksum_sha256)
        || !valid_raw_digest(&reference.index_checksum_sha256)
    {
        return Err(invariant_violation("invalid cache segment owner"));
    }
    Ok(())
}
fn validate_rows(reference: &ControlMvpSegmentRef, rows: &[ControlMvpSegmentRow]) -> Result<()> {
    let l0 = reference.level == ControlMvpSegmentLevel::L0;
    for row in rows {
        if row.logical_sequence != reference.logical_sequence {
            return Err(invariant_violation(if l0 {
                "control MVP L0 row sequence does not match transaction sequence"
            } else {
                "control MVP L1 segment row sequence does not match its reference"
            }));
        }
        match row.record_kind {
            SEGMENT_RECORD_KV => {
                if row.generation == 0
                    || row.generation > reference.logical_sequence
                    || (l0 && row.generation != reference.logical_sequence)
                    || row.origin_sequence.is_some()
                    || row.tombstone != row.value.is_none()
                {
                    return Err(invariant_violation(if l0 {
                        "control MVP L0 key/value row metadata is invalid"
                    } else {
                        "control MVP L1 segment contains an invalid key generation"
                    }));
                }
            }
            SEGMENT_RECORD_OUTBOX => {
                if l0 {
                    if row.tombstone
                        || row.generation != 0
                        || row.origin_sequence != Some(reference.logical_sequence)
                    {
                        return Err(invariant_violation(
                            "control MVP L0 outbox row metadata is invalid",
                        ));
                    }
                } else {
                    let origin = row.origin_sequence.ok_or_else(|| {
                        invariant_violation("control MVP L1 outbox row is missing origin sequence")
                    })?;
                    if row.generation != 0 || origin == 0 || origin > reference.logical_sequence {
                        return Err(invariant_violation(
                            "control MVP L1 outbox row origin metadata is invalid",
                        ));
                    }
                }
                std::str::from_utf8(&row.key).map_err(|error| {
                    segment_serialization_error(
                        if l0 {
                            "decode L0 outbox record id"
                        } else {
                            "decode L1 outbox record id"
                        },
                        error,
                    )
                })?;
                if row.value.is_none() {
                    return Err(invariant_violation(if l0 {
                        "control MVP L0 outbox row has no payload"
                    } else {
                        "control MVP L1 outbox row has no payload"
                    }));
                }
            }
            SEGMENT_RECORD_OUTBOX_TRIM if l0 => {
                if !row.tombstone
                    || row.generation != 0
                    || row.value.is_some()
                    || row.origin_sequence.is_none()
                {
                    return Err(invariant_violation(
                        "control MVP L0 outbox-trim row metadata is invalid",
                    ));
                }
                std::str::from_utf8(&row.key).map_err(|error| {
                    segment_serialization_error("decode L0 outbox trim record id", error)
                })?;
            }
            SEGMENT_RECORD_OUTBOX_TRIM => {
                return Err(invariant_violation(
                    "control MVP consolidated L1 state contains an outbox trim row",
                ));
            }
            _ => {
                return Err(invariant_violation(if l0 {
                    "control MVP L0 segment contains an unknown row kind"
                } else {
                    "control MVP consolidated L1 state contains an unknown row kind"
                }));
            }
        }
    }
    Ok(())
}

impl ControlMvpStateStore {
    /// A certificate and all its blocks are acquired under the directory lock in `load()`.
    /// Only complete raw validation below can create a certificate.
    #[allow(clippy::too_many_lines)]
    pub(super) async fn cached_complete_rows(
        &self,
        reference: &ControlMvpSegmentRef,
        index_bytes: &[u8],
        index: &ControlMvpSegmentIndex,
    ) -> Result<Vec<ControlMvpSegmentRow>> {
        let direct = async {
            let bytes = self.load_complete_segment(reference).await?;
            decode_segment_rows(&bytes, index_bytes, reference, &self.scope)
        };
        let Some(cache) = &self.read_cache else {
            return direct.await;
        };
        lock(&cache.0.ledger).demands += 1;
        validate_owner(reference)?;
        let path = segment_path(self, reference);
        let directory_path = self.paths.segment_index(&reference.segment_id);
        let Some(version) = self
            .cache_version(cache, &path, reference.segment_size_bytes)
            .await
        else {
            return direct.await;
        };
        let Some(directory_version) = self
            .cache_version(cache, &directory_path, reference.index_size_bytes)
            .await
        else {
            return direct.await;
        };
        validate_segment_index_identity(index, reference, &self.scope)?;
        let index = index.clone();
        let request = self.cache_request(
            "complete",
            &path,
            &(reference, &directory_version),
            &version,
            Pool::Metadata,
            (bounded_size(reference.index_size_bytes))
                .saturating_mul(16)
                .saturating_add(heap(directory_version.len())),
        )?;
        let store = self.clone().without_read_cache();
        let owner = reference.clone();
        let directory_bytes = Bytes::copy_from_slice(index_bytes);
        let shared = cache.clone();
        let loaded = cache
            .load(request, move || async move {
                let mut reservations = Vec::new();
                for block in &index.blocks {
                    let request = store.cache_request(
                        "block",
                        &path,
                        &(&owner, block),
                        &version,
                        Pool::Decoded,
                        (bounded_size(block.length))
                            .saturating_mul(3)
                            .saturating_add((bounded_size(block.row_count)).saturating_mul(
                                2 * size_of::<ControlMvpSegmentRow>() + 2 * ALLOCATION,
                            )),
                    )?;
                    let reservation =
                        shared.reserve_locked(&request, &mut lock(&shared.0.directory));
                    let Some(reservation) = reservation else {
                        return Ok((Value::Unavailable, false));
                    };
                    reservations.push((request, reservation));
                }
                let bytes = store.load_complete_segment(&owner).await?;
                let rows = decode_segment_rows(&bytes, &directory_bytes, &owner, &store.scope)?;
                validate_rows(&owner, &rows)?;
                let eligible = store
                    .cache_version_matches(&path, owner.segment_size_bytes, &version)
                    .await
                    && store
                        .cache_version_matches(
                            &directory_path,
                            owner.index_size_bytes,
                            &directory_version,
                        )
                        .await;
                let mut cursor = 0;
                let mut blocks = Vec::with_capacity(index.blocks.len());
                for (descriptor, (request, reservation)) in index.blocks.iter().zip(reservations) {
                    let end = cursor + bounded_size(descriptor.row_count);
                    let selected_rows = rows
                        .get(cursor..end)
                        .ok_or_else(|| invariant_violation("complete cache row span"))?;
                    let block_rows = cost::allocated(34, || selected_rows.to_vec());
                    cursor = end;
                    let mut entry =
                        reservation.finish(request.key, Value::Block(block_rows), eligible)?;
                    if eligible && entry.charge <= MAX_ENTRY {
                        entry = shared.insert(entry);
                    }
                    blocks.push(entry);
                }
                if cursor != rows.len() {
                    return Err(invariant_violation("complete cache row total"));
                }
                let resident =
                    eligible && blocks.iter().all(|b| b.resident.load(Ordering::Relaxed));
                Ok((Value::Certificate(blocks), resident))
            })
            .await?;
        if let Some(entry) = loaded {
            match &entry.value {
                Value::Certificate(blocks) => {
                    return cost::allocated(26, || {
                        let mut rows = Vec::new();
                        for block in blocks {
                            if let Value::Block(values) = &block.value {
                                rows.extend(values.iter().cloned());
                            } else {
                                return Err(invariant_violation("complete cache block type"));
                            }
                        }
                        Ok(rows)
                    });
                }
                Value::Unavailable => (),
                _ => return Err(invariant_violation("complete cache certificate type")),
            }
        }
        direct.await
    }
}

fn bounded_size(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}
