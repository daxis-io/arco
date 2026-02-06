# Orchestration Read API Hardening Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Harden orchestration read-only APIs by fixing response correctness, pagination validation, OpenAPI accuracy, and backfill listing performance without changing core storage behavior.

**Architecture:** Keep changes localized to `crates/arco-api/src/routes/orchestration.rs` and OpenAPI docs. Add small helper functions for pagination and chunk counting; extend response enums to reflect domain types. Update OpenAPI responses and regenerate the checked-in spec.

**Tech Stack:** Rust, axum, utoipa (OpenAPI), serde/serde_json, cargo tests.

---

### Task 0: Create an isolated worktree

**Files:**
- None

**Step 1: Create worktree**

Run (follow @superpowers:using-git-worktrees): `git worktree add ../arco-orch-read-api-hardening -b orch-read-api-hardening`
Expected: new worktree directory created.

**Step 2: Confirm clean status**

Run: `git status --short`
Expected: no changes.

---

### Task 1: Add shared pagination validation and apply to list endpoints

**Files:**
- Modify: `crates/arco-api/src/routes/orchestration.rs`
- Test: `crates/arco-api/src/routes/orchestration.rs`

**Step 1: Write failing tests for pagination validation**

Add to `mod tests` in `crates/arco-api/src/routes/orchestration.rs`:

```rust
#[test]
fn test_parse_pagination_rejects_zero_limit() {
    let err = parse_pagination(0, None).expect_err("expected error");
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        err.message(),
        format!("limit must be between 1 and {MAX_LIST_LIMIT}")
    );
}

#[test]
fn test_parse_pagination_rejects_invalid_cursor() {
    let err = parse_pagination(1, Some("abc")).expect_err("expected error");
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    assert_eq!(err.message(), "invalid cursor");
}

#[test]
fn test_parse_pagination_parses_cursor() {
    let (limit, offset) = parse_pagination(5, Some("10")).expect("parse");
    assert_eq!(limit, 5);
    assert_eq!(offset, 10);
}
```

**Step 2: Run tests to see failure**

Run: `cargo test -p arco-api test_parse_pagination_`
Expected: FAIL (unresolved `parse_pagination`).

**Step 3: Implement pagination helper**

Add near other helpers in `crates/arco-api/src/routes/orchestration.rs`:

```rust
fn parse_pagination(limit: u32, cursor: Option<&str>) -> Result<(usize, usize), ApiError> {
    if limit == 0 || limit > MAX_LIST_LIMIT {
        return Err(ApiError::bad_request(format!(
            "limit must be between 1 and {MAX_LIST_LIMIT}"
        )));
    }

    let offset = cursor
        .map(|value| value.parse::<usize>().map_err(|_| ApiError::bad_request("invalid cursor")))
        .transpose()?
        .unwrap_or(0);

    Ok((limit as usize, offset))
}
```

**Step 4: Apply helper to list endpoints**

Update each endpoint to use the helper:

- `list_schedule_ticks`
- `list_sensor_evals`
- `list_backfills`
- `list_backfill_chunks`
- `list_partitions`

Example replacement:

```rust
let (limit, offset) = parse_pagination(query.limit, query.cursor.as_deref())?;
let end = (offset + limit).min(items.len());
let page = items.get(offset..end).unwrap_or_default().to_vec();
let next_cursor = if end < items.len() { Some(end.to_string()) } else { None };
```

**Step 5: Run tests**

Run: `cargo test -p arco-api test_parse_pagination_`
Expected: PASS.

**Step 6: Commit**

Run:
```bash
git add crates/arco-api/src/routes/orchestration.rs
git commit -m "fix(api): validate pagination for orchestration lists"
```

---

### Task 2: Represent filter-based partition selectors correctly

**Files:**
- Modify: `crates/arco-api/src/routes/orchestration.rs`
- Test: `crates/arco-api/src/routes/orchestration.rs`

**Step 1: Write failing test**

Add to `mod tests`:

```rust
#[test]
fn test_partition_selector_filter_maps_to_response() {
    let mut filters = HashMap::new();
    filters.insert("region".to_string(), "us-*".to_string());

    let selector = PartitionSelector::Filter { filters: filters.clone() };
    let response = map_partition_selector(&selector);
    let json = serde_json::to_value(&response).expect("serialize");

    assert_eq!(json["type"], "filter");
    assert_eq!(json["filters"]["region"], "us-*");
}
```

**Step 2: Run test to see failure**

Run: `cargo test -p arco-api test_partition_selector_filter_maps_to_response`
Expected: FAIL (missing enum variant).

**Step 3: Update response enum + mapping**

Update `PartitionSelectorResponse` and mapping:

```rust
/// Partition selector response.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PartitionSelectorResponse {
    /// Range of partitions.
    Range {
        /// Start partition (inclusive).
        start: String,
        /// End partition (inclusive).
        end: String,
    },
    /// Explicit list of partitions.
    Explicit {
        /// Partition keys.
        partitions: Vec<String>,
    },
    /// Filter-based selection.
    Filter {
        /// Filter expressions.
        filters: HashMap<String, String>,
    },
}

fn map_partition_selector(selector: &PartitionSelector) -> PartitionSelectorResponse {
    match selector {
        PartitionSelector::Range { start, end } => PartitionSelectorResponse::Range {
            start: start.clone(),
            end: end.clone(),
        },
        PartitionSelector::Explicit { partition_keys } => PartitionSelectorResponse::Explicit {
            partitions: partition_keys.clone(),
        },
        PartitionSelector::Filter { filters } => PartitionSelectorResponse::Filter {
            filters: filters.clone(),
        },
    }
}
```

**Step 4: Run test to verify pass**

Run: `cargo test -p arco-api test_partition_selector_filter_maps_to_response`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-api/src/routes/orchestration.rs
git commit -m "fix(api): return filter partition selectors"
```

---

### Task 3: Make schedule tick `evaluatedAt` optional and omit when unknown

**Files:**
- Modify: `crates/arco-api/src/routes/orchestration.rs`
- Test: `crates/arco-api/src/routes/orchestration.rs`

**Step 1: Write failing test**

Add to `mod tests`:

```rust
#[test]
fn test_schedule_tick_response_omits_evaluated_at_when_unavailable() {
    let row = ScheduleTickRow {
        tenant_id: "tenant".to_string(),
        workspace_id: "workspace".to_string(),
        tick_id: "tick_01".to_string(),
        schedule_id: "sched_01".to_string(),
        scheduled_for: Utc::now(),
        definition_version: "def_01".to_string(),
        asset_selection: vec!["analytics.daily".to_string()],
        partition_selection: None,
        status: TickStatus::Triggered,
        run_key: Some("sched:daily:1".to_string()),
        run_id: None,
        request_fingerprint: None,
        row_version: "01HQXYZ123".to_string(),
    };

    let response = map_schedule_tick(&row);
    let json = serde_json::to_value(&response).expect("serialize");

    assert!(json.get("evaluatedAt").is_none());
}
```

**Step 2: Run test to see failure**

Run: `cargo test -p arco-api test_schedule_tick_response_omits_evaluated_at_when_unavailable`
Expected: FAIL (field present).

**Step 3: Update response type + mapping**

Change `ScheduleTickResponse` and mapping:

```rust
/// Schedule tick response.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleTickResponse {
    // ...
    /// When the tick was evaluated (when available).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evaluated_at: Option<DateTime<Utc>>,
}

fn map_schedule_tick(row: &ScheduleTickRow) -> ScheduleTickResponse {
    ScheduleTickResponse {
        // ...
        evaluated_at: None,
    }
}
```

**Step 4: Run test to verify pass**

Run: `cargo test -p arco-api test_schedule_tick_response_omits_evaluated_at_when_unavailable`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-api/src/routes/orchestration.rs
git commit -m "fix(api): omit schedule tick evaluatedAt when unavailable"
```

---

### Task 4: Optimize backfill list chunk counting

**Files:**
- Modify: `crates/arco-api/src/routes/orchestration.rs`
- Test: `crates/arco-api/src/routes/orchestration.rs`

**Step 1: Write failing test**

Add to `mod tests`:

```rust
#[test]
fn test_build_chunk_counts_index_groups_by_backfill() {
    let rows = vec![
        BackfillChunkRow {
            tenant_id: "tenant".to_string(),
            workspace_id: "workspace".to_string(),
            chunk_id: "bf_a:0".to_string(),
            backfill_id: "bf_a".to_string(),
            chunk_index: 0,
            partition_keys: vec!["2025-01-01".to_string()],
            run_key: "rk_a0".to_string(),
            run_id: None,
            state: ChunkState::Pending,
            row_version: "01HQ1".to_string(),
        },
        BackfillChunkRow {
            tenant_id: "tenant".to_string(),
            workspace_id: "workspace".to_string(),
            chunk_id: "bf_a:1".to_string(),
            backfill_id: "bf_a".to_string(),
            chunk_index: 1,
            partition_keys: vec!["2025-01-02".to_string()],
            run_key: "rk_a1".to_string(),
            run_id: None,
            state: ChunkState::Planned,
            row_version: "01HQ2".to_string(),
        },
        BackfillChunkRow {
            tenant_id: "tenant".to_string(),
            workspace_id: "workspace".to_string(),
            chunk_id: "bf_a:2".to_string(),
            backfill_id: "bf_a".to_string(),
            chunk_index: 2,
            partition_keys: vec!["2025-01-03".to_string()],
            run_key: "rk_a2".to_string(),
            run_id: None,
            state: ChunkState::Running,
            row_version: "01HQ3".to_string(),
        },
        BackfillChunkRow {
            tenant_id: "tenant".to_string(),
            workspace_id: "workspace".to_string(),
            chunk_id: "bf_b:0".to_string(),
            backfill_id: "bf_b".to_string(),
            chunk_index: 0,
            partition_keys: vec!["2025-01-01".to_string()],
            run_key: "rk_b0".to_string(),
            run_id: None,
            state: ChunkState::Succeeded,
            row_version: "01HQ4".to_string(),
        },
        BackfillChunkRow {
            tenant_id: "tenant".to_string(),
            workspace_id: "workspace".to_string(),
            chunk_id: "bf_b:1".to_string(),
            backfill_id: "bf_b".to_string(),
            chunk_index: 1,
            partition_keys: vec!["2025-01-02".to_string()],
            run_key: "rk_b1".to_string(),
            run_id: None,
            state: ChunkState::Failed,
            row_version: "01HQ5".to_string(),
        },
    ];

    let counts = build_chunk_counts_index(rows.iter());

    let a = counts.get("bf_a").expect("bf_a");
    assert_eq!(a.total, 3);
    assert_eq!(a.pending, 2);
    assert_eq!(a.running, 1);
    assert_eq!(a.succeeded, 0);
    assert_eq!(a.failed, 0);

    let b = counts.get("bf_b").expect("bf_b");
    assert_eq!(b.total, 2);
    assert_eq!(b.pending, 0);
    assert_eq!(b.running, 0);
    assert_eq!(b.succeeded, 1);
    assert_eq!(b.failed, 1);
}
```

**Step 2: Run test to see failure**

Run: `cargo test -p arco-api test_build_chunk_counts_index_groups_by_backfill`
Expected: FAIL (missing helper).

**Step 3: Add count-index helper + map helper**

Add helpers:

```rust
fn build_chunk_counts_index<'a, I>(chunks: I) -> HashMap<String, ChunkCounts>
where
    I: IntoIterator<Item = &'a BackfillChunkRow>,
{
    let mut counts = HashMap::new();

    for chunk in chunks {
        let entry = counts
            .entry(chunk.backfill_id.clone())
            .or_insert_with(ChunkCounts::default);

        entry.total = entry.total.saturating_add(1);

        match chunk.state {
            ChunkState::Pending => entry.pending = entry.pending.saturating_add(1),
            ChunkState::Planned => entry.pending = entry.pending.saturating_add(1),
            ChunkState::Running => entry.running = entry.running.saturating_add(1),
            ChunkState::Succeeded => entry.succeeded = entry.succeeded.saturating_add(1),
            ChunkState::Failed => entry.failed = entry.failed.saturating_add(1),
        }
    }

    counts
}

fn map_backfill_with_counts(row: &BackfillRow, counts: ChunkCounts) -> BackfillResponse {
    BackfillResponse {
        backfill_id: row.backfill_id.clone(),
        asset_selection: row.asset_selection.clone(),
        partition_selector: map_partition_selector(&row.partition_selector),
        chunk_size: row.chunk_size,
        max_concurrent_runs: row.max_concurrent_runs,
        state: map_backfill_state(row.state),
        state_version: row.state_version,
        created_by: None,
        created_at: row.created_at,
        updated_at: None,
        chunk_counts: counts,
    }
}

fn map_backfill(row: &BackfillRow, chunks: &[&BackfillChunkRow]) -> BackfillResponse {
    map_backfill_with_counts(row, build_chunk_counts(chunks))
}
```

**Step 4: Use index in `list_backfills`**

Replace the per-backfill chunk scan with:

```rust
let counts_by_backfill = build_chunk_counts_index(fold_state.backfill_chunks.values());

let mut backfills: Vec<BackfillResponse> = fold_state
    .backfills
    .values()
    .map(|b| {
        let counts = counts_by_backfill
            .get(&b.backfill_id)
            .cloned()
            .unwrap_or_default();
        map_backfill_with_counts(b, counts)
    })
    .collect();
```

**Step 5: Run test to verify pass**

Run: `cargo test -p arco-api test_build_chunk_counts_index_groups_by_backfill`
Expected: PASS.

**Step 6: Commit**

```bash
git add crates/arco-api/src/routes/orchestration.rs
git commit -m "perf(api): preindex backfill chunk counts"
```

---

### Task 5: Align OpenAPI docs for list history endpoints

**Files:**
- Modify: `crates/arco-api/src/routes/orchestration.rs`
- Create: `crates/arco-api/tests/openapi_orchestration_routes.rs`
- Update (generated): `crates/arco-api/openapi.json`

**Step 1: Write failing OpenAPI test**

Create `crates/arco-api/tests/openapi_orchestration_routes.rs`:

```rust
use anyhow::Result;

#[test]
fn orchestration_list_history_endpoints_do_not_document_404() -> Result<()> {
    let json = arco_api::openapi::openapi_json()?;
    let spec: serde_json::Value = serde_json::from_str(&json)?;

    let ticks = &spec["paths"]["/api/v1/workspaces/{workspace_id}/schedules/{schedule_id}/ticks"]
        ["get"]["responses"];
    assert!(ticks.get("404").is_none());

    let evals = &spec["paths"]["/api/v1/workspaces/{workspace_id}/sensors/{sensor_id}/evals"]
        ["get"]["responses"];
    assert!(evals.get("404").is_none());

    Ok(())
}
```

**Step 2: Run test to see failure**

Run: `cargo test -p arco-api --test openapi_orchestration_routes`
Expected: FAIL (404 still present).

**Step 3: Remove 404 responses in route docs**

Update the `#[utoipa::path]` blocks:

- `list_schedule_ticks`: remove the 404 response entry.
- `list_sensor_evals`: remove the 404 response entry.

**Step 4: Re-run the new test**

Run: `cargo test -p arco-api --test openapi_orchestration_routes`
Expected: PASS.

**Step 5: Regenerate OpenAPI JSON**

Run:
```bash
cargo run -p arco-api --bin gen_openapi > crates/arco-api/openapi.json
```
Expected: file updated.

**Step 6: Verify OpenAPI contract**

Run: `cargo test -p arco-api --test openapi_contract`
Expected: PASS.

**Step 7: Commit**

```bash
git add crates/arco-api/src/routes/orchestration.rs crates/arco-api/tests/openapi_orchestration_routes.rs crates/arco-api/openapi.json
git commit -m "docs(api): align orchestration list history OpenAPI responses"
```

---

### Task 6: Final verification

**Files:**
- None

**Step 1: Run full API test suite**

Run: `cargo test -p arco-api`
Expected: PASS.

**Step 2: Review git status**

Run: `git status --short`
Expected: clean working tree.
