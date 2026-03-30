# BULK_ONLY Strategy — Data Verification & PK-less Mode

**Date:** 2026-03-30
**Status:** Approved

## Problem

1. BULK_ONLY mode blocks migrations for tables without PK/UK/user-defined keys, even though ROWID-based chunking and INSERT don't require keys.
2. BULK_ONLY completes immediately after INDEXES_ENABLING with no data verification — no guarantee that source and target match.

## Solution

Three changes to the BULK_ONLY pipeline:

1. **Remove PK requirement** for BULK_ONLY — allow keyless tables.
2. **Add `DATA_VERIFYING` phase** — automatic COUNT + HASH verification via existing `data_compare` infrastructure.
3. **Add `DATA_MISMATCH` phase** — pause on mismatch, let user decide (retry, force complete, cancel).

## State Machine Changes

### Current BULK_ONLY path
```
... → INDEXES_ENABLING → COMPLETED
```

### New BULK_ONLY path
```
... → INDEXES_ENABLING → DATA_VERIFYING → COMPLETED
                                        ↘ DATA_MISMATCH
                                            ├─ retry_verify  → DATA_VERIFYING
                                            ├─ force_complete → COMPLETED
                                            └─ cancel         → CANCELLING
```

CDC mode is unaffected — no DATA_VERIFYING phase for CDC.

## Detailed Changes

### 1. Remove PK gate for BULK_ONLY (`orchestrator.py` — `_handle_new`)

Current code (line ~330-339):
```python
if not pk and not uk and not key_cols:
    _fail(mid, "Таблица не имеет PK/UK...")
```

Change: wrap this check with `if mode != "BULK_ONLY"`. For BULK_ONLY, keys are optional — ROWID chunking, INSERT, and COUNT+HASH verification all work without keys.

### 2. New phase: DATA_VERIFYING

**Entry:** From `_handle_indexes_enabling` when BULK_ONLY — transition to `DATA_VERIFYING` instead of `COMPLETED`.

**Handler: `_handle_data_verifying`**

On first tick (no `data_compare_task_id` yet):
1. Create a `data_compare_task` via existing `data_compare` service logic:
   - source_schema, source_table, target_schema, target_table from migration record
   - Chunk both source and target by ROWID
   - Workers process chunks (COUNT + SUM(ORA_HASH(columns)))
2. Store `data_compare_task_id` in migration record.

On subsequent ticks:
1. Query `data_compare_tasks` by task_id.
2. If `chunks_done < chunks_total` — still in progress, do nothing.
3. If complete and `counts_match=true AND hash_match=true` — transition to `COMPLETED` with message "Data verification passed".
4. If complete and mismatch — transition to `DATA_MISMATCH` with details (source_count, target_count, counts_match, hash_match).

### 3. New phase: DATA_MISMATCH

**Idle phase.** Orchestrator does nothing on tick — waits for user action.

Migration record stores verification results (from data_compare_task): source_count, target_count, counts_match, hash_match.

**User actions** (via `POST /api/migrations/<id>/action`):
- `retry_verify` — delete old data_compare_task, clear `data_compare_task_id`, transition back to `DATA_VERIFYING`
- `force_complete` — transition to `COMPLETED` with message "Verification skipped by user"
- `cancel` — transition to `CANCELLING`

### 4. Database Changes (`state_db.py`)

**New column on `migrations` table:**
```sql
ALTER TABLE migrations ADD COLUMN data_compare_task_id UUID
    REFERENCES data_compare_tasks(task_id) ON DELETE SET NULL;
```

**Update phase sets:**
- `_VALID_PHASES`: add `DATA_VERIFYING`, `DATA_MISMATCH`
- `_ACTIVE_PHASES` in `get_active_migrations()`: add `DATA_VERIFYING`, `DATA_MISMATCH`
- `_HEAVY_PHASES` in orchestrator: `DATA_VERIFYING` should NOT be heavy (verification can run in parallel with other migrations' heavy phases)

### 5. API Changes (`routes/migrations.py`)

**New actions in action handler:**
- `retry_verify` — allowed from `DATA_MISMATCH`, transitions to `DATA_VERIFYING`
- `force_complete` — allowed from `DATA_MISMATCH`, transitions to `COMPLETED`

**Existing actions unchanged.** `cancel` already works from any phase.

### 6. Orchestrator Integration (`orchestrator.py`)

**Modified handler: `_handle_indexes_enabling`**
- Current: BULK_ONLY → enable triggers → `COMPLETED`
- New: BULK_ONLY → enable triggers → `DATA_VERIFYING`

**New handler: `_handle_data_verifying`**
- Creates data_compare task on entry (daemon thread, same pattern as other threaded phases)
- Monitors task completion on each tick
- Transitions based on result

**New handler: `_handle_data_mismatch`**
- No-op on tick. Waits for user action.

**Tick dispatch map:** Add entries for both new phases.

### 7. Frontend Changes

**Phase display:** `DATA_VERIFYING` and `DATA_MISMATCH` should appear in migration phase indicators with appropriate colors/labels.

**DATA_VERIFYING view:**
- Show progress: chunks_done / chunks_total from linked data_compare_task
- Spinner/loading state

**DATA_MISMATCH view:**
- Show verification results: source_count, target_count, counts_match, hash_match
- Three action buttons:
  - "Повторить сверку" → `retry_verify`
  - "Завершить принудительно" → `force_complete` (with confirmation)
  - "Отменить миграцию" → `cancel` (with confirmation)

## Files to Modify

| File | Change |
|------|--------|
| `backend/services/orchestrator.py` | New handlers, modify indexes_enabling handler, update tick dispatch |
| `backend/db/state_db.py` | Add column, update phase sets, ensure_schema |
| `backend/routes/migrations.py` | New actions, update _VALID_PHASES |
| `frontend/src/components/MigrationDetail.tsx` (or equivalent) | Phase display, DATA_MISMATCH action buttons |

## Non-Goals

- No changes to CDC mode pipeline
- No changes to STAGE vs DIRECT strategy logic
- No changes to existing `data_compare` service internals
- No automatic retry on mismatch
