# BULK_ONLY Pipeline Redesign — Design Spec

**Date:** 2026-03-31

## Goal

Redesign the BULK_ONLY migration pipeline to include: structure verification, pre-transfer data comparison (skip if already matching), target clearing, bulk load, and post-load verification. Add parallel chunked data compare via workers.

## New Pipeline (BULK_ONLY phases)

```
NEW
 → PREPARING           — ensure target table, sync columns
 → STRUCTURE_READY      — structure verified (NEW PHASE)
 → DATA_COMPARING       — chunked data compare via workers (NEW PHASE)
 → [if match]          → COMPLETED (skip everything)
 → [if diff]           ↓
 → TARGET_CLEARING      — TRUNCATE, disable triggers, unusable indexes (NEW PHASE)
 → CHUNKING             — create ROWID chunks
 → BULK_LOADING         — workers load chunks (STAGE or DIRECT)
 → BULK_LOADED
    [if STAGE]:         → STAGE_VALIDATING → BASELINE_PUBLISHING → BASELINE_LOADING → STAGE_DROPPING
    [if DIRECT]:        → (skip stage phases)
 → INDEXES_ENABLING     — rebuild indexes, enable constraints
 → DATA_VERIFYING       — final chunked data compare via workers
 → COMPLETED / DATA_MISMATCH
```

**New phases:** STRUCTURE_READY, DATA_COMPARING, TARGET_CLEARING

**Removed for BULK_ONLY:** SCN_FIXED (no CDC), CONNECTOR_STARTING, CDC_BUFFERING (already skipped)

## Phase Handlers

### PREPARING (modified for BULK_ONLY)

Instead of stage table + SCN fixation:
1. Check target table exists via existing `/api/target-prep/ensure-table` logic
2. Sync columns via existing `/api/target-prep/sync-columns` logic
3. If STAGE strategy: create stage table
4. Transition → STRUCTURE_READY

### STRUCTURE_READY (new)

Immediate transition → DATA_COMPARING

### DATA_COMPARING (new)

1. Create `data_compare_task` with status PENDING
2. Create ROWID chunks on source via DBMS_PARALLEL_EXECUTE
3. Store chunks in `data_compare_chunks` (side=both — single chunk covers source+target)
4. Workers claim and execute compare chunks
5. Monitor completion:
   - All chunks DONE + all counts_match + all hashes_match → COMPLETED (migration done, data identical)
   - Any mismatch → TARGET_CLEARING
   - Any FAILED → DATA_MISMATCH (error state)

### TARGET_CLEARING (new)

In a thread:
1. TRUNCATE target table
2. Disable triggers
3. Mark non-PK indexes UNUSABLE
4. Set NOLOGGING
5. Transition → CHUNKING

### Existing phases (unchanged)

CHUNKING, BULK_LOADING, BULK_LOADED, STAGE_VALIDATING, BASELINE_PUBLISHING, BASELINE_LOADING, STAGE_DROPPING, INDEXES_ENABLING — work as-is.

### DATA_VERIFYING (modified)

Uses the same chunked worker-based compare (same as DATA_COMPARING), but this is the POST-load verification. On match → COMPLETED. On mismatch → DATA_MISMATCH.

## Worker Changes

### worker.py — add compare mode

The existing `worker.py` dispatches between bulk and CDC modes. Add a third mode: `compare`.

**Compare loop:**
1. `POST /api/worker/compare/claim` → get a compare chunk (task_id, chunk_id, source/target connection info, ROWID range, column list)
2. For the ROWID range, execute on BOTH source and target:
   ```sql
   SELECT COUNT(*), SUM(ORA_HASH(col1 || col2 || ... || colN))
   FROM schema.table
   WHERE ROWID BETWEEN :start AND :end
   ```
3. `POST /api/worker/compare/complete` with `{chunk_id, source_count, source_hash, target_count, target_hash}`
4. If error: `POST /api/worker/compare/fail` with `{chunk_id, error_text}`

**Claim priority:** Compare chunks are claimed when no bulk chunks are available (bulk takes priority).

### New API endpoints

- `POST /api/worker/compare/claim` — claim a PENDING compare chunk
- `POST /api/worker/compare/complete` — report compare results
- `POST /api/worker/compare/fail` — report compare failure

## Frontend: Create Migration Modal

**Opens from:** Dashboard — single table or multi-select.

**Single screen with sections:**

### Source (pre-filled from dashboard)
- Source schema (SearchSelect)
- Source table (text, or list for multi-select)

### Target (auto-generated defaults)
- Target schema (default: source_schema.toLowerCase())
- Target table (default: source_table.toLowerCase())

### Load Parameters
- Strategy: STAGE / DIRECT (radio, default STAGE)
- Chunk size (number, default 500,000)
- Max parallel workers (number, default 10)
- Baseline parallel degree (number, default 10, STAGE only)
- Stage table name (default: STG_{SOURCE_TABLE}, STAGE only)
- Stage tablespace (default: PAYSTAGE, STAGE only)

### Buttons
- "Создать" — POST /api/migrations, close modal
- "Отмена" — close

For multi-select: same parameters apply to all tables. Target names auto-generated per table.

## Data Compare Mechanism

### data_compare_chunks table (existing, reused)

Chunks store ROWID ranges. Each chunk is processed by a worker that queries BOTH source and target for that range.

### Compare result per chunk

Worker reports: `source_count`, `source_hash`, `target_count`, `target_hash`.

Backend computes: `counts_match = (source_count == target_count)`, `hash_match = (source_hash == target_hash)`.

### Task-level aggregation

When all chunks are DONE:
- `counts_match = ALL chunks have matching counts`
- `hash_match = ALL chunks have matching hashes`
- If both true → data matches → proceed accordingly (COMPLETED or continue pipeline)

## Files Modified

### Backend (new)
- `orchestrator/phases/bulk_pipeline.py` — handle_structure_ready, handle_data_comparing, handle_target_clearing
- `routes/workers.py` — add compare claim/complete/fail endpoints

### Backend (modified)
- `models/enums.py` — add STRUCTURE_READY, DATA_COMPARING, TARGET_CLEARING
- `orchestrator/phases/preparing.py` — modify handle_preparing for BULK_ONLY (ensure table, sync columns)
- `orchestrator/phases/data_verify.py` — modify to use worker-based compare
- `orchestrator/engine.py` — add new phases to dispatch tables
- `orchestrator/queue.py` — add TARGET_CLEARING to HEAVY_PHASES
- `workers/worker.py` — add compare mode dispatch
- `workers/common.py` — add compare API client functions (if needed)

### Frontend (new)
- `components/Dashboard/CreateBulkModal.tsx` — full-featured creation modal

### Frontend (modified)
- `types/migration.ts` — add new phases + colors
- `components/Dashboard/Dashboard.tsx` — wire CreateBulkModal
- `components/Dashboard/TableDetail.tsx` — use CreateBulkModal instead of inline create

## Defaults Summary

| Parameter | Default |
|-----------|---------|
| Strategy | STAGE |
| Chunk size | 500,000 |
| Max parallel workers | 10 |
| Baseline parallel degree | 10 |
| Stage tablespace | PAYSTAGE |
| Stage table name | STG_{TABLE} |
| Migration mode | BULK_ONLY |
