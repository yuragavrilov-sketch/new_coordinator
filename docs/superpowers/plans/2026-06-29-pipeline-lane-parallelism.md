# Pipeline Lane Parallelism + Index-Enable Job — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run one non-CDC bulk-load in parallel with one CDC bulk-load, let the next table start without waiting for the previous table's index enabling, and move index enabling off the coordinator into a worker-claimed job.

**Architecture:** Split the orchestrator's single global "loading slot" into two strategy-scoped lanes (CDC / non-CDC), each width 1, and remove `INDEXES_ENABLING` (plus the tail) from the blocking set. Index enabling becomes a row in a new `index_enable_jobs` table that the universal worker claims via `SELECT ... FOR UPDATE SKIP LOCKED`; the orchestrator's `INDEXES_ENABLING` handler creates the job and polls its state (mirroring `_handle_bulk_loading`).

**Tech Stack:** Python 3.10, Flask, psycopg2 (PostgreSQL State DB), oracledb. No new dependencies.

## Global Constraints

- Python 3.10 compatible (the codebase targets 3.10; `Strategy` is `str, Enum`). Type hints like `str | None` are already used — keep that style.
- No new third-party dependencies.
- New State DB schema is added idempotently in `backend/db/state_db.py::_init_schema_legacy` via `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS` (no separate Alembic revision — baseline delegates to the legacy bootstrap).
- The `workers/` package is standalone and MUST NOT import from `backend/`. Worker-side Oracle/State-DB code lives under `workers/`.
- Worker DB mutations on a claimable row MUST be guarded by `AND worker_id = %s` (consistency with the existing chunk helpers).
- Lane classification: a migration is in the **CDC lane** iff `LEFT(COALESCE(strategy,''),4) = 'CDC_'`, else the **non-CDC (BULK) lane**.
- Commit after each task. Use Conventional Commit prefixes (`feat:`, `fix:`, `test:`, `refactor:`).
- Run backend tests from `backend/` with `python -m pytest tests -q`.

---

## File Structure

- `backend/services/orchestrator.py` — lane constants + pure helpers, lane-scoped gate in `_handle_new`, lane-aware `_update_queue_positions`, rewritten `_handle_indexes_enabling` (job create + poll), rewritten `trigger_indexes_enabling` (re-queue job), index-enable-job stale reset in `_tick`.
- `backend/services/index_enable_jobs.py` *(new)* — orchestrator-side helpers: ensure/create a PENDING job, read latest job state, reset stale jobs.
- `backend/db/state_db.py` — `index_enable_jobs` table in `_init_schema_legacy`.
- `workers/common.py` — `claim_index_enable_job`, `complete_index_enable_job`, `fail_index_enable_job`.
- `workers/oracle_ddl.py` *(new)* — port of the enable-objects DDL logic from `backend/db/oracle_browser.py`.
- `workers/worker.py` — `index_enable_loop` thread, wired into `main()`.
- `backend/tests/test_orchestrator_lanes.py` *(new)* — lane pure-helper tests.
- `backend/tests/test_index_enable_jobs.py` *(new)* — worker claim/complete/fail + orchestrator next-phase mapping + DDL partitioned-skip tests.

---

## PHASE 1 — Parallel lanes

### Task 1: Lane constants and pure decision helpers

**Files:**
- Modify: `backend/services/orchestrator.py` (near the `_HEAVY_PHASES` definition, ~lines 44-55)
- Test: `backend/tests/test_orchestrator_lanes.py` (create)

**Interfaces:**
- Produces:
  - `BULK_LANE_PHASES: frozenset[str]` — the blocking bulk phases (excludes `INDEXES_ENABLING`).
  - `_migration_lane(strategy: str | None) -> str` — returns `"CDC"` or `"BULK"`.
  - `_lane_is_free(candidate_lane: str, other_migrations: list[tuple[str | None, str]]) -> bool` — `other_migrations` is a list of `(strategy, phase)` for **other** migrations; returns True iff none of them is in the same lane AND in `BULK_LANE_PHASES`.

- [ ] **Step 1: Write the failing test**

Create `backend/tests/test_orchestrator_lanes.py`:

```python
from __future__ import annotations

import os
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import orchestrator as orch


def test_migration_lane_classification():
    assert orch._migration_lane("CDC_STAGE") == "CDC"
    assert orch._migration_lane("CDC_DIRECT") == "CDC"
    assert orch._migration_lane("BULK_STAGE") == "BULK"
    assert orch._migration_lane("BULK_DIRECT") == "BULK"
    assert orch._migration_lane("") == "BULK"
    assert orch._migration_lane(None) == "BULK"


def test_indexes_enabling_is_not_a_blocking_phase():
    assert "INDEXES_ENABLING" not in orch.BULK_LANE_PHASES
    assert "BULK_LOADING" in orch.BULK_LANE_PHASES


def test_cdc_lane_free_when_only_bulk_is_loading():
    # A BULK migration in BULK_LOADING must not block a CDC start.
    assert orch._lane_is_free("CDC", [("BULK_DIRECT", "BULK_LOADING")])


def test_bulk_lane_free_when_only_cdc_is_loading():
    assert orch._lane_is_free("BULK", [("CDC_DIRECT", "BULK_LOADING")])


def test_cdc_lane_blocked_by_other_cdc_in_bulk_phase():
    assert not orch._lane_is_free("CDC", [("CDC_STAGE", "CHUNKING")])


def test_lane_free_when_other_cdc_is_in_indexes_enabling():
    # INDEXES_ENABLING is the tail, not a blocking phase.
    assert orch._lane_is_free("CDC", [("CDC_STAGE", "INDEXES_ENABLING")])


def test_lane_free_when_no_other_migrations():
    assert orch._lane_is_free("CDC", [])
    assert orch._lane_is_free("BULK", [])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && python -m pytest tests/test_orchestrator_lanes.py -q`
Expected: FAIL — `AttributeError: module 'services.orchestrator' has no attribute '_migration_lane'` (and `BULK_LANE_PHASES`).

- [ ] **Step 3: Add the constants and helpers**

In `backend/services/orchestrator.py`, replace the `_HEAVY_PHASES` block (currently ~lines 44-55) with:

```python
# Phases that occupy a bulk "lane".  Each lane (CDC / non-CDC) admits only ONE
# migration in these phases at a time; the rest wait in NEW.  INDEXES_ENABLING
# and everything after it are the non-blocking "tail" — they do NOT hold a lane,
# so the next table can start while the previous one enables indexes / catches
# up CDC.
BULK_LANE_PHASES = frozenset({
    "TOPIC_CREATING",
    "CHUNKING",
    "BULK_LOADING", "BULK_LOADED",
    "STAGE_VALIDATING", "STAGE_VALIDATED",
    "BASELINE_PUBLISHING", "BASELINE_LOADING", "BASELINE_PUBLISHED",
    "STAGE_DROPPING",
})


def _migration_lane(strategy: str | None) -> str:
    """Return the lane a migration belongs to: 'CDC' or 'BULK'."""
    return "CDC" if (strategy or "")[:4] == "CDC_" else "BULK"


def _lane_is_free(candidate_lane: str,
                  other_migrations: list[tuple[str | None, str]]) -> bool:
    """True iff no OTHER migration in the same lane occupies a blocking phase.

    other_migrations: list of (strategy, phase) for every migration except the
    candidate.
    """
    return not any(
        _migration_lane(strat) == candidate_lane and phase in BULK_LANE_PHASES
        for strat, phase in other_migrations
    )
```

Then update any remaining references to `_HEAVY_PHASES` (Task 2 handles the gate and queue; grep `_HEAVY_PHASES` to confirm only those two sites remain).

- [ ] **Step 4: Run test to verify it passes**

Run: `cd backend && python -m pytest tests/test_orchestrator_lanes.py -q`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add backend/services/orchestrator.py backend/tests/test_orchestrator_lanes.py
git commit -m "feat: add bulk-lane phase set and pure lane-decision helpers"
```

---

### Task 2: Lane-scoped start gate and lane-aware queue positions

**Files:**
- Modify: `backend/services/orchestrator.py` — the `slot_busy` gate in `_handle_new` (~lines 1437-1455) and `_update_queue_positions` (~lines 598-662)

**Interfaces:**
- Consumes: `BULK_LANE_PHASES`, `_migration_lane`, `_lane_is_free` (Task 1).
- Produces: no new public symbols; behavioral change only.

- [ ] **Step 1: Replace the single-slot gate with a lane-scoped check**

In `_handle_new`, replace the gate block:

```python
    # Queue gate (same as legacy)
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(_HEAVY_PHASES))
            cur.execute(
                f"""SELECT 1 FROM migrations
                    WHERE  phase IN ({placeholders})
                      AND  migration_id != %s
                    LIMIT 1""",
                (*_HEAVY_PHASES, mid),
            )
            slot_busy = cur.fetchone() is not None
    finally:
        conn.close()

    if slot_busy:
        _update_queue_positions()
        return
```

with:

```python
    # Lane gate: only block on OTHER migrations in the SAME lane (CDC vs non-CDC)
    # that occupy a blocking bulk phase. Cross-lane work runs in parallel.
    candidate_lane = _migration_lane(m.get("strategy"))
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(BULK_LANE_PHASES))
            cur.execute(
                f"""SELECT strategy, phase FROM migrations
                    WHERE  phase IN ({placeholders})
                      AND  migration_id != %s""",
                (*BULK_LANE_PHASES, mid),
            )
            others = cur.fetchall()
    finally:
        conn.close()

    if not _lane_is_free(candidate_lane, others):
        _update_queue_positions()
        return
```

- [ ] **Step 2: Make `_update_queue_positions` lane-aware**

In `_update_queue_positions`, change the `active_slot` and `candidates` CTEs so the "busy" flag and the row numbering are computed **per lane**. Replace the SQL passed to `cur.execute` (the big `WITH active_slot ... UPDATE` statement) with:

```python
            cur.execute("""
                WITH lanes AS (
                    SELECT m.migration_id,
                           m.phase,
                           m.state_changed_at,
                           CASE WHEN LEFT(COALESCE(m.strategy,''),4) = 'CDC_'
                                THEN 'CDC' ELSE 'BULK' END AS lane,
                           cg.status AS group_status
                    FROM   migrations m
                    LEFT JOIN connector_groups cg ON cg.group_id = m.group_id
                ),
                busy_lane AS (
                    SELECT lane, BOOL_OR(phase = ANY(%s)) AS busy
                    FROM   lanes
                    GROUP BY lane
                ),
                candidates AS (
                    SELECT l.migration_id, l.lane,
                           ROW_NUMBER() OVER (
                               PARTITION BY l.lane ORDER BY l.state_changed_at ASC
                           ) AS pos
                    FROM   lanes l
                    WHERE  l.phase = 'NEW'
                      AND  (l.lane = 'BULK' OR l.group_status = 'RUNNING')
                ),
                desired AS (
                    SELECT c.migration_id,
                           CASE
                             WHEN b.busy THEN c.pos
                             WHEN c.pos = 1 THEN NULL
                             ELSE c.pos - 1
                           END AS queue_position
                    FROM candidates c
                    JOIN busy_lane b ON b.lane = c.lane
                )
                UPDATE migrations m
                SET    queue_position = desired.queue_position
                FROM   desired
                WHERE  m.migration_id = desired.migration_id
                  AND  m.queue_position IS DISTINCT FROM desired.queue_position
            """, (list(BULK_LANE_PHASES),))
```

Leave the second statement (clearing stale `queue_position` on non-NEW / non-runnable rows) unchanged.

- [ ] **Step 3: Confirm no stale `_HEAVY_PHASES` references remain**

Run: `cd backend && python -c "import ast,sys; src=open('services/orchestrator.py',encoding='utf-8').read(); assert '_HEAVY_PHASES' not in src, 'replace remaining _HEAVY_PHASES'; print('clean')"`
Expected: `clean`

- [ ] **Step 4: Run the full backend suite**

Run: `cd backend && python -m pytest tests -q`
Expected: PASS (existing 175 + new lane tests).

- [ ] **Step 5: Commit**

```bash
git add backend/services/orchestrator.py
git commit -m "feat: gate migration start and queue positions per lane"
```

---

## PHASE 2 — Index enabling as a worker-claimed job

### Task 3: `index_enable_jobs` table

**Files:**
- Modify: `backend/db/state_db.py::_init_schema_legacy` (add alongside the other `CREATE TABLE IF NOT EXISTS` blocks, e.g. after the `migration_state_history` table ~line 227)

**Interfaces:**
- Produces: `index_enable_jobs` table + `idx_iej_active` partial unique index.

- [ ] **Step 1: Add the table DDL**

Insert into `_init_schema_legacy` (inside the `with conn.cursor() as cur:` block, after the advisory lock and existing tables):

```python
            cur.execute("""
                CREATE TABLE IF NOT EXISTS index_enable_jobs (
                    job_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    migration_id  UUID NOT NULL
                                  REFERENCES migrations(migration_id) ON DELETE CASCADE,
                    state         VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
                    worker_id     VARCHAR(200),
                    result_json   JSONB,
                    error_text    TEXT,
                    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                    claimed_at    TIMESTAMPTZ,
                    started_at    TIMESTAMPTZ,
                    completed_at  TIMESTAMPTZ
                )
            """)
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_iej_active
                    ON index_enable_jobs (migration_id)
                    WHERE state IN ('PENDING', 'CLAIMED', 'RUNNING')
            """)
```

- [ ] **Step 2: Verify the module still imports**

Run: `cd backend && python -c "import ast; ast.parse(open('db/state_db.py',encoding='utf-8').read()); print('ok')"`
Expected: `ok`

- [ ] **Step 3: Run the suite**

Run: `cd backend && python -m pytest tests -q`
Expected: PASS (no regression).

- [ ] **Step 4: Commit**

```bash
git add backend/db/state_db.py
git commit -m "feat: add index_enable_jobs table to State DB schema"
```

---

### Task 4: Worker-side claim/complete/fail helpers

**Files:**
- Modify: `workers/common.py` (add near the chunk helpers)
- Test: `backend/tests/test_index_enable_jobs.py` (create)

**Interfaces:**
- Consumes: module constant `WORKER_ID` (already defined in `workers/common.py`).
- Produces:
  - `claim_index_enable_job(conn) -> dict | None` — claims one PENDING job, returns `{"job_id": str, "migration_id": str, "target_connection_id", "target_schema", "target_table", "strategy"}` or `None`.
  - `complete_index_enable_job(conn, job_id: str, result: dict) -> None` — sets `DONE` (guarded by `worker_id`).
  - `fail_index_enable_job(conn, job_id: str, error_text: str) -> None` — sets `FAILED` (guarded by `worker_id`).

- [ ] **Step 1: Write the failing tests**

Create `backend/tests/test_index_enable_jobs.py`:

```python
from __future__ import annotations

import sys
from pathlib import Path

WORKERS_DIR = Path(__file__).resolve().parents[2] / "workers"
if str(WORKERS_DIR) not in sys.path:
    sys.path.insert(0, str(WORKERS_DIR))

import common as worker_common  # noqa: E402


class CursorStub:
    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.executed: list[tuple[str, tuple]] = []
        self._last = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params or ()))
        self._last = self.rows.pop(0) if self.rows else None

    def fetchone(self):
        return self._last


class ConnStub:
    def __init__(self, rows=None):
        self.cur = CursorStub(rows)
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self.cur

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def test_claim_returns_none_when_no_pending_job():
    conn = ConnStub(rows=[None])
    assert worker_common.claim_index_enable_job(conn) is None
    assert conn.rolled_back


def test_claim_returns_job_dict():
    conn = ConnStub(rows=[(
        "job-1", "mig-1", "tgt-conn", "TGT", "ORDERS", "CDC_STAGE",
    )])
    job = worker_common.claim_index_enable_job(conn)
    assert job["job_id"] == "job-1"
    assert job["migration_id"] == "mig-1"
    assert job["target_schema"] == "TGT"
    assert job["strategy"] == "CDC_STAGE"
    assert conn.committed


def test_complete_is_guarded_by_worker_id():
    conn = ConnStub()
    worker_common.complete_index_enable_job(conn, "job-1", {"enabled": {}})
    sql, params = conn.cur.executed[-1]
    assert "worker_id = %s" in sql
    assert worker_common.WORKER_ID in params
    assert conn.committed


def test_fail_is_guarded_by_worker_id():
    conn = ConnStub()
    worker_common.fail_index_enable_job(conn, "job-1", "boom")
    sql, params = conn.cur.executed[-1]
    assert "worker_id = %s" in sql
    assert worker_common.WORKER_ID in params
    assert conn.committed
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd backend && python -m pytest tests/test_index_enable_jobs.py -q`
Expected: FAIL — `AttributeError: module 'common' has no attribute 'claim_index_enable_job'`.

- [ ] **Step 3: Implement the helpers**

In `workers/common.py` add:

```python
def claim_index_enable_job(conn):
    """Claim one PENDING index-enable job (FOR UPDATE SKIP LOCKED).

    Returns a dict with the job + target migration fields, or None.
    """
    with conn.cursor() as cur:
        cur.execute("""
            WITH candidate AS (
                SELECT job_id
                FROM   index_enable_jobs
                WHERE  state = 'PENDING'
                ORDER BY created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE index_enable_jobs
            SET    state = 'CLAIMED', worker_id = %s, claimed_at = NOW()
            WHERE  job_id = (SELECT job_id FROM candidate)
            RETURNING job_id, migration_id
        """, (WORKER_ID,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None
        job_id, migration_id = row

        cur.execute("""
            SELECT target_connection_id, target_schema, target_table, strategy
            FROM   migrations
            WHERE  migration_id = %s
        """, (migration_id,))
        mrow = cur.fetchone()
        if not mrow:
            conn.rollback()
            return None
        tgt_conn_id, tgt_schema, tgt_table, strategy = mrow
    conn.commit()
    return {
        "job_id":                str(job_id),
        "migration_id":          str(migration_id),
        "target_connection_id":  tgt_conn_id,
        "target_schema":         tgt_schema,
        "target_table":          tgt_table,
        "strategy":              (strategy or "").upper(),
    }


def complete_index_enable_job(conn, job_id: str, result: dict) -> None:
    import json
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE index_enable_jobs
            SET    state = 'DONE', result_json = %s::jsonb, completed_at = NOW()
            WHERE  job_id = %s AND worker_id = %s
        """, (json.dumps(result), job_id, WORKER_ID))
    conn.commit()


def fail_index_enable_job(conn, job_id: str, error_text: str) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE index_enable_jobs
            SET    state = 'FAILED', error_text = %s, completed_at = NOW()
            WHERE  job_id = %s AND worker_id = %s
        """, (error_text[:4000], job_id, WORKER_ID))
    conn.commit()
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd backend && python -m pytest tests/test_index_enable_jobs.py -q`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add workers/common.py backend/tests/test_index_enable_jobs.py
git commit -m "feat: worker claim/complete/fail helpers for index-enable jobs"
```

---

### Task 5: Port enable-objects DDL into the workers package

**Files:**
- Create: `workers/oracle_ddl.py`
- Test: extend `backend/tests/test_index_enable_jobs.py`

**Interfaces:**
- Produces: `enable_table_objects(conn, schema: str, table: str) -> dict` — opens nothing (caller passes an open oracledb connection); sets the table back to LOGGING, rebuilds UNUSABLE indexes, enables DISABLED constraints, **skips partitioned indexes** (M4 parity). Returns `{"enabled": {"indexes": [...], "constraints": [...], "fk_novalidate": [...]}, "errors": {"indexes": [...], "constraints": [...]}}`.

**Port source (verbatim behaviour):** copy the bodies of `set_table_logging`,
`is_temporary_table`, `_constraint_backing_indexes`, `enable_all_disabled_objects`,
and the index-shape query from `backend/db/oracle_browser.py` into `workers/oracle_ddl.py`,
preserving the recently-added partitioned-index skips. Combine `set_table_logging(nologging=False)`
+ `enable_all_disabled_objects` into a single `enable_table_objects(conn, schema, table)`
entry point (the orchestrator's old inline handler did both in sequence). Keep the
`partitioned` flag handling: indexes where `partitioned is True` are skipped for
REBUILD.

- [ ] **Step 1: Write the failing test (partitioned-skip parity)**

Add to `backend/tests/test_index_enable_jobs.py`:

```python
def test_enable_table_objects_skips_partitioned_indexes(monkeypatch):
    import oracle_ddl

    executed = []

    class _Cur:
        def __init__(self, fetch):
            self._fetch = fetch
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False
        def execute(self, sql, params=None):
            executed.append(sql)
        def fetchall(self):
            return self._fetch
        def fetchone(self):
            return None

    # Force the helper that lists indexes to return one partitioned UNUSABLE idx.
    monkeypatch.setattr(
        oracle_ddl, "_list_indexes",
        lambda conn, s, t: [
            {"name": "PIDX", "status": "UNUSABLE", "partitioned": True},
            {"name": "NIDX", "status": "UNUSABLE", "partitioned": False},
        ],
    )
    monkeypatch.setattr(oracle_ddl, "_list_constraints", lambda conn, s, t: [])
    monkeypatch.setattr(oracle_ddl, "set_table_logging", lambda *a, **k: None)
    monkeypatch.setattr(oracle_ddl, "is_temporary_table", lambda *a, **k: False)

    class _Conn:
        def cursor(self):
            return _Cur([])
        def commit(self):
            pass

    result = oracle_ddl.enable_table_objects(_Conn(), "TGT", "ORDERS")
    assert "NIDX" in result["enabled"]["indexes"]
    assert "PIDX" not in result["enabled"]["indexes"]
    assert not any("PIDX" in s for s in executed)
```

> Note: factor the index/constraint listing into `_list_indexes` / `_list_constraints`
> helpers inside `oracle_ddl.py` so the rebuild/enable loop can be tested without a
> live Oracle connection (as the test monkeypatches them).

- [ ] **Step 2: Run to verify it fails**

Run: `cd backend && python -m pytest tests/test_index_enable_jobs.py::test_enable_table_objects_skips_partitioned_indexes -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'oracle_ddl'`.

- [ ] **Step 3: Implement `workers/oracle_ddl.py`**

Port the named functions from `backend/db/oracle_browser.py`. Structure:

```python
"""Target-side DDL for enabling indexes/constraints after a bulk load.

Ported from backend/db/oracle_browser.py so the universal worker can run index
enabling as a claimed job (the workers package must not import backend).
Behaviour must match the backend version, including skipping partitioned indexes.
"""


def set_table_logging(conn, schema: str, table: str, nologging: bool) -> None:
    # ... port from oracle_browser.set_table_logging ...
    ...


def is_temporary_table(conn, schema: str, table: str) -> bool:
    # ... port from oracle_browser.is_temporary_table ...
    ...


def _list_indexes(conn, schema: str, table: str) -> list[dict]:
    """Return [{name, status, partitioned}] for all_indexes on schema.table."""
    ...


def _list_constraints(conn, schema: str, table: str) -> list[dict]:
    """Return [{name, type_code, status}] for all_constraints on schema.table."""
    ...


def _constraint_backing_indexes(conn, schema: str, table: str) -> set[str]:
    # ... port ...
    ...


def enable_table_objects(conn, schema: str, table: str) -> dict:
    """Set the table back to LOGGING, rebuild UNUSABLE indexes (skipping
    partitioned ones), and enable DISABLED constraints. Returns the same shape
    as oracle_browser.enable_all_disabled_objects.
    """
    set_table_logging(conn, schema, table, nologging=False)
    s = schema.upper()
    is_temp = is_temporary_table(conn, s, table)
    rebuild_clause = "REBUILD" if is_temp else "REBUILD NOLOGGING"
    enabled = {"indexes": [], "constraints": [], "fk_novalidate": []}
    errors = {"indexes": [], "constraints": []}
    with conn.cursor() as cur:
        for idx in _list_indexes(conn, s, table):
            if idx.get("partitioned"):
                continue
            if idx["status"] == "UNUSABLE":
                try:
                    cur.execute(f'ALTER INDEX "{s}"."{idx["name"]}" {rebuild_clause}')
                    enabled["indexes"].append(idx["name"])
                except Exception as exc:
                    errors["indexes"].append({"name": idx["name"], "error": str(exc)})
        for con in _list_constraints(conn, s, table):
            if con["status"] == "DISABLED":
                # ... port the constraint-enable branch (incl. FK NOVALIDATE) ...
                ...
    conn.commit()
    return {"enabled": enabled, "errors": errors}
```

Fill the `...` bodies by porting verbatim from `backend/db/oracle_browser.py`
(`set_table_logging`, `is_temporary_table`, `_constraint_backing_indexes`, and the
constraint-enable branch of `enable_all_disabled_objects`). `_list_indexes` uses the
same `all_indexes` query as `get_full_ddl_info` (selecting `partitioned`); `_list_constraints`
selects `constraint_name, constraint_type, status` from `all_constraints`.

- [ ] **Step 4: Run to verify it passes**

Run: `cd backend && python -m pytest tests/test_index_enable_jobs.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add workers/oracle_ddl.py backend/tests/test_index_enable_jobs.py
git commit -m "feat: port index/constraint enable DDL into workers package"
```

---

### Task 6: `index_enable_loop` worker thread

**Files:**
- Modify: `workers/worker.py` — add the loop function and start it in `main()` (next to `compare_loop`, ~lines 1030-1034)

**Interfaces:**
- Consumes: `claim_index_enable_job`, `complete_index_enable_job`, `fail_index_enable_job` (Task 4); `enable_table_objects` (Task 5); `db.open_oracle`, `db.get_pg_conn` (existing worker helpers — confirm exact names in `workers/common.py`).
- Produces: `index_enable_loop(stop_event)`.

- [ ] **Step 1: Implement the loop**

In `workers/worker.py` add (mirroring `compare_loop`):

```python
def index_enable_loop(stop_event) -> None:
    import oracle_ddl
    pg = db.get_pg_conn()
    while not stop_event.is_set():
        try:
            job = db.claim_index_enable_job(pg)
            if job is None:
                time.sleep(BULK_POLL_INTERVAL)
                continue
            print(f"[index-enable] claimed job {job['job_id']} "
                  f"for {job['target_schema']}.{job['target_table']}")
            ora = db.open_oracle(job["target_connection_id"], _load_configs())
            try:
                result = oracle_ddl.enable_table_objects(
                    ora, job["target_schema"], job["target_table"],
                )
            finally:
                ora.close()

            err_count = (len(result["errors"]["indexes"])
                         + len(result["errors"]["constraints"]))
            if err_count:
                db.fail_index_enable_job(
                    pg, job["job_id"], str(result["errors"])[:4000])
            else:
                db.complete_index_enable_job(pg, job["job_id"], result)
        except Exception as exc:
            print(f"[index-enable] loop error: {exc}")
            try:
                pg.close()
            except Exception:
                pass
            pg = db.get_pg_conn()
            time.sleep(BULK_POLL_INTERVAL)
```

> Confirm the helper names used here against `workers/common.py`/`worker.py`:
> the bulk loop uses `db.open_oracle(connection_id, configs)` and `db.get_pg_conn()`.
> Match how `compare_loop` obtains `configs` (e.g. a module-level `_load_configs()`
> or inline load) and reuse that exact mechanism instead of `_load_configs()` if the
> name differs.

- [ ] **Step 2: Start the loop in `main()`**

After the `compare_loop` thread block in `main()`:

```python
    iel = threading.Thread(
        target=index_enable_loop, args=(main_stop,),
        name="index-enable-loop", daemon=True,
    )
    iel.start()
```

- [ ] **Step 3: Verify the module parses**

Run: `cd backend && python -c "import ast; ast.parse(open('../workers/worker.py',encoding='utf-8').read()); print('ok')"`
Expected: `ok`

- [ ] **Step 4: Commit**

```bash
git add workers/worker.py
git commit -m "feat: add index-enable worker loop to the universal worker"
```

---

### Task 7: Orchestrator-side job helpers + `_handle_indexes_enabling` polls the job

**Files:**
- Create: `backend/services/index_enable_jobs.py`
- Modify: `backend/services/orchestrator.py` — `_handle_indexes_enabling` (~lines 1598-1670)
- Test: extend `backend/tests/test_index_enable_jobs.py`

**Interfaces:**
- Produces (in `backend/services/index_enable_jobs.py`):
  - `ensure_pending_job(conn, migration_id: str) -> None` — insert a PENDING job if no active one exists (relies on the partial unique index; uses `ON CONFLICT DO NOTHING`).
  - `latest_job_state(conn, migration_id: str) -> tuple[str | None, dict | None, str | None]` — returns `(state, result_json, error_text)` of the most recent job, or `(None, None, None)`.
  - `reset_stale_jobs(conn, stale_minutes: int = 15) -> int` — CLAIMED/RUNNING jobs older than the cutoff → PENDING.
- Produces (in `orchestrator.py`): `_next_phase_after_indexes(strategy: str | None) -> str` — `"CDC_APPLYING"` for CDC, `"DATA_VERIFYING"` for non-CDC.

- [ ] **Step 1: Write the failing test for the next-phase mapping**

Add to `backend/tests/test_index_enable_jobs.py`:

```python
def test_next_phase_after_indexes_by_strategy():
    import sys
    from pathlib import Path
    backend = Path(__file__).resolve().parents[1]
    if str(backend) not in sys.path:
        sys.path.insert(0, str(backend))
    from services import orchestrator as orch

    assert orch._next_phase_after_indexes("CDC_STAGE") == "CDC_APPLYING"
    assert orch._next_phase_after_indexes("CDC_DIRECT") == "CDC_APPLYING"
    assert orch._next_phase_after_indexes("BULK_STAGE") == "DATA_VERIFYING"
    assert orch._next_phase_after_indexes(None) == "DATA_VERIFYING"
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd backend && python -m pytest tests/test_index_enable_jobs.py::test_next_phase_after_indexes_by_strategy -q`
Expected: FAIL — `AttributeError: ... '_next_phase_after_indexes'`.

- [ ] **Step 3: Implement `backend/services/index_enable_jobs.py`**

```python
"""Orchestrator-side helpers for index_enable_jobs (worker-claimed)."""


def ensure_pending_job(conn, migration_id: str) -> None:
    """Create a PENDING job unless an active one already exists.

    The partial unique index idx_iej_active makes the insert a no-op when a
    PENDING/CLAIMED/RUNNING job is already present.
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO index_enable_jobs (migration_id, state)
            SELECT %s, 'PENDING'
            WHERE NOT EXISTS (
                SELECT 1 FROM index_enable_jobs
                WHERE  migration_id = %s
                  AND  state IN ('PENDING', 'CLAIMED', 'RUNNING')
            )
        """, (migration_id, migration_id))
    conn.commit()


def latest_job_state(conn, migration_id: str):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT state, result_json, error_text
            FROM   index_enable_jobs
            WHERE  migration_id = %s
            ORDER BY created_at DESC
            LIMIT 1
        """, (migration_id,))
        row = cur.fetchone()
    return (row[0], row[1], row[2]) if row else (None, None, None)


def reset_stale_jobs(conn, stale_minutes: int = 15) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE index_enable_jobs
            SET    state = 'PENDING', worker_id = NULL, claimed_at = NULL, started_at = NULL
            WHERE  state IN ('CLAIMED', 'RUNNING')
              AND  claimed_at < NOW() - (%s || ' minutes')::interval
        """, (stale_minutes,))
        n = cur.rowcount
    conn.commit()
    return n
```

- [ ] **Step 4: Add `_next_phase_after_indexes` and rewrite `_handle_indexes_enabling`**

In `orchestrator.py`, add the import near the other service imports:

```python
import services.index_enable_jobs as index_enable_jobs
```

Add the pure helper next to the other lane helpers:

```python
def _next_phase_after_indexes(strategy: str | None) -> str:
    """Where INDEXES_ENABLING goes once the job is DONE."""
    return "CDC_APPLYING" if _migration_lane(strategy) == "CDC" else "DATA_VERIFYING"
```

Replace the body of `_handle_indexes_enabling` (the whole `_try_mark_in_prog` +
threaded `_run` implementation) with a job create + poll handler:

```python
def _handle_indexes_enabling(mid: str, m: dict) -> None:
    """Drive index enabling via a worker-claimed job (not a coordinator thread).

    On first entry (no job yet) queue a PENDING job, then poll its state each
    tick. A FAILED job is surfaced as INDEXES_ENABLE_ERROR and NOT auto-requeued
    — the user re-runs it via trigger_indexes_enabling (which queues a fresh
    job). Auto-requeuing here would mask the failure forever.
    """
    conn = _state["get_conn"]()
    try:
        state, _result, error_text = index_enable_jobs.latest_job_state(conn, mid)
        if state is None:
            index_enable_jobs.ensure_pending_job(conn, mid)
            state = "PENDING"
    finally:
        conn.close()

    if state == "DONE":
        to_phase = _next_phase_after_indexes(m.get("strategy"))
        _safe_transition(
            mid, "INDEXES_ENABLING", to_phase,
            message="Индексы/констрейнты включены (worker job)",
            extra_fields={"error_code": None, "error_text": None},
        )
    elif state == "FAILED" and m.get("error_code") != "INDEXES_ENABLE_ERROR":
        # Surface once; the guard avoids rewriting history every tick.
        _transition(
            mid, "INDEXES_ENABLING",
            message="Ошибка пересчёта индексов. Нажмите «Включить индексы» для повтора.",
            extra_fields={
                "error_code": "INDEXES_ENABLE_ERROR",
                "error_text": (error_text or "")[:2000],
            },
        )
    # PENDING / CLAIMED / RUNNING (or already-surfaced FAILED) → wait.
```

> The `set_table_logging` call the old handler did is now inside the worker's
> `enable_table_objects` (Task 5), so it is not repeated here.

- [ ] **Step 5: Run the tests**

Run: `cd backend && python -m pytest tests -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add backend/services/index_enable_jobs.py backend/services/orchestrator.py backend/tests/test_index_enable_jobs.py
git commit -m "feat: orchestrator drives index enabling via worker job state"
```

---

### Task 8: Re-queue on manual retry + stale-job reset in the tick

**Files:**
- Modify: `backend/services/orchestrator.py` — `trigger_indexes_enabling` (~lines 1170-1215) and `_tick` (~lines 103-127)

**Interfaces:**
- Consumes: `index_enable_jobs.ensure_pending_job`, `index_enable_jobs.reset_stale_jobs` (Task 7).

- [ ] **Step 1: Reset stale index-enable jobs each tick**

In `_tick`, alongside `job_queue.reset_stale_chunks(conn)`:

```python
        job_queue.reset_stale_chunks(conn)
        index_enable_jobs.reset_stale_jobs(conn)
```

- [ ] **Step 2: Make `trigger_indexes_enabling` re-queue a job**

`trigger_indexes_enabling` currently (after the FAILED-recovery transition to
INDEXES_ENABLING) ran the enable inline. Replace the inline enable invocation at the
end of the function with a fresh PENDING job so the worker re-runs it:

```python
    # Re-queue a worker job and clear the prior error so a new failure can be
    # surfaced again (the handler guards on error_code to avoid history spam).
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE migrations SET error_code = NULL, error_text = NULL "
                "WHERE migration_id = %s",
                (migration_id,))
        conn.commit()
        index_enable_jobs.ensure_pending_job(conn, migration_id)
    finally:
        conn.close()
```

Keep the existing phase validation (accepts `INDEXES_ENABLING`, and `FAILED` with
`error_code == "INDEXES_ENABLE_ERROR"` via the recovery transition). Remove any
remaining reference to the old threaded enable path in this function.

- [ ] **Step 3: Confirm no inline enable remains in the orchestrator**

Run: `cd backend && python -c "src=open('services/orchestrator.py',encoding='utf-8').read(); assert 'enable_all_disabled_objects' not in src, 'inline enable still present'; print('clean')"`
Expected: `clean`

- [ ] **Step 4: Run the full suite**

Run: `cd backend && python -m pytest tests -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/services/orchestrator.py
git commit -m "feat: re-queue index-enable job on manual retry and reset stale jobs"
```

---

## Final verification

- [ ] Run the whole backend suite: `cd backend && python -m pytest tests -q` → all pass.
- [ ] Parse every touched module: `cd backend && python -m py_compile db/state_db.py services/orchestrator.py services/index_enable_jobs.py ../workers/common.py ../workers/worker.py ../workers/oracle_ddl.py`.
- [ ] Manual/staging smoke (not unit-testable here): start one CDC and one non-CDC migration → confirm both enter bulk phases concurrently; confirm a second CDC table starts while the first is in `INDEXES_ENABLING`; confirm a worker claims the index-enable job and the migration advances to `DATA_VERIFYING`/`CDC_APPLYING`.
