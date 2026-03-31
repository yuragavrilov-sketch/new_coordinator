# Iteration 2: Orchestrator Decomposition — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split `services/orchestrator.py` (1,812 LOC) into ~10 focused modules under `orchestrator/` package.

**Architecture:** Extract shared infrastructure (engine loop, transition helpers, queue gating) first, then move phase handlers into domain-grouped modules. The old `orchestrator.py` is replaced entirely. No logic changes — pure structural refactoring.

**Tech Stack:** Python 3.11+, threading, existing services layer

**Key constraint:** This is a move-only refactoring. The handlers' logic must remain identical. The only "test" is that the app starts and the dispatch table is correctly wired.

---

### Task 1: Create orchestrator package and shared helpers

**Files:**
- Create: `backend/orchestrator/__init__.py`
- Create: `backend/orchestrator/helpers.py`

This task extracts the shared infrastructure used by all phase handlers: `_state` dict, config access, oracle config lookup, `_in_prog` tracking, `_current_phase` reader.

- [ ] **Step 1: Create directory**

```bash
mkdir -p backend/orchestrator/phases
touch backend/orchestrator/__init__.py backend/orchestrator/phases/__init__.py
```

- [ ] **Step 2: Create `backend/orchestrator/helpers.py`**

This module holds the mutable shared state and helper functions that all phase handlers need. Extract from `orchestrator.py` lines 19-24, 39, 55-57, 59, 66-69, 234-292.

```python
"""Shared helpers for orchestrator modules.

All phase handlers import from here to access DB connections,
config, broadcasting, and in-progress tracking.
"""

import threading
from datetime import datetime

from db.state_db import (
    get_active_migrations,
    row_to_dict,
    transition_phase,
    update_migration_fields,
)

# Module-level state populated by init()
_state: dict = {}

# Track migrations running in a dedicated thread
_in_progress: set[str] = set()
_in_progress_lock = threading.Lock()

# Track groups running in a dedicated thread
_group_in_progress: set[str] = set()
_group_in_progress_lock = threading.Lock()


def init(get_conn_fn, load_configs_fn, broadcast_fn) -> None:
    _state["get_conn"] = get_conn_fn
    _state["load_configs"] = load_configs_fn
    _state["broadcast"] = broadcast_fn


def get_conn():
    return _state["get_conn"]()


def configs() -> dict:
    return _state["load_configs"]()


def oracle_cfg(connection_id: str) -> dict:
    return configs().get(connection_id, {})


def broadcast(event: dict) -> None:
    _state["broadcast"](event)


def transition(migration_id: str, to_phase: str,
               message: str | None = None,
               error_code: str | None = None,
               error_text: str | None = None,
               extra_fields: dict | None = None) -> None:
    conn = get_conn()
    try:
        from_phase = transition_phase(
            conn, migration_id, to_phase,
            message=message,
            error_code=error_code,
            error_text=error_text,
            extra_fields=extra_fields,
        )
        conn.commit()
    finally:
        conn.close()

    broadcast({
        "type":         "migration_phase",
        "migration_id": migration_id,
        "from_phase":   from_phase,
        "phase":        to_phase,
        "ts":           datetime.utcnow().isoformat() + "Z",
    })
    print(f"[orchestrator] {migration_id}: {from_phase} → {to_phase}"
          + (f" ({message})" if message else ""))


def fail(migration_id: str, error_text: str,
         error_code: str = "ORCHESTRATOR_ERROR") -> None:
    try:
        transition(
            migration_id, "FAILED",
            message=error_text[:500],
            error_code=error_code,
            error_text=error_text[:2000],
        )
    except Exception as exc:
        print(f"[orchestrator] could not set FAILED for {migration_id}: {exc}")


def update(migration_id: str, fields: dict) -> None:
    conn = get_conn()
    try:
        update_migration_fields(conn, migration_id, fields)
        conn.commit()
    finally:
        conn.close()


def current_phase(migration_id: str) -> str | None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT phase FROM migrations WHERE migration_id = %s",
                (migration_id,),
            )
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def safe_transition(migration_id: str, expected_phase: str, to_phase: str,
                    **kwargs) -> bool:
    cur = current_phase(migration_id)
    if cur != expected_phase:
        print(f"[orchestrator] {migration_id}: skip transition {expected_phase}→{to_phase}, "
              f"current phase is {cur} (cancelled?)")
        return False
    transition(migration_id, to_phase, **kwargs)
    return True


def in_prog(migration_id: str) -> bool:
    with _in_progress_lock:
        return migration_id in _in_progress


def mark_in_prog(migration_id: str) -> None:
    with _in_progress_lock:
        _in_progress.add(migration_id)


def unmark_in_prog(migration_id: str) -> None:
    with _in_progress_lock:
        _in_progress.discard(migration_id)


def group_in_prog(group_id: str) -> bool:
    with _group_in_progress_lock:
        return group_id in _group_in_progress


def mark_group_in_prog(group_id: str) -> None:
    with _group_in_progress_lock:
        _group_in_progress.add(group_id)


def unmark_group_in_prog(group_id: str) -> None:
    with _group_in_progress_lock:
        _group_in_progress.discard(group_id)
```

- [ ] **Step 3: Commit**

```bash
git add backend/orchestrator/
git commit -m "refactor: create orchestrator package with shared helpers"
```

---

### Task 2: Create queue module

**Files:**
- Create: `backend/orchestrator/queue.py`

Extract queue gating logic: `_HEAVY_PHASES`, `_update_queue_positions`, and the shared gate-check used by both `_handle_new` and `_handle_new_group`.

- [ ] **Step 1: Create `backend/orchestrator/queue.py`**

Extract from `orchestrator.py` lines 44-53, 295-321, and the queue-gate pattern from lines 346-382. The `check_loading_slot` function consolidates the duplicated gating logic.

```python
"""Queue gating — ensures only one migration at a time in heavy phases."""

from orchestrator.helpers import get_conn, update

_HEAVY_PHASES = frozenset({
    "PREPARING", "SCN_FIXED",
    "CONNECTOR_STARTING", "CDC_BUFFERING",
    "TOPIC_CREATING",
    "CHUNKING",
    "BULK_LOADING", "BULK_LOADED",
    "STAGE_VALIDATING", "STAGE_VALIDATED",
    "BASELINE_PUBLISHING", "BASELINE_LOADING", "BASELINE_PUBLISHED",
    "STAGE_DROPPING",
})


def update_queue_positions() -> None:
    """Recalculate queue_position for all migrations waiting in NEW."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE migrations m
                SET    queue_position = sub.pos
                FROM (
                    SELECT migration_id,
                           ROW_NUMBER() OVER (ORDER BY state_changed_at ASC) AS pos
                    FROM   migrations
                    WHERE  phase = 'NEW'
                ) sub
                WHERE m.migration_id = sub.migration_id
                  AND  m.phase = 'NEW'
            """)
            cur.execute("""
                UPDATE migrations
                SET    queue_position = NULL
                WHERE  phase != 'NEW'
                  AND  queue_position IS NOT NULL
            """)
        conn.commit()
    finally:
        conn.close()


def check_loading_slot(mid: str) -> bool:
    """Check if the loading slot is free and it's this migration's turn.

    Returns True if the migration may proceed, False if it should wait.
    """
    conn = get_conn()
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
        update_queue_positions()
        return False

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT migration_id FROM migrations
                WHERE  phase = 'NEW'
                ORDER BY state_changed_at ASC
                LIMIT 1
            """)
            first = cur.fetchone()
    finally:
        conn.close()

    if first and first[0] != mid:
        update_queue_positions()
        return False

    return True
```

- [ ] **Step 2: Commit**

```bash
git add backend/orchestrator/queue.py
git commit -m "refactor: extract queue gating logic to orchestrator/queue.py"
```

---

### Task 3: Create phase handler modules

**Files:**
- Create: `backend/orchestrator/phases/preparing.py`
- Create: `backend/orchestrator/phases/chunking.py`
- Create: `backend/orchestrator/phases/baseline.py`
- Create: `backend/orchestrator/phases/cdc.py`
- Create: `backend/orchestrator/phases/data_verify.py`
- Create: `backend/orchestrator/phases/cleanup.py`

Move handlers from `orchestrator.py` into domain-grouped modules. Each handler function keeps its original name but drops the leading underscore (e.g., `_handle_preparing` → `handle_preparing`). All functions import from `orchestrator.helpers`.

- [ ] **Step 1: Create `backend/orchestrator/phases/preparing.py`**

Contains: `handle_new`, `handle_preparing`, `handle_scn_fixed`, `handle_connector_starting`, `handle_cdc_buffering`.
Source lines: 328-575 from orchestrator.py.

Key imports:
```python
import json
import threading
from datetime import datetime

import services.debezium as debezium
import services.oracle_scn as oracle_scn
import services.oracle_stage as oracle_stage
import services.oracle_chunker as oracle_chunker
import services.job_queue as job_queue

from orchestrator.helpers import (
    oracle_cfg, transition, fail, update, safe_transition,
    current_phase, in_prog, mark_in_prog, unmark_in_prog, get_conn,
)
from orchestrator.queue import check_loading_slot
```

Move these functions verbatim (just renaming `_handle_X` → `handle_X` and `_oracle_cfg` → `oracle_cfg`, etc.):
- `handle_new` (was `_handle_new`, lines 328-385)
- `handle_preparing` (was `_handle_preparing`, lines 388-445)
- `handle_scn_fixed` (was `_handle_scn_fixed`, lines 448-466)
- `handle_connector_starting` (was `_handle_connector_starting`, lines 469-491)
- `handle_cdc_buffering` (was `_handle_cdc_buffering`, lines 570-575)
- `create_chunks_and_transition` (was `_create_chunks_and_transition`, lines 494-567) — shared with chunking, keep here

- [ ] **Step 2: Create `backend/orchestrator/phases/chunking.py`**

Contains: `handle_chunking`, `handle_bulk_loading`, `handle_bulk_loaded`.
Source lines: 578-626.

```python
import services.job_queue as job_queue
from datetime import datetime
from orchestrator.helpers import (
    get_conn, transition, fail, update, broadcast,
)
```

Move verbatim:
- `handle_chunking` (lines 578-581)
- `handle_bulk_loading` (lines 584-617)
- `handle_bulk_loaded` (lines 619-626)

- [ ] **Step 3: Create `backend/orchestrator/phases/baseline.py`**

Contains: `handle_stage_validating`, `handle_stage_validated`, `handle_baseline_publishing`, `handle_baseline_loading`, `handle_baseline_published`, `handle_stage_dropping`.
Source lines: 629-865.

```python
import json
import threading
import time
from datetime import datetime

import services.oracle_scn as oracle_scn
import services.oracle_stage as oracle_stage
import services.oracle_chunker as oracle_chunker
import services.validator as validator
import services.job_queue as job_queue
import db.oracle_browser as oracle_browser

from orchestrator.helpers import (
    oracle_cfg, get_conn, transition, fail, update, safe_transition,
    current_phase, in_prog, mark_in_prog, unmark_in_prog, broadcast,
)
```

Move verbatim:
- `handle_stage_validating` (lines 629-654)
- `handle_stage_validated` (lines 657-658)
- `handle_baseline_publishing` (lines 661-793)
- `handle_baseline_loading` (lines 796-837)
- `handle_baseline_published` (lines 840-842)
- `handle_stage_dropping` (lines 845-865)

- [ ] **Step 4: Create `backend/orchestrator/phases/cleanup.py`**

Contains: `handle_indexes_enabling`, `handle_indexes_enabling_group`, `handle_cancelling`.
Source lines: 868-965, 1390-1471.

```python
import threading
import services.oracle_scn as oracle_scn
import db.oracle_browser as oracle_browser

from orchestrator.helpers import (
    oracle_cfg, transition, fail, safe_transition,
    current_phase, in_prog, mark_in_prog, unmark_in_prog,
)
```

Move verbatim:
- `handle_indexes_enabling` (lines 868-959)
- `handle_indexes_enabling_group` (lines 1390-1471)
- `handle_cancelling` (lines 961-965)

- [ ] **Step 5: Create `backend/orchestrator/phases/cdc.py`**

Contains: `handle_cdc_apply_starting`, `handle_cdc_applying`, `handle_cdc_catching_up`, `handle_cdc_caught_up`, `handle_steady_state`.
Source lines: 1130-1212, 1590-1607.

```python
from datetime import datetime
from orchestrator.helpers import get_conn, transition, update, broadcast
```

Move verbatim:
- `handle_cdc_apply_starting` (lines 1130-1147)
- `handle_cdc_applying` (lines 1590-1607)
- `handle_cdc_catching_up` (lines 1149-1178)
- `handle_cdc_caught_up` (lines 1181-1183)
- `handle_steady_state` (lines 1186-1212)

- [ ] **Step 6: Create `backend/orchestrator/phases/data_verify.py`**

Contains: `handle_data_verifying`, `handle_data_mismatch`.
Source lines: 1474-1587.

```python
import threading
from orchestrator.helpers import (
    get_conn, fail, safe_transition, current_phase,
    in_prog, mark_in_prog, unmark_in_prog,
)
```

Move verbatim:
- `handle_data_verifying` (lines 1474-1582)
- `handle_data_mismatch` (lines 1585-1587)

- [ ] **Step 7: Commit**

```bash
git add backend/orchestrator/phases/
git commit -m "refactor: move phase handlers to orchestrator/phases/ modules"
```

---

### Task 4: Create group lifecycle and triggers modules

**Files:**
- Create: `backend/orchestrator/groups.py`
- Create: `backend/orchestrator/triggers.py`
- Create: `backend/orchestrator/phases/group_new.py`

- [ ] **Step 1: Create `backend/orchestrator/phases/group_new.py`**

Contains: `handle_new_group`, `handle_topic_creating`.
Source lines: 1219-1387.

```python
import json
import threading

import services.oracle_scn as oracle_scn
import services.oracle_stage as oracle_stage
import services.kafka_topics as kafka_topics
import services.connector_groups as connector_groups_svc

from orchestrator.helpers import (
    oracle_cfg, configs, get_conn, transition, fail, update, safe_transition,
    current_phase, in_prog, mark_in_prog, unmark_in_prog,
)
from orchestrator.queue import check_loading_slot
from orchestrator.phases.preparing import create_chunks_and_transition
```

Move verbatim:
- `handle_new_group` (lines 1219-1344)
- `handle_topic_creating` (lines 1347-1387)

- [ ] **Step 2: Create `backend/orchestrator/groups.py`**

Contains: `tick_groups`, `check_group_connectors`, and group lifecycle handlers.
Source lines: 1613-1811.

```python
import threading

import services.debezium as debezium
import services.connector_groups as connector_groups_svc

from orchestrator.helpers import (
    get_conn, fail, broadcast,
    mark_group_in_prog, unmark_group_in_prog, group_in_prog,
)
```

Move verbatim:
- `tick_groups` (lines 1617-1651)
- `_handle_group_topics_creating` (lines 1654-1694)
- `_handle_group_connector_starting` (lines 1697-1725)
- `_handle_group_stopping` (lines 1728-1755)
- `check_group_connectors` (lines 1767-1811)

- [ ] **Step 3: Create `backend/orchestrator/triggers.py`**

Contains public API functions called by routes.
Source lines: 972-1127.

```python
from db.state_db import row_to_dict
import services.oracle_scn as oracle_scn
import db.oracle_browser as oracle_browser

from orchestrator.helpers import get_conn, transition, oracle_cfg
from orchestrator.phases.cleanup import handle_indexes_enabling
```

Move verbatim:
- `trigger_indexes_enabling` (lines 972-1014)
- `trigger_enable_triggers` (lines 1017-1062)
- `trigger_baseline_restart` (lines 1065-1127)

- [ ] **Step 4: Commit**

```bash
git add backend/orchestrator/groups.py backend/orchestrator/triggers.py backend/orchestrator/phases/group_new.py
git commit -m "refactor: extract group lifecycle, triggers, and group_new handlers"
```

---

### Task 5: Create engine module and wire dispatch tables

**Files:**
- Create: `backend/orchestrator/engine.py`
- Modify: `backend/orchestrator/__init__.py`

- [ ] **Step 1: Create `backend/orchestrator/engine.py`**

This is the new entry point — replaces the top-level tick/dispatch/start logic.

```python
"""Orchestrator engine — background thread driving migration state transitions."""

import threading
import time

import services.job_queue as job_queue
from db.state_db import get_active_migrations

from orchestrator.helpers import get_conn, fail
from orchestrator.groups import tick_groups, check_group_connectors

# Phase handler imports
from orchestrator.phases import preparing, chunking, baseline, cleanup, cdc, data_verify
from orchestrator.phases import group_new

TICK_INTERVAL = 5

_LEGACY_HANDLERS = {
    "NEW":                  lambda mid, m: preparing.handle_new(mid, m),
    "PREPARING":            lambda mid, m: preparing.handle_preparing(mid, m),
    "SCN_FIXED":            lambda mid, m: preparing.handle_scn_fixed(mid, m),
    "CONNECTOR_STARTING":   lambda mid, m: preparing.handle_connector_starting(mid, m),
    "CDC_BUFFERING":        lambda mid, m: preparing.handle_cdc_buffering(mid, m),
    "CHUNKING":             lambda mid, m: chunking.handle_chunking(mid, m),
    "BULK_LOADING":         lambda mid, m: chunking.handle_bulk_loading(mid, m),
    "BULK_LOADED":          lambda mid, m: chunking.handle_bulk_loaded(mid, m),
    "STAGE_VALIDATING":     lambda mid, m: baseline.handle_stage_validating(mid, m),
    "STAGE_VALIDATED":      lambda mid, m: baseline.handle_stage_validated(mid, m),
    "BASELINE_PUBLISHING":  lambda mid, m: baseline.handle_baseline_publishing(mid, m),
    "BASELINE_LOADING":     lambda mid, m: baseline.handle_baseline_loading(mid, m),
    "BASELINE_PUBLISHED":   lambda mid, m: baseline.handle_baseline_published(mid, m),
    "STAGE_DROPPING":       lambda mid, m: baseline.handle_stage_dropping(mid, m),
    "INDEXES_ENABLING":     lambda mid, m: cleanup.handle_indexes_enabling(mid, m),
    "DATA_VERIFYING":       lambda mid, m: data_verify.handle_data_verifying(mid, m),
    "DATA_MISMATCH":        lambda mid, m: data_verify.handle_data_mismatch(mid, m),
    "CDC_APPLY_STARTING":   lambda mid, m: cdc.handle_cdc_apply_starting(mid, m),
    "CDC_CATCHING_UP":      lambda mid, m: cdc.handle_cdc_catching_up(mid, m),
    "CDC_CAUGHT_UP":        lambda mid, m: cdc.handle_cdc_caught_up(mid, m),
    "STEADY_STATE":         lambda mid, m: cdc.handle_steady_state(mid, m),
    "CANCELLING":           lambda mid, m: cleanup.handle_cancelling(mid, m),
}

_GROUP_HANDLERS = {
    "NEW":                  lambda mid, m: group_new.handle_new_group(mid, m),
    "TOPIC_CREATING":       lambda mid, m: group_new.handle_topic_creating(mid, m),
    "CHUNKING":             lambda mid, m: chunking.handle_chunking(mid, m),
    "BULK_LOADING":         lambda mid, m: chunking.handle_bulk_loading(mid, m),
    "BULK_LOADED":          lambda mid, m: chunking.handle_bulk_loaded(mid, m),
    "STAGE_VALIDATING":     lambda mid, m: baseline.handle_stage_validating(mid, m),
    "STAGE_VALIDATED":      lambda mid, m: baseline.handle_stage_validated(mid, m),
    "BASELINE_PUBLISHING":  lambda mid, m: baseline.handle_baseline_publishing(mid, m),
    "BASELINE_LOADING":     lambda mid, m: baseline.handle_baseline_loading(mid, m),
    "BASELINE_PUBLISHED":   lambda mid, m: baseline.handle_baseline_published(mid, m),
    "STAGE_DROPPING":       lambda mid, m: baseline.handle_stage_dropping(mid, m),
    "INDEXES_ENABLING":     lambda mid, m: cleanup.handle_indexes_enabling_group(mid, m),
    "DATA_VERIFYING":       lambda mid, m: data_verify.handle_data_verifying(mid, m),
    "DATA_MISMATCH":        lambda mid, m: data_verify.handle_data_mismatch(mid, m),
    "CDC_APPLYING":         lambda mid, m: cdc.handle_cdc_applying(mid, m),
    "CDC_CATCHING_UP":      lambda mid, m: cdc.handle_cdc_catching_up(mid, m),
    "CDC_CAUGHT_UP":        lambda mid, m: cdc.handle_cdc_caught_up(mid, m),
    "STEADY_STATE":         lambda mid, m: cdc.handle_steady_state(mid, m),
    "CANCELLING":           lambda mid, m: cleanup.handle_cancelling(mid, m),
}

_orchestrator_started = False


def is_running() -> bool:
    return _orchestrator_started


def start_orchestrator() -> None:
    global _orchestrator_started
    if _orchestrator_started:
        return
    _orchestrator_started = True

    def _run():
        time.sleep(3)
        while True:
            try:
                _tick()
            except Exception as exc:
                print(f"[orchestrator] tick error: {exc}")
            time.sleep(TICK_INTERVAL)

    threading.Thread(target=_run, daemon=True, name="orchestrator").start()
    print("[orchestrator] started")


def _tick() -> None:
    conn = get_conn()
    try:
        job_queue.reset_stale_chunks(conn)
        migrations = get_active_migrations(conn)
    finally:
        conn.close()

    for m in migrations:
        mid = m["migration_id"]
        phase = m["phase"]
        try:
            _dispatch(mid, phase, m)
        except Exception as exc:
            print(f"[orchestrator] migration {mid} phase {phase} error: {exc}")
            fail(mid, str(exc))

    tick_groups()
    check_group_connectors()


def _dispatch(migration_id: str, phase: str, m: dict) -> None:
    if m.get("group_id"):
        handler = _GROUP_HANDLERS.get(phase)
    else:
        handler = _LEGACY_HANDLERS.get(phase)
    if handler:
        handler(migration_id, m)
```

- [ ] **Step 2: Update `backend/orchestrator/__init__.py`**

```python
"""Orchestrator package — migration phase state machine."""

from orchestrator.helpers import init
from orchestrator.engine import start_orchestrator, is_running
from orchestrator.triggers import (
    trigger_indexes_enabling,
    trigger_enable_triggers,
    trigger_baseline_restart,
)

__all__ = [
    "init",
    "start_orchestrator",
    "is_running",
    "trigger_indexes_enabling",
    "trigger_enable_triggers",
    "trigger_baseline_restart",
]
```

- [ ] **Step 3: Commit**

```bash
git add backend/orchestrator/engine.py backend/orchestrator/__init__.py
git commit -m "refactor: create orchestrator engine with dispatch tables"
```

---

### Task 6: Update app.py and routes to use new orchestrator package

**Files:**
- Modify: `backend/app.py`
- Modify: `backend/routes/migrations.py` (if it imports from `services.orchestrator`)
- Modify: `backend/routes/config.py` (if it imports from `services.orchestrator`)

- [ ] **Step 1: Update imports in app.py**

Replace:
```python
import services.orchestrator as orchestrator
# or
from services.orchestrator import ...
```

With:
```python
import orchestrator
```

The `orchestrator` package exposes the same API: `init()`, `start_orchestrator()`, `is_running()`, `trigger_indexes_enabling()`, `trigger_enable_triggers()`, `trigger_baseline_restart()`.

- [ ] **Step 2: Update imports in routes/migrations.py**

Find any `import services.orchestrator` or `from services.orchestrator import` and replace with `import orchestrator`.

Specifically, the routes reference:
- `orchestrator_mod.trigger_indexes_enabling`
- `orchestrator_mod.trigger_enable_triggers`
- `orchestrator_mod.trigger_baseline_restart`

These are now available via `import orchestrator` (re-exported from `__init__.py`).

- [ ] **Step 3: Search for any other files importing from services.orchestrator**

Run: `grep -r "services.orchestrator\|from services import orchestrator" backend/ --include="*.py"`

Update all found imports.

- [ ] **Step 4: Rename old orchestrator to backup**

```bash
mv backend/services/orchestrator.py backend/services/orchestrator_old.py
```

- [ ] **Step 5: Verify the app can import the new orchestrator**

Run: `cd /mnt/c/work/database_migration/new/front && docker run --rm -v "$(pwd)/backend:/app" -w /app python:3.11-slim bash -c "pip install -q pydantic structlog flask flask-cors python-dotenv psycopg2-binary 2>/dev/null && python -c 'import orchestrator; print(\"OK: init, start_orchestrator, is_running, triggers all importable\")' 2>&1"`

- [ ] **Step 6: Verify all tests still pass**

Run: `cd /mnt/c/work/database_migration/new/front && docker run --rm -v "$(pwd)/backend:/app" -w /app python:3.11-slim bash -c "pip install -q pydantic structlog pytest 2>/dev/null && python -m pytest tests/ -v"`

- [ ] **Step 7: Commit**

```bash
git add backend/app.py backend/routes/ backend/services/orchestrator_old.py
git commit -m "refactor: wire app.py and routes to new orchestrator package"
```

---

### Task 7: Delete old orchestrator and final verification

**Files:**
- Delete: `backend/services/orchestrator_old.py`

- [ ] **Step 1: Remove backup file**

```bash
rm backend/services/orchestrator_old.py
```

- [ ] **Step 2: Verify new orchestrator file structure**

Run: `find backend/orchestrator -type f -name "*.py" | sort`

Expected:
```
backend/orchestrator/__init__.py
backend/orchestrator/engine.py
backend/orchestrator/groups.py
backend/orchestrator/helpers.py
backend/orchestrator/phases/__init__.py
backend/orchestrator/phases/baseline.py
backend/orchestrator/phases/cdc.py
backend/orchestrator/phases/chunking.py
backend/orchestrator/phases/cleanup.py
backend/orchestrator/phases/data_verify.py
backend/orchestrator/phases/group_new.py
backend/orchestrator/phases/preparing.py
backend/orchestrator/queue.py
backend/orchestrator/triggers.py
```

- [ ] **Step 3: Count LOC per file**

Run: `wc -l backend/orchestrator/*.py backend/orchestrator/phases/*.py`

Expected: Each file should be well under 300 LOC.

- [ ] **Step 4: Run all tests**

Run: `cd /mnt/c/work/database_migration/new/front && docker run --rm -v "$(pwd)/backend:/app" -w /app python:3.11-slim bash -c "pip install -q pydantic structlog pytest 2>/dev/null && python -m pytest tests/ -v"`

- [ ] **Step 5: Commit**

```bash
git add -A backend/services/orchestrator_old.py
git commit -m "refactor: remove old monolithic orchestrator.py (replaced by orchestrator/ package)"
```
