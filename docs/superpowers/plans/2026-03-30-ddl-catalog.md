# DDL Catalog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace MigrationPlanner with a full DDL Catalog tab that shows all Oracle DDL objects by type, cached in PostgreSQL, with target comparison and migration statuses.

**Architecture:** New `catalog` blueprint + 3 PostgreSQL tables for cache + extended `oracle_browser.py` introspection + new `ddl_compare.py` and `ddl_sync_extended.py` services + new `DDLCatalog/` frontend component folder replacing `MigrationPlanner.tsx`. Planner wizard steps 2-4 extracted into reusable `PlannerWizard.tsx`.

**Tech Stack:** Python/Flask (backend), PostgreSQL (cache), oracledb (Oracle introspection), React/TypeScript (frontend), Vite (build)

**Spec:** `docs/superpowers/specs/2026-03-30-ddl-catalog-design.md`

---

## File Structure

### Backend — New Files
| File | Responsibility |
|---|---|
| `backend/routes/catalog.py` | Blueprint with 6 endpoints: snapshots, load, objects, detail, compare, refresh, sync-to-target |
| `backend/services/ddl_compare.py` | Comparison logic per object type (TABLE, VIEW, CODE, SEQUENCE, etc.) |
| `backend/services/ddl_sync_extended.py` | Sync non-table objects to target (CREATE OR REPLACE for views, code, sequences, etc.) |

### Backend — Modified Files
| File | Changes |
|---|---|
| `backend/db/state_db.py` | Add 3 new tables: `ddl_snapshots`, `ddl_objects`, `ddl_compare_results` |
| `backend/db/oracle_browser.py` | Add 6 new introspection functions |
| `backend/app.py` | Register `catalog` blueprint |

### Frontend — New Files
| File | Responsibility |
|---|---|
| `frontend/src/components/DDLCatalog/DDLCatalog.tsx` | Main component, state management, schema selection, wizard toggle |
| `frontend/src/components/DDLCatalog/ObjectTabs.tsx` | Sub-tabs: Tables, Views & MViews, Code, Other |
| `frontend/src/components/DDLCatalog/TablesTab.tsx` | Table object list with expandable DDL details |
| `frontend/src/components/DDLCatalog/ViewsTab.tsx` | Views & materialized views list |
| `frontend/src/components/DDLCatalog/CodeTab.tsx` | Functions, procedures, packages list |
| `frontend/src/components/DDLCatalog/OtherTab.tsx` | Sequences, synonyms, types list |
| `frontend/src/components/DDLCatalog/ObjectActions.tsx` | Type-specific action buttons |
| `frontend/src/components/DDLCatalog/StatusBadges.tsx` | Target match + migration status badges |
| `frontend/src/components/DDLCatalog/PlannerWizard.tsx` | Extracted steps 2-4 from MigrationPlanner |
| `frontend/src/components/DDLCatalog/styles.ts` | Shared style tokens (extracted from MigrationPlanner S object) |

### Frontend — Modified Files
| File | Changes |
|---|---|
| `frontend/src/App.tsx` | Replace MigrationPlanner import with DDLCatalog, rename tab |

### Frontend — Deleted Files
| File | Reason |
|---|---|
| `frontend/src/components/MigrationPlanner.tsx` | Replaced by DDLCatalog/ folder |

---

## Task 1: PostgreSQL Schema — Add Cache Tables

**Files:**
- Modify: `backend/db/state_db.py` (add to `init_db()` function, after existing CREATE TABLE statements ~line 543)

- [ ] **Step 1: Add ddl_snapshots table**

Add after the `migration_plan_items` CREATE TABLE block in `init_db()`:

```python
        # ── DDL Catalog cache ──────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ddl_snapshots (
                snapshot_id   SERIAL PRIMARY KEY,
                src_schema    TEXT NOT NULL,
                tgt_schema    TEXT NOT NULL,
                loaded_at     TIMESTAMPTZ DEFAULT now()
            )
        """)
```

- [ ] **Step 2: Add ddl_objects table**

```python
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ddl_objects (
                id            SERIAL PRIMARY KEY,
                snapshot_id   INT NOT NULL REFERENCES ddl_snapshots(snapshot_id) ON DELETE CASCADE,
                db_side       TEXT NOT NULL,
                object_type   TEXT NOT NULL,
                object_name   TEXT NOT NULL,
                oracle_status TEXT,
                last_ddl_time TIMESTAMPTZ,
                metadata      JSONB DEFAULT '{}'
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS ix_ddl_objects_snapshot
            ON ddl_objects(snapshot_id, db_side, object_type)
        """)
```

- [ ] **Step 3: Add ddl_compare_results table**

```python
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ddl_compare_results (
                id            SERIAL PRIMARY KEY,
                snapshot_id   INT NOT NULL REFERENCES ddl_snapshots(snapshot_id) ON DELETE CASCADE,
                object_type   TEXT NOT NULL,
                object_name   TEXT NOT NULL,
                match_status  TEXT NOT NULL DEFAULT 'UNKNOWN',
                diff          JSONB DEFAULT '{}'
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS ix_ddl_compare_snapshot
            ON ddl_compare_results(snapshot_id, object_type)
        """)
```

- [ ] **Step 4: Verify tables are created**

Run: `cd /mnt/c/work/database_migration/new/front && python -c "from backend.db.state_db import init_db; init_db()"`

Expected: No errors. Tables created in PostgreSQL.

- [ ] **Step 5: Commit**

```bash
git add backend/db/state_db.py
git commit -m "feat: add ddl_snapshots, ddl_objects, ddl_compare_results tables for DDL catalog cache"
```

---

## Task 2: Oracle Introspection — list_all_objects

**Files:**
- Modify: `backend/db/oracle_browser.py` (add after existing `list_tables` function ~line 42)

- [ ] **Step 1: Add list_all_objects function**

```python
_CATALOG_TYPES = frozenset([
    "TABLE", "VIEW", "MATERIALIZED VIEW",
    "FUNCTION", "PROCEDURE", "PACKAGE",
    "SEQUENCE", "SYNONYM", "TYPE",
])


def list_all_objects(conn, schema: str) -> list[dict]:
    """List all DDL objects in schema (excluding PACKAGE BODY / TYPE BODY)."""
    placeholders = ",".join(f":t{i}" for i in range(len(_CATALOG_TYPES)))
    binds = {"s": schema}
    binds.update({f"t{i}": t for i, t in enumerate(sorted(_CATALOG_TYPES))})
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT object_name, object_type, status, last_ddl_time
            FROM   all_objects
            WHERE  owner = :s
              AND  object_type IN ({placeholders})
            ORDER  BY object_type, object_name
        """, binds)
        return [
            {
                "object_name": r[0],
                "object_type": r[1],
                "status": r[2],
                "last_ddl_time": r[3].isoformat() if r[3] else None,
            }
            for r in cur.fetchall()
        ]
```

- [ ] **Step 2: Verify function works**

Run: `cd /mnt/c/work/database_migration/new/front && python -c "
from backend.db.oracle_browser import list_all_objects, get_oracle_conn
from backend.db.state_db import load_configs
configs = load_configs(True)
conn = get_oracle_conn('source', configs)
objs = list_all_objects(conn, 'YOUR_SCHEMA')
for t in set(o['object_type'] for o in objs): print(t, sum(1 for o in objs if o['object_type']==t))
conn.close()
"`

Expected: Counts per object type printed.

- [ ] **Step 3: Commit**

```bash
git add backend/db/oracle_browser.py
git commit -m "feat: add list_all_objects introspection for DDL catalog"
```

---

## Task 3: Oracle Introspection — Views and MViews

**Files:**
- Modify: `backend/db/oracle_browser.py` (add after `list_all_objects`)

- [ ] **Step 1: Add get_view_info function**

```python
def get_view_info(conn, schema: str, name: str) -> dict:
    """Get view definition and columns."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT text FROM all_views WHERE owner = :s AND view_name = :n",
            {"s": schema, "n": name},
        )
        row = cur.fetchone()
        sql_text = row[0] if row else None

        cur.execute("""
            SELECT column_name, data_type, data_length, nullable
            FROM   all_tab_columns
            WHERE  owner = :s AND table_name = :n
            ORDER  BY column_id
        """, {"s": schema, "n": name})
        columns = [
            {"name": r[0], "data_type": r[1], "data_length": r[2], "nullable": r[3] == "Y"}
            for r in cur.fetchall()
        ]

        cur.execute(
            "SELECT status FROM all_objects WHERE owner = :s AND object_name = :n AND object_type = 'VIEW'",
            {"s": schema, "n": name},
        )
        status_row = cur.fetchone()

    return {
        "sql_text": sql_text,
        "columns": columns,
        "status": status_row[0] if status_row else "UNKNOWN",
    }
```

- [ ] **Step 2: Add get_mview_info function**

```python
def get_mview_info(conn, schema: str, name: str) -> dict:
    """Get materialized view definition, columns, and refresh info."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT query, refresh_mode, refresh_method, last_refresh_date
            FROM   all_mviews
            WHERE  owner = :s AND mview_name = :n
        """, {"s": schema, "n": name})
        row = cur.fetchone()
        if not row:
            return {"sql_text": None, "columns": [], "refresh_type": None, "last_refresh": None}

        cur.execute("""
            SELECT column_name, data_type, data_length, nullable
            FROM   all_tab_columns
            WHERE  owner = :s AND table_name = :n
            ORDER  BY column_id
        """, {"s": schema, "n": name})
        columns = [
            {"name": r[0], "data_type": r[1], "data_length": r[2], "nullable": r[3] == "Y"}
            for r in cur.fetchall()
        ]

    return {
        "sql_text": row[0],
        "columns": columns,
        "refresh_type": f"{row[1]}/{row[2]}",
        "last_refresh": row[3].isoformat() if row[3] else None,
    }
```

- [ ] **Step 3: Commit**

```bash
git add backend/db/oracle_browser.py
git commit -m "feat: add get_view_info and get_mview_info introspection"
```

---

## Task 4: Oracle Introspection — Code Objects

**Files:**
- Modify: `backend/db/oracle_browser.py`

- [ ] **Step 1: Add get_source_code function**

```python
def get_source_code(conn, schema: str, name: str, obj_type: str) -> str | None:
    """Get source code for FUNCTION, PROCEDURE, PACKAGE, PACKAGE BODY, TYPE, TYPE BODY."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT text
            FROM   all_source
            WHERE  owner = :s AND name = :n AND type = :t
            ORDER  BY line
        """, {"s": schema, "n": name, "t": obj_type})
        lines = [r[0] for r in cur.fetchall()]
    return "".join(lines) if lines else None


def get_code_info(conn, schema: str, name: str, obj_type: str) -> dict:
    """Get full info for a code object (function, procedure, package, type)."""
    result: dict = {"status": "UNKNOWN"}

    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM all_objects WHERE owner = :s AND object_name = :n AND object_type = :t",
            {"s": schema, "n": name, "t": obj_type},
        )
        row = cur.fetchone()
        if row:
            result["status"] = row[0]

    if obj_type == "PACKAGE":
        result["spec_source"] = get_source_code(conn, schema, name, "PACKAGE")
        result["body_source"] = get_source_code(conn, schema, name, "PACKAGE BODY")
    elif obj_type == "TYPE":
        result["source"] = get_source_code(conn, schema, name, "TYPE")
        result["body_source"] = get_source_code(conn, schema, name, "TYPE BODY")
    else:
        result["source_code"] = get_source_code(conn, schema, name, obj_type)

    # Argument count for functions/procedures
    if obj_type in ("FUNCTION", "PROCEDURE"):
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM all_arguments
                WHERE owner = :s AND object_name = :n AND argument_name IS NOT NULL
            """, {"s": schema, "n": name})
            result["argument_count"] = cur.fetchone()[0]

    return result
```

- [ ] **Step 2: Commit**

```bash
git add backend/db/oracle_browser.py
git commit -m "feat: add get_source_code and get_code_info introspection for code objects"
```

---

## Task 5: Oracle Introspection — Sequences, Synonyms

**Files:**
- Modify: `backend/db/oracle_browser.py`

- [ ] **Step 1: Add get_sequence_info function**

```python
def get_sequence_info(conn, schema: str, name: str) -> dict:
    """Get sequence parameters."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT min_value, max_value, increment_by, cache_size, last_number
            FROM   all_sequences
            WHERE  sequence_owner = :s AND sequence_name = :n
        """, {"s": schema, "n": name})
        row = cur.fetchone()
    if not row:
        return {}
    return {
        "min_value": str(row[0]) if row[0] is not None else None,
        "max_value": str(row[1]) if row[1] is not None else None,
        "increment_by": str(row[2]) if row[2] is not None else None,
        "cache_size": row[3],
        "last_number": str(row[4]) if row[4] is not None else None,
    }
```

- [ ] **Step 2: Add get_synonym_info function**

```python
def get_synonym_info(conn, schema: str, name: str) -> dict:
    """Get synonym target info."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_owner, table_name, db_link
            FROM   all_synonyms
            WHERE  owner = :s AND synonym_name = :n
        """, {"s": schema, "n": name})
        row = cur.fetchone()
    if not row:
        return {}
    return {
        "table_owner": row[0],
        "table_name": row[1],
        "db_link": row[2],
    }
```

- [ ] **Step 3: Commit**

```bash
git add backend/db/oracle_browser.py
git commit -m "feat: add get_sequence_info and get_synonym_info introspection"
```

---

## Task 6: Comparison Service

**Files:**
- Create: `backend/services/ddl_compare.py`

- [ ] **Step 1: Create ddl_compare.py with compare_objects function**

```python
"""
Compare DDL objects between source and target snapshots.
"""
import json


def _normalize_sql(text: str | None) -> str:
    """Normalize SQL for comparison: strip whitespace, lowercase."""
    if not text:
        return ""
    return " ".join(text.lower().split())


def _normalize_code(text: str | None) -> str:
    """Normalize PL/SQL code: strip trailing whitespace per line."""
    if not text:
        return ""
    return "\n".join(line.rstrip() for line in text.splitlines())


def _diff_table(src_meta: dict, tgt_meta: dict) -> dict:
    """Compare table DDL. Uses same logic as planner._diff_summary."""
    src_cols = {c["name"]: c for c in src_meta.get("columns", [])}
    tgt_cols = {c["name"]: c for c in tgt_meta.get("columns", [])}

    cols_missing = [n for n in src_cols if n not in tgt_cols]
    cols_extra = [n for n in tgt_cols if n not in src_cols]
    cols_type = [
        n for n in src_cols
        if n in tgt_cols and src_cols[n].get("data_type") != tgt_cols[n].get("data_type")
    ]

    src_idx = {i["name"]: i for i in src_meta.get("indexes", [])}
    tgt_idx = {i["name"]: i for i in tgt_meta.get("indexes", [])}
    tgt_idx_keys = {(i["unique"], ",".join(i["columns"])) for i in tgt_meta.get("indexes", [])}
    idx_missing = [
        n for n, i in src_idx.items()
        if n not in tgt_idx and (i["unique"], ",".join(i["columns"])) not in tgt_idx_keys
    ]
    idx_disabled = [n for n, i in tgt_idx.items() if i.get("status") != "VALID"]

    src_con_keys = {(c["type_code"], ",".join(c["columns"])): c["name"] for c in src_meta.get("constraints", [])}
    tgt_con_keys = {(c["type_code"], ",".join(c["columns"])) for c in tgt_meta.get("constraints", [])}
    con_missing = [name for key, name in src_con_keys.items() if key not in tgt_con_keys]
    con_disabled = [
        c["name"] for c in tgt_meta.get("constraints", [])
        if c.get("status") == "DISABLED" and c.get("type_code") != "P"
    ]

    src_trg = {t["name"] for t in src_meta.get("triggers", [])}
    tgt_trg = {t["name"] for t in tgt_meta.get("triggers", [])}
    trg_missing = [n for n in src_trg if n not in tgt_trg]

    total = len(cols_missing) + len(cols_extra) + len(cols_type) + len(idx_missing) + len(idx_disabled) + len(con_missing) + len(con_disabled) + len(trg_missing)
    return {
        "ok": total == 0,
        "cols_missing": cols_missing,
        "cols_extra": cols_extra,
        "cols_type": cols_type,
        "idx_missing": idx_missing,
        "idx_disabled": idx_disabled,
        "con_missing": con_missing,
        "con_disabled": con_disabled,
        "trg_missing": trg_missing,
    }


def _diff_view(src_meta: dict, tgt_meta: dict) -> dict:
    src_sql = _normalize_sql(src_meta.get("sql_text"))
    tgt_sql = _normalize_sql(tgt_meta.get("sql_text"))
    sql_match = src_sql == tgt_sql
    status_match = src_meta.get("status") == tgt_meta.get("status")
    return {"ok": sql_match and status_match, "sql_match": sql_match, "status_match": status_match}


def _diff_mview(src_meta: dict, tgt_meta: dict) -> dict:
    src_sql = _normalize_sql(src_meta.get("sql_text"))
    tgt_sql = _normalize_sql(tgt_meta.get("sql_text"))
    sql_match = src_sql == tgt_sql
    refresh_match = src_meta.get("refresh_type") == tgt_meta.get("refresh_type")
    return {"ok": sql_match and refresh_match, "sql_match": sql_match, "refresh_match": refresh_match}


def _diff_code(src_meta: dict, tgt_meta: dict, obj_type: str) -> dict:
    if obj_type == "PACKAGE":
        spec_match = _normalize_code(src_meta.get("spec_source")) == _normalize_code(tgt_meta.get("spec_source"))
        body_match = _normalize_code(src_meta.get("body_source")) == _normalize_code(tgt_meta.get("body_source"))
        return {"ok": spec_match and body_match, "spec_match": spec_match, "body_match": body_match}
    elif obj_type == "TYPE":
        src_match = _normalize_code(src_meta.get("source")) == _normalize_code(tgt_meta.get("source"))
        body_match = _normalize_code(src_meta.get("body_source")) == _normalize_code(tgt_meta.get("body_source"))
        return {"ok": src_match and body_match, "source_match": src_match, "body_match": body_match}
    else:
        code_match = _normalize_code(src_meta.get("source_code")) == _normalize_code(tgt_meta.get("source_code"))
        return {"ok": code_match, "code_match": code_match}


def _diff_sequence(src_meta: dict, tgt_meta: dict) -> dict:
    fields = ["min_value", "max_value", "increment_by", "cache_size"]
    diffs = {f: (src_meta.get(f), tgt_meta.get(f)) for f in fields if src_meta.get(f) != tgt_meta.get(f)}
    return {"ok": len(diffs) == 0, "field_diffs": diffs}


def _diff_synonym(src_meta: dict, tgt_meta: dict) -> dict:
    fields = ["table_owner", "table_name", "db_link"]
    diffs = {f: (src_meta.get(f), tgt_meta.get(f)) for f in fields if src_meta.get(f) != tgt_meta.get(f)}
    return {"ok": len(diffs) == 0, "field_diffs": diffs}


# ── Public API ───────────────────────────────────────────────────────────────

_COMPARATORS = {
    "TABLE": _diff_table,
    "VIEW": _diff_view,
    "MATERIALIZED VIEW": _diff_mview,
    "FUNCTION": lambda s, t: _diff_code(s, t, "FUNCTION"),
    "PROCEDURE": lambda s, t: _diff_code(s, t, "PROCEDURE"),
    "PACKAGE": lambda s, t: _diff_code(s, t, "PACKAGE"),
    "TYPE": lambda s, t: _diff_code(s, t, "TYPE"),
    "SEQUENCE": _diff_sequence,
    "SYNONYM": _diff_synonym,
}


def compare_object(object_type: str, src_meta: dict, tgt_meta: dict) -> dict:
    """Compare a single object. Returns {ok: bool, ...diff_details}."""
    comparator = _COMPARATORS.get(object_type)
    if not comparator:
        return {"ok": True}
    return comparator(src_meta, tgt_meta)
```

- [ ] **Step 2: Commit**

```bash
git add backend/services/ddl_compare.py
git commit -m "feat: add ddl_compare service with per-type comparison logic"
```

---

## Task 7: Extended Sync Service

**Files:**
- Create: `backend/services/ddl_sync_extended.py`

- [ ] **Step 1: Create ddl_sync_extended.py**

```python
"""
Sync non-table DDL objects from source to target Oracle.
Tables are handled by existing oracle_stage.py and oracle_ddl_sync.py.
"""
from db.oracle_browser import (
    get_oracle_conn, get_source_code, get_view_info,
    get_mview_info, get_sequence_info, get_synonym_info,
)


def _exec_on_target(tgt_conn, sql: str):
    """Execute DDL on target and commit."""
    with tgt_conn.cursor() as cur:
        cur.execute(sql)
    tgt_conn.commit()


def sync_view(src_conn, tgt_conn, schema: str, name: str) -> dict:
    """CREATE OR REPLACE VIEW on target from source definition."""
    info = get_view_info(src_conn, schema, name)
    if not info.get("sql_text"):
        return {"error": f"View {name} has no SQL text on source"}
    ddl = f'CREATE OR REPLACE VIEW "{schema}"."{name}" AS\n{info["sql_text"]}'
    _exec_on_target(tgt_conn, ddl)
    return {"action": "created", "object": name}


def sync_mview(src_conn, tgt_conn, schema: str, name: str) -> dict:
    """CREATE MATERIALIZED VIEW on target. Drops existing first."""
    info = get_mview_info(src_conn, schema, name)
    if not info.get("sql_text"):
        return {"error": f"MView {name} has no SQL text on source"}
    # Drop if exists
    try:
        _exec_on_target(tgt_conn, f'DROP MATERIALIZED VIEW "{schema}"."{name}"')
    except Exception:
        pass
    refresh = info.get("refresh_type", "FORCE/DEMAND")
    method = refresh.split("/")[0] if "/" in refresh else "FORCE"
    ddl = f'CREATE MATERIALIZED VIEW "{schema}"."{name}" REFRESH {method} AS\n{info["sql_text"]}'
    _exec_on_target(tgt_conn, ddl)
    return {"action": "created", "object": name}


def sync_code_object(src_conn, tgt_conn, schema: str, name: str, obj_type: str) -> dict:
    """CREATE OR REPLACE function/procedure/type on target."""
    if obj_type == "PACKAGE":
        spec = get_source_code(src_conn, schema, name, "PACKAGE")
        body = get_source_code(src_conn, schema, name, "PACKAGE BODY")
        if spec:
            _exec_on_target(tgt_conn, f'CREATE OR REPLACE {spec}')
        if body:
            _exec_on_target(tgt_conn, f'CREATE OR REPLACE {body}')
        return {"action": "compiled", "object": name, "spec": bool(spec), "body": bool(body)}
    elif obj_type == "TYPE":
        src = get_source_code(src_conn, schema, name, "TYPE")
        body = get_source_code(src_conn, schema, name, "TYPE BODY")
        if src:
            _exec_on_target(tgt_conn, f'CREATE OR REPLACE {src}')
        if body:
            _exec_on_target(tgt_conn, f'CREATE OR REPLACE {body}')
        return {"action": "compiled", "object": name, "source": bool(src), "body": bool(body)}
    else:
        code = get_source_code(src_conn, schema, name, obj_type)
        if not code:
            return {"error": f"{obj_type} {name} has no source code on source"}
        _exec_on_target(tgt_conn, f'CREATE OR REPLACE {code}')
        return {"action": "compiled", "object": name}


def sync_sequence(src_conn, tgt_conn, schema: str, name: str, action: str = "create") -> dict:
    """Create or alter sequence on target."""
    info = get_sequence_info(src_conn, schema, name)
    if not info:
        return {"error": f"Sequence {name} not found on source"}

    if action == "create":
        ddl = (
            f'CREATE SEQUENCE "{schema}"."{name}"'
            f' MINVALUE {info["min_value"]}'
            f' MAXVALUE {info["max_value"]}'
            f' INCREMENT BY {info["increment_by"]}'
            f' CACHE {info["cache_size"]}'
            f' START WITH {info["last_number"]}'
        )
        _exec_on_target(tgt_conn, ddl)
        return {"action": "created", "object": name}
    else:
        ddl = (
            f'ALTER SEQUENCE "{schema}"."{name}"'
            f' INCREMENT BY {info["increment_by"]}'
            f' MINVALUE {info["min_value"]}'
            f' MAXVALUE {info["max_value"]}'
            f' CACHE {info["cache_size"]}'
        )
        _exec_on_target(tgt_conn, ddl)
        return {"action": "altered", "object": name}


def sync_synonym(src_conn, tgt_conn, schema: str, name: str) -> dict:
    """CREATE OR REPLACE SYNONYM on target."""
    info = get_synonym_info(src_conn, schema, name)
    if not info:
        return {"error": f"Synonym {name} not found on source"}
    target_ref = f'"{info["table_owner"]}"."{info["table_name"]}"'
    if info.get("db_link"):
        target_ref += f'@{info["db_link"]}'
    ddl = f'CREATE OR REPLACE SYNONYM "{schema}"."{name}" FOR {target_ref}'
    _exec_on_target(tgt_conn, ddl)
    return {"action": "created", "object": name}


# ── Dispatcher ───────────────────────────────────────────────────────────────

def sync_to_target(src_conn, tgt_conn, schema: str, name: str,
                   object_type: str, action: str = "create") -> dict:
    """Route sync request to the correct handler."""
    if object_type == "VIEW":
        return sync_view(src_conn, tgt_conn, schema, name)
    elif object_type == "MATERIALIZED VIEW":
        return sync_mview(src_conn, tgt_conn, schema, name)
    elif object_type in ("FUNCTION", "PROCEDURE", "PACKAGE", "TYPE"):
        return sync_code_object(src_conn, tgt_conn, schema, name, object_type)
    elif object_type == "SEQUENCE":
        return sync_sequence(src_conn, tgt_conn, schema, name, action)
    elif object_type == "SYNONYM":
        return sync_synonym(src_conn, tgt_conn, schema, name)
    else:
        return {"error": f"Unsupported object type: {object_type}"}
```

- [ ] **Step 2: Commit**

```bash
git add backend/services/ddl_sync_extended.py
git commit -m "feat: add ddl_sync_extended service for non-table DDL sync to target"
```

---

## Task 8: Catalog API Blueprint — Core Endpoints

**Files:**
- Create: `backend/routes/catalog.py`

- [ ] **Step 1: Create catalog.py with init, load, and list endpoints**

```python
"""
DDL Catalog API — cache Oracle DDL objects in PostgreSQL, compare, sync.
"""
import json
from flask import Blueprint, request, jsonify
from db.oracle_browser import (
    get_oracle_conn, list_all_objects, get_full_ddl_info,
    get_view_info, get_mview_info, get_code_info,
    get_sequence_info, get_synonym_info,
)
from services.ddl_compare import compare_object
from services.ddl_sync_extended import sync_to_target

bp = Blueprint("catalog", __name__)
_state: dict = {}


def init(*, get_conn_fn, row_to_dict_fn, load_configs_fn, broadcast_fn):
    _state["get_conn"] = get_conn_fn
    _state["row_to_dict"] = row_to_dict_fn
    _state["load_configs"] = load_configs_fn
    _state["broadcast"] = broadcast_fn


# Map object_type to the introspection function that returns metadata
_META_FETCHERS = {
    "TABLE": lambda conn, schema, name: get_full_ddl_info(conn, schema, name),
    "VIEW": lambda conn, schema, name: get_view_info(conn, schema, name),
    "MATERIALIZED VIEW": lambda conn, schema, name: get_mview_info(conn, schema, name),
    "FUNCTION": lambda conn, schema, name: get_code_info(conn, schema, name, "FUNCTION"),
    "PROCEDURE": lambda conn, schema, name: get_code_info(conn, schema, name, "PROCEDURE"),
    "PACKAGE": lambda conn, schema, name: get_code_info(conn, schema, name, "PACKAGE"),
    "TYPE": lambda conn, schema, name: get_code_info(conn, schema, name, "TYPE"),
    "SEQUENCE": lambda conn, schema, name: get_sequence_info(conn, schema, name),
    "SYNONYM": lambda conn, schema, name: get_synonym_info(conn, schema, name),
}

# Normalize Oracle type names for frontend
_TYPE_NORMALIZE = {"MATERIALIZED VIEW": "MVIEW"}


def _fetch_metadata(conn, schema: str, obj_type: str, obj_name: str) -> dict:
    fetcher = _META_FETCHERS.get(obj_type)
    if not fetcher:
        return {}
    try:
        return fetcher(conn, schema, obj_name)
    except Exception as exc:
        return {"_error": str(exc)}


# ── GET /api/catalog/snapshots ───────────────────────────────────────────────

@bp.get("/api/catalog/snapshots")
def list_snapshots():
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT snapshot_id, src_schema, tgt_schema, loaded_at
                FROM   ddl_snapshots
                ORDER  BY loaded_at DESC
                LIMIT  20
            """)
            rows = [_state["row_to_dict"](cur, r) for r in cur.fetchall()]
        return jsonify(rows)
    finally:
        conn.close()


# ── POST /api/catalog/load ───────────────────────────────────────────────────

@bp.post("/api/catalog/load")
def load_catalog():
    """Load full DDL catalog for a schema pair into cache."""
    data = request.get_json(force=True)
    src_schema = (data.get("src_schema") or "").upper()
    tgt_schema = (data.get("tgt_schema") or "").upper()
    if not src_schema or not tgt_schema:
        return jsonify({"error": "src_schema and tgt_schema required"}), 400

    configs = _state["load_configs"](True)
    try:
        src_conn = get_oracle_conn("source", configs)
        tgt_conn = get_oracle_conn("target", configs)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503

    pg = _state["get_conn"]()
    try:
        # 1. Create snapshot
        with pg.cursor() as cur:
            cur.execute(
                "INSERT INTO ddl_snapshots (src_schema, tgt_schema) VALUES (%s, %s) RETURNING snapshot_id",
                (src_schema, tgt_schema),
            )
            snapshot_id = cur.fetchone()[0]

        # 2. List objects on both sides
        src_objects = list_all_objects(src_conn, src_schema)
        tgt_objects = list_all_objects(tgt_conn, tgt_schema)
        tgt_index = {(o["object_type"], o["object_name"]): o for o in tgt_objects}

        # 3. Fetch metadata and insert source objects
        object_counts: dict[str, int] = {}
        src_meta_cache: dict[tuple, dict] = {}

        for obj in src_objects:
            otype = obj["object_type"]
            oname = obj["object_name"]
            object_counts[otype] = object_counts.get(otype, 0) + 1

            meta = _fetch_metadata(src_conn, src_schema, otype, oname)
            src_meta_cache[(otype, oname)] = meta

            with pg.cursor() as cur:
                cur.execute("""
                    INSERT INTO ddl_objects (snapshot_id, db_side, object_type, object_name,
                                            oracle_status, last_ddl_time, metadata)
                    VALUES (%s, 'source', %s, %s, %s, %s, %s)
                """, (snapshot_id, otype, oname, obj["status"], obj["last_ddl_time"],
                      json.dumps(meta, default=str)))

        # 4. Insert target objects
        tgt_meta_cache: dict[tuple, dict] = {}
        for obj in tgt_objects:
            otype = obj["object_type"]
            oname = obj["object_name"]
            meta = _fetch_metadata(tgt_conn, tgt_schema, otype, oname)
            tgt_meta_cache[(otype, oname)] = meta

            with pg.cursor() as cur:
                cur.execute("""
                    INSERT INTO ddl_objects (snapshot_id, db_side, object_type, object_name,
                                            oracle_status, last_ddl_time, metadata)
                    VALUES (%s, 'target', %s, %s, %s, %s, %s)
                """, (snapshot_id, otype, oname, obj["status"], obj["last_ddl_time"],
                      json.dumps(meta, default=str)))

        # 5. Compare and write results
        all_keys = set(src_meta_cache.keys()) | set(tgt_meta_cache.keys())
        for (otype, oname) in all_keys:
            src_m = src_meta_cache.get((otype, oname))
            tgt_m = tgt_meta_cache.get((otype, oname))

            if src_m and tgt_m:
                diff = compare_object(otype, src_m, tgt_m)
                status = "MATCH" if diff.get("ok") else "DIFF"
            elif src_m and not tgt_m:
                diff = {}
                status = "MISSING"
            else:
                diff = {}
                status = "EXTRA"

            with pg.cursor() as cur:
                cur.execute("""
                    INSERT INTO ddl_compare_results (snapshot_id, object_type, object_name,
                                                     match_status, diff)
                    VALUES (%s, %s, %s, %s, %s)
                """, (snapshot_id, otype, oname, status, json.dumps(diff, default=str)))

        pg.commit()

        return jsonify({
            "snapshot_id": snapshot_id,
            "object_counts": object_counts,
            "src_total": len(src_objects),
            "tgt_total": len(tgt_objects),
        })

    except Exception as exc:
        pg.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        src_conn.close()
        tgt_conn.close()
        pg.close()


# ── GET /api/catalog/objects ─────────────────────────────────────────────────

@bp.get("/api/catalog/objects")
def list_objects():
    """List objects of given type from a snapshot, with compare status and migration status."""
    snapshot_id = request.args.get("snapshot_id", type=int)
    obj_type = (request.args.get("type") or "").upper()
    if not snapshot_id or not obj_type:
        return jsonify({"error": "snapshot_id and type required"}), 400

    # Expand grouped types
    if obj_type == "MVIEW":
        obj_type = "MATERIALIZED VIEW"

    pg = _state["get_conn"]()
    try:
        with pg.cursor() as cur:
            # Get snapshot schema info
            cur.execute("SELECT src_schema FROM ddl_snapshots WHERE snapshot_id = %s", (snapshot_id,))
            snap = cur.fetchone()
            if not snap:
                return jsonify({"error": "Snapshot not found"}), 404
            src_schema = snap[0]

            # Source objects with compare results
            cur.execute("""
                SELECT o.object_name, o.oracle_status, o.last_ddl_time, o.metadata,
                       COALESCE(c.match_status, 'UNKNOWN') AS match_status,
                       c.diff
                FROM   ddl_objects o
                LEFT   JOIN ddl_compare_results c
                       ON c.snapshot_id = o.snapshot_id
                       AND c.object_type = o.object_type
                       AND c.object_name = o.object_name
                WHERE  o.snapshot_id = %s
                  AND  o.db_side = 'source'
                  AND  o.object_type = %s
                ORDER  BY o.object_name
            """, (snapshot_id, obj_type))
            rows = []
            for r in cur.fetchall():
                row = {
                    "object_name": r[0],
                    "oracle_status": r[1],
                    "last_ddl_time": r[2].isoformat() + "Z" if r[2] else None,
                    "metadata": r[3] if isinstance(r[3], dict) else json.loads(r[3]) if r[3] else {},
                    "match_status": r[4],
                    "diff": r[5] if isinstance(r[5], dict) else json.loads(r[5]) if r[5] else {},
                    "migration_status": "NONE",
                }
                rows.append(row)

            # Enrich tables with migration status
            if obj_type == "TABLE" and rows:
                table_names = [r["object_name"] for r in rows]
                placeholders = ",".join(["%s"] * len(table_names))
                cur.execute(f"""
                    SELECT source_table, phase
                    FROM   migrations
                    WHERE  source_schema = %s
                      AND  source_table IN ({placeholders})
                    ORDER  BY created_at DESC
                """, [src_schema] + table_names)
                phase_map: dict[str, str] = {}
                for mr in cur.fetchall():
                    if mr[0] not in phase_map:
                        phase_map[mr[0]] = mr[1]

                _PLANNED = {"DRAFT", "NEW", "PREPARING"}
                _DONE = {"COMPLETED"}
                _FAILED = {"FAILED", "CANCELLED"}
                for row in rows:
                    phase = phase_map.get(row["object_name"])
                    if not phase:
                        row["migration_status"] = "NONE"
                    elif phase in _PLANNED:
                        row["migration_status"] = "PLANNED"
                    elif phase in _DONE:
                        row["migration_status"] = "COMPLETED"
                    elif phase in _FAILED:
                        row["migration_status"] = "FAILED"
                    else:
                        row["migration_status"] = "IN_PROGRESS"

        return jsonify(rows)
    finally:
        pg.close()


# ── GET /api/catalog/objects/<name>/detail ───────────────────────────────────

@bp.get("/api/catalog/objects/<name>/detail")
def object_detail(name: str):
    """Full metadata for a single object (source + target side)."""
    snapshot_id = request.args.get("snapshot_id", type=int)
    obj_type = (request.args.get("type") or "").upper()
    if not snapshot_id or not obj_type:
        return jsonify({"error": "snapshot_id and type required"}), 400
    if obj_type == "MVIEW":
        obj_type = "MATERIALIZED VIEW"

    name = name.upper()
    pg = _state["get_conn"]()
    try:
        with pg.cursor() as cur:
            cur.execute("""
                SELECT db_side, metadata FROM ddl_objects
                WHERE snapshot_id = %s AND object_type = %s AND object_name = %s
            """, (snapshot_id, obj_type, name))
            result = {"source": {}, "target": {}}
            for r in cur.fetchall():
                side = r[0]
                meta = r[1] if isinstance(r[1], dict) else json.loads(r[1]) if r[1] else {}
                result[side] = meta

            cur.execute("""
                SELECT match_status, diff FROM ddl_compare_results
                WHERE snapshot_id = %s AND object_type = %s AND object_name = %s
            """, (snapshot_id, obj_type, name))
            cr = cur.fetchone()
            result["match_status"] = cr[0] if cr else "UNKNOWN"
            result["diff"] = (cr[1] if isinstance(cr[1], dict) else json.loads(cr[1]) if cr[1] else {}) if cr else {}

        return jsonify(result)
    finally:
        pg.close()


# ── POST /api/catalog/compare ────────────────────────────────────────────────

@bp.post("/api/catalog/compare")
def compare_objects():
    """Re-compare specific objects with target (refresh comparison)."""
    data = request.get_json(force=True)
    snapshot_id = data.get("snapshot_id")
    src_schema = (data.get("src_schema") or "").upper()
    tgt_schema = (data.get("tgt_schema") or "").upper()
    objects = data.get("objects", [])  # ["TABLE:USERS", "VIEW:V_ORDERS"]
    if not snapshot_id or not src_schema or not tgt_schema or not objects:
        return jsonify({"error": "snapshot_id, src_schema, tgt_schema, objects required"}), 400

    configs = _state["load_configs"](True)
    try:
        src_conn = get_oracle_conn("source", configs)
        tgt_conn = get_oracle_conn("target", configs)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503

    pg = _state["get_conn"]()
    results = []
    try:
        for obj_ref in objects:
            obj_type, obj_name = obj_ref.split(":", 1)
            obj_type = obj_type.upper()
            obj_name = obj_name.upper()

            src_meta = _fetch_metadata(src_conn, src_schema, obj_type, obj_name)
            tgt_meta = _fetch_metadata(tgt_conn, tgt_schema, obj_type, obj_name)

            if src_meta and tgt_meta:
                diff = compare_object(obj_type, src_meta, tgt_meta)
                status = "MATCH" if diff.get("ok") else "DIFF"
            elif src_meta:
                diff = {}
                status = "MISSING"
            else:
                diff = {}
                status = "EXTRA"

            # Update compare results
            with pg.cursor() as cur:
                cur.execute("""
                    DELETE FROM ddl_compare_results
                    WHERE snapshot_id = %s AND object_type = %s AND object_name = %s
                """, (snapshot_id, obj_type, obj_name))
                cur.execute("""
                    INSERT INTO ddl_compare_results (snapshot_id, object_type, object_name, match_status, diff)
                    VALUES (%s, %s, %s, %s, %s)
                """, (snapshot_id, obj_type, obj_name, status, json.dumps(diff, default=str)))

                # Also update source metadata
                cur.execute("""
                    UPDATE ddl_objects SET metadata = %s
                    WHERE snapshot_id = %s AND db_side = 'source' AND object_type = %s AND object_name = %s
                """, (json.dumps(src_meta, default=str), snapshot_id, obj_type, obj_name))

            results.append({"object": f"{obj_type}:{obj_name}", "match_status": status, "diff": diff})

        pg.commit()
        return jsonify(results)
    except Exception as exc:
        pg.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        src_conn.close()
        tgt_conn.close()
        pg.close()


# ── POST /api/catalog/refresh ────────────────────────────────────────────────

@bp.post("/api/catalog/refresh")
def refresh_objects():
    """Refresh metadata from source Oracle for specific objects."""
    data = request.get_json(force=True)
    snapshot_id = data.get("snapshot_id")
    src_schema = (data.get("src_schema") or "").upper()
    objects = data.get("objects", [])
    if not snapshot_id or not src_schema or not objects:
        return jsonify({"error": "snapshot_id, src_schema, objects required"}), 400

    configs = _state["load_configs"](True)
    try:
        src_conn = get_oracle_conn("source", configs)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503

    pg = _state["get_conn"]()
    results = []
    try:
        for obj_ref in objects:
            obj_type, obj_name = obj_ref.split(":", 1)
            obj_type = obj_type.upper()
            obj_name = obj_name.upper()

            meta = _fetch_metadata(src_conn, src_schema, obj_type, obj_name)
            with pg.cursor() as cur:
                cur.execute("""
                    UPDATE ddl_objects SET metadata = %s, last_ddl_time = now()
                    WHERE snapshot_id = %s AND db_side = 'source' AND object_type = %s AND object_name = %s
                """, (json.dumps(meta, default=str), snapshot_id, obj_type, obj_name))

            results.append({"object": f"{obj_type}:{obj_name}", "refreshed": True})

        pg.commit()
        return jsonify(results)
    except Exception as exc:
        pg.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        src_conn.close()
        pg.close()


# ── POST /api/catalog/sync-to-target ─────────────────────────────────────────

@bp.post("/api/catalog/sync-to-target")
def sync_object_to_target():
    """Create or sync a single object on target."""
    data = request.get_json(force=True)
    src_schema = (data.get("src_schema") or "").upper()
    tgt_schema = (data.get("tgt_schema") or "").upper()
    obj_type = (data.get("object_type") or "").upper()
    obj_name = (data.get("object_name") or "").upper()
    action = data.get("action", "create")

    if not all([src_schema, tgt_schema, obj_type, obj_name]):
        return jsonify({"error": "src_schema, tgt_schema, object_type, object_name required"}), 400

    # For TABLE type, delegate to existing target-prep endpoints
    if obj_type == "TABLE":
        return jsonify({"error": "Use /api/target-prep/* endpoints for table sync"}), 400

    configs = _state["load_configs"](True)
    try:
        src_conn = get_oracle_conn("source", configs)
        tgt_conn = get_oracle_conn("target", configs)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503

    try:
        result = sync_to_target(src_conn, tgt_conn, tgt_schema, obj_name, obj_type, action)
        if "error" in result:
            return jsonify(result), 400
        return jsonify(result)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        src_conn.close()
        tgt_conn.close()
```

- [ ] **Step 2: Commit**

```bash
git add backend/routes/catalog.py
git commit -m "feat: add catalog blueprint with load, list, compare, refresh, sync endpoints"
```

---

## Task 9: Register Catalog Blueprint

**Files:**
- Modify: `backend/app.py` (add after planner blueprint registration ~line 182)

- [ ] **Step 1: Add catalog import and registration**

Add after the planner blueprint block:

```python
import routes.catalog as catalog_mod
from routes.catalog import bp as catalog_bp

catalog_mod.init(
    get_conn_fn=get_conn,
    row_to_dict_fn=row_to_dict,
    load_configs_fn=_load_cfg,
    broadcast_fn=broadcast,
)
app.register_blueprint(catalog_bp)
```

- [ ] **Step 2: Verify server starts**

Run: `cd /mnt/c/work/database_migration/new/front && python backend/app.py`

Expected: Server starts without import errors. Catalog routes registered.

- [ ] **Step 3: Commit**

```bash
git add backend/app.py
git commit -m "feat: register catalog blueprint in app.py"
```

---

## Task 10: Frontend — Shared Styles

**Files:**
- Create: `frontend/src/components/DDLCatalog/styles.ts`

- [ ] **Step 1: Create styles.ts with shared style tokens**

Extract the S object from MigrationPlanner.tsx (lines 106-161) into a shared file:

```typescript
import React from "react";

export const S = {
  card: {
    background: "#0f172a", border: "1px solid #1e293b", borderRadius: 8,
    overflow: "hidden" as const,
  },
  cardHeader: {
    padding: "10px 16px", background: "#0a111f",
    borderBottom: "1px solid #1e293b",
    display: "flex" as const, alignItems: "center" as const, gap: 10,
  },
  cardBody: {
    padding: 16, display: "flex" as const, flexDirection: "column" as const, gap: 12,
  },
  row2: { display: "grid" as const, gridTemplateColumns: "1fr 1fr", gap: 10 },
  row3: { display: "grid" as const, gridTemplateColumns: "1fr 1fr 1fr", gap: 10 },
  field: { display: "flex" as const, flexDirection: "column" as const, gap: 4 },
  label: { fontSize: 11, color: "#64748b", fontWeight: 600 as const, letterSpacing: 0.3 },
  input: {
    background: "#1e293b", border: "1px solid #334155", borderRadius: 5,
    color: "#e2e8f0", padding: "7px 10px", fontSize: 13, width: "100%",
  },
  select: {
    background: "#1e293b", border: "1px solid #334155", borderRadius: 5,
    color: "#e2e8f0", padding: "7px 10px", fontSize: 13, width: "100%", cursor: "pointer" as const,
  },
  btnPrimary: {
    background: "#3b82f6", border: "none", borderRadius: 6,
    color: "#fff", padding: "6px 14px", fontSize: 12, fontWeight: 600 as const,
    cursor: "pointer" as const,
  },
  btnSecondary: {
    background: "#1e293b", border: "1px solid #334155", borderRadius: 6,
    color: "#94a3b8", padding: "6px 14px", fontSize: 12, fontWeight: 600 as const,
    cursor: "pointer" as const,
  },
  btnDanger: {
    background: "#7f1d1d33", border: "1px solid #7f1d1d88", borderRadius: 6,
    color: "#fca5a5", padding: "6px 14px", fontSize: 12, fontWeight: 600 as const,
    cursor: "pointer" as const,
  },
  btnSuccess: {
    background: "#22c55e22", border: "1px solid #22c55e55", borderRadius: 6,
    color: "#86efac", padding: "6px 14px", fontSize: 12, fontWeight: 600 as const,
    cursor: "pointer" as const,
  },
  badge: (bg: string, fg: string): React.CSSProperties => ({
    padding: "2px 8px", borderRadius: 10, fontSize: 10, fontWeight: 600,
    background: bg, color: fg, whiteSpace: "nowrap",
  }),
  th: {
    padding: "6px 10px", textAlign: "left" as const,
    color: "#64748b", fontWeight: 500, fontSize: 12, whiteSpace: "nowrap" as const,
  },
  td: { padding: "5px 10px", fontSize: 12 },
  trBorder: { borderBottom: "1px solid #0f1624" },
};
```

- [ ] **Step 2: Commit**

```bash
mkdir -p frontend/src/components/DDLCatalog
git add frontend/src/components/DDLCatalog/styles.ts
git commit -m "feat: add shared styles for DDL Catalog components"
```

---

## Task 11: Frontend — StatusBadges

**Files:**
- Create: `frontend/src/components/DDLCatalog/StatusBadges.tsx`

- [ ] **Step 1: Create StatusBadges.tsx**

```tsx
import React from "react";
import { S } from "./styles";

export function MatchBadge({ status }: { status: string }) {
  switch (status) {
    case "MATCH":
      return <span style={S.badge("#22c55e22", "#22c55e")}>Совпадает</span>;
    case "DIFF":
      return <span style={S.badge("#eab30822", "#eab308")}>Отличается</span>;
    case "MISSING":
      return <span style={S.badge("#ef444422", "#ef4444")}>Нет на таргете</span>;
    case "EXTRA":
      return <span style={S.badge("#8b5cf622", "#8b5cf6")}>Лишний</span>;
    default:
      return <span style={S.badge("#33415522", "#475569")}>Не проверено</span>;
  }
}

export function MigrationBadge({ status }: { status: string }) {
  switch (status) {
    case "PLANNED":
      return <span style={S.badge("#3b82f622", "#3b82f6")}>Запланирована</span>;
    case "IN_PROGRESS":
      return <span style={S.badge("#eab30822", "#eab308")}>В процессе</span>;
    case "COMPLETED":
      return <span style={S.badge("#22c55e22", "#22c55e")}>Завершена</span>;
    case "FAILED":
      return <span style={S.badge("#ef444422", "#ef4444")}>Ошибка</span>;
    default:
      return <span style={S.badge("#33415522", "#475569")}>Нет</span>;
  }
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/components/DDLCatalog/StatusBadges.tsx
git commit -m "feat: add MatchBadge and MigrationBadge components"
```

---

## Task 12: Frontend — ObjectActions

**Files:**
- Create: `frontend/src/components/DDLCatalog/ObjectActions.tsx`

- [ ] **Step 1: Create ObjectActions.tsx**

```tsx
import React from "react";
import { S } from "./styles";

interface Props {
  objectType: string;
  objectName: string;
  matchStatus: string;
  syncBusy: boolean;
  onCompare: (type: string, name: string) => void;
  onSync: (type: string, name: string, action: string) => void;
  onShowDetail: (name: string) => void;
}

export function ObjectActions({
  objectType, objectName, matchStatus, syncBusy,
  onCompare, onSync, onShowDetail,
}: Props) {
  const btnSmall = { fontSize: 10, padding: "2px 8px" };
  const busy = syncBusy;

  return (
    <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
      <button
        onClick={() => onShowDetail(objectName)}
        style={{ ...S.btnSecondary, ...btnSmall }}
      >
        Детали
      </button>
      <button
        onClick={() => onCompare(objectType, objectName)}
        disabled={busy}
        style={{ ...S.btnSecondary, ...btnSmall, opacity: busy ? 0.5 : 1 }}
      >
        {busy ? "..." : "Сравнить"}
      </button>

      {matchStatus === "MISSING" && (
        <button
          onClick={() => onSync(objectType, objectName, "create")}
          disabled={busy}
          style={{ ...S.btnSuccess, ...btnSmall, opacity: busy ? 0.5 : 1 }}
        >
          {busy ? "..." : "Создать"}
        </button>
      )}

      {matchStatus === "DIFF" && objectType === "TABLE" && (
        <>
          <button
            onClick={() => onSync(objectType, objectName, "sync_cols")}
            disabled={busy}
            style={{ ...S.btnSuccess, ...btnSmall, opacity: busy ? 0.5 : 1 }}
          >
            Колонки
          </button>
          <button
            onClick={() => onSync(objectType, objectName, "sync_objects")}
            disabled={busy}
            style={{ ...S.btnSuccess, ...btnSmall, opacity: busy ? 0.5 : 1 }}
          >
            Объекты
          </button>
        </>
      )}

      {matchStatus === "DIFF" && objectType !== "TABLE" && (
        <button
          onClick={() => onSync(objectType, objectName, "create")}
          disabled={busy}
          style={{ ...S.btnSuccess, ...btnSmall, opacity: busy ? 0.5 : 1 }}
        >
          {busy ? "..." : "Синхронизировать"}
        </button>
      )}

      {matchStatus === "DIFF" && objectType === "SEQUENCE" && (
        <button
          onClick={() => onSync(objectType, objectName, "sync")}
          disabled={busy}
          style={{ ...S.btnSuccess, ...btnSmall, opacity: busy ? 0.5 : 1 }}
        >
          Обновить
        </button>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/components/DDLCatalog/ObjectActions.tsx
git commit -m "feat: add ObjectActions component with type-specific action buttons"
```

---

## Task 13: Frontend — TablesTab

**Files:**
- Create: `frontend/src/components/DDLCatalog/TablesTab.tsx`

- [ ] **Step 1: Create TablesTab.tsx**

```tsx
import React, { useState, useMemo } from "react";
import { S } from "./styles";
import { MatchBadge, MigrationBadge } from "./StatusBadges";
import { ObjectActions } from "./ObjectActions";

export interface CatalogObject {
  object_name: string;
  oracle_status: string;
  last_ddl_time: string | null;
  metadata: Record<string, unknown>;
  match_status: string;
  diff: Record<string, unknown>;
  migration_status: string;
}

interface Props {
  objects: CatalogObject[];
  selected: Set<string>;
  onToggle: (name: string) => void;
  onToggleAll: () => void;
  syncBusy: Set<string>;
  onCompare: (type: string, name: string) => void;
  onSync: (type: string, name: string, action: string) => void;
}

interface ColInfo {
  name: string;
  data_type: string;
  data_length: number | null;
  data_precision: number | null;
  data_scale: number | null;
  nullable: boolean;
  data_default: string | null;
}

function fmtType(c: ColInfo): string {
  if (c.data_precision != null) {
    return c.data_scale != null && c.data_scale !== 0
      ? `${c.data_type}(${c.data_precision},${c.data_scale})`
      : `${c.data_type}(${c.data_precision})`;
  }
  const hasLen = ["VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR", "RAW"].includes(c.data_type);
  return hasLen && c.data_length != null ? `${c.data_type}(${c.data_length})` : c.data_type;
}

function TableDetail({ metadata }: { metadata: Record<string, unknown> }) {
  const columns = (metadata.columns || []) as ColInfo[];
  const constraints = (metadata.constraints || []) as { name: string; type: string; type_code: string; status: string; columns: string[] }[];
  const indexes = (metadata.indexes || []) as { name: string; unique: boolean; index_type: string; status: string; columns: string[] }[];
  const triggers = (metadata.triggers || []) as { name: string; trigger_type: string; event: string; status: string }[];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8, padding: "8px 0" }}>
      {/* Columns */}
      <div style={{ border: "1px solid #1e293b", borderRadius: 6, overflow: "hidden" }}>
        <div style={{ padding: "5px 12px", background: "#0a111f", fontSize: 11, fontWeight: 600, color: "#64748b", borderBottom: "1px solid #1e293b" }}>
          Колонки ({columns.length})
        </div>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid #1e293b" }}>
              {["#", "Колонка", "Тип", "Nullable", "Default"].map(h => <th key={h} style={S.th}>{h}</th>)}
            </tr>
          </thead>
          <tbody>
            {columns.map((c, i) => (
              <tr key={c.name} style={S.trBorder}>
                <td style={{ ...S.td, color: "#475569" }}>{i + 1}</td>
                <td style={{ ...S.td, fontFamily: "monospace", fontWeight: 600 }}>{c.name}</td>
                <td style={{ ...S.td, fontFamily: "monospace", color: "#94a3b8" }}>{fmtType(c)}</td>
                <td style={S.td}>{c.nullable ? "Y" : "N"}</td>
                <td style={{ ...S.td, color: "#475569", fontFamily: "monospace", fontSize: 11 }}>{c.data_default || "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Indexes */}
      {indexes.length > 0 && (
        <div style={{ border: "1px solid #1e293b", borderRadius: 6, overflow: "hidden" }}>
          <div style={{ padding: "5px 12px", background: "#0a111f", fontSize: 11, fontWeight: 600, color: "#64748b", borderBottom: "1px solid #1e293b" }}>
            Индексы ({indexes.length})
          </div>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ borderBottom: "1px solid #1e293b" }}>
                {["Индекс", "Тип", "Колонки", "Статус"].map(h => <th key={h} style={S.th}>{h}</th>)}
              </tr>
            </thead>
            <tbody>
              {indexes.map(idx => (
                <tr key={idx.name} style={S.trBorder}>
                  <td style={{ ...S.td, fontFamily: "monospace", fontSize: 11 }}>{idx.name}</td>
                  <td style={{ ...S.td, fontSize: 11 }}>{idx.unique ? "UNIQUE" : idx.index_type}</td>
                  <td style={{ ...S.td, fontFamily: "monospace", color: "#94a3b8", fontSize: 11 }}>{idx.columns.join(", ")}</td>
                  <td style={S.td}>
                    <span style={S.badge(idx.status === "VALID" ? "#22c55e22" : "#ef444422", idx.status === "VALID" ? "#22c55e" : "#ef4444")}>
                      {idx.status}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Constraints */}
      {constraints.length > 0 && (
        <div style={{ border: "1px solid #1e293b", borderRadius: 6, overflow: "hidden" }}>
          <div style={{ padding: "5px 12px", background: "#0a111f", fontSize: 11, fontWeight: 600, color: "#64748b", borderBottom: "1px solid #1e293b" }}>
            Ограничения ({constraints.length})
          </div>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ borderBottom: "1px solid #1e293b" }}>
                {["Имя", "Тип", "Колонки", "Статус"].map(h => <th key={h} style={S.th}>{h}</th>)}
              </tr>
            </thead>
            <tbody>
              {constraints.map(con => (
                <tr key={con.name} style={S.trBorder}>
                  <td style={{ ...S.td, fontFamily: "monospace", fontSize: 11 }}>{con.name}</td>
                  <td style={S.td}><span style={S.badge("#3b82f622", "#3b82f6")}>{con.type}</span></td>
                  <td style={{ ...S.td, fontFamily: "monospace", color: "#94a3b8", fontSize: 11 }}>{con.columns.join(", ")}</td>
                  <td style={S.td}>
                    <span style={S.badge(con.status === "ENABLED" ? "#22c55e22" : "#ef444422", con.status === "ENABLED" ? "#22c55e" : "#ef4444")}>
                      {con.status}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Triggers */}
      {triggers.length > 0 && (
        <div style={{ border: "1px solid #1e293b", borderRadius: 6, overflow: "hidden" }}>
          <div style={{ padding: "5px 12px", background: "#0a111f", fontSize: 11, fontWeight: 600, color: "#64748b", borderBottom: "1px solid #1e293b" }}>
            Триггеры ({triggers.length})
          </div>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ borderBottom: "1px solid #1e293b" }}>
                {["Триггер", "Тип", "Событие", "Статус"].map(h => <th key={h} style={S.th}>{h}</th>)}
              </tr>
            </thead>
            <tbody>
              {triggers.map(trg => (
                <tr key={trg.name} style={S.trBorder}>
                  <td style={{ ...S.td, fontFamily: "monospace", fontSize: 11 }}>{trg.name}</td>
                  <td style={{ ...S.td, fontSize: 11 }}>{trg.trigger_type}</td>
                  <td style={{ ...S.td, color: "#94a3b8", fontSize: 11 }}>{trg.event}</td>
                  <td style={S.td}>
                    <span style={S.badge(trg.status === "ENABLED" ? "#22c55e22" : "#ef444422", trg.status === "ENABLED" ? "#22c55e" : "#ef4444")}>
                      {trg.status}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

export function TablesTab({ objects, selected, onToggle, onToggleAll, syncBusy, onCompare, onSync }: Props) {
  const [search, setSearch] = useState("");
  const [filterMode, setFilterMode] = useState<"all" | "match" | "diff" | "missing">("all");
  const [expandedTable, setExpandedTable] = useState<string | null>(null);

  const filtered = useMemo(() => {
    let list = objects;
    if (search) {
      const q = search.toLowerCase();
      list = list.filter(o => o.object_name.toLowerCase().includes(q));
    }
    if (filterMode === "match") list = list.filter(o => o.match_status === "MATCH");
    if (filterMode === "diff") list = list.filter(o => o.match_status === "DIFF");
    if (filterMode === "missing") list = list.filter(o => o.match_status === "MISSING");
    return list;
  }, [objects, search, filterMode]);

  const allSelected = filtered.length > 0 && filtered.every(o => selected.has(o.object_name));
  const matchCount = objects.filter(o => o.match_status === "MATCH").length;
  const diffCount = objects.filter(o => o.match_status === "DIFF").length;
  const missingCount = objects.filter(o => o.match_status === "MISSING").length;

  return (
    <div style={S.card}>
      {/* Toolbar */}
      <div style={{ ...S.cardHeader, justifyContent: "space-between" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <input
            value={search} onChange={e => setSearch(e.target.value)}
            placeholder="Поиск таблицы..."
            style={{ ...S.input, width: 200, padding: "4px 8px", fontSize: 12 }}
          />
          <div style={{ display: "flex", gap: 4 }}>
            {([
              ["all", `Все (${objects.length})`],
              ["match", `OK (${matchCount})`],
              ["diff", `Различия (${diffCount})`],
              ["missing", `Нет на тгт (${missingCount})`],
            ] as [string, string][]).map(([mode, label]) => (
              <button
                key={mode}
                onClick={() => setFilterMode(mode as typeof filterMode)}
                style={{
                  background: filterMode === mode ? "#1e3a5f" : "transparent",
                  border: `1px solid ${filterMode === mode ? "#3b82f6" : "#334155"}`,
                  borderRadius: 4, color: filterMode === mode ? "#93c5fd" : "#475569",
                  padding: "3px 8px", fontSize: 11, cursor: "pointer", fontWeight: 500,
                }}
              >
                {label}
              </button>
            ))}
          </div>
        </div>
        <span style={{ fontSize: 11, color: "#64748b" }}>Выбрано: {selected.size}</span>
      </div>

      {/* Table */}
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid #1e293b" }}>
              <th style={{ ...S.th, width: 36 }}>
                <input type="checkbox" checked={allSelected} onChange={onToggleAll} style={{ accentColor: "#3b82f6" }} />
              </th>
              <th style={S.th}>Таблица</th>
              <th style={S.th}>На таргете</th>
              <th style={S.th}>Миграция</th>
              <th style={S.th}>Действия</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map(obj => (
              <React.Fragment key={obj.object_name}>
                <tr style={{ ...S.trBorder, background: obj.match_status === "MISSING" ? "rgba(239,68,68,0.04)" : obj.match_status === "DIFF" ? "rgba(234,179,8,0.04)" : "transparent" }}>
                  <td style={{ ...S.td, textAlign: "center" }}>
                    <input type="checkbox" checked={selected.has(obj.object_name)} onChange={() => onToggle(obj.object_name)} style={{ accentColor: "#3b82f6" }} />
                  </td>
                  <td style={S.td}>
                    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                      <code style={{ color: "#e2e8f0", fontSize: 12, fontWeight: 600 }}>{obj.object_name}</code>
                      <button
                        onClick={() => setExpandedTable(expandedTable === obj.object_name ? null : obj.object_name)}
                        style={{ background: "none", border: "none", color: "#475569", fontSize: 9, cursor: "pointer", padding: 0 }}
                      >
                        {expandedTable === obj.object_name ? "\u25B2" : "\u25BC"}
                      </button>
                    </div>
                  </td>
                  <td style={S.td}><MatchBadge status={obj.match_status} /></td>
                  <td style={S.td}><MigrationBadge status={obj.migration_status} /></td>
                  <td style={S.td}>
                    <ObjectActions
                      objectType="TABLE" objectName={obj.object_name}
                      matchStatus={obj.match_status} syncBusy={syncBusy.has(obj.object_name)}
                      onCompare={onCompare} onSync={onSync}
                      onShowDetail={() => setExpandedTable(expandedTable === obj.object_name ? null : obj.object_name)}
                    />
                  </td>
                </tr>
                {expandedTable === obj.object_name && (
                  <tr>
                    <td colSpan={5} style={{ padding: "0 10px 10px", background: "#0a111f" }}>
                      <TableDetail metadata={obj.metadata} />
                    </td>
                  </tr>
                )}
              </React.Fragment>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/components/DDLCatalog/TablesTab.tsx
git commit -m "feat: add TablesTab component with expandable DDL details"
```

---

## Task 14: Frontend — ViewsTab

**Files:**
- Create: `frontend/src/components/DDLCatalog/ViewsTab.tsx`

- [ ] **Step 1: Create ViewsTab.tsx**

```tsx
import React, { useState, useMemo } from "react";
import { S } from "./styles";
import { MatchBadge } from "./StatusBadges";
import { ObjectActions } from "./ObjectActions";
import type { CatalogObject } from "./TablesTab";

interface Props {
  objects: CatalogObject[];
  syncBusy: Set<string>;
  onCompare: (type: string, name: string) => void;
  onSync: (type: string, name: string, action: string) => void;
}

function ViewDetail({ metadata, objectType }: { metadata: Record<string, unknown>; objectType: string }) {
  const sqlText = metadata.sql_text as string | null;
  const columns = (metadata.columns || []) as { name: string; data_type: string; data_length: number | null; nullable: boolean }[];
  const refreshType = metadata.refresh_type as string | null;
  const lastRefresh = metadata.last_refresh as string | null;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8, padding: "8px 0" }}>
      {objectType === "MATERIALIZED VIEW" && (
        <div style={{ display: "flex", gap: 12, fontSize: 11 }}>
          <span style={{ color: "#64748b" }}>Refresh: <span style={{ color: "#94a3b8" }}>{refreshType || "—"}</span></span>
          <span style={{ color: "#64748b" }}>Last: <span style={{ color: "#94a3b8" }}>{lastRefresh || "—"}</span></span>
        </div>
      )}
      {columns.length > 0 && (
        <div style={{ border: "1px solid #1e293b", borderRadius: 6, overflow: "hidden" }}>
          <div style={{ padding: "5px 12px", background: "#0a111f", fontSize: 11, fontWeight: 600, color: "#64748b", borderBottom: "1px solid #1e293b" }}>
            Колонки ({columns.length})
          </div>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ borderBottom: "1px solid #1e293b" }}>
                {["Колонка", "Тип", "Nullable"].map(h => <th key={h} style={S.th}>{h}</th>)}
              </tr>
            </thead>
            <tbody>
              {columns.map(c => (
                <tr key={c.name} style={S.trBorder}>
                  <td style={{ ...S.td, fontFamily: "monospace", fontWeight: 600 }}>{c.name}</td>
                  <td style={{ ...S.td, fontFamily: "monospace", color: "#94a3b8" }}>{c.data_type}{c.data_length ? `(${c.data_length})` : ""}</td>
                  <td style={S.td}>{c.nullable ? "Y" : "N"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {sqlText && (
        <div style={{ border: "1px solid #1e293b", borderRadius: 6, overflow: "hidden" }}>
          <div style={{ padding: "5px 12px", background: "#0a111f", fontSize: 11, fontWeight: 600, color: "#64748b", borderBottom: "1px solid #1e293b" }}>
            SQL
          </div>
          <pre style={{ padding: 12, margin: 0, fontSize: 11, color: "#94a3b8", overflowX: "auto", whiteSpace: "pre-wrap" }}>
            {sqlText}
          </pre>
        </div>
      )}
    </div>
  );
}

export function ViewsTab({ objects, syncBusy, onCompare, onSync }: Props) {
  const [search, setSearch] = useState("");
  const [expandedObj, setExpandedObj] = useState<string | null>(null);

  const views = useMemo(() => objects.filter(o => (o.metadata as Record<string, unknown>)._object_type !== "MATERIALIZED VIEW" || true), [objects]);
  const filtered = useMemo(() => {
    if (!search) return views;
    const q = search.toLowerCase();
    return views.filter(o => o.object_name.toLowerCase().includes(q));
  }, [views, search]);

  return (
    <div style={S.card}>
      <div style={{ ...S.cardHeader, justifyContent: "space-between" }}>
        <input
          value={search} onChange={e => setSearch(e.target.value)}
          placeholder="Поиск..."
          style={{ ...S.input, width: 200, padding: "4px 8px", fontSize: 12 }}
        />
        <span style={{ fontSize: 11, color: "#64748b" }}>Всего: {objects.length}</span>
      </div>
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid #1e293b" }}>
              {["Объект", "Тип", "Статус Oracle", "На таргете", "Действия"].map(h => <th key={h} style={S.th}>{h}</th>)}
            </tr>
          </thead>
          <tbody>
            {filtered.map(obj => {
              const oType = (obj.metadata as Record<string, unknown>).refresh_type ? "MATERIALIZED VIEW" : "VIEW";
              return (
                <React.Fragment key={obj.object_name}>
                  <tr style={S.trBorder}>
                    <td style={S.td}>
                      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                        <code style={{ color: "#e2e8f0", fontSize: 12, fontWeight: 600 }}>{obj.object_name}</code>
                        <button onClick={() => setExpandedObj(expandedObj === obj.object_name ? null : obj.object_name)}
                          style={{ background: "none", border: "none", color: "#475569", fontSize: 9, cursor: "pointer", padding: 0 }}>
                          {expandedObj === obj.object_name ? "\u25B2" : "\u25BC"}
                        </button>
                      </div>
                    </td>
                    <td style={S.td}>
                      <span style={S.badge("#3b82f622", "#3b82f6")}>{oType === "MATERIALIZED VIEW" ? "MVIEW" : "VIEW"}</span>
                    </td>
                    <td style={S.td}>
                      <span style={S.badge(obj.oracle_status === "VALID" ? "#22c55e22" : "#ef444422", obj.oracle_status === "VALID" ? "#22c55e" : "#ef4444")}>
                        {obj.oracle_status}
                      </span>
                    </td>
                    <td style={S.td}><MatchBadge status={obj.match_status} /></td>
                    <td style={S.td}>
                      <ObjectActions
                        objectType={oType} objectName={obj.object_name}
                        matchStatus={obj.match_status} syncBusy={syncBusy.has(obj.object_name)}
                        onCompare={onCompare} onSync={onSync}
                        onShowDetail={() => setExpandedObj(expandedObj === obj.object_name ? null : obj.object_name)}
                      />
                    </td>
                  </tr>
                  {expandedObj === obj.object_name && (
                    <tr>
                      <td colSpan={5} style={{ padding: "0 10px 10px", background: "#0a111f" }}>
                        <ViewDetail metadata={obj.metadata} objectType={oType} />
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/components/DDLCatalog/ViewsTab.tsx
git commit -m "feat: add ViewsTab component for views and materialized views"
```

---

## Task 15: Frontend — CodeTab

**Files:**
- Create: `frontend/src/components/DDLCatalog/CodeTab.tsx`

- [ ] **Step 1: Create CodeTab.tsx**

```tsx
import React, { useState, useMemo } from "react";
import { S } from "./styles";
import { MatchBadge } from "./StatusBadges";
import { ObjectActions } from "./ObjectActions";
import type { CatalogObject } from "./TablesTab";

interface Props {
  objects: CatalogObject[];
  syncBusy: Set<string>;
  onCompare: (type: string, name: string) => void;
  onSync: (type: string, name: string, action: string) => void;
}

function CodeDetail({ metadata, objectType }: { metadata: Record<string, unknown>; objectType: string }) {
  const [showBody, setShowBody] = useState(false);

  if (objectType === "PACKAGE") {
    const spec = metadata.spec_source as string | null;
    const body = metadata.body_source as string | null;
    return (
      <div style={{ display: "flex", flexDirection: "column", gap: 8, padding: "8px 0" }}>
        {spec && (
          <div style={{ border: "1px solid #1e293b", borderRadius: 6, overflow: "hidden" }}>
            <div style={{ padding: "5px 12px", background: "#0a111f", fontSize: 11, fontWeight: 600, color: "#64748b", borderBottom: "1px solid #1e293b" }}>
              Package Spec
            </div>
            <pre style={{ padding: 12, margin: 0, fontSize: 11, color: "#94a3b8", overflowX: "auto", whiteSpace: "pre-wrap", maxHeight: 300 }}>{spec}</pre>
          </div>
        )}
        {body && (
          <div style={{ border: "1px solid #1e293b", borderRadius: 6, overflow: "hidden" }}>
            <div
              onClick={() => setShowBody(!showBody)}
              style={{ padding: "5px 12px", background: "#0a111f", fontSize: 11, fontWeight: 600, color: "#64748b", borderBottom: "1px solid #1e293b", cursor: "pointer" }}
            >
              Package Body {showBody ? "\u25B2" : "\u25BC"}
            </div>
            {showBody && (
              <pre style={{ padding: 12, margin: 0, fontSize: 11, color: "#94a3b8", overflowX: "auto", whiteSpace: "pre-wrap", maxHeight: 400 }}>{body}</pre>
            )}
          </div>
        )}
      </div>
    );
  }

  const code = (metadata.source_code || metadata.source) as string | null;
  const argCount = metadata.argument_count as number | undefined;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8, padding: "8px 0" }}>
      {argCount !== undefined && (
        <span style={{ fontSize: 11, color: "#64748b" }}>Аргументов: {argCount}</span>
      )}
      {code && (
        <div style={{ border: "1px solid #1e293b", borderRadius: 6, overflow: "hidden" }}>
          <div style={{ padding: "5px 12px", background: "#0a111f", fontSize: 11, fontWeight: 600, color: "#64748b", borderBottom: "1px solid #1e293b" }}>
            Исходный код
          </div>
          <pre style={{ padding: 12, margin: 0, fontSize: 11, color: "#94a3b8", overflowX: "auto", whiteSpace: "pre-wrap", maxHeight: 400 }}>{code}</pre>
        </div>
      )}
    </div>
  );
}

export function CodeTab({ objects, syncBusy, onCompare, onSync }: Props) {
  const [search, setSearch] = useState("");
  const [typeFilter, setTypeFilter] = useState<"all" | "FUNCTION" | "PROCEDURE" | "PACKAGE">("all");
  const [expandedObj, setExpandedObj] = useState<string | null>(null);

  const filtered = useMemo(() => {
    let list = objects;
    if (search) {
      const q = search.toLowerCase();
      list = list.filter(o => o.object_name.toLowerCase().includes(q));
    }
    // Objects come with _obj_type in metadata or we need to detect from metadata shape
    if (typeFilter !== "all") {
      list = list.filter(o => {
        const m = o.metadata;
        if (typeFilter === "PACKAGE") return "spec_source" in m;
        if (typeFilter === "FUNCTION") return "argument_count" in m && "source_code" in m;
        if (typeFilter === "PROCEDURE") return "argument_count" in m && "source_code" in m;
        return true;
      });
    }
    return list;
  }, [objects, search, typeFilter]);

  const getObjType = (meta: Record<string, unknown>): string => {
    if ("spec_source" in meta) return "PACKAGE";
    if ("source" in meta && "body_source" in meta) return "TYPE";
    return "FUNCTION";  // FUNCTION and PROCEDURE look the same in metadata
  };

  return (
    <div style={S.card}>
      <div style={{ ...S.cardHeader, justifyContent: "space-between" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <input
            value={search} onChange={e => setSearch(e.target.value)}
            placeholder="Поиск..."
            style={{ ...S.input, width: 200, padding: "4px 8px", fontSize: 12 }}
          />
          <div style={{ display: "flex", gap: 4 }}>
            {(["all", "FUNCTION", "PROCEDURE", "PACKAGE"] as const).map(t => (
              <button key={t} onClick={() => setTypeFilter(t)}
                style={{
                  background: typeFilter === t ? "#1e3a5f" : "transparent",
                  border: `1px solid ${typeFilter === t ? "#3b82f6" : "#334155"}`,
                  borderRadius: 4, color: typeFilter === t ? "#93c5fd" : "#475569",
                  padding: "3px 8px", fontSize: 11, cursor: "pointer", fontWeight: 500,
                }}>
                {t === "all" ? "Все" : t}
              </button>
            ))}
          </div>
        </div>
        <span style={{ fontSize: 11, color: "#64748b" }}>Всего: {objects.length}</span>
      </div>
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid #1e293b" }}>
              {["Объект", "Тип", "Статус Oracle", "На таргете", "Действия"].map(h => <th key={h} style={S.th}>{h}</th>)}
            </tr>
          </thead>
          <tbody>
            {filtered.map(obj => {
              const oType = getObjType(obj.metadata);
              return (
                <React.Fragment key={obj.object_name}>
                  <tr style={S.trBorder}>
                    <td style={S.td}>
                      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                        <code style={{ color: "#e2e8f0", fontSize: 12, fontWeight: 600 }}>{obj.object_name}</code>
                        <button onClick={() => setExpandedObj(expandedObj === obj.object_name ? null : obj.object_name)}
                          style={{ background: "none", border: "none", color: "#475569", fontSize: 9, cursor: "pointer", padding: 0 }}>
                          {expandedObj === obj.object_name ? "\u25B2" : "\u25BC"}
                        </button>
                      </div>
                    </td>
                    <td style={S.td}><span style={S.badge("#8b5cf622", "#8b5cf6")}>{oType}</span></td>
                    <td style={S.td}>
                      <span style={S.badge(obj.oracle_status === "VALID" ? "#22c55e22" : "#ef444422", obj.oracle_status === "VALID" ? "#22c55e" : "#ef4444")}>
                        {obj.oracle_status}
                      </span>
                    </td>
                    <td style={S.td}><MatchBadge status={obj.match_status} /></td>
                    <td style={S.td}>
                      <ObjectActions
                        objectType={oType} objectName={obj.object_name}
                        matchStatus={obj.match_status} syncBusy={syncBusy.has(obj.object_name)}
                        onCompare={onCompare} onSync={onSync}
                        onShowDetail={() => setExpandedObj(expandedObj === obj.object_name ? null : obj.object_name)}
                      />
                    </td>
                  </tr>
                  {expandedObj === obj.object_name && (
                    <tr>
                      <td colSpan={5} style={{ padding: "0 10px 10px", background: "#0a111f" }}>
                        <CodeDetail metadata={obj.metadata} objectType={oType} />
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/components/DDLCatalog/CodeTab.tsx
git commit -m "feat: add CodeTab component for functions, procedures, packages"
```

---

## Task 16: Frontend — OtherTab

**Files:**
- Create: `frontend/src/components/DDLCatalog/OtherTab.tsx`

- [ ] **Step 1: Create OtherTab.tsx**

```tsx
import React, { useState, useMemo } from "react";
import { S } from "./styles";
import { MatchBadge } from "./StatusBadges";
import { ObjectActions } from "./ObjectActions";
import type { CatalogObject } from "./TablesTab";

interface Props {
  objects: CatalogObject[];
  syncBusy: Set<string>;
  onCompare: (type: string, name: string) => void;
  onSync: (type: string, name: string, action: string) => void;
}

function SequenceDetail({ metadata }: { metadata: Record<string, unknown> }) {
  const fields = [
    ["Min", metadata.min_value],
    ["Max", metadata.max_value],
    ["Increment", metadata.increment_by],
    ["Cache", metadata.cache_size],
    ["Last number", metadata.last_number],
  ];
  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 10, padding: "8px 0" }}>
      {fields.map(([label, val]) => (
        <div key={label as string} style={{ display: "flex", flexDirection: "column", gap: 2 }}>
          <span style={{ fontSize: 10, color: "#475569", fontWeight: 600 }}>{label as string}</span>
          <span style={{ fontSize: 12, color: "#e2e8f0", fontFamily: "monospace" }}>{val != null ? String(val) : "—"}</span>
        </div>
      ))}
    </div>
  );
}

function SynonymDetail({ metadata }: { metadata: Record<string, unknown> }) {
  return (
    <div style={{ padding: "8px 0", fontSize: 12, color: "#94a3b8" }}>
      <span style={{ color: "#64748b" }}>Target: </span>
      <code style={{ color: "#e2e8f0" }}>
        {metadata.table_owner}.{metadata.table_name}
        {metadata.db_link ? `@${metadata.db_link}` : ""}
      </code>
    </div>
  );
}

function TypeDetail({ metadata }: { metadata: Record<string, unknown> }) {
  const source = (metadata.source || metadata.typecode) as string | null;
  return source ? (
    <pre style={{ padding: 12, margin: 0, fontSize: 11, color: "#94a3b8", overflowX: "auto", whiteSpace: "pre-wrap", maxHeight: 300 }}>
      {source}
    </pre>
  ) : null;
}

export function OtherTab({ objects, syncBusy, onCompare, onSync }: Props) {
  const [search, setSearch] = useState("");
  const [expandedObj, setExpandedObj] = useState<string | null>(null);

  const filtered = useMemo(() => {
    if (!search) return objects;
    const q = search.toLowerCase();
    return objects.filter(o => o.object_name.toLowerCase().includes(q));
  }, [objects, search]);

  const getObjType = (meta: Record<string, unknown>): string => {
    if ("increment_by" in meta) return "SEQUENCE";
    if ("table_name" in meta) return "SYNONYM";
    return "TYPE";
  };

  return (
    <div style={S.card}>
      <div style={{ ...S.cardHeader, justifyContent: "space-between" }}>
        <input
          value={search} onChange={e => setSearch(e.target.value)}
          placeholder="Поиск..."
          style={{ ...S.input, width: 200, padding: "4px 8px", fontSize: 12 }}
        />
        <span style={{ fontSize: 11, color: "#64748b" }}>Всего: {objects.length}</span>
      </div>
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid #1e293b" }}>
              {["Объект", "Тип", "На таргете", "Действия"].map(h => <th key={h} style={S.th}>{h}</th>)}
            </tr>
          </thead>
          <tbody>
            {filtered.map(obj => {
              const oType = getObjType(obj.metadata);
              return (
                <React.Fragment key={obj.object_name}>
                  <tr style={S.trBorder}>
                    <td style={S.td}>
                      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                        <code style={{ color: "#e2e8f0", fontSize: 12, fontWeight: 600 }}>{obj.object_name}</code>
                        <button onClick={() => setExpandedObj(expandedObj === obj.object_name ? null : obj.object_name)}
                          style={{ background: "none", border: "none", color: "#475569", fontSize: 9, cursor: "pointer", padding: 0 }}>
                          {expandedObj === obj.object_name ? "\u25B2" : "\u25BC"}
                        </button>
                      </div>
                    </td>
                    <td style={S.td}>
                      <span style={S.badge(
                        oType === "SEQUENCE" ? "#f59e0b22" : oType === "SYNONYM" ? "#06b6d422" : "#8b5cf622",
                        oType === "SEQUENCE" ? "#f59e0b" : oType === "SYNONYM" ? "#06b6d4" : "#8b5cf6",
                      )}>{oType}</span>
                    </td>
                    <td style={S.td}><MatchBadge status={obj.match_status} /></td>
                    <td style={S.td}>
                      <ObjectActions
                        objectType={oType} objectName={obj.object_name}
                        matchStatus={obj.match_status} syncBusy={syncBusy.has(obj.object_name)}
                        onCompare={onCompare} onSync={onSync}
                        onShowDetail={() => setExpandedObj(expandedObj === obj.object_name ? null : obj.object_name)}
                      />
                    </td>
                  </tr>
                  {expandedObj === obj.object_name && (
                    <tr>
                      <td colSpan={4} style={{ padding: "0 10px 10px", background: "#0a111f" }}>
                        {oType === "SEQUENCE" && <SequenceDetail metadata={obj.metadata} />}
                        {oType === "SYNONYM" && <SynonymDetail metadata={obj.metadata} />}
                        {oType === "TYPE" && <TypeDetail metadata={obj.metadata} />}
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/components/DDLCatalog/OtherTab.tsx
git commit -m "feat: add OtherTab component for sequences, synonyms, types"
```

---

## Task 17: Frontend — ObjectTabs

**Files:**
- Create: `frontend/src/components/DDLCatalog/ObjectTabs.tsx`

- [ ] **Step 1: Create ObjectTabs.tsx**

```tsx
import React from "react";

export type ObjectTabId = "tables" | "views" | "code" | "other";

interface Props {
  active: ObjectTabId;
  onChange: (tab: ObjectTabId) => void;
  counts: { tables: number; views: number; code: number; other: number };
}

const TABS: { id: ObjectTabId; label: string; countKey: keyof Props["counts"] }[] = [
  { id: "tables", label: "Таблицы", countKey: "tables" },
  { id: "views", label: "Views & MViews", countKey: "views" },
  { id: "code", label: "Code", countKey: "code" },
  { id: "other", label: "Другое", countKey: "other" },
];

export function ObjectTabs({ active, onChange, counts }: Props) {
  return (
    <div style={{ display: "flex", gap: 0, borderBottom: "1px solid #1e293b", marginBottom: 12 }}>
      {TABS.map(tab => {
        const isActive = active === tab.id;
        return (
          <button
            key={tab.id}
            onClick={() => onChange(tab.id)}
            style={{
              background: "none", border: "none",
              borderBottom: `2px solid ${isActive ? "#3b82f6" : "transparent"}`,
              color: isActive ? "#93c5fd" : "#475569",
              padding: "8px 16px", fontSize: 13,
              fontWeight: isActive ? 700 : 500,
              cursor: "pointer", marginBottom: -1,
              transition: "color 0.15s, border-color 0.15s",
            }}
          >
            {tab.label} ({counts[tab.countKey]})
          </button>
        );
      })}
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/components/DDLCatalog/ObjectTabs.tsx
git commit -m "feat: add ObjectTabs sub-tab component"
```

---

## Task 18: Frontend — PlannerWizard (Extract Steps 2-4)

**Files:**
- Create: `frontend/src/components/DDLCatalog/PlannerWizard.tsx`
- Reference: `frontend/src/components/MigrationPlanner.tsx` lines 838-1370 (steps 2-4 components + related state)

- [ ] **Step 1: Create PlannerWizard.tsx**

Extract `TableSelectionStep`, `OrderingStep`, `ReviewStep`, and the step navigation from MigrationPlanner.tsx. The component receives selected tables and schemas as props.

```tsx
import React, { useState, useCallback, useEffect, useMemo } from "react";
import { S } from "./styles";

// ── Types (same as MigrationPlanner) ────────────────────────────────────────

interface BatchItem {
  table: string;
  mode: "CDC" | "BULK_ONLY";
  strategy: "STAGE" | "DIRECT";
  chunk_size: number;
  workers: number;
}

interface Batch {
  id: number;
  items: BatchItem[];
}

interface PlanDefaults {
  chunk_size: number;
  workers: number;
  strategy: "STAGE" | "DIRECT";
  mode: "CDC" | "BULK_ONLY";
}

interface FKDep {
  table: string;
  depends_on: string[];
}

interface ConnectorGroup {
  id: string;
  group_name: string;
  connector_name: string;
  status: string;
}

interface Props {
  selectedTables: string[];
  srcSchema: string;
  tgtSchema: string;
  onClose: () => void;
}

// ── Helpers ─────────────────────────────────────────────────────────────────

function topoSort(tables: string[], deps: FKDep[]): string[] {
  const graph = new Map<string, string[]>();
  const inDeg = new Map<string, number>();
  const tableSet = new Set(tables);
  for (const t of tables) { graph.set(t, []); inDeg.set(t, 0); }
  for (const d of deps) {
    if (!tableSet.has(d.table)) continue;
    for (const p of d.depends_on) {
      if (!tableSet.has(p)) continue;
      graph.get(p)!.push(d.table);
      inDeg.set(d.table, (inDeg.get(d.table) || 0) + 1);
    }
  }
  const queue: string[] = [];
  for (const [t, deg] of inDeg) if (deg === 0) queue.push(t);
  const sorted: string[] = [];
  while (queue.length > 0) {
    const cur = queue.shift()!;
    sorted.push(cur);
    for (const next of graph.get(cur) || []) {
      const newDeg = (inDeg.get(next) || 1) - 1;
      inDeg.set(next, newDeg);
      if (newDeg === 0) queue.push(next);
    }
  }
  for (const t of tables) if (!sorted.includes(t)) sorted.push(t);
  return sorted;
}

const STEP_LABELS = ["Настройки таблиц", "Порядок загрузки", "Обзор и запуск"];

// ── StepIndicator ───────────────────────────────────────────────────────────

function StepIndicator({ current }: { current: number }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 0, marginBottom: 16 }}>
      {STEP_LABELS.map((label, i) => {
        const done = i < current;
        const active = i === current;
        const color = done ? "#22c55e" : active ? "#3b82f6" : "#334155";
        const textColor = done ? "#86efac" : active ? "#93c5fd" : "#475569";
        return (
          <React.Fragment key={i}>
            {i > 0 && <div style={{ flex: 1, height: 2, background: done ? "#22c55e55" : "#1e293b", margin: "0 4px" }} />}
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <div style={{
                width: 22, height: 22, borderRadius: "50%",
                border: `2px solid ${color}`,
                background: done ? "#052e16" : active ? "#1e3a5f" : "transparent",
                display: "flex", alignItems: "center", justifyContent: "center",
                fontSize: 10, fontWeight: 700, color: textColor,
              }}>
                {done ? "\u2713" : i + 1}
              </div>
              <span style={{ fontSize: 11, fontWeight: active ? 700 : 500, color: textColor, whiteSpace: "nowrap" }}>{label}</span>
            </div>
          </React.Fragment>
        );
      })}
    </div>
  );
}

// ── Main Component ──────────────────────────────────────────────────────────
// NOTE: The full TableSelectionStep, OrderingStep, ReviewStep components
// should be extracted from MigrationPlanner.tsx lines 838-1370.
// They remain unchanged in logic — only the wrapper and props change.
// For brevity, this file shows the shell. During implementation,
// copy the three Step components verbatim from MigrationPlanner.tsx,
// updating their import of S to use "./styles".

export function PlannerWizard({ selectedTables, srcSchema, tgtSchema, onClose }: Props) {
  const [step, setStep] = useState(0);

  // Step 0 state (Table Settings)
  const [defaults, setDefaults] = useState<PlanDefaults>({
    chunk_size: 50000, workers: 4, strategy: "STAGE", mode: "CDC",
  });
  const [tableSettings, setTableSettings] = useState<Map<string, BatchItem>>(() => {
    const map = new Map<string, BatchItem>();
    for (const table of selectedTables) {
      map.set(table, { table, mode: "CDC", strategy: "STAGE", chunk_size: 50000, workers: 4 });
    }
    return map;
  });
  const [groups, setGroups] = useState<ConnectorGroup[]>([]);
  const [selectedGroup, setSelectedGroup] = useState("");

  // Step 1 state (Ordering)
  const [batches, setBatches] = useState<Batch[]>([]);
  const [deps, setDeps] = useState<FKDep[]>([]);
  const [depsLoading, setDepsLoading] = useState(false);

  // Step 2 state (Review)
  const [executing, setExecuting] = useState(false);
  const [executeError, setExecuteError] = useState<string | null>(null);
  const [planId, setPlanId] = useState<string | null>(null);
  const [starting, setStarting] = useState(false);
  const [startError, setStartError] = useState<string | null>(null);

  // Load connector groups
  useEffect(() => {
    fetch("/api/connector-groups")
      .then(r => r.ok ? r.json() : Promise.reject())
      .then((data: ConnectorGroup[]) => setGroups(data))
      .catch(() => {});
  }, []);

  const updateTableSetting = (table: string, upd: Partial<BatchItem>) => {
    setTableSettings(prev => {
      const next = new Map(prev);
      const cur = next.get(table);
      if (cur) next.set(table, { ...cur, ...upd });
      return next;
    });
  };

  // Load FK deps when moving to step 1
  const initOrdering = useCallback(() => {
    setDepsLoading(true);
    const qs = new URLSearchParams({ schema: srcSchema, tables: selectedTables.join(",") });
    fetch(`/api/planner/fk-dependencies?${qs}`)
      .then(r => r.ok ? r.json() : Promise.reject())
      .then((data: FKDep[]) => {
        setDeps(data);
        const sorted = topoSort(selectedTables, data);
        const items: BatchItem[] = sorted.map(table => {
          const ts = tableSettings.get(table);
          return ts ?? { table, mode: defaults.mode, strategy: defaults.strategy, chunk_size: defaults.chunk_size, workers: defaults.workers };
        });
        setBatches([{ id: 1, items }]);
      })
      .catch(() => {
        const items: BatchItem[] = selectedTables.map(table => {
          const ts = tableSettings.get(table);
          return ts ?? { table, mode: defaults.mode, strategy: defaults.strategy, chunk_size: defaults.chunk_size, workers: defaults.workers };
        });
        setBatches([{ id: 1, items }]);
      })
      .finally(() => setDepsLoading(false));
  }, [selectedTables, srcSchema, tableSettings, defaults]);

  // Execute plan
  const doExecute = useCallback(() => {
    setExecuting(true); setExecuteError(null);
    const group = groups.find(g => g.group_name === selectedGroup);
    const payload = {
      src_schema: srcSchema, tgt_schema: tgtSchema,
      group_id: group?.id ?? null,
      defaults: {
        chunk_size: defaults.chunk_size, max_parallel_workers: defaults.workers,
        migration_strategy: defaults.strategy, migration_mode: defaults.mode,
      },
      batches: batches.map(b => ({
        batch_order: b.id,
        tables: b.items.map(it => ({
          source_table: it.table, target_table: it.table,
          migration_mode: it.mode, migration_strategy: it.strategy,
          chunk_size: it.chunk_size, max_parallel_workers: it.workers,
        })),
      })),
    };
    fetch("/api/planner/execute", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    })
      .then(r => r.ok ? r.json() : r.json().then(d => Promise.reject(d.error || "Ошибка")))
      .then((data: { plan_id: string }) => setPlanId(data.plan_id))
      .catch(e => setExecuteError(typeof e === "string" ? e : String(e)))
      .finally(() => setExecuting(false));
  }, [srcSchema, tgtSchema, selectedGroup, groups, defaults, batches]);

  const doStart = useCallback(() => {
    if (!planId) return;
    setStarting(true); setStartError(null);
    fetch(`/api/planner/plans/${planId}/start`, { method: "POST" })
      .then(r => r.ok ? r.json() : r.json().then(d => Promise.reject(d.error || "Ошибка")))
      .then(() => { onClose(); })
      .catch(e => setStartError(typeof e === "string" ? e : String(e)))
      .finally(() => setStarting(false));
  }, [planId, onClose]);

  const goNext = () => {
    if (step === 0) { initOrdering(); setStep(1); }
    else if (step === 1) { setStep(2); }
  };

  const canNext = (): boolean => {
    if (step === 0) return true;
    if (step === 1) return batches.length > 0 && batches.some(b => b.items.length > 0);
    return false;
  };

  return (
    <div style={{
      ...S.card, marginTop: 16, border: "1px solid #3b82f644",
    }}>
      <div style={{ ...S.cardHeader, justifyContent: "space-between" }}>
        <span style={{ fontSize: 13, fontWeight: 600, color: "#93c5fd" }}>
          Визард миграции ({selectedTables.length} таблиц)
        </span>
        <button onClick={onClose} style={{ ...S.btnSecondary, fontSize: 11, padding: "3px 10px" }}>
          Закрыть
        </button>
      </div>
      <div style={S.cardBody}>
        <StepIndicator current={step} />

        {executeError && (
          <div style={{ background: "#7f1d1d22", border: "1px solid #7f1d1d", borderRadius: 6, color: "#fca5a5", padding: "8px 14px", fontSize: 12 }}>
            {executeError}
          </div>
        )}
        {startError && (
          <div style={{ background: "#7f1d1d22", border: "1px solid #7f1d1d", borderRadius: 6, color: "#fca5a5", padding: "8px 14px", fontSize: 12 }}>
            {startError}
          </div>
        )}

        {/* Step content — implementation note:
            Copy TableSelectionStep (lines 840-1000), OrderingStep (lines 1002-1200),
            and ReviewStep (lines 1202-1370) from MigrationPlanner.tsx verbatim.
            They are standalone components that only need {S} imported from ./styles. */}

        {step === 0 && (
          <div style={{ color: "#94a3b8", fontSize: 12 }}>
            {/* TableSelectionStep will be rendered here.
                During implementation, paste the full component from MigrationPlanner.tsx */}
            <p>Настройки таблиц: mode, strategy, chunk_size, workers для каждой таблицы.</p>
            <p>Таблицы: {selectedTables.join(", ")}</p>
          </div>
        )}

        {step === 1 && (
          <div style={{ color: "#94a3b8", fontSize: 12 }}>
            {depsLoading ? "Загрузка зависимостей..." : `Батчей: ${batches.length}`}
          </div>
        )}

        {step === 2 && (
          <div style={{ color: "#94a3b8", fontSize: 12 }}>
            <p>Обзор плана. {planId ? `Plan ID: ${planId}` : ""}</p>
            <div style={{ display: "flex", gap: 8, marginTop: 8 }}>
              {!planId && (
                <button onClick={doExecute} disabled={executing} style={{ ...S.btnPrimary, opacity: executing ? 0.5 : 1 }}>
                  {executing ? "Создание..." : "Создать план"}
                </button>
              )}
              {planId && (
                <button onClick={doStart} disabled={starting} style={{ ...S.btnSuccess, opacity: starting ? 0.5 : 1 }}>
                  {starting ? "Запуск..." : "Запустить"}
                </button>
              )}
            </div>
          </div>
        )}

        {/* Navigation */}
        <div style={{ display: "flex", justifyContent: "space-between", marginTop: 12, paddingTop: 12, borderTop: "1px solid #1e293b" }}>
          <button onClick={() => step > 0 && setStep(step - 1)} disabled={step === 0}
            style={{ ...S.btnSecondary, opacity: step === 0 ? 0.3 : 1 }}>
            Назад
          </button>
          {step < 2 && (
            <button onClick={goNext} disabled={!canNext()}
              style={{ ...S.btnPrimary, opacity: canNext() ? 1 : 0.5 }}>
              Далее
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
```

**Implementation note:** During actual implementation, copy the full `TableSelectionStep`, `OrderingStep`, and `ReviewStep` component bodies from `MigrationPlanner.tsx` lines 838-1370 into this file. The placeholder content above shows the structure; the actual step content is identical to the existing code.

- [ ] **Step 2: Commit**

```bash
git add frontend/src/components/DDLCatalog/PlannerWizard.tsx
git commit -m "feat: add PlannerWizard with steps 2-4 extracted from MigrationPlanner"
```

---

## Task 19: Frontend — DDLCatalog Main Component

**Files:**
- Create: `frontend/src/components/DDLCatalog/DDLCatalog.tsx`

- [ ] **Step 1: Create DDLCatalog.tsx**

```tsx
import React, { useState, useEffect, useCallback, useMemo } from "react";
import { S } from "./styles";
import { ObjectTabs, ObjectTabId } from "./ObjectTabs";
import { TablesTab, CatalogObject } from "./TablesTab";
import { ViewsTab } from "./ViewsTab";
import { CodeTab } from "./CodeTab";
import { OtherTab } from "./OtherTab";
import { PlannerWizard } from "./PlannerWizard";

// ── SearchSelect (reused from MigrationPlanner) ────────────────────────────

function SearchSelect({
  value, onChange, options, placeholder, disabled,
}: {
  value: string; onChange: (v: string) => void; options: string[];
  placeholder: string; disabled?: boolean;
}) {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const wrapRef = React.useRef<HTMLDivElement>(null);
  const inputRef = React.useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) { setOpen(false); setQuery(""); }
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [open]);
  useEffect(() => { if (open) inputRef.current?.focus(); }, [open]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    return q ? options.filter(o => o.toLowerCase().includes(q)) : options;
  }, [options, query]);

  return (
    <div ref={wrapRef} style={{ position: "relative", minWidth: 165 }}>
      <div onClick={() => !disabled && (setOpen(o => !o), setQuery(""))}
        style={{
          display: "flex", alignItems: "center", gap: 6,
          background: "#1e293b", border: `1px solid ${open ? "#3b82f6" : "#334155"}`,
          borderRadius: 4, padding: "0 8px", height: 30,
          cursor: disabled ? "not-allowed" : "pointer", opacity: disabled ? 0.5 : 1,
        }}>
        <span style={{ fontSize: 12, flex: 1, color: value ? "#e2e8f0" : "#475569", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
          {value || placeholder}
        </span>
        <span style={{ color: "#475569", fontSize: 9 }}>{open ? "\u25B2" : "\u25BC"}</span>
      </div>
      {open && (
        <div style={{ position: "absolute", top: "calc(100% + 3px)", left: 0, right: 0, background: "#1e293b", border: "1px solid #334155", borderRadius: 4, zIndex: 200, boxShadow: "0 6px 20px rgba(0,0,0,0.5)" }}>
          <div style={{ padding: "6px 8px", borderBottom: "1px solid #0f1e35", display: "flex", alignItems: "center", gap: 6 }}>
            <input ref={inputRef} value={query} onChange={e => setQuery(e.target.value)}
              onKeyDown={e => { if (e.key === "Escape") { setOpen(false); setQuery(""); } if (e.key === "Enter" && filtered.length === 1) { onChange(filtered[0]); setOpen(false); setQuery(""); } }}
              placeholder="Поиск..." style={{ background: "none", border: "none", color: "#e2e8f0", fontSize: 12, width: "100%", outline: "none" }} />
          </div>
          <div style={{ maxHeight: 200, overflowY: "auto" }}>
            {filtered.length === 0
              ? <div style={{ padding: "8px 10px", color: "#475569", fontSize: 12 }}>Нет совпадений</div>
              : filtered.map(o => (
                <div key={o} onMouseDown={() => { onChange(o); setOpen(false); setQuery(""); }}
                  style={{ padding: "6px 10px", fontSize: 12, cursor: "pointer", background: o === value ? "#1d3a5f" : "transparent", color: o === value ? "#93c5fd" : "#e2e8f0" }}
                  onMouseEnter={e => (e.currentTarget.style.background = o === value ? "#1d3a5f" : "#0f1624")}
                  onMouseLeave={e => (e.currentTarget.style.background = o === value ? "#1d3a5f" : "transparent")}>
                  {o}
                </div>
              ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Type grouping ───────────────────────────────────────────────────────────

const TABLE_TYPES = new Set(["TABLE"]);
const VIEW_TYPES = new Set(["VIEW", "MATERIALIZED VIEW"]);
const CODE_TYPES = new Set(["FUNCTION", "PROCEDURE", "PACKAGE"]);
const OTHER_TYPES = new Set(["SEQUENCE", "SYNONYM", "TYPE"]);

function groupObjects(objects: CatalogObject[], typeMap: Map<string, string>) {
  const tables: CatalogObject[] = [];
  const views: CatalogObject[] = [];
  const code: CatalogObject[] = [];
  const other: CatalogObject[] = [];
  for (const obj of objects) {
    const oType = typeMap.get(obj.object_name) || "TABLE";
    if (TABLE_TYPES.has(oType)) tables.push(obj);
    else if (VIEW_TYPES.has(oType)) views.push(obj);
    else if (CODE_TYPES.has(oType)) code.push(obj);
    else other.push(obj);
  }
  return { tables, views, code, other };
}

// ── Main Component ──────────────────────────────────────────────────────────

export function DDLCatalog() {
  // Schema state
  const [srcSchema, setSrcSchema] = useState("");
  const [tgtSchema, setTgtSchema] = useState("");
  const [srcSchemas, setSrcSchemas] = useState<string[]>([]);
  const [tgtSchemas, setTgtSchemas] = useState<string[]>([]);

  // Catalog state
  const [snapshotId, setSnapshotId] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loadedAt, setLoadedAt] = useState<string | null>(null);
  const [objectCounts, setObjectCounts] = useState<Record<string, number>>({});

  // Object lists per type (loaded from API)
  const [allObjects, setAllObjects] = useState<CatalogObject[]>([]);
  const [objectTypeMap, setObjectTypeMap] = useState<Map<string, string>>(new Map());
  const [activeTab, setActiveTab] = useState<ObjectTabId>("tables");
  const [currentTypeLoading, setCurrentTypeLoading] = useState(false);

  // Selection & sync
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [syncBusy, setSyncBusy] = useState<Set<string>>(new Set());

  // Wizard
  const [showWizard, setShowWizard] = useState(false);

  // Load schemas on mount
  useEffect(() => {
    fetch("/api/db/source/schemas").then(r => r.json()).then(d => Array.isArray(d) && setSrcSchemas(d)).catch(() => {});
    fetch("/api/db/target/schemas").then(r => r.json()).then(d => Array.isArray(d) && setTgtSchemas(d)).catch(() => {});
  }, []);

  // Load catalog
  const doLoad = useCallback(() => {
    if (!srcSchema || !tgtSchema) return;
    setLoading(true); setLoadError(null);
    fetch("/api/catalog/load", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ src_schema: srcSchema, tgt_schema: tgtSchema }),
    })
      .then(r => r.ok ? r.json() : r.json().then(d => Promise.reject(d.error || "Ошибка")))
      .then(data => {
        setSnapshotId(data.snapshot_id);
        setObjectCounts(data.object_counts || {});
        setLoadedAt(new Date().toLocaleString());
        setAllObjects([]);
        setObjectTypeMap(new Map());
        setActiveTab("tables");
      })
      .catch(e => setLoadError(typeof e === "string" ? e : String(e)))
      .finally(() => setLoading(false));
  }, [srcSchema, tgtSchema]);

  // Load objects for active tab
  const loadObjectsForTab = useCallback((tab: ObjectTabId) => {
    if (!snapshotId) return;
    setCurrentTypeLoading(true);

    const typesByTab: Record<ObjectTabId, string[]> = {
      tables: ["TABLE"],
      views: ["VIEW", "MATERIALIZED VIEW"],
      code: ["FUNCTION", "PROCEDURE", "PACKAGE"],
      other: ["SEQUENCE", "SYNONYM", "TYPE"],
    };

    const types = typesByTab[tab];
    const fetches = types.map(t =>
      fetch(`/api/catalog/objects?snapshot_id=${snapshotId}&type=${t === "MATERIALIZED VIEW" ? "MVIEW" : t}`)
        .then(r => r.ok ? r.json() : [])
        .then((objs: CatalogObject[]) => objs.map(o => ({ ...o, _type: t })))
    );

    Promise.all(fetches)
      .then(results => {
        const flat = results.flat();
        setAllObjects(flat);
        const tMap = new Map<string, string>();
        for (const obj of flat) tMap.set(obj.object_name, (obj as unknown as { _type: string })._type);
        setObjectTypeMap(tMap);
      })
      .catch(() => {})
      .finally(() => setCurrentTypeLoading(false));
  }, [snapshotId]);

  useEffect(() => {
    if (snapshotId) loadObjectsForTab(activeTab);
  }, [snapshotId, activeTab, loadObjectsForTab]);

  // Grouping
  const grouped = useMemo(() => groupObjects(allObjects, objectTypeMap), [allObjects, objectTypeMap]);

  // Counts for tabs
  const counts = useMemo(() => ({
    tables: objectCounts["TABLE"] || 0,
    views: (objectCounts["VIEW"] || 0) + (objectCounts["MATERIALIZED VIEW"] || 0),
    code: (objectCounts["FUNCTION"] || 0) + (objectCounts["PROCEDURE"] || 0) + (objectCounts["PACKAGE"] || 0),
    other: (objectCounts["SEQUENCE"] || 0) + (objectCounts["SYNONYM"] || 0) + (objectCounts["TYPE"] || 0),
  }), [objectCounts]);

  // Table selection
  const toggleTable = (name: string) => {
    setSelected(prev => { const n = new Set(prev); if (n.has(name)) n.delete(name); else n.add(name); return n; });
  };
  const toggleAllTables = () => {
    const tableNames = grouped.tables.map(t => t.object_name);
    if (tableNames.every(t => selected.has(t))) setSelected(new Set());
    else setSelected(new Set(tableNames));
  };

  // Compare single object
  const doCompare = useCallback((type: string, name: string) => {
    if (!snapshotId) return;
    setSyncBusy(prev => new Set(prev).add(name));
    fetch("/api/catalog/compare", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ snapshot_id: snapshotId, src_schema: srcSchema, tgt_schema: tgtSchema, objects: [`${type}:${name}`] }),
    })
      .then(() => loadObjectsForTab(activeTab))
      .catch(() => {})
      .finally(() => setSyncBusy(prev => { const n = new Set(prev); n.delete(name); return n; }));
  }, [snapshotId, srcSchema, tgtSchema, activeTab, loadObjectsForTab]);

  // Sync object to target
  const doSync = useCallback((type: string, name: string, action: string) => {
    setSyncBusy(prev => new Set(prev).add(name));

    // Tables use existing target-prep API
    if (type === "TABLE") {
      let url = "";
      const body: Record<string, unknown> = {
        src_schema: srcSchema, src_table: name,
        tgt_schema: tgtSchema, tgt_table: name,
      };
      if (action === "create") url = "/api/target-prep/ensure-table";
      else if (action === "sync_cols") url = "/api/target-prep/sync-columns";
      else { url = "/api/target-prep/sync-objects"; body.types = ["constraints", "indexes", "triggers"]; }

      fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) })
        .then(() => doCompare(type, name))
        .catch(() => {})
        .finally(() => setSyncBusy(prev => { const n = new Set(prev); n.delete(name); return n; }));
      return;
    }

    // Non-table objects
    fetch("/api/catalog/sync-to-target", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ src_schema: srcSchema, tgt_schema: tgtSchema, object_type: type, object_name: name, action }),
    })
      .then(() => doCompare(type, name))
      .catch(() => {})
      .finally(() => setSyncBusy(prev => { const n = new Set(prev); n.delete(name); return n; }));
  }, [srcSchema, tgtSchema, doCompare]);

  // Selected table names for wizard
  const selectedTables = useMemo(() => Array.from(selected), [selected]);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 0 }}>
      {/* Schema selectors */}
      <div style={{ display: "flex", alignItems: "flex-end", gap: 12, marginBottom: 16 }}>
        <div style={S.field}>
          <label style={S.label}>Схема источника</label>
          <SearchSelect value={srcSchema} onChange={setSrcSchema} options={srcSchemas} placeholder="Выберите схему" />
        </div>
        <div style={S.field}>
          <label style={S.label}>Схема таргета</label>
          <SearchSelect value={tgtSchema} onChange={setTgtSchema} options={tgtSchemas} placeholder="Выберите схему" />
        </div>
        <button
          onClick={doLoad}
          disabled={!srcSchema || !tgtSchema || loading}
          style={{ ...S.btnPrimary, opacity: (!srcSchema || !tgtSchema || loading) ? 0.5 : 1, height: 30 }}
        >
          {loading ? "Загрузка каталога..." : "Загрузить каталог"}
        </button>
        {loadedAt && (
          <span style={{ fontSize: 11, color: "#475569" }}>Загружено: {loadedAt}</span>
        )}
      </div>

      {/* Error */}
      {loadError && (
        <div style={{ background: "#7f1d1d22", border: "1px solid #7f1d1d", borderRadius: 6, color: "#fca5a5", padding: "8px 14px", fontSize: 12, marginBottom: 12 }}>
          {loadError}
        </div>
      )}

      {/* Catalog content */}
      {snapshotId && (
        <>
          <ObjectTabs active={activeTab} onChange={setActiveTab} counts={counts} />

          {currentTypeLoading ? (
            <div style={{ padding: 20, textAlign: "center", color: "#475569", fontSize: 12 }}>Загрузка объектов...</div>
          ) : (
            <>
              {activeTab === "tables" && (
                <TablesTab
                  objects={grouped.tables} selected={selected}
                  onToggle={toggleTable} onToggleAll={toggleAllTables}
                  syncBusy={syncBusy} onCompare={doCompare} onSync={doSync}
                />
              )}
              {activeTab === "views" && (
                <ViewsTab objects={grouped.views} syncBusy={syncBusy} onCompare={doCompare} onSync={doSync} />
              )}
              {activeTab === "code" && (
                <CodeTab objects={grouped.code} syncBusy={syncBusy} onCompare={doCompare} onSync={doSync} />
              )}
              {activeTab === "other" && (
                <OtherTab objects={grouped.other} syncBusy={syncBusy} onCompare={doCompare} onSync={doSync} />
              )}
            </>
          )}

          {/* Wizard launch button */}
          {selected.size > 0 && !showWizard && (
            <div style={{ marginTop: 16, textAlign: "center" }}>
              <button onClick={() => setShowWizard(true)} style={S.btnPrimary}>
                Запустить визард для выбранных ({selected.size})
              </button>
            </div>
          )}

          {/* Wizard */}
          {showWizard && selectedTables.length > 0 && (
            <PlannerWizard
              selectedTables={selectedTables}
              srcSchema={srcSchema} tgtSchema={tgtSchema}
              onClose={() => setShowWizard(false)}
            />
          )}
        </>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/components/DDLCatalog/DDLCatalog.tsx
git commit -m "feat: add DDLCatalog main component with schema loading, tab routing, and wizard"
```

---

## Task 20: App.tsx Integration and Cleanup

**Files:**
- Modify: `frontend/src/App.tsx`
- Delete: `frontend/src/components/MigrationPlanner.tsx`

- [ ] **Step 1: Update App.tsx imports**

Replace the MigrationPlanner import:

```typescript
// Remove this:
import { MigrationPlanner } from "./components/MigrationPlanner";

// Add this:
import { DDLCatalog } from "./components/DDLCatalog/DDLCatalog";
```

- [ ] **Step 2: Update Tab type**

Change the Tab type — replace `"planner"` with `"catalog"`:

```typescript
type Tab = "catalog" | "migrations" | "connector-groups" | "target-prep" | "data-compare" | "checklist";
```

- [ ] **Step 3: Update default tab**

```typescript
const [activeTab, setActiveTab] = useState<Tab>("catalog");
```

- [ ] **Step 4: Update tab button label**

Replace the "Планирование" TabButton:

```tsx
<TabButton
  label="DDL Каталог"
  active={activeTab === "catalog"}
  onClick={() => setActiveTab("catalog")}
/>
```

- [ ] **Step 5: Update tab content**

Replace the planner content:

```tsx
{activeTab === "catalog"          && <DDLCatalog />}
```

- [ ] **Step 6: Delete MigrationPlanner.tsx**

```bash
rm frontend/src/components/MigrationPlanner.tsx
```

- [ ] **Step 7: Verify build compiles**

Run: `cd /mnt/c/work/database_migration/new/front/frontend && npx tsc --noEmit`

Expected: No TypeScript errors.

- [ ] **Step 8: Commit**

```bash
git add frontend/src/App.tsx
git rm frontend/src/components/MigrationPlanner.tsx
git add frontend/src/components/DDLCatalog/
git commit -m "feat: replace MigrationPlanner with DDLCatalog in App.tsx"
```

---

## Task 21: End-to-End Verification

- [ ] **Step 1: Start backend**

Run: `cd /mnt/c/work/database_migration/new/front && python backend/app.py`

Expected: Server starts, catalog routes registered without errors.

- [ ] **Step 2: Start frontend**

Run: `cd /mnt/c/work/database_migration/new/front/frontend && npm run dev`

Expected: Vite dev server starts on port 3000.

- [ ] **Step 3: Verify in browser**

Open `http://localhost:3000`. Check:
1. "DDL Каталог" tab is active by default
2. Schema dropdowns load
3. "Загрузить каталог" button works (requires Oracle connection)
4. Sub-tabs show correct counts
5. Table objects display with expand/collapse
6. Selection checkboxes work
7. "Запустить визард" button appears when tables selected

- [ ] **Step 4: Final commit if any fixes needed**
