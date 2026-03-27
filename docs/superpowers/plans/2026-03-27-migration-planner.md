# Migration Planner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a 4-step wizard ("Планирование") that unifies schema comparison, table selection, connector group + migration creation, and batch ordering into one pipeline.

**Architecture:** New backend blueprint `routes/planner.py` with 5 endpoints, reusing existing `oracle_browser`, `target_prep`, and `migrations` logic. New React component `MigrationPlanner.tsx` as a wizard with step navigation. Two new PostgreSQL tables (`migration_plans`, `migration_plan_items`) for plan persistence.

**Tech Stack:** Flask (Python), React 18 + TypeScript, PostgreSQL 16, inline CSS-in-JS (project convention)

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `backend/routes/planner.py` | Blueprint: compare-schema, fk-deps, execute, plan CRUD, start |
| Modify | `backend/db/state_db.py` | Add `migration_plans` + `migration_plan_items` tables |
| Modify | `backend/app.py` | Register planner blueprint |
| Create | `frontend/src/components/MigrationPlanner.tsx` | Wizard UI: 4 steps |
| Modify | `frontend/src/App.tsx` | Add "Планирование" tab |

---

### Task 1: PostgreSQL schema — plan tables

**Files:**
- Modify: `backend/db/state_db.py` (inside `init_db()`, after the `data_compare_chunks` block ~line 509)

- [ ] **Step 1: Add migration_plans and migration_plan_items tables**

Add the following SQL block inside `init_db()`, right before the `group_id FK on migrations` comment (line 511):

```python
            # ── migration_plans ───────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS migration_plans (
                    plan_id         SERIAL PRIMARY KEY,
                    name            TEXT NOT NULL,
                    src_schema      TEXT NOT NULL,
                    tgt_schema      TEXT NOT NULL,
                    connector_group_id UUID REFERENCES connector_groups(group_id),
                    defaults_json   JSONB NOT NULL DEFAULT '{}',
                    status          TEXT NOT NULL DEFAULT 'DRAFT',
                    created_at      TIMESTAMPTZ DEFAULT NOW(),
                    started_at      TIMESTAMPTZ,
                    completed_at    TIMESTAMPTZ
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS migration_plan_items (
                    item_id         SERIAL PRIMARY KEY,
                    plan_id         INTEGER NOT NULL REFERENCES migration_plans(plan_id) ON DELETE CASCADE,
                    table_name      TEXT NOT NULL,
                    mode            TEXT NOT NULL DEFAULT 'CDC',
                    batch_order     INTEGER NOT NULL DEFAULT 1,
                    sort_order      INTEGER NOT NULL DEFAULT 0,
                    overrides_json  JSONB NOT NULL DEFAULT '{}',
                    migration_id    UUID REFERENCES migrations(migration_id),
                    status          TEXT NOT NULL DEFAULT 'PENDING'
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_mpi_plan_id
                    ON migration_plan_items(plan_id)
            """)
```

- [ ] **Step 2: Verify by restarting backend**

Run: `cd c:/work/database_migration/new/front && python backend/app.py`

Expected: `[state_db] schema init complete` in logs, no errors.

- [ ] **Step 3: Commit**

```bash
git add backend/db/state_db.py
git commit -m "feat(planner): add migration_plans + migration_plan_items tables"
```

---

### Task 2: Backend — planner blueprint (compare-schema + fk-deps)

**Files:**
- Create: `backend/routes/planner.py`

- [ ] **Step 1: Create planner.py with compare-schema endpoint**

```python
"""Migration Planner API — schema comparison, plan CRUD, batch execution."""

import uuid
import json
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request
from db.oracle_browser import get_oracle_conn, get_full_ddl_info, list_tables

bp = Blueprint("planner", __name__)

_state: dict = {}


def init(*, get_conn_fn, row_to_dict_fn, load_configs_fn, broadcast_fn):
    _state["get_conn"] = get_conn_fn
    _state["row_to_dict"] = row_to_dict_fn
    _state["load_configs"] = load_configs_fn
    _state["broadcast"] = broadcast_fn


# ── helpers (reused from target_prep) ────────────────────────────────────────

def _diff_summary(src: dict, tgt: dict) -> dict:
    tgt_col = {c["name"] for c in tgt["columns"]}
    src_col_map = {c["name"]: c for c in src["columns"]}
    tgt_col_map = {c["name"]: c for c in tgt["columns"]}

    cols_missing = sum(1 for c in src["columns"] if c["name"] not in tgt_col)
    cols_extra = sum(1 for c in tgt["columns"] if c["name"] not in src_col_map)
    cols_type = sum(
        1 for c in src["columns"]
        if c["name"] in tgt_col_map
        and c["data_type"] != tgt_col_map[c["name"]]["data_type"]
    )

    def _idx_key(i: dict) -> tuple:
        return (i["unique"], ",".join(i["columns"]))

    tgt_idx_names = {i["name"] for i in tgt["indexes"]}
    tgt_idx_keys = {_idx_key(i) for i in tgt["indexes"]}
    idx_missing = sum(
        1 for i in src["indexes"]
        if i["name"] not in tgt_idx_names and _idx_key(i) not in tgt_idx_keys
    )
    idx_disabled = sum(1 for i in tgt["indexes"] if i["status"] != "VALID")

    tgt_con_keys = {(c["type_code"], ",".join(c["columns"])) for c in tgt["constraints"]}
    con_missing = sum(
        1 for c in src["constraints"]
        if (c["type_code"], ",".join(c["columns"])) not in tgt_con_keys
    )
    con_disabled = sum(
        1 for c in tgt["constraints"]
        if c["status"] == "DISABLED" and c["type_code"] != "P"
    )

    tgt_trg = {t["name"] for t in tgt["triggers"]}
    trg_missing = sum(1 for t in src["triggers"] if t["name"] not in tgt_trg)

    total = (cols_missing + cols_extra + cols_type + idx_missing
             + idx_disabled + con_missing + con_disabled + trg_missing)
    return {
        "ok": total == 0,
        "total": total,
        "cols_missing": cols_missing,
        "cols_extra": cols_extra,
        "cols_type": cols_type,
        "idx_missing": idx_missing,
        "idx_disabled": idx_disabled,
        "con_missing": con_missing,
        "con_disabled": con_disabled,
        "trg_missing": trg_missing,
    }


# ── Step 1: Schema comparison ────────────────────────────────────────────────

@bp.get("/api/planner/compare-schema")
def compare_schema():
    src_schema = request.args.get("src_schema", "").strip().upper()
    tgt_schema = request.args.get("tgt_schema", "").strip().upper()
    if not src_schema or not tgt_schema:
        return jsonify({"error": "src_schema and tgt_schema required"}), 400

    configs = _state["load_configs"]()
    try:
        src_conn = get_oracle_conn("source", configs)
        tgt_conn = get_oracle_conn("target", configs)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503

    try:
        src_tables = list_tables(src_conn, src_schema)
        tgt_tables_set = set(list_tables(tgt_conn, tgt_schema))

        results = []
        for tbl in src_tables:
            if tbl not in tgt_tables_set:
                results.append({
                    "table": tbl,
                    "exists_in_target": False,
                    "diff": None,
                    "error": None,
                })
                continue
            try:
                si = get_full_ddl_info(src_conn, src_schema, tbl)
                ti = get_full_ddl_info(tgt_conn, tgt_schema, tbl)
                results.append({
                    "table": tbl,
                    "exists_in_target": True,
                    "diff": _diff_summary(si, ti),
                    "error": None,
                })
            except Exception as exc:
                results.append({
                    "table": tbl,
                    "exists_in_target": True,
                    "diff": None,
                    "error": str(exc)[:120],
                })

        return jsonify(results)
    finally:
        src_conn.close()
        tgt_conn.close()


# ── Step 3: FK dependency detection ──────────────────────────────────────────

@bp.get("/api/planner/fk-dependencies")
def fk_dependencies():
    schema = request.args.get("schema", "").strip().upper()
    tables_csv = request.args.get("tables", "").strip().upper()
    if not schema or not tables_csv:
        return jsonify({"error": "schema and tables required"}), 400

    table_set = set(t.strip() for t in tables_csv.split(",") if t.strip())

    configs = _state["load_configs"]()
    try:
        conn = get_oracle_conn("source", configs)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503

    try:
        deps: dict[str, set] = {t: set() for t in table_set}
        with conn.cursor() as cur:
            placeholders = ",".join(f":t{i}" for i in range(len(table_set)))
            bind = {f"t{i}": t for i, t in enumerate(table_set)}
            bind["s"] = schema
            cur.execute(f"""
                SELECT ac.table_name,
                       rc.table_name AS ref_table
                FROM   all_constraints ac
                JOIN   all_constraints rc
                       ON ac.r_constraint_name = rc.constraint_name
                       AND ac.r_owner = rc.owner
                WHERE  ac.owner = :s
                  AND  ac.constraint_type = 'R'
                  AND  ac.table_name IN ({placeholders})
                  AND  rc.table_name IN ({placeholders})
            """, bind)
            for row in cur.fetchall():
                child, parent = row[0], row[1]
                if child != parent:
                    deps[child].add(parent)

        result = [
            {"table": t, "depends_on": sorted(d)}
            for t, d in deps.items() if d
        ]
        return jsonify(result)
    finally:
        conn.close()
```

- [ ] **Step 2: Verify syntax**

Run: `cd c:/work/database_migration/new/front && python -c "import backend.routes.planner"`

Expected: No import errors.

- [ ] **Step 3: Commit**

```bash
git add backend/routes/planner.py
git commit -m "feat(planner): add compare-schema and fk-dependencies endpoints"
```

---

### Task 3: Backend — planner blueprint (plan CRUD + execute + start)

**Files:**
- Modify: `backend/routes/planner.py` (append to end)

- [ ] **Step 1: Add plan CRUD, execute, and start endpoints**

Append to `backend/routes/planner.py`:

```python
# ── Plan CRUD ─────────────────────────────────────────────────────────────────

@bp.get("/api/planner/plans")
def list_plans():
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT p.*,
                       COUNT(i.item_id) AS item_count,
                       COUNT(i.item_id) FILTER (WHERE i.status = 'DONE') AS items_done
                FROM migration_plans p
                LEFT JOIN migration_plan_items i ON i.plan_id = p.plan_id
                GROUP BY p.plan_id
                ORDER BY p.created_at DESC
            """)
            rows = [_state["row_to_dict"](cur, r) for r in cur.fetchall()]
        return jsonify(rows)
    finally:
        conn.close()


@bp.get("/api/planner/plans/<int:plan_id>")
def get_plan(plan_id):
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM migration_plans WHERE plan_id = %s", (plan_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Plan not found"}), 404
            plan = _state["row_to_dict"](cur, row)

            cur.execute("""
                SELECT * FROM migration_plan_items
                WHERE plan_id = %s ORDER BY batch_order, sort_order
            """, (plan_id,))
            plan["items"] = [_state["row_to_dict"](cur, r) for r in cur.fetchall()]
        return jsonify(plan)
    finally:
        conn.close()


@bp.delete("/api/planner/plans/<int:plan_id>")
def delete_plan(plan_id):
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM migration_plans WHERE plan_id = %s", (plan_id,))
        conn.commit()
        return jsonify({"ok": True})
    finally:
        conn.close()


# ── Execute plan (create migrations) ─────────────────────────────────────────

@bp.post("/api/planner/execute")
def execute_plan():
    """Create a plan + all its migrations in DRAFT phase."""
    data = request.json or {}
    src_schema = data.get("src_schema", "").strip().upper()
    tgt_schema = data.get("tgt_schema", "").strip().upper()
    plan_name = data.get("name", "").strip() or f"Plan {src_schema}->{tgt_schema}"
    batches = data.get("batches", [])
    defaults = data.get("defaults", {})
    connector_group_id = data.get("connector_group_id")
    create_group = data.get("create_connector_group")

    if not src_schema or not tgt_schema or not batches:
        return jsonify({"error": "src_schema, tgt_schema, batches required"}), 400

    conn = _state["get_conn"]()
    configs = _state["load_configs"]()
    now = datetime.now(timezone.utc).isoformat()

    try:
        with conn.cursor() as cur:
            # Optionally create connector group
            group_id = None
            if connector_group_id:
                group_id = connector_group_id
            elif create_group:
                group_id = str(uuid.uuid4())
                gname = create_group.get("group_name", f"plan_{src_schema.lower()}")
                tprefix = create_group.get("topic_prefix", f"{src_schema.lower()}")
                cname = create_group.get("connector_name", f"{gname}_connector")
                cprefix = create_group.get("consumer_group_prefix", f"{gname}_cg")
                cur.execute("""
                    INSERT INTO connector_groups
                        (group_id, group_name, source_connection_id,
                         connector_name, topic_prefix, consumer_group_prefix)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (group_id, gname, "oracle_source",
                      cname, tprefix, cprefix))

            # Create plan
            cur.execute("""
                INSERT INTO migration_plans
                    (name, src_schema, tgt_schema, connector_group_id, defaults_json, status)
                VALUES (%s, %s, %s, %s, %s, 'DRAFT')
                RETURNING plan_id
            """, (plan_name, src_schema, tgt_schema, group_id,
                  json.dumps(defaults)))
            plan_id = cur.fetchone()[0]

            # Create migrations + plan items
            items_created = []
            for batch in batches:
                batch_order = batch.get("order", 1)
                for idx, tbl_cfg in enumerate(batch.get("tables", [])):
                    table_name = tbl_cfg.get("table", "").strip().upper()
                    mode = tbl_cfg.get("mode", defaults.get("mode", "CDC"))
                    overrides = tbl_cfg.get("overrides", {})

                    # Merge defaults with overrides
                    chunk_size = overrides.get("chunk_size", defaults.get("chunk_size", 1_000_000))
                    max_workers = overrides.get("max_parallel_workers", defaults.get("max_parallel_workers", 1))
                    strategy = overrides.get("migration_strategy", defaults.get("migration_strategy", "STAGE"))
                    baseline_pd = overrides.get("baseline_parallel_degree", defaults.get("baseline_parallel_degree", 4))

                    mid = str(uuid.uuid4())

                    # Build connector fields for CDC
                    connector_name = ""
                    topic_prefix = ""
                    consumer_group = ""
                    if mode == "CDC" and group_id:
                        # Will be filled by group start process
                        connector_name = ""
                        topic_prefix = ""
                        consumer_group = ""

                    cur.execute("""
                        INSERT INTO migrations (
                            migration_id, migration_name, phase, state_changed_at,
                            source_connection_id, target_connection_id,
                            source_schema, source_table,
                            target_schema, target_table,
                            chunk_size, max_parallel_workers,
                            baseline_parallel_degree,
                            migration_strategy, migration_mode,
                            group_id,
                            created_at, updated_at
                        ) VALUES (
                            %s, %s, 'DRAFT', %s,
                            'oracle_source', 'oracle_target',
                            %s, %s,
                            %s, %s,
                            %s, %s,
                            %s,
                            %s, %s,
                            %s,
                            %s, %s
                        )
                    """, (
                        mid, f"{src_schema}.{table_name}", now,
                        src_schema, table_name,
                        tgt_schema, table_name,
                        chunk_size, max(1, int(max_workers)),
                        max(1, int(baseline_pd)),
                        strategy, mode,
                        group_id if mode == "CDC" else None,
                        now, now,
                    ))

                    # Record state history
                    cur.execute("""
                        INSERT INTO migration_state_history
                            (migration_id, from_phase, to_phase, message, actor_type)
                        VALUES (%s, NULL, 'DRAFT', %s, 'USER')
                    """, (mid, f"Created by planner (plan {plan_id})"))

                    # Create plan item
                    cur.execute("""
                        INSERT INTO migration_plan_items
                            (plan_id, table_name, mode, batch_order, sort_order,
                             overrides_json, migration_id, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, 'PENDING')
                    """, (plan_id, table_name, mode, batch_order, idx,
                          json.dumps(overrides), mid))

                    items_created.append({
                        "table": table_name,
                        "migration_id": mid,
                        "batch_order": batch_order,
                        "mode": mode,
                    })

            # Mark plan as READY
            cur.execute("""
                UPDATE migration_plans SET status = 'READY'
                WHERE plan_id = %s
            """, (plan_id,))

        conn.commit()

        return jsonify({
            "plan_id": plan_id,
            "items": items_created,
            "connector_group_id": str(group_id) if group_id else None,
        })
    except Exception as exc:
        conn.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


# ── Start plan (DRAFT -> NEW by batches) ──────────────────────────────────────

@bp.post("/api/planner/plans/<int:plan_id>/start")
def start_plan(plan_id):
    """Start first batch: transition DRAFT -> NEW for batch_order = 1."""
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM migration_plans WHERE plan_id = %s", (plan_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Plan not found"}), 404
            plan = _state["row_to_dict"](cur, row)

            if plan["status"] not in ("READY", "RUNNING"):
                return jsonify({"error": f"Cannot start plan in status {plan['status']}"}), 400

            # Find the lowest batch that has PENDING items
            cur.execute("""
                SELECT DISTINCT batch_order
                FROM migration_plan_items
                WHERE plan_id = %s AND status = 'PENDING'
                ORDER BY batch_order
                LIMIT 1
            """, (plan_id,))
            batch_row = cur.fetchone()
            if not batch_row:
                return jsonify({"error": "No pending batches"}), 400
            next_batch = batch_row[0]

            # Get items for this batch
            cur.execute("""
                SELECT item_id, migration_id
                FROM migration_plan_items
                WHERE plan_id = %s AND batch_order = %s AND status = 'PENDING'
            """, (plan_id, next_batch))
            items = cur.fetchall()

            now = datetime.now(timezone.utc).isoformat()
            started_ids = []
            for item_id, migration_id in items:
                # Transition migration DRAFT -> NEW
                cur.execute("""
                    UPDATE migrations SET phase = 'NEW', state_changed_at = %s, updated_at = %s
                    WHERE migration_id = %s AND phase = 'DRAFT'
                """, (now, now, str(migration_id)))

                cur.execute("""
                    INSERT INTO migration_state_history
                        (migration_id, from_phase, to_phase, message, actor_type)
                    VALUES (%s, 'DRAFT', 'NEW', %s, 'USER')
                """, (str(migration_id), f"Started by planner (batch {next_batch})"))

                cur.execute("""
                    UPDATE migration_plan_items SET status = 'RUNNING'
                    WHERE item_id = %s
                """, (item_id,))
                started_ids.append(str(migration_id))

            # Update plan status
            cur.execute("""
                UPDATE migration_plans SET status = 'RUNNING', started_at = COALESCE(started_at, %s)
                WHERE plan_id = %s
            """, (now, plan_id))

        conn.commit()

        # Broadcast phase changes
        broadcast = _state["broadcast"]
        for mid in started_ids:
            broadcast({
                "type": "migration_phase",
                "migration_id": mid,
                "phase": "NEW",
            })

        return jsonify({
            "batch": next_batch,
            "started": started_ids,
        })
    finally:
        conn.close()
```

- [ ] **Step 2: Verify syntax**

Run: `cd c:/work/database_migration/new/front && python -c "import backend.routes.planner"`

Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add backend/routes/planner.py
git commit -m "feat(planner): add plan CRUD, execute, and start endpoints"
```

---

### Task 4: Backend — register planner blueprint in app.py

**Files:**
- Modify: `backend/app.py` (after connector groups blueprint, ~line 170)

- [ ] **Step 1: Add planner blueprint registration**

After the connector groups block (line 170 `app.register_blueprint(cg_bp)`), add:

```python
# ── Planner blueprint ────────────────────────────────────────────────────────
import routes.planner as planner_mod
from routes.planner import bp as planner_bp

planner_mod.init(
    get_conn_fn=get_conn,
    row_to_dict_fn=row_to_dict,
    load_configs_fn=_load_cfg,
    broadcast_fn=broadcast,
)
app.register_blueprint(planner_bp)
```

- [ ] **Step 2: Verify backend starts**

Run: `cd c:/work/database_migration/new/front && python backend/app.py`

Expected: Server starts, `schema init complete` in logs.

- [ ] **Step 3: Commit**

```bash
git add backend/app.py
git commit -m "feat(planner): register planner blueprint"
```

---

### Task 5: Frontend — MigrationPlanner wizard (Step 1: Schema Compare)

**Files:**
- Create: `frontend/src/components/MigrationPlanner.tsx`

- [ ] **Step 1: Create MigrationPlanner.tsx with Step 1 (Schema Compare)**

```tsx
import React, { useState, useEffect, useCallback, useRef } from "react";

// ── Types ────────────────────────────────────────────────────────────────────

interface DiffSummary {
  ok: boolean;
  total: number;
  cols_missing: number;
  cols_extra: number;
  cols_type: number;
  idx_missing: number;
  idx_disabled: number;
  con_missing: number;
  con_disabled: number;
  trg_missing: number;
}

interface TableCompare {
  table: string;
  exists_in_target: boolean;
  diff: DiffSummary | null;
  error: string | null;
}

interface ColInfo {
  name: string;
  data_type: string;
  data_length: number | null;
  data_precision: number | null;
  data_scale: number | null;
  nullable: boolean;
  data_default: string | null;
  column_id: number;
}

interface Constraint { name: string; type: string; type_code: string; status: string; columns: string[] }
interface OraIndex { name: string; unique: boolean; index_type: string; status: string; columns: string[] }
interface Trigger { name: string; trigger_type: string; event: string; status: string }

interface TableDDL {
  schema: string; table: string;
  columns: ColInfo[]; constraints: Constraint[];
  indexes: OraIndex[]; triggers: Trigger[];
}

interface DDLData { source: TableDDL; target: TableDDL }

interface BatchItem { table: string; mode: "CDC" | "BULK_ONLY"; overrides: Record<string, unknown> }
interface Batch { order: number; tables: BatchItem[] }

interface PlanDefaults {
  chunk_size: number;
  max_parallel_workers: number;
  migration_strategy: "STAGE" | "DIRECT";
  mode: "CDC" | "BULK_ONLY";
  baseline_parallel_degree: number;
}

interface FKDep { table: string; depends_on: string[] }

type WizardStep = 1 | 2 | 3 | 4;
type StatusFilter = "all" | "diff" | "missing" | "ok" | "error";

// ── Style tokens ─────────────────────────────────────────────────────────────

const S = {
  card: { background: "#0f172a", border: "1px solid #1e293b", borderRadius: 8, overflow: "hidden" as const },
  headerBar: { padding: "12px 16px", borderBottom: "1px solid #1e293b", display: "flex" as const, alignItems: "center" as const, gap: 12 },
  input: { background: "#1e293b", border: "1px solid #334155", borderRadius: 5, color: "#e2e8f0", padding: "7px 10px", fontSize: 13, width: "100%", outline: "none" } as React.CSSProperties,
  select: { background: "#1e293b", border: "1px solid #334155", borderRadius: 5, color: "#e2e8f0", padding: "7px 10px", fontSize: 13, width: "100%", cursor: "pointer", outline: "none" } as React.CSSProperties,
  label: { fontSize: 11, color: "#64748b", fontWeight: 600, letterSpacing: 0.3, marginBottom: 4 } as React.CSSProperties,
  btn: (bg: string, color = "#e2e8f0") => ({
    background: bg, border: "1px solid #334155", borderRadius: 6,
    color, padding: "6px 14px", fontSize: 12, fontWeight: 600,
    cursor: "pointer",
  }) as React.CSSProperties,
  btnSm: (bg: string, color = "#e2e8f0") => ({
    background: bg, border: "none", borderRadius: 4,
    color, padding: "3px 10px", fontSize: 11, fontWeight: 600,
    cursor: "pointer",
  }) as React.CSSProperties,
};

// ── Helpers ──────────────────────────────────────────────────────────────────

function fmtType(c: ColInfo): string {
  if (c.data_precision != null) {
    return c.data_scale ? `${c.data_type}(${c.data_precision},${c.data_scale})` : `${c.data_type}(${c.data_precision})`;
  }
  if (["VARCHAR2", "CHAR", "NVARCHAR2", "RAW"].includes(c.data_type) && c.data_length) {
    return `${c.data_type}(${c.data_length})`;
  }
  return c.data_type;
}

function statusLabel(t: TableCompare): { text: string; color: string; bg: string } {
  if (t.error) return { text: "Ошибка", color: "#fca5a5", bg: "#450a0a" };
  if (!t.exists_in_target) return { text: "Нет в target", color: "#fbbf24", bg: "#422006" };
  if (t.diff?.ok) return { text: "OK", color: "#86efac", bg: "#052e16" };
  return { text: `Различий: ${t.diff?.total ?? "?"}`, color: "#fca5a5", bg: "#450a0a" };
}

function diffBadge(missing: number, disabled: number): React.ReactNode {
  if (!missing && !disabled) return <span style={{ color: "#22c55e", fontSize: 11 }}>ok</span>;
  const parts: React.ReactNode[] = [];
  if (missing) parts.push(<span key="m" style={{ color: "#ef4444" }}>+{missing}</span>);
  if (disabled) parts.push(<span key="d" style={{ color: "#eab308" }}>!{disabled}</span>);
  return <span style={{ fontSize: 11, display: "flex", gap: 4 }}>{parts}</span>;
}


// ── SearchSelect (reusable) ─────────────────────────────────────────────────

function SearchSelect({ value, options, onChange, placeholder }: {
  value: string; options: string[]; onChange: (v: string) => void; placeholder?: string;
}) {
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState("");
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const h = (e: MouseEvent) => { if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false); };
    document.addEventListener("mousedown", h);
    return () => document.removeEventListener("mousedown", h);
  }, []);

  const filtered = q ? options.filter(o => o.toLowerCase().includes(q.toLowerCase())) : options;

  return (
    <div ref={ref} style={{ position: "relative" }}>
      <div
        onClick={() => setOpen(!open)}
        style={{ ...S.input, cursor: "pointer", display: "flex", justifyContent: "space-between", alignItems: "center" }}
      >
        <span style={{ color: value ? "#e2e8f0" : "#475569" }}>{value || placeholder || "Выбрать..."}</span>
        <span style={{ color: "#475569", fontSize: 10 }}>&#9662;</span>
      </div>
      {open && (
        <div style={{
          position: "absolute", top: "calc(100% + 3px)", left: 0, right: 0, zIndex: 200,
          background: "#1e293b", border: "1px solid #334155", borderRadius: 4,
          boxShadow: "0 6px 20px rgba(0,0,0,0.5)", maxHeight: 260, overflow: "auto",
        }}>
          <div style={{ padding: "6px 8px", borderBottom: "1px solid #334155" }}>
            <input
              value={q} onChange={e => setQ(e.target.value)} placeholder="Поиск..."
              autoFocus
              style={{ ...S.input, border: "none", padding: "4px 6px", fontSize: 12, background: "transparent" }}
            />
          </div>
          {filtered.map(o => (
            <div
              key={o}
              onClick={() => { onChange(o); setOpen(false); setQ(""); }}
              style={{
                padding: "6px 10px", fontSize: 12, cursor: "pointer",
                background: o === value ? "#1d3a5f" : "transparent",
                color: o === value ? "#93c5fd" : "#e2e8f0",
              }}
              onMouseEnter={e => { if (o !== value) (e.target as HTMLElement).style.background = "#334155"; }}
              onMouseLeave={e => { if (o !== value) (e.target as HTMLElement).style.background = "transparent"; }}
            >
              {o}
            </div>
          ))}
          {filtered.length === 0 && <div style={{ padding: "8px 10px", color: "#475569", fontSize: 12 }}>Не найдено</div>}
        </div>
      )}
    </div>
  );
}


// ── Step Indicator ──────────────────────────────────────────────────────────

function StepIndicator({ current, onGo }: { current: WizardStep; onGo: (s: WizardStep) => void }) {
  const steps: { n: WizardStep; label: string }[] = [
    { n: 1, label: "Сравнение схем" },
    { n: 2, label: "Выбор и настройка" },
    { n: 3, label: "Очерёдность" },
    { n: 4, label: "Обзор и запуск" },
  ];
  return (
    <div style={{ display: "flex", gap: 0, marginBottom: 16 }}>
      {steps.map(({ n, label }, i) => (
        <div
          key={n}
          onClick={() => onGo(n)}
          style={{
            flex: 1, display: "flex", alignItems: "center", gap: 8,
            padding: "10px 16px", cursor: "pointer",
            background: n === current ? "#1e293b" : "transparent",
            borderBottom: `2px solid ${n === current ? "#3b82f6" : n < current ? "#22c55e" : "#334155"}`,
            transition: "all 0.15s",
          }}
        >
          <div style={{
            width: 22, height: 22, borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center",
            fontSize: 11, fontWeight: 700,
            background: n < current ? "#052e16" : n === current ? "#1e3a5f" : "#1e293b",
            color: n < current ? "#22c55e" : n === current ? "#93c5fd" : "#475569",
            border: `1px solid ${n < current ? "#22c55e" : n === current ? "#3b82f6" : "#334155"}`,
          }}>
            {n < current ? "✓" : n}
          </div>
          <span style={{
            fontSize: 12, fontWeight: n === current ? 700 : 500,
            color: n === current ? "#e2e8f0" : n < current ? "#86efac" : "#475569",
          }}>
            {label}
          </span>
        </div>
      ))}
    </div>
  );
}


// ── DDL Detail Panel (expandable row) ────────────────────────────────────────

function DDLDetail({ srcSchema, tgtSchema, table }: { srcSchema: string; tgtSchema: string; table: string }) {
  const [ddl, setDdl] = useState<DDLData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    fetch(`/api/target-prep/ddl?src_schema=${srcSchema}&src_table=${table}&tgt_schema=${tgtSchema}&tgt_table=${table}`)
      .then(r => r.ok ? r.json() : r.json().then(d => Promise.reject(d.error || "Error")))
      .then(d => { setDdl(d); setError(null); })
      .catch(e => setError(String(e)))
      .finally(() => setLoading(false));
  }, [srcSchema, tgtSchema, table]);

  if (loading) return <div style={{ padding: 16, color: "#475569", fontSize: 12 }}>Загрузка DDL...</div>;
  if (error) return <div style={{ padding: 16, color: "#fca5a5", fontSize: 12 }}>Ошибка: {error}</div>;
  if (!ddl) return null;

  const { source: src, target: tgt } = ddl;
  const srcMap = Object.fromEntries(src.columns.map(c => [c.name, c]));
  const tgtMap = Object.fromEntries(tgt.columns.map(c => [c.name, c]));
  const allCols = [...new Set([...src.columns.map(c => c.name), ...tgt.columns.map(c => c.name)])];

  return (
    <div style={{ padding: "12px 16px", background: "#080f1a", borderTop: "1px solid #1e293b" }}>
      {/* Columns */}
      <div style={{ marginBottom: 12 }}>
        <div style={{ fontSize: 11, fontWeight: 700, color: "#64748b", marginBottom: 6 }}>Колонки</div>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
          <thead>
            <tr style={{ borderBottom: "1px solid #1e293b" }}>
              <th style={thSt}>Имя</th><th style={thSt}>Source</th><th style={thSt}>Target</th><th style={thSt}>Статус</th>
            </tr>
          </thead>
          <tbody>
            {allCols.map(name => {
              const sc = srcMap[name], tc = tgtMap[name];
              let st: string, stColor: string;
              if (sc && !tc) { st = "Нет в target"; stColor = "#fca5a5"; }
              else if (!sc && tc) { st = "Лишняя"; stColor = "#fbbf24"; }
              else if (sc && tc && sc.data_type !== tc.data_type) { st = "Тип различается"; stColor = "#fbbf24"; }
              else { st = "OK"; stColor = "#22c55e"; }
              return (
                <tr key={name} style={{ borderBottom: "1px solid #0f1624" }}>
                  <td style={tdSt}>{name}</td>
                  <td style={{ ...tdSt, fontFamily: "monospace" }}>{sc ? fmtType(sc) : "—"}</td>
                  <td style={{ ...tdSt, fontFamily: "monospace" }}>{tc ? fmtType(tc) : "—"}</td>
                  <td style={{ ...tdSt, color: stColor }}>{st}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Indexes */}
      {(src.indexes.length > 0 || tgt.indexes.length > 0) && (
        <div style={{ marginBottom: 12 }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: "#64748b", marginBottom: 6 }}>Индексы</div>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead><tr style={{ borderBottom: "1px solid #1e293b" }}>
              <th style={thSt}>Имя</th><th style={thSt}>Колонки</th><th style={thSt}>Unique</th><th style={thSt}>В target</th>
            </tr></thead>
            <tbody>
              {src.indexes.map(si => {
                const ti = tgt.indexes.find(i => i.name === si.name);
                return (
                  <tr key={si.name} style={{ borderBottom: "1px solid #0f1624" }}>
                    <td style={tdSt}>{si.name}</td>
                    <td style={{ ...tdSt, fontFamily: "monospace" }}>{si.columns.join(", ")}</td>
                    <td style={tdSt}>{si.unique ? "Да" : ""}</td>
                    <td style={{ ...tdSt, color: ti ? "#22c55e" : "#fca5a5" }}>{ti ? ti.status : "Нет"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* Constraints */}
      {(src.constraints.length > 0 || tgt.constraints.length > 0) && (
        <div style={{ marginBottom: 12 }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: "#64748b", marginBottom: 6 }}>Constraints</div>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead><tr style={{ borderBottom: "1px solid #1e293b" }}>
              <th style={thSt}>Имя</th><th style={thSt}>Тип</th><th style={thSt}>Колонки</th><th style={thSt}>В target</th>
            </tr></thead>
            <tbody>
              {src.constraints.map(sc => {
                const tc = tgt.constraints.find(c => c.name === sc.name);
                return (
                  <tr key={sc.name} style={{ borderBottom: "1px solid #0f1624" }}>
                    <td style={tdSt}>{sc.name}</td>
                    <td style={tdSt}>{sc.type}</td>
                    <td style={{ ...tdSt, fontFamily: "monospace" }}>{sc.columns.join(", ")}</td>
                    <td style={{ ...tdSt, color: tc ? (tc.status === "ENABLED" ? "#22c55e" : "#fbbf24") : "#fca5a5" }}>
                      {tc ? tc.status : "Нет"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* Triggers */}
      {(src.triggers.length > 0 || tgt.triggers.length > 0) && (
        <div>
          <div style={{ fontSize: 11, fontWeight: 700, color: "#64748b", marginBottom: 6 }}>Триггеры</div>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead><tr style={{ borderBottom: "1px solid #1e293b" }}>
              <th style={thSt}>Имя</th><th style={thSt}>Тип</th><th style={thSt}>Событие</th><th style={thSt}>В target</th>
            </tr></thead>
            <tbody>
              {src.triggers.map(st => {
                const tt = tgt.triggers.find(t => t.name === st.name);
                return (
                  <tr key={st.name} style={{ borderBottom: "1px solid #0f1624" }}>
                    <td style={tdSt}>{st.name}</td>
                    <td style={tdSt}>{st.trigger_type}</td>
                    <td style={tdSt}>{st.event}</td>
                    <td style={{ ...tdSt, color: tt ? "#22c55e" : "#fca5a5" }}>{tt ? tt.status : "Нет"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

const thSt: React.CSSProperties = { padding: "6px 10px", textAlign: "left", color: "#64748b", fontSize: 11, fontWeight: 600 };
const tdSt: React.CSSProperties = { padding: "5px 10px", fontSize: 12 };


// ── SchemaCompareStep ────────────────────────────────────────────────────────

function SchemaCompareStep({
  srcSchema, setSrcSchema, tgtSchema, setTgtSchema,
  tables, setTables, selected, setSelected,
}: {
  srcSchema: string; setSrcSchema: (v: string) => void;
  tgtSchema: string; setTgtSchema: (v: string) => void;
  tables: TableCompare[]; setTables: (t: TableCompare[]) => void;
  selected: Set<string>; setSelected: (s: Set<string>) => void;
}) {
  const [srcSchemas, setSrcSchemas] = useState<string[]>([]);
  const [tgtSchemas, setTgtSchemas] = useState<string[]>([]);
  const [comparing, setComparing] = useState(false);
  const [filter, setFilter] = useState<StatusFilter>("all");
  const [search, setSearch] = useState("");
  const [expanded, setExpanded] = useState<string | null>(null);
  const [syncing, setSyncing] = useState<Set<string>>(new Set());

  // Load schemas on mount
  useEffect(() => {
    fetch("/api/db/source/schemas").then(r => r.json()).then(setSrcSchemas).catch(() => {});
    fetch("/api/db/target/schemas").then(r => r.json()).then(setTgtSchemas).catch(() => {});
  }, []);

  const runCompare = useCallback(() => {
    if (!srcSchema || !tgtSchema) return;
    setComparing(true);
    fetch(`/api/planner/compare-schema?src_schema=${srcSchema}&tgt_schema=${tgtSchema}`)
      .then(r => r.ok ? r.json() : Promise.reject("Error"))
      .then((data: TableCompare[]) => { setTables(data); setSelected(new Set()); })
      .catch(() => {})
      .finally(() => setComparing(false));
  }, [srcSchema, tgtSchema, setTables, setSelected]);

  const filtered = tables.filter(t => {
    if (search && !t.table.toLowerCase().includes(search.toLowerCase())) return false;
    if (filter === "diff") return t.exists_in_target && t.diff && !t.diff.ok;
    if (filter === "missing") return !t.exists_in_target;
    if (filter === "ok") return t.diff?.ok;
    if (filter === "error") return !!t.error;
    return true;
  });

  const toggleSelect = (table: string) => {
    const next = new Set(selected);
    next.has(table) ? next.delete(table) : next.add(table);
    setSelected(next);
  };

  const selectAllVisible = () => {
    const next = new Set(selected);
    filtered.forEach(t => next.add(t.table));
    setSelected(next);
  };

  const deselectAllVisible = () => {
    const visibleNames = new Set(filtered.map(t => t.table));
    const next = new Set([...selected].filter(s => !visibleNames.has(s)));
    setSelected(next);
  };

  const handleSync = (table: string, action: "ensure" | "sync") => {
    setSyncing(prev => new Set(prev).add(table));
    const url = action === "ensure" ? "/api/target-prep/ensure-table" : "/api/target-prep/sync-columns";
    const body = { src_schema: srcSchema, src_table: table, tgt_schema: tgtSchema, tgt_table: table };
    fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) })
      .then(r => r.json())
      .then(() => {
        // Also sync objects after columns
        if (action === "ensure" || action === "sync") {
          return fetch("/api/target-prep/sync-objects", {
            method: "POST", headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ ...body, types: ["constraints", "indexes", "triggers"] }),
          });
        }
      })
      .then(() => {
        // Re-compare this single table
        return fetch(`/api/target-prep/compare-summary`, {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });
      })
      .then(r => r?.json())
      .then(diff => {
        if (diff) {
          setTables(tables.map(t =>
            t.table === table ? { ...t, exists_in_target: true, diff, error: null } : t
          ));
        }
      })
      .finally(() => setSyncing(prev => { const n = new Set(prev); n.delete(table); return n; }));
  };

  const stats = {
    total: tables.length,
    ok: tables.filter(t => t.diff?.ok).length,
    diff: tables.filter(t => t.exists_in_target && t.diff && !t.diff.ok).length,
    missing: tables.filter(t => !t.exists_in_target).length,
    error: tables.filter(t => !!t.error).length,
  };

  const FILTERS: { key: StatusFilter; label: string; count: number }[] = [
    { key: "all", label: "Все", count: stats.total },
    { key: "diff", label: "Различаются", count: stats.diff },
    { key: "missing", label: "Нет в target", count: stats.missing },
    { key: "ok", label: "OK", count: stats.ok },
    { key: "error", label: "Ошибки", count: stats.error },
  ];

  return (
    <div>
      {/* Schema selectors */}
      <div style={{ display: "flex", gap: 12, marginBottom: 16, alignItems: "flex-end" }}>
        <div style={{ flex: 1 }}>
          <div style={S.label}>Source схема</div>
          <SearchSelect value={srcSchema} options={srcSchemas} onChange={setSrcSchema} placeholder="Выберите source" />
        </div>
        <div style={{ flex: 1 }}>
          <div style={S.label}>Target схема</div>
          <SearchSelect value={tgtSchema} options={tgtSchemas} onChange={setTgtSchema} placeholder="Выберите target" />
        </div>
        <button
          onClick={runCompare}
          disabled={!srcSchema || !tgtSchema || comparing}
          style={{
            ...S.btn(comparing ? "#1e293b" : "#1e3a5f", "#93c5fd"),
            opacity: (!srcSchema || !tgtSchema) ? 0.4 : 1,
            whiteSpace: "nowrap",
          }}
        >
          {comparing ? "Сравнение..." : "Сравнить"}
        </button>
      </div>

      {tables.length > 0 && (
        <>
          {/* Filter + search bar */}
          <div style={{ display: "flex", gap: 8, marginBottom: 12, alignItems: "center", flexWrap: "wrap" }}>
            {FILTERS.map(f => (
              <button
                key={f.key}
                onClick={() => setFilter(f.key)}
                style={{
                  ...S.btnSm(filter === f.key ? "#1e3a5f" : "transparent", filter === f.key ? "#93c5fd" : "#475569"),
                  border: `1px solid ${filter === f.key ? "#3b82f6" : "#334155"}`,
                  borderRadius: 20,
                }}
              >
                {f.label} <span style={{ opacity: 0.6 }}>({f.count})</span>
              </button>
            ))}
            <div style={{ flex: 1 }} />
            <input
              value={search} onChange={e => setSearch(e.target.value)}
              placeholder="Поиск по имени..."
              style={{ ...S.input, maxWidth: 240 }}
            />
          </div>

          {/* Bulk select */}
          <div style={{ display: "flex", gap: 8, marginBottom: 8, fontSize: 11 }}>
            <button onClick={selectAllVisible} style={S.btnSm("#1e293b", "#94a3b8")}>
              Выбрать видимые ({filtered.length})
            </button>
            <button onClick={deselectAllVisible} style={S.btnSm("#1e293b", "#94a3b8")}>
              Снять видимые
            </button>
            <span style={{ color: "#64748b", marginLeft: 8, alignSelf: "center" }}>
              Выбрано: <strong style={{ color: "#93c5fd" }}>{selected.size}</strong>
            </span>
          </div>

          {/* Table grid */}
          <div style={{ ...S.card, maxHeight: "calc(100vh - 380px)", overflowY: "auto" }}>
            {/* Header */}
            <div style={{
              display: "grid",
              gridTemplateColumns: "36px 1fr 140px 70px 70px 80px 64px 160px",
              padding: "8px 12px", borderBottom: "1px solid #1e293b",
              fontSize: 11, fontWeight: 600, color: "#64748b",
              position: "sticky", top: 0, background: "#0f172a", zIndex: 10,
            }}>
              <div />
              <div>Таблица</div>
              <div>Статус</div>
              <div>Колонки</div>
              <div>Индексы</div>
              <div>Constraints</div>
              <div>Триг.</div>
              <div>Действия</div>
            </div>

            {/* Rows */}
            {filtered.map(t => {
              const st = statusLabel(t);
              const isExpanded = expanded === t.table;
              return (
                <React.Fragment key={t.table}>
                  <div
                    style={{
                      display: "grid",
                      gridTemplateColumns: "36px 1fr 140px 70px 70px 80px 64px 160px",
                      padding: "6px 12px", alignItems: "center",
                      borderBottom: "1px solid #0f1624",
                      background: selected.has(t.table) ? "#1e293b44" : "transparent",
                      cursor: "pointer",
                    }}
                    onClick={() => setExpanded(isExpanded ? null : t.table)}
                  >
                    <div onClick={e => { e.stopPropagation(); toggleSelect(t.table); }}>
                      <input
                        type="checkbox"
                        checked={selected.has(t.table)}
                        readOnly
                        style={{ accentColor: "#3b82f6", cursor: "pointer" }}
                      />
                    </div>
                    <div style={{ fontSize: 12, fontWeight: 500 }}>
                      <span style={{ color: "#475569", fontSize: 10, marginRight: 6 }}>{isExpanded ? "▼" : "▶"}</span>
                      {t.table}
                    </div>
                    <div>
                      <span style={{
                        background: st.bg, color: st.color,
                        padding: "2px 8px", borderRadius: 10, fontSize: 10, fontWeight: 600,
                      }}>
                        {st.text}
                      </span>
                    </div>
                    <div>{t.diff ? diffBadge(t.diff.cols_missing + t.diff.cols_extra + t.diff.cols_type, 0) : "—"}</div>
                    <div>{t.diff ? diffBadge(t.diff.idx_missing, t.diff.idx_disabled) : "—"}</div>
                    <div>{t.diff ? diffBadge(t.diff.con_missing, t.diff.con_disabled) : "—"}</div>
                    <div>{t.diff ? diffBadge(t.diff.trg_missing, 0) : "—"}</div>
                    <div style={{ display: "flex", gap: 4 }} onClick={e => e.stopPropagation()}>
                      {!t.exists_in_target && (
                        <button
                          onClick={() => handleSync(t.table, "ensure")}
                          disabled={syncing.has(t.table)}
                          style={S.btnSm("#22c55e22", "#22c55e")}
                        >
                          {syncing.has(t.table) ? "..." : "Создать"}
                        </button>
                      )}
                      {t.exists_in_target && t.diff && !t.diff.ok && (
                        <button
                          onClick={() => handleSync(t.table, "sync")}
                          disabled={syncing.has(t.table)}
                          style={S.btnSm("#3b82f622", "#3b82f6")}
                        >
                          {syncing.has(t.table) ? "..." : "Синхр."}
                        </button>
                      )}
                    </div>
                  </div>
                  {isExpanded && t.exists_in_target && (
                    <DDLDetail srcSchema={srcSchema} tgtSchema={tgtSchema} table={t.table} />
                  )}
                </React.Fragment>
              );
            })}

            {filtered.length === 0 && (
              <div style={{ padding: 24, textAlign: "center", color: "#475569", fontSize: 13 }}>
                Нет таблиц для отображения
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
}


// ── TableSelectionStep ──────────────────────────────────────────────────────

function TableSelectionStep({
  tables, selected, defaults, setDefaults, perTableMode, setPerTableMode,
  connectorGroupId, setConnectorGroupId,
  newGroupName, setNewGroupName,
}: {
  tables: TableCompare[];
  selected: Set<string>;
  defaults: PlanDefaults; setDefaults: (d: PlanDefaults) => void;
  perTableMode: Record<string, "CDC" | "BULK_ONLY">;
  setPerTableMode: (m: Record<string, "CDC" | "BULK_ONLY">) => void;
  connectorGroupId: string; setConnectorGroupId: (v: string) => void;
  newGroupName: string; setNewGroupName: (v: string) => void;
}) {
  const [groups, setGroups] = useState<{ group_id: string; group_name: string }[]>([]);
  const selectedTables = tables.filter(t => selected.has(t.table));

  useEffect(() => {
    fetch("/api/connector-groups").then(r => r.json()).then(setGroups).catch(() => {});
  }, []);

  const hasCDC = selectedTables.some(t => (perTableMode[t.table] || defaults.mode) === "CDC");

  return (
    <div>
      {/* Global defaults */}
      <div style={{ ...S.card, marginBottom: 16 }}>
        <div style={{ ...S.headerBar, fontSize: 13, fontWeight: 700 }}>Общие настройки</div>
        <div style={{ padding: 16, display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12 }}>
          <div>
            <div style={S.label}>Режим по умолчанию</div>
            <select value={defaults.mode} onChange={e => setDefaults({ ...defaults, mode: e.target.value as "CDC" | "BULK_ONLY" })} style={S.select}>
              <option value="CDC">CDC</option>
              <option value="BULK_ONLY">BULK_ONLY</option>
            </select>
          </div>
          <div>
            <div style={S.label}>Стратегия</div>
            <select value={defaults.migration_strategy} onChange={e => setDefaults({ ...defaults, migration_strategy: e.target.value as "STAGE" | "DIRECT" })} style={S.select}>
              <option value="STAGE">STAGE</option>
              <option value="DIRECT">DIRECT</option>
            </select>
          </div>
          <div>
            <div style={S.label}>Chunk size</div>
            <input type="number" value={defaults.chunk_size} onChange={e => setDefaults({ ...defaults, chunk_size: +e.target.value })} style={S.input} />
          </div>
          <div>
            <div style={S.label}>Max parallel workers</div>
            <input type="number" value={defaults.max_parallel_workers} onChange={e => setDefaults({ ...defaults, max_parallel_workers: +e.target.value })} style={S.input} />
          </div>
          <div>
            <div style={S.label}>Baseline parallel degree</div>
            <input type="number" value={defaults.baseline_parallel_degree} onChange={e => setDefaults({ ...defaults, baseline_parallel_degree: +e.target.value })} style={S.input} />
          </div>
        </div>
      </div>

      {/* Connector group (for CDC) */}
      {hasCDC && (
        <div style={{ ...S.card, marginBottom: 16 }}>
          <div style={{ ...S.headerBar, fontSize: 13, fontWeight: 700 }}>Группа коннекторов (CDC)</div>
          <div style={{ padding: 16, display: "flex", gap: 12, alignItems: "flex-end" }}>
            <div style={{ flex: 1 }}>
              <div style={S.label}>Существующая группа</div>
              <select
                value={connectorGroupId}
                onChange={e => { setConnectorGroupId(e.target.value); if (e.target.value) setNewGroupName(""); }}
                style={S.select}
              >
                <option value="">— Создать новую —</option>
                {groups.map(g => <option key={g.group_id} value={g.group_id}>{g.group_name}</option>)}
              </select>
            </div>
            {!connectorGroupId && (
              <div style={{ flex: 1 }}>
                <div style={S.label}>Имя новой группы</div>
                <input value={newGroupName} onChange={e => setNewGroupName(e.target.value)} placeholder="my_group" style={S.input} />
              </div>
            )}
          </div>
        </div>
      )}

      {/* Per-table mode */}
      <div style={S.card}>
        <div style={{ ...S.headerBar, fontSize: 13, fontWeight: 700 }}>
          Таблицы ({selectedTables.length})
        </div>
        <div style={{ maxHeight: "calc(100vh - 500px)", overflowY: "auto" }}>
          <div style={{
            display: "grid", gridTemplateColumns: "1fr 100px 140px",
            padding: "8px 16px", fontSize: 11, fontWeight: 600, color: "#64748b",
            borderBottom: "1px solid #1e293b",
            position: "sticky", top: 0, background: "#0f172a", zIndex: 5,
          }}>
            <div>Таблица</div>
            <div>Готовность</div>
            <div>Режим</div>
          </div>
          {selectedTables.map(t => {
            const st = statusLabel(t);
            const mode = perTableMode[t.table] || defaults.mode;
            return (
              <div
                key={t.table}
                style={{
                  display: "grid", gridTemplateColumns: "1fr 100px 140px",
                  padding: "8px 16px", alignItems: "center",
                  borderBottom: "1px solid #0f1624",
                }}
              >
                <div style={{ fontSize: 12 }}>{t.table}</div>
                <div>
                  <span style={{ background: st.bg, color: st.color, padding: "2px 8px", borderRadius: 10, fontSize: 10, fontWeight: 600 }}>
                    {st.text}
                  </span>
                </div>
                <div>
                  <select
                    value={mode}
                    onChange={e => setPerTableMode({ ...perTableMode, [t.table]: e.target.value as "CDC" | "BULK_ONLY" })}
                    style={{ ...S.select, padding: "4px 8px", fontSize: 11 }}
                  >
                    <option value="CDC">CDC</option>
                    <option value="BULK_ONLY">BULK_ONLY</option>
                  </select>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}


// ── OrderingStep ────────────────────────────────────────────────────────────

function OrderingStep({
  selected, batches, setBatches, srcSchema, fkDeps, setFkDeps,
  defaults, perTableMode,
}: {
  selected: Set<string>;
  batches: Batch[]; setBatches: (b: Batch[]) => void;
  srcSchema: string;
  fkDeps: FKDep[]; setFkDeps: (d: FKDep[]) => void;
  defaults: PlanDefaults;
  perTableMode: Record<string, "CDC" | "BULK_ONLY">;
}) {
  const [loadingDeps, setLoadingDeps] = useState(false);

  // Initialize batches from selected if empty
  useEffect(() => {
    if (batches.length === 0 && selected.size > 0) {
      const tables = [...selected].sort();
      setBatches([{
        order: 1,
        tables: tables.map(t => ({
          table: t,
          mode: perTableMode[t] || defaults.mode,
          overrides: {},
        })),
      }]);
    }
  }, [selected, batches.length, setBatches, perTableMode, defaults.mode]);

  const loadFKDeps = useCallback(() => {
    if (!srcSchema || selected.size === 0) return;
    setLoadingDeps(true);
    const tablesCSV = [...selected].join(",");
    fetch(`/api/planner/fk-dependencies?schema=${srcSchema}&tables=${tablesCSV}`)
      .then(r => r.json())
      .then(setFkDeps)
      .catch(() => {})
      .finally(() => setLoadingDeps(false));
  }, [srcSchema, selected, setFkDeps]);

  // Auto-load on mount
  useEffect(() => { loadFKDeps(); }, [loadFKDeps]);

  const autoOrder = () => {
    // Topological sort
    const allTables = [...selected];
    const depMap = new Map<string, Set<string>>();
    fkDeps.forEach(d => depMap.set(d.table, new Set(d.depends_on)));

    const ordered: string[][] = [];
    const placed = new Set<string>();
    const remaining = new Set(allTables);

    while (remaining.size > 0) {
      const batch: string[] = [];
      for (const t of remaining) {
        const deps = depMap.get(t);
        if (!deps || [...deps].every(d => placed.has(d) || !remaining.has(d))) {
          batch.push(t);
        }
      }
      if (batch.length === 0) {
        // Circular deps — just dump remaining
        batch.push(...remaining);
        remaining.clear();
      }
      batch.sort();
      ordered.push(batch);
      batch.forEach(t => { placed.add(t); remaining.delete(t); });
    }

    setBatches(ordered.map((tables, i) => ({
      order: i + 1,
      tables: tables.map(t => ({
        table: t,
        mode: perTableMode[t] || defaults.mode,
        overrides: {},
      })),
    })));
  };

  const addBatch = () => {
    setBatches([...batches, { order: batches.length + 1, tables: [] }]);
  };

  const moveTable = (fromBatch: number, tableIdx: number, toBatch: number) => {
    const next = batches.map(b => ({ ...b, tables: [...b.tables] }));
    const [item] = next[fromBatch].tables.splice(tableIdx, 1);
    next[toBatch].tables.push(item);
    setBatches(next.filter(b => b.tables.length > 0).map((b, i) => ({ ...b, order: i + 1 })));
  };

  const moveUp = (batchIdx: number, tableIdx: number) => {
    if (tableIdx === 0) return;
    const next = batches.map(b => ({ ...b, tables: [...b.tables] }));
    const t = next[batchIdx].tables;
    [t[tableIdx - 1], t[tableIdx]] = [t[tableIdx], t[tableIdx - 1]];
    setBatches(next);
  };

  const moveDown = (batchIdx: number, tableIdx: number) => {
    const next = batches.map(b => ({ ...b, tables: [...b.tables] }));
    const t = next[batchIdx].tables;
    if (tableIdx >= t.length - 1) return;
    [t[tableIdx], t[tableIdx + 1]] = [t[tableIdx + 1], t[tableIdx]];
    setBatches(next);
  };

  return (
    <div>
      {/* FK deps info */}
      <div style={{ display: "flex", gap: 8, marginBottom: 16, alignItems: "center" }}>
        <button onClick={autoOrder} style={S.btn("#1e3a5f", "#93c5fd")} disabled={loadingDeps}>
          {loadingDeps ? "Загрузка FK..." : "Авто-порядок (по FK)"}
        </button>
        <button onClick={addBatch} style={S.btn("#1e293b", "#94a3b8")}>
          + Batch
        </button>
        {fkDeps.length > 0 && (
          <span style={{ fontSize: 11, color: "#fbbf24" }}>
            FK-зависимости: {fkDeps.map(d => `${d.table} → ${d.depends_on.join(", ")}`).join("; ")}
          </span>
        )}
      </div>

      {/* Batches */}
      {batches.map((batch, bi) => (
        <div key={bi} style={{ ...S.card, marginBottom: 12 }}>
          <div style={{ ...S.headerBar, fontSize: 12, fontWeight: 700, color: "#93c5fd" }}>
            Batch {batch.order}
            <span style={{ fontSize: 11, color: "#475569", marginLeft: 8 }}>
              ({batch.tables.length} таблиц — запускаются параллельно)
            </span>
          </div>
          {batch.tables.map((item, ti) => (
            <div
              key={item.table}
              style={{
                display: "flex", alignItems: "center", gap: 8,
                padding: "6px 16px", borderBottom: "1px solid #0f1624",
              }}
            >
              <span style={{ fontSize: 12, flex: 1 }}>{item.table}</span>
              <span style={{
                fontSize: 10, padding: "2px 6px", borderRadius: 4,
                background: item.mode === "CDC" ? "#1e3a5f" : "#422006",
                color: item.mode === "CDC" ? "#93c5fd" : "#fbbf24",
              }}>
                {item.mode}
              </span>
              <button onClick={() => moveUp(bi, ti)} style={S.btnSm("#1e293b", "#475569")} title="Вверх">&#9650;</button>
              <button onClick={() => moveDown(bi, ti)} style={S.btnSm("#1e293b", "#475569")} title="Вниз">&#9660;</button>
              {batches.length > 1 && (
                <select
                  value={bi}
                  onChange={e => moveTable(bi, ti, +e.target.value)}
                  style={{ ...S.select, width: 100, padding: "2px 6px", fontSize: 10 }}
                >
                  {batches.map((_, idx) => (
                    <option key={idx} value={idx}>Batch {idx + 1}</option>
                  ))}
                </select>
              )}
            </div>
          ))}
          {batch.tables.length === 0 && (
            <div style={{ padding: 16, textAlign: "center", color: "#334155", fontSize: 12 }}>
              Перетащите таблицы сюда
            </div>
          )}
        </div>
      ))}
    </div>
  );
}


// ── ReviewStep ──────────────────────────────────────────────────────────────

function ReviewStep({
  srcSchema, tgtSchema, batches, defaults,
  connectorGroupId, newGroupName,
  onExecute, executing,
}: {
  srcSchema: string; tgtSchema: string;
  batches: Batch[]; defaults: PlanDefaults;
  connectorGroupId: string; newGroupName: string;
  onExecute: () => void; executing: boolean;
}) {
  const totalTables = batches.reduce((sum, b) => sum + b.tables.length, 0);
  const cdcCount = batches.reduce((sum, b) => sum + b.tables.filter(t => t.mode === "CDC").length, 0);
  const bulkCount = totalTables - cdcCount;

  return (
    <div>
      {/* Summary */}
      <div style={{ ...S.card, marginBottom: 16 }}>
        <div style={{ ...S.headerBar, fontSize: 13, fontWeight: 700 }}>Сводка плана</div>
        <div style={{ padding: 16, display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 12 }}>
          <div>
            <div style={S.label}>Таблиц</div>
            <div style={{ fontSize: 20, fontWeight: 700, color: "#e2e8f0" }}>{totalTables}</div>
          </div>
          <div>
            <div style={S.label}>Batch'ей</div>
            <div style={{ fontSize: 20, fontWeight: 700, color: "#e2e8f0" }}>{batches.length}</div>
          </div>
          <div>
            <div style={S.label}>CDC / BULK</div>
            <div style={{ fontSize: 20, fontWeight: 700 }}>
              <span style={{ color: "#93c5fd" }}>{cdcCount}</span>
              <span style={{ color: "#475569" }}> / </span>
              <span style={{ color: "#fbbf24" }}>{bulkCount}</span>
            </div>
          </div>
          <div>
            <div style={S.label}>Схема</div>
            <div style={{ fontSize: 14, fontWeight: 600, color: "#e2e8f0" }}>
              {srcSchema} → {tgtSchema}
            </div>
          </div>
        </div>
      </div>

      {/* Defaults */}
      <div style={{ ...S.card, marginBottom: 16 }}>
        <div style={{ ...S.headerBar, fontSize: 12, fontWeight: 700 }}>Настройки</div>
        <div style={{ padding: 12, fontSize: 12, display: "flex", gap: 16, flexWrap: "wrap", color: "#94a3b8" }}>
          <span>Стратегия: <strong>{defaults.migration_strategy}</strong></span>
          <span>Chunk: <strong>{defaults.chunk_size.toLocaleString()}</strong></span>
          <span>Workers: <strong>{defaults.max_parallel_workers}</strong></span>
          <span>Baseline PD: <strong>{defaults.baseline_parallel_degree}</strong></span>
          {connectorGroupId ? <span>Группа: <strong>{connectorGroupId.slice(0, 8)}...</strong></span>
            : newGroupName ? <span>Новая группа: <strong>{newGroupName}</strong></span>
            : null}
        </div>
      </div>

      {/* Batch list */}
      {batches.map(batch => (
        <div key={batch.order} style={{ ...S.card, marginBottom: 12 }}>
          <div style={{ ...S.headerBar, fontSize: 12, fontWeight: 700, color: "#93c5fd" }}>
            Batch {batch.order} ({batch.tables.length} таблиц)
          </div>
          {batch.tables.map(item => (
            <div
              key={item.table}
              style={{
                padding: "6px 16px", borderBottom: "1px solid #0f1624",
                display: "flex", alignItems: "center", gap: 8,
              }}
            >
              <span style={{ fontSize: 12, flex: 1 }}>{item.table}</span>
              <span style={{
                fontSize: 10, padding: "2px 6px", borderRadius: 4,
                background: item.mode === "CDC" ? "#1e3a5f" : "#422006",
                color: item.mode === "CDC" ? "#93c5fd" : "#fbbf24",
              }}>
                {item.mode}
              </span>
            </div>
          ))}
        </div>
      ))}

      {/* Execute */}
      <div style={{ display: "flex", gap: 12, marginTop: 16 }}>
        <button
          onClick={onExecute}
          disabled={executing}
          style={{
            ...S.btn(executing ? "#1e293b" : "#16a34a", "#fff"),
            fontSize: 14, padding: "10px 24px",
          }}
        >
          {executing ? "Создание миграций..." : "Создать миграции"}
        </button>
      </div>
    </div>
  );
}


// ── Main Wizard ─────────────────────────────────────────────────────────────

export function MigrationPlanner() {
  const [step, setStep] = useState<WizardStep>(1);

  // Step 1 state
  const [srcSchema, setSrcSchema] = useState("");
  const [tgtSchema, setTgtSchema] = useState("");
  const [tables, setTables] = useState<TableCompare[]>([]);
  const [selected, setSelected] = useState<Set<string>>(new Set());

  // Step 2 state
  const [defaults, setDefaults] = useState<PlanDefaults>({
    chunk_size: 1_000_000,
    max_parallel_workers: 1,
    migration_strategy: "STAGE",
    mode: "CDC",
    baseline_parallel_degree: 4,
  });
  const [perTableMode, setPerTableMode] = useState<Record<string, "CDC" | "BULK_ONLY">>({});
  const [connectorGroupId, setConnectorGroupId] = useState("");
  const [newGroupName, setNewGroupName] = useState("");

  // Step 3 state
  const [batches, setBatches] = useState<Batch[]>([]);
  const [fkDeps, setFkDeps] = useState<FKDep[]>([]);

  // Step 4 state
  const [executing, setExecuting] = useState(false);
  const [planResult, setPlanResult] = useState<{ plan_id: number; items: { table: string; migration_id: string }[] } | null>(null);
  const [startingPlan, setStartingPlan] = useState(false);
  const [startResult, setStartResult] = useState<{ batch: number; started: string[] } | null>(null);

  // Reset batches when selected changes
  useEffect(() => { setBatches([]); }, [selected]);

  const canGoNext = (s: WizardStep): boolean => {
    if (s === 1) return selected.size > 0;
    if (s === 2) return true;
    if (s === 3) return batches.length > 0 && batches.some(b => b.tables.length > 0);
    return false;
  };

  const handleExecute = () => {
    setExecuting(true);
    const body: Record<string, unknown> = {
      src_schema: srcSchema,
      tgt_schema: tgtSchema,
      name: `Plan ${srcSchema} → ${tgtSchema}`,
      defaults,
      batches: batches.map(b => ({
        order: b.order,
        tables: b.tables.map(t => ({ table: t.table, mode: t.mode, overrides: t.overrides })),
      })),
    };

    if (connectorGroupId) {
      body.connector_group_id = connectorGroupId;
    } else if (newGroupName) {
      body.create_connector_group = {
        group_name: newGroupName,
        topic_prefix: newGroupName.toLowerCase().replace(/[^a-z0-9]/g, "_"),
        connector_name: `${newGroupName.toLowerCase().replace(/[^a-z0-9]/g, "_")}_connector`,
        consumer_group_prefix: `${newGroupName.toLowerCase().replace(/[^a-z0-9]/g, "_")}_cg`,
      };
    }

    fetch("/api/planner/execute", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })
      .then(r => r.ok ? r.json() : r.json().then(d => Promise.reject(d.error || "Error")))
      .then(data => setPlanResult(data))
      .catch(e => alert(`Ошибка: ${e}`))
      .finally(() => setExecuting(false));
  };

  const handleStartPlan = () => {
    if (!planResult) return;
    setStartingPlan(true);
    fetch(`/api/planner/plans/${planResult.plan_id}/start`, { method: "POST" })
      .then(r => r.ok ? r.json() : r.json().then(d => Promise.reject(d.error || "Error")))
      .then(data => setStartResult(data))
      .catch(e => alert(`Ошибка: ${e}`))
      .finally(() => setStartingPlan(false));
  };

  return (
    <div>
      <StepIndicator current={step} onGo={setStep} />

      {step === 1 && (
        <SchemaCompareStep
          srcSchema={srcSchema} setSrcSchema={setSrcSchema}
          tgtSchema={tgtSchema} setTgtSchema={setTgtSchema}
          tables={tables} setTables={setTables}
          selected={selected} setSelected={setSelected}
        />
      )}

      {step === 2 && (
        <TableSelectionStep
          tables={tables} selected={selected}
          defaults={defaults} setDefaults={setDefaults}
          perTableMode={perTableMode} setPerTableMode={setPerTableMode}
          connectorGroupId={connectorGroupId} setConnectorGroupId={setConnectorGroupId}
          newGroupName={newGroupName} setNewGroupName={setNewGroupName}
        />
      )}

      {step === 3 && (
        <OrderingStep
          selected={selected} batches={batches} setBatches={setBatches}
          srcSchema={srcSchema} fkDeps={fkDeps} setFkDeps={setFkDeps}
          defaults={defaults} perTableMode={perTableMode}
        />
      )}

      {step === 4 && !planResult && (
        <ReviewStep
          srcSchema={srcSchema} tgtSchema={tgtSchema}
          batches={batches} defaults={defaults}
          connectorGroupId={connectorGroupId} newGroupName={newGroupName}
          onExecute={handleExecute} executing={executing}
        />
      )}

      {step === 4 && planResult && (
        <div>
          <div style={{ ...S.card, marginBottom: 16 }}>
            <div style={{ ...S.headerBar, fontSize: 13, fontWeight: 700, color: "#86efac" }}>
              План создан (ID: {planResult.plan_id})
            </div>
            <div style={{ padding: 16 }}>
              <div style={{ fontSize: 12, color: "#94a3b8", marginBottom: 12 }}>
                Создано миграций: <strong style={{ color: "#e2e8f0" }}>{planResult.items.length}</strong> в статусе DRAFT
              </div>
              {planResult.items.map(item => (
                <div key={item.migration_id} style={{
                  padding: "4px 12px", fontSize: 12, borderBottom: "1px solid #0f1624",
                  display: "flex", gap: 8,
                }}>
                  <span style={{ flex: 1 }}>{item.table}</span>
                  <span style={{ color: "#475569", fontFamily: "monospace", fontSize: 10 }}>{item.migration_id.slice(0, 8)}</span>
                </div>
              ))}
            </div>
          </div>

          {!startResult ? (
            <button
              onClick={handleStartPlan}
              disabled={startingPlan}
              style={{ ...S.btn(startingPlan ? "#1e293b" : "#1e3a5f", "#93c5fd"), fontSize: 14, padding: "10px 24px" }}
            >
              {startingPlan ? "Запуск..." : "Запустить план (Batch 1)"}
            </button>
          ) : (
            <div style={{ ...S.card }}>
              <div style={{ ...S.headerBar, fontSize: 13, fontWeight: 700, color: "#86efac" }}>
                Batch {startResult.batch} запущен
              </div>
              <div style={{ padding: 16, fontSize: 12, color: "#94a3b8" }}>
                Запущено миграций: <strong style={{ color: "#e2e8f0" }}>{startResult.started.length}</strong>
                <div style={{ marginTop: 8, fontSize: 11, color: "#475569" }}>
                  Перейдите на вкладку «Миграции» для мониторинга.
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Navigation */}
      {!(step === 4 && planResult) && (
        <div style={{ display: "flex", justifyContent: "space-between", marginTop: 20 }}>
          <button
            onClick={() => setStep((step - 1) as WizardStep)}
            disabled={step === 1}
            style={{ ...S.btn("#1e293b", step === 1 ? "#334155" : "#94a3b8"), opacity: step === 1 ? 0.4 : 1 }}
          >
            ← Назад
          </button>
          {step < 4 && (
            <button
              onClick={() => setStep((step + 1) as WizardStep)}
              disabled={!canGoNext(step)}
              style={{ ...S.btn(canGoNext(step) ? "#1e3a5f" : "#1e293b", canGoNext(step) ? "#93c5fd" : "#334155") }}
            >
              Далее →
            </button>
          )}
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Verify TypeScript compiles**

Run: `cd c:/work/database_migration/new/front/frontend && npx tsc --noEmit`

Expected: No errors (or only pre-existing ones).

- [ ] **Step 3: Commit**

```bash
git add frontend/src/components/MigrationPlanner.tsx
git commit -m "feat(planner): add MigrationPlanner wizard component (4 steps)"
```

---

### Task 6: Frontend — integrate Planner tab in App.tsx

**Files:**
- Modify: `frontend/src/App.tsx`

- [ ] **Step 1: Add import**

After the existing imports (~line 9), add:

```tsx
import { MigrationPlanner } from "./components/MigrationPlanner";
```

- [ ] **Step 2: Add "planner" to Tab type**

Change the Tab type (line 15) from:

```tsx
type Tab = "migrations" | "connector-groups" | "target-prep" | "data-compare" | "checklist";
```

to:

```tsx
type Tab = "planner" | "migrations" | "connector-groups" | "target-prep" | "data-compare" | "checklist";
```

- [ ] **Step 3: Add tab button (first position, before "Миграции")**

In the tab bar section (~line 151), add before the "Миграции" TabButton:

```tsx
        <TabButton
          label="Планирование"
          active={activeTab === "planner"}
          onClick={() => setActiveTab("planner")}
        />
```

- [ ] **Step 4: Add tab content**

In the tab content section (~line 180), add before the migrations line:

```tsx
        {activeTab === "planner"          && <MigrationPlanner />}
```

- [ ] **Step 5: Set default tab to "planner"**

Change the initial state (~line 93) from:

```tsx
const [activeTab, setActiveTab] = useState<Tab>("migrations");
```

to:

```tsx
const [activeTab, setActiveTab] = useState<Tab>("planner");
```

- [ ] **Step 6: Verify the app starts**

Run: `cd c:/work/database_migration/new/front/frontend && npm run dev`

Expected: App opens with "Планирование" as the active first tab.

- [ ] **Step 7: Commit**

```bash
git add frontend/src/App.tsx
git commit -m "feat(planner): integrate Planner tab into App.tsx"
```

---

### Task 7: Manual verification

- [ ] **Step 1: Start backend**

Run: `cd c:/work/database_migration/new/front && python backend/app.py`

Expected: Server starts, `schema init complete` in logs, new tables created.

- [ ] **Step 2: Start frontend**

Run: `cd c:/work/database_migration/new/front/frontend && npm run dev`

Expected: App opens on Планирование tab.

- [ ] **Step 3: Verify Step 1 (Schema Compare)**

1. Select source and target schemas
2. Click "Сравнить"
3. Verify table list appears with status badges
4. Verify filters work (Все / Различаются / Нет в target / OK)
5. Click a table row — verify DDL detail panel expands
6. Verify "Создать" and "Синхр." buttons work

- [ ] **Step 4: Verify Step 2 (Selection + Config)**

1. Select tables with checkboxes, proceed to step 2
2. Verify defaults form (chunk_size, workers, strategy)
3. Verify per-table mode toggle (CDC / BULK_ONLY)
4. Verify connector group selector

- [ ] **Step 5: Verify Step 3 (Ordering)**

1. Verify auto-order button loads FK deps and sorts batches
2. Verify "+" Batch creates new batch
3. Verify tables can be moved between batches
4. Verify up/down arrows work

- [ ] **Step 6: Verify Step 4 (Review + Execute)**

1. Verify summary shows correct counts
2. Click "Создать миграции" — verify plan + migrations created
3. Click "Запустить план" — verify batch 1 starts
4. Switch to "Миграции" tab — verify migrations appear

- [ ] **Step 7: Final commit**

```bash
git add -A
git commit -m "feat(planner): complete Migration Planner wizard"
```
