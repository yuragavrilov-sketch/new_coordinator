"""Schema-migration aggregator.

A schema_migration is the operational unit shown on the Dashboard: one source
schema being migrated to a target schema as a single coordinated effort.
Persistent state lives in `schema_migrations` (metadata only); operational
state — status / stage / progress / KPIs — is *computed* from the child
`migrations` rows (joined via plan_id → migration_plan_items.migration_id) at
read time. This keeps the orchestrator unchanged and avoids sync issues.
"""

import json
from typing import Any

# ── Phase → dashboard-stage map (6 buckets) ─────────────────────────────────
#
# The orchestrator has 30+ phases; the dashboard collapses them into 6
# canonical stages. We take the *furthest-along* stage across all child
# migrations as the schema-level stage.
_STAGE_ORDER = ["assess", "schema", "bulk", "cdc", "validate", "cutover"]

_PHASE_TO_STAGE: dict[str, str] = {
    # assess — precheck / draft
    "DRAFT":               "assess",
    "NEW":                 "assess",
    # schema — DDL prep before data movement
    "PREPARING":           "schema",
    "SCN_FIXED":           "schema",
    "TOPIC_CREATING":      "schema",
    "CONNECTOR_STARTING":  "schema",
    # bulk — data loading + stage validation + baseline publish
    "CHUNKING":            "bulk",
    "BULK_LOADING":        "bulk",
    "BULK_LOADED":         "bulk",
    "STAGE_VALIDATING":    "bulk",
    "STAGE_VALIDATED":     "bulk",
    "BASELINE_PUBLISHING": "bulk",
    "BASELINE_LOADING":    "bulk",
    "BASELINE_PUBLISHED":  "bulk",
    # cdc — change-data-capture apply
    "CDC_BUFFERING":       "cdc",
    "CDC_APPLY_STARTING":  "cdc",
    "CDC_APPLYING":        "cdc",
    "CDC_CATCHING_UP":     "cdc",
    "CDC_CAUGHT_UP":       "cdc",
    "STEADY_STATE":        "cdc",
    # validate — post-load checks, drop stage, enable constraints/indexes
    "STAGE_DROPPING":      "validate",
    "INDEXES_ENABLING":    "validate",
    "DATA_VERIFYING":      "validate",
    "DATA_MISMATCH":       "validate",
    # cutover — terminal
    "COMPLETED":           "cutover",
    "FAILED":              "cutover",
    "CANCELLING":          "cutover",
    "CANCELLED":           "cutover",
    "PAUSED":              "schema",   # paused mid-flow → keep prior stage
}

# Phases considered "active" for KPI counting
_ACTIVE_PHASES = {
    "NEW", "PREPARING", "SCN_FIXED",
    "CONNECTOR_STARTING", "CDC_BUFFERING", "TOPIC_CREATING",
    "CHUNKING", "BULK_LOADING", "BULK_LOADED",
    "STAGE_VALIDATING", "STAGE_VALIDATED",
    "BASELINE_PUBLISHING", "BASELINE_LOADING", "BASELINE_PUBLISHED",
    "STAGE_DROPPING", "INDEXES_ENABLING",
    "DATA_VERIFYING", "DATA_MISMATCH",
    "CDC_APPLY_STARTING", "CDC_APPLYING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP",
    "STEADY_STATE",
}

_DONE_PHASES   = {"COMPLETED"}
_FAILED_PHASES = {"FAILED", "CANCELLED"}


def _phase_to_object_status(phase: str, has_error: bool) -> str:
    """Map a per-migration phase to the dashboard's ObjectStatus enum.

    Matches frontend dashboard/types.ts ObjectStatus.
    """
    if has_error and phase != "COMPLETED":
        return "error"
    if phase == "COMPLETED":
        return "done"
    if phase in ("FAILED",):
        return "error"
    if phase in ("CANCELLED", "CANCELLING"):
        return "skipped"
    if phase == "PAUSED":
        return "paused"
    if phase in ("DATA_VERIFYING", "STAGE_VALIDATING"):
        return "validating"
    if phase in ("DRAFT", "NEW"):
        return "queued"
    if phase in _ACTIVE_PHASES:
        return "running"
    return "queued"


def _aggregate_stage(children_phases: list[str], any_failed: bool) -> str:
    """Furthest-along stage across children; if all done → cutover."""
    if not children_phases:
        return "assess"
    if all(p in _DONE_PHASES for p in children_phases):
        return "cutover"
    indices = [_STAGE_ORDER.index(_PHASE_TO_STAGE.get(p, "assess"))
               for p in children_phases]
    return _STAGE_ORDER[max(indices)]


def _aggregate_status(children_phases: list[str], any_failed: bool, paused: bool) -> str:
    """Frontend SchemaStatus: running/cdc/paused/error/validating/done/queued."""
    if paused:
        return "paused"
    if any_failed:
        return "error"
    if not children_phases:
        return "queued"
    if all(p in _DONE_PHASES for p in children_phases):
        return "done"
    # If any child in CDC phase, consider whole schema in CDC
    cdc_phases = {"CDC_BUFFERING", "CDC_APPLY_STARTING", "CDC_APPLYING",
                  "CDC_CATCHING_UP", "CDC_CAUGHT_UP", "STEADY_STATE"}
    if any(p in cdc_phases for p in children_phases):
        return "cdc"
    if any(p in {"DATA_VERIFYING", "STAGE_VALIDATING"} for p in children_phases):
        return "validating"
    if any(p in _ACTIVE_PHASES for p in children_phases):
        return "running"
    return "queued"


# ── Object table: TABLE migrations from `migrations` table ──────────────────

# Oracle object_type (as stored in ddl_objects) → frontend ObjectType enum
_DDL_TYPE_MAP = {
    "TABLE":             "TABLE",
    "MATERIALIZED VIEW": "MVIEW",
    "PACKAGE":           "PACKAGE",
    "PROCEDURE":         "PROCEDURE",
    "FUNCTION":          "FUNCTION",
    "VIEW":              "VIEW",
    "TRIGGER":           "TRIGGER",
    "SEQUENCE":          "SEQUENCE",
    "SYNONYM":           "SYNONYM",
    "TYPE":              "TYPE",
}

# match_status → (ObjectStatus, compat%, warn_count, err_count, note_prefix)
_MATCH_MAP = {
    "MATCH":   ("done",   100, 0, 0, ""),
    "DIFF":    ("warn",    85, 1, 0, "DDL отличается"),
    "MISSING": ("queued", 100, 0, 0, "ещё нет в target"),
    "EXTRA":   ("skipped",100, 0, 0, "только в target"),
    "ERROR":   ("error",   70, 0, 1, ""),
    "UNKNOWN": ("queued", 100, 0, 0, ""),
}


def _build_ddl_object(
    object_type: str, object_name: str, oracle_status: str | None,
    match_status: str, diff_json,
) -> dict:
    """Convert a (ddl_objects + ddl_compare_results) row → SchemaObject."""
    fe_type = _DDL_TYPE_MAP.get(object_type, object_type)
    status, compat, warn, err, note = _MATCH_MAP.get(match_status, _MATCH_MAP["UNKNOWN"])

    # INVALID oracle objects → warn even if DDL matches
    if oracle_status and oracle_status.upper() == "INVALID" and status == "done":
        status = "warn"
        warn = 1
        note = note or "INVALID в Oracle"

    # Light note from diff (first failing field)
    if not note and isinstance(diff_json, dict):
        if not diff_json.get("ok", True):
            for k, v in diff_json.items():
                if k == "ok" or not v:
                    continue
                if isinstance(v, bool) and v is False:
                    note = f"{k.replace('_match', '').replace('_', ' ')} mismatch"
                    break

    return {
        "id":         f"ddl-{object_type}-{object_name}",
        "type":       fe_type,
        "name":       object_name,
        "rows":       None,
        "rowsDone":   None,
        "sizeMb":     0,
        "status":     status,
        "progress":   100 if status == "done" else 0,
        "rowsPerSec": 0,
        "mbPerSec":   0,
        "compat":     compat,
        "warn":       warn,
        "err":        err,
        "eta":        "—",
        "dur":        "<1s",
        "note":       note,
    }


def _build_object(row: dict) -> dict:
    """Convert a `migrations` row into a dashboard SchemaObject."""
    has_error = bool(row.get("error_text"))
    status    = _phase_to_object_status(row.get("phase", ""), has_error)
    rows_loaded = row.get("rows_loaded") or 0
    total_rows  = row.get("total_rows")
    progress = 0.0
    if total_rows and total_rows > 0:
        progress = min(100.0, (rows_loaded / total_rows) * 100.0)
    elif status == "done":
        progress = 100.0
    return {
        "id":        str(row.get("migration_id")),
        "type":      "TABLE",
        "name":      row.get("source_table") or row.get("migration_name") or "—",
        "rows":      total_rows,
        "rowsDone":  rows_loaded,
        "sizeMb":    0,                            # not yet captured in `migrations`
        "status":    status,
        "progress":  round(progress, 1),
        "rowsPerSec": 0,                           # would come from worker metrics
        "mbPerSec":  0,
        "compat":    100,                          # tables don't have PL/SQL compat
        "warn":      0,
        "err":       1 if has_error else 0,
        "eta":       "—",
        "dur":       "—",
        "note":      row.get("error_text") or "",
    }


# ── Public API ──────────────────────────────────────────────────────────────

def list_schema_migrations(conn) -> list[dict]:
    """List all schema_migrations with computed status/stage/progress."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                sm.schema_migration_id, sm.name, sm.src_schema, sm.tgt_schema,
                sm.source_host, sm.source_version, sm.target_host, sm.target_version,
                sm.priority, sm.owner, sm.plan_id, sm.group_id, sm.paused,
                sm.started_at, sm.window_at, sm.completed_at,
                sm.created_at, sm.updated_at
            FROM schema_migrations sm
            ORDER BY sm.created_at DESC
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

        result = []
        for r in rows:
            sm = dict(zip(cols, r))
            child = _load_children(conn, sm["plan_id"])
            result.append(_to_dashboard_view(sm, child))
        return result


def get_schema_migration(conn, sm_id: str) -> dict | None:
    """Load one schema_migration (without objects/events — those have own endpoints)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                sm.schema_migration_id, sm.name, sm.src_schema, sm.tgt_schema,
                sm.source_host, sm.source_version, sm.target_host, sm.target_version,
                sm.priority, sm.owner, sm.description, sm.plan_id, sm.group_id,
                sm.paused, sm.started_at, sm.window_at, sm.completed_at,
                sm.created_at, sm.updated_at
            FROM schema_migrations sm
            WHERE sm.schema_migration_id = %s
        """, (sm_id,))
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
        sm = dict(zip(cols, row))
        child = _load_children(conn, sm["plan_id"])
        return _to_dashboard_view(sm, child)


def get_objects(conn, sm_id: str) -> list[dict]:
    """Return SchemaObject[]: union of
       (a) TABLE rows from migrations linked to this schema_migration's plan,
       (b) all source-side objects from the latest ddl_snapshot for this
           schema pair — TABLE rows already covered by (a) are skipped to
           avoid duplicates, non-TABLE DDL is always included.
    Tables present in the snapshot but not yet in any migration appear as
    `queued` — they're known to exist on source but not planned yet."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT src_schema, tgt_schema FROM schema_migrations WHERE schema_migration_id = %s",
            (sm_id,),
        )
        row = cur.fetchone()
        if not row:
            return []
        src_schema, tgt_schema = row

    # (a) TABLE objects driven by migrations — they have phase/progress/errors
    tables: list[dict] = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT m.*
            FROM schema_migrations sm
            JOIN migration_plan_items mpi ON mpi.plan_id = sm.plan_id
            JOIN migrations m ON m.migration_id = mpi.migration_id
            WHERE sm.schema_migration_id = %s
            ORDER BY m.created_at
        """, (sm_id,))
        cols = [d[0] for d in cur.description]
        tables = [_build_object(dict(zip(cols, r))) for r in cur.fetchall()]
    migrated_table_names = {t["name"].upper() for t in tables}

    # (b) DDL objects from the most recent snapshot
    ddl_objects: list[dict] = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT snapshot_id FROM ddl_snapshots
            WHERE src_schema = %s AND tgt_schema = %s
            ORDER BY loaded_at DESC
            FETCH FIRST 1 ROWS ONLY
        """, (src_schema, tgt_schema))
        snap = cur.fetchone()
        if snap:
            snapshot_id = snap[0]
            cur.execute("""
                SELECT o.object_type, o.object_name, o.oracle_status,
                       COALESCE(c.match_status, 'UNKNOWN') AS match_status,
                       c.diff
                FROM   ddl_objects o
                LEFT   JOIN ddl_compare_results c
                       ON c.snapshot_id = o.snapshot_id
                       AND c.object_type = o.object_type
                       AND c.object_name = o.object_name
                WHERE  o.snapshot_id = %s
                  AND  o.db_side = 'source'
                ORDER BY o.object_type, o.object_name
            """, (snapshot_id,))
            for r in cur.fetchall():
                otype, oname = r[0], r[1]
                # Skip TABLE rows already represented by a migration
                if otype == "TABLE" and oname.upper() in migrated_table_names:
                    continue
                ddl_objects.append(_build_ddl_object(*r))

    # Stable ID space: TABLE migrations use UUID strings; DDL objects get
    # synthetic "ddl-<TYPE>-<NAME>" IDs to avoid collisions.
    return tables + ddl_objects


def get_events(conn, sm_id: str, limit: int = 100) -> list[dict]:
    """Return recent migration_state_history entries across all child migrations."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                m.source_table AS obj,
                msh.created_at,
                msh.from_phase, msh.to_phase, msh.transition_status,
                msh.transition_reason, msh.message
            FROM schema_migrations sm
            JOIN migration_plan_items mpi ON mpi.plan_id = sm.plan_id
            JOIN migrations m ON m.migration_id = mpi.migration_id
            JOIN migration_state_history msh ON msh.migration_id = m.migration_id
            WHERE sm.schema_migration_id = %s
            ORDER BY msh.created_at DESC
            LIMIT %s
        """, (sm_id, limit))
        events = []
        for r in cur.fetchall():
            obj, created_at, from_phase, to_phase, status, reason, message = r
            level = "error" if status == "FAILED" else "warn" if to_phase == "DATA_MISMATCH" else "info"
            time_str = created_at.strftime("%H:%M:%S") if created_at else ""
            msg = message or f"{from_phase or '—'} → {to_phase}"
            events.append({
                "t": time_str,
                "obj": obj or "—",
                "level": level,
                "msg": msg,
            })
        return events


def get_metrics(conn, sm_id: str) -> dict:
    """Best-effort live metrics. For Phase 4 returns zeros + flat sparklines —
    real Oracle V$SYSSTAT integration is future work."""
    return {
        "sourceCpu": 0,
        "network":   0,
        "redoPerSec": 0,
        "cdcLagMs":  0,
        "cpuSpark":  [0] * 10,
        "netSpark":  [0] * 10,
        "redoSpark": [0] * 10,
        "lagSpark":  [0] * 10,
    }


def create_schema_migration(conn, payload: dict) -> str:
    """Create a new schema_migration row. Returns its UUID."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO schema_migrations (
                name, src_schema, tgt_schema,
                source_host, source_version, target_host, target_version,
                priority, owner, description, plan_id, group_id, window_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING schema_migration_id
        """, (
            payload.get("name") or payload.get("src_schema") or "",
            payload.get("src_schema") or "",
            payload.get("tgt_schema") or "",
            payload.get("source_host"),
            payload.get("source_version"),
            payload.get("target_host"),
            payload.get("target_version"),
            payload.get("priority", "P2"),
            payload.get("owner"),
            payload.get("description"),
            payload.get("plan_id"),
            payload.get("group_id"),
            payload.get("window_at"),
        ))
        new_id = cur.fetchone()[0]
        conn.commit()
        return str(new_id)


def set_paused(conn, sm_id: str, paused: bool) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE schema_migrations
            SET paused = %s, updated_at = NOW()
            WHERE schema_migration_id = %s
        """, (paused, sm_id))
        affected = cur.rowcount
        conn.commit()
        return affected > 0


# ── Internal helpers ────────────────────────────────────────────────────────

def _load_children(conn, plan_id: int | None) -> list[dict]:
    """Fetch child migrations linked to this schema_migration via plan_id."""
    if plan_id is None:
        return []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT m.migration_id, m.phase, m.error_text,
                   m.total_rows, m.rows_loaded, m.source_table
            FROM migration_plan_items mpi
            JOIN migrations m ON m.migration_id = mpi.migration_id
            WHERE mpi.plan_id = %s
        """, (plan_id,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _to_dashboard_view(sm: dict, children: list[dict]) -> dict:
    phases = [c["phase"] for c in children]
    any_failed = any(c.get("error_text") for c in children) or any(p == "FAILED" for p in phases)
    paused = bool(sm.get("paused"))

    status = _aggregate_status(phases, any_failed, paused)
    stage  = _aggregate_stage(phases, any_failed)

    total_rows = sum((c.get("total_rows") or 0) for c in children)
    rows_done  = sum((c.get("rows_loaded") or 0) for c in children)
    progress   = (rows_done / total_rows * 100.0) if total_rows > 0 else 0.0

    done_count = sum(1 for p in phases if p in _DONE_PHASES)
    err_count  = sum(1 for c in children if c.get("error_text"))

    def _iso(v: Any) -> str | None:
        if v is None:
            return None
        if hasattr(v, "isoformat"):
            return v.isoformat() + ("Z" if v.tzinfo is None else "")
        return str(v)

    return {
        "id":        str(sm["schema_migration_id"]),
        "name":      sm["name"],
        "source":    {
            "host":    sm.get("source_host") or "",
            "version": sm.get("source_version") or "",
            "tns":     sm.get("src_schema") or "",
        },
        "target":    {
            "host":    sm.get("target_host") or "",
            "version": sm.get("target_version") or "",
            "tns":     sm.get("tgt_schema") or "",
        },
        "src_schema": sm.get("src_schema"),
        "tgt_schema": sm.get("tgt_schema"),
        "owner":      sm.get("owner") or "",
        "priority":   sm.get("priority") or "P2",
        "status":     status,
        "stage":      stage,
        "startedAt":  _iso(sm.get("started_at")) or _iso(sm.get("created_at")),
        "windowAt":   _iso(sm.get("window_at")),
        "schemaCompat": 100.0,
        "sizeGb":     0,
        "totals":     { "rowsPerSec": 0, "mbPerSec": 0 },
        "kpi":        {
            "totalObjects":  len(children),
            "doneObjects":   done_count,
            "errorObjects":  err_count,
            "totalRows":     total_rows,
            "rowsDone":      rows_done,
            "progress":      round(progress, 1),
        },
        "planId":   sm.get("plan_id"),
        "groupId":  str(sm["group_id"]) if sm.get("group_id") else None,
        "paused":   paused,
    }
