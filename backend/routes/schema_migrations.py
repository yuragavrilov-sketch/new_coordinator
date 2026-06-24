"""Schema-migration dashboard API.

GET    /api/schema-migrations              → list (each with computed KPIs)
GET    /api/schema-migrations/:id          → detail (header + KPIs)
GET    /api/schema-migrations/:id/objects  → object table rows (SchemaObject[])
GET    /api/schema-migrations/:id/events   → recent events
GET    /api/schema-migrations/:id/metrics  → live metrics (placeholder)
POST   /api/schema-migrations              → create (Wizard submit)
POST   /api/schema-migrations/:id/pause    → pause whole schema migration
POST   /api/schema-migrations/:id/resume   → resume
POST   /api/schema-migrations/:id/rollback → cancel (mark child migrations)
"""

import json
import uuid
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

import services.schema_migrations as svc
import services.ddl_apply_jobs as ddl_jobs
from services.strategy import Strategy

bp = Blueprint("schema_migrations", __name__)

_state: dict = {}


def init(*, get_conn_fn, db_available_ref, broadcast_fn):
    _state["get_conn"]     = get_conn_fn
    _state["db_available"] = db_available_ref
    _state["broadcast"]    = broadcast_fn


def _db_ok() -> bool:
    return _state["db_available"]["value"]


@bp.get("/api/schema-migrations")
def list_schema_migrations():
    if not _db_ok():
        return jsonify([])
    conn = _state["get_conn"]()
    try:
        return jsonify(svc.list_schema_migrations(conn))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.get("/api/schema-migrations/<sm_id>")
def get_schema_migration(sm_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    conn = _state["get_conn"]()
    try:
        sm = svc.get_schema_migration(conn, sm_id)
        if not sm:
            return jsonify({"error": "Not found"}), 404
        return jsonify(sm)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.get("/api/schema-migrations/<sm_id>/objects")
def get_objects(sm_id: str):
    if not _db_ok():
        return jsonify([])
    conn = _state["get_conn"]()
    try:
        return jsonify(svc.get_objects(conn, sm_id))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.get("/api/schema-migrations/<sm_id>/objects/<path:obj_id>/detail")
def get_object_detail(sm_id: str, obj_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    conn = _state["get_conn"]()
    try:
        detail = svc.get_object_detail(conn, sm_id, obj_id)
        if detail is None:
            return jsonify({"error": "Not found"}), 404
        return jsonify(detail)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.get("/api/schema-migrations/<sm_id>/events")
def get_events(sm_id: str):
    if not _db_ok():
        return jsonify([])
    limit = int(request.args.get("limit", 100))
    conn = _state["get_conn"]()
    try:
        return jsonify(svc.get_events(conn, sm_id, limit=limit))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.get("/api/schema-migrations/<sm_id>/metrics")
def get_metrics(sm_id: str):
    if not _db_ok():
        return jsonify({})
    conn = _state["get_conn"]()
    try:
        return jsonify(svc.get_metrics(conn, sm_id))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.post("/api/schema-migrations")
def create_schema_migration():
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    payload = request.get_json(silent=True) or {}
    if not payload.get("src_schema") or not payload.get("tgt_schema"):
        return jsonify({"error": "src_schema and tgt_schema required"}), 400
    conn = _state["get_conn"]()
    try:
        new_id = svc.create_schema_migration(conn, payload)
        _state["broadcast"]({"type": "schema_migration.created", "id": new_id})
        return jsonify({"schema_migration_id": new_id}), 201
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.post("/api/schema-migrations/<sm_id>/pause")
def pause(sm_id: str):
    return _set_paused(sm_id, True)


@bp.post("/api/schema-migrations/<sm_id>/resume")
def resume(sm_id: str):
    return _set_paused(sm_id, False)


def _set_paused(sm_id: str, paused: bool):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    conn = _state["get_conn"]()
    try:
        ok = svc.set_paused(conn, sm_id, paused)
        if not ok:
            return jsonify({"error": "Not found"}), 404
        _state["broadcast"]({
            "type": "schema_migration.paused" if paused else "schema_migration.resumed",
            "id": sm_id,
        })
        return jsonify({"ok": True, "paused": paused})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.post("/api/schema-migrations/<sm_id>/ddl-apply")
def ddl_apply(sm_id: str):
    """Queue DDL apply jobs.

    Body: {"action": "create_missing"|"sync_diff"|"recreate",
           "objects": [{"type": "TABLE"|"MVIEW"|..., "name": "FOO"}]}
    """
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    payload = request.get_json(silent=True) or {}
    action  = payload.get("action") or ""
    objects = payload.get("objects") or []
    if not action:
        return jsonify({"error": "action required"}), 400
    if not isinstance(objects, list) or not objects:
        return jsonify({"error": "objects required"}), 400
    conn = _state["get_conn"]()
    try:
        result = ddl_jobs.submit_jobs(conn, sm_id, action, objects)
        _state["broadcast"]({
            "type":   "ddl_apply.queued",
            "sm_id":  sm_id,
            "action": action,
            "queued": result["queued"],
        })
        return jsonify(result), 202
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.get("/api/schema-migrations/<sm_id>/ddl-jobs")
def list_ddl_jobs(sm_id: str):
    if not _db_ok():
        return jsonify([])
    limit = int(request.args.get("limit", 100))
    conn = _state["get_conn"]()
    try:
        return jsonify(ddl_jobs.list_jobs(conn, sm_id, limit=limit))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.post("/api/schema-migrations/<sm_id>/plan/items")
def add_plan_items(sm_id: str):
    """Create or extend this schema migration's table-migration plan."""
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    payload = request.get_json(silent=True) or {}
    tables = payload.get("tables") or []
    if not isinstance(tables, list) or not tables:
        return jsonify({"error": "tables required"}), 400

    raw_strategy = payload.get("strategy") or "BULK_DIRECT"
    try:
        strategy = Strategy.parse(raw_strategy)
    except ValueError as exc:
        return jsonify({"error": f"Invalid strategy: {exc}"}), 400
    truncate_target = bool(payload.get("truncate_target", True))
    if strategy.uses_stage and not truncate_target:
        return jsonify({"error": "STAGE strategy requires truncate_target=true"}), 400
    connector_group_id = payload.get("connector_group_id")
    if strategy.has_cdc and not connector_group_id:
        return jsonify({"error": "connector_group_id required for CDC strategy"}), 400

    sequential = bool(payload.get("sequential", True))
    chunk_size = int(payload.get("chunk_size") or 1_000_000)
    max_workers = int(payload.get("max_parallel_workers") or 1)
    baseline_pd = int(payload.get("baseline_parallel_degree") or 4)
    stage_tablespace = (payload.get("stage_tablespace") or "PAYSTAGE").strip().upper()

    conn = _state["get_conn"]()
    now = datetime.now(timezone.utc).isoformat()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT schema_migration_id, name, src_schema, tgt_schema, plan_id
                FROM   schema_migrations
                WHERE  schema_migration_id = %s
                FOR UPDATE
            """, (sm_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "schema_migration not found"}), 404
            _, sm_name, src_schema, tgt_schema, plan_id = row
            src_schema = (src_schema or "").strip().upper()
            tgt_schema = (tgt_schema or "").strip().upper()
            if not src_schema or not tgt_schema:
                return jsonify({"error": "schema migration src_schema/tgt_schema required"}), 400
            if strategy.has_cdc:
                cur.execute("""
                    SELECT status, topic_prefix, run_id
                    FROM   connector_groups
                    WHERE  group_id = %s
                """, (connector_group_id,))
                group_row = cur.fetchone()
                if not group_row:
                    return jsonify({"error": "connector group not found"}), 400
                group_status, group_topic_prefix, group_run_id = group_row
                if group_status != "RUNNING":
                    return jsonify({"error": f"connector group is {group_status}, expected RUNNING"}), 400
                active_topic_prefix = (
                    f"{group_topic_prefix}.{group_run_id}"
                    if group_run_id else group_topic_prefix
                )
            else:
                active_topic_prefix = None

            if plan_id is None:
                defaults = {
                    "strategy": strategy.value,
                    "truncate_target": truncate_target,
                    "chunk_size": chunk_size,
                    "max_parallel_workers": max_workers,
                }
                cur.execute("""
                    INSERT INTO migration_plans
                        (name, src_schema, tgt_schema, connector_group_id, defaults_json, status)
                    VALUES (%s, %s, %s, %s, %s, 'READY')
                    RETURNING plan_id
                """, (
                    sm_name or f"{src_schema}->{tgt_schema}",
                    src_schema, tgt_schema,
                    connector_group_id if strategy.has_cdc else None,
                    json.dumps(defaults),
                ))
                plan_id = cur.fetchone()[0]
                cur.execute("""
                    UPDATE schema_migrations
                    SET    plan_id = %s,
                           updated_at = NOW()
                    WHERE  schema_migration_id = %s
                """, (plan_id, sm_id))
            elif strategy.has_cdc:
                cur.execute("""
                    UPDATE migration_plans
                    SET    connector_group_id = COALESCE(connector_group_id, %s)
                    WHERE  plan_id = %s
                """, (connector_group_id, plan_id))

            cur.execute("""
                SELECT COALESCE(MAX(batch_order), 0)
                FROM   migration_plan_items
                WHERE  plan_id = %s
            """, (plan_id,))
            batch_base = cur.fetchone()[0] or 0

            created = []
            for idx, table in enumerate(tables):
                if isinstance(table, dict):
                    table_name = (table.get("source_table") or table.get("table") or "").strip().upper()
                    target_table = (table.get("target_table") or table_name).strip().upper()
                else:
                    table_name = str(table).strip().upper()
                    target_table = table_name
                if not table_name:
                    continue

                batch_order = batch_base + idx + 1 if sequential else batch_base + 1
                mid = str(uuid.uuid4())
                stage_table = f"STG_{src_schema}_{table_name}"[:128]
                if strategy.has_cdc:
                    topic_name = f"{active_topic_prefix}.{src_schema}.{table_name}".replace("#", "_")
                    cur.execute("""
                        INSERT INTO group_tables
                            (id, group_id, source_schema, source_table,
                             target_schema, target_table,
                             effective_key_type, effective_key_columns_json,
                             source_pk_exists, source_uk_exists, topic_name)
                        VALUES (%s, %s, %s, %s, %s, %s, 'NONE', '[]', FALSE, FALSE, %s)
                        ON CONFLICT (group_id, source_schema, source_table) DO NOTHING
                    """, (
                        str(uuid.uuid4()), connector_group_id,
                        src_schema, table_name,
                        tgt_schema, target_table,
                        topic_name,
                    ))

                cur.execute("""
                    INSERT INTO migrations (
                        migration_id, migration_name, phase, state_changed_at,
                        source_connection_id, target_connection_id,
                        source_schema, source_table,
                        target_schema, target_table,
                        stage_table_name, stage_tablespace,
                        chunk_size, max_parallel_workers,
                        baseline_parallel_degree,
                        strategy,
                        truncate_target,
                        group_id,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, 'DRAFT', %s,
                        'oracle_source', 'oracle_target',
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s, %s
                    )
                """, (
                    mid, f"{src_schema}.{table_name}", now,
                    src_schema, table_name,
                    tgt_schema, target_table,
                    stage_table, stage_tablespace,
                    chunk_size, max(1, max_workers),
                    max(1, baseline_pd),
                    strategy.value,
                    truncate_target,
                    connector_group_id if strategy.has_cdc else None,
                    now, now,
                ))

                cur.execute("""
                    INSERT INTO migration_state_history
                        (migration_id, from_phase, to_phase, message, actor_type)
                    VALUES (%s, NULL, 'DRAFT', %s, 'USER')
                """, (mid, f"Added to schema migration plan {plan_id}"))

                overrides = {
                    "strategy": strategy.value,
                    "truncate_target": truncate_target,
                    "chunk_size": chunk_size,
                    "max_parallel_workers": max_workers,
                }
                cur.execute("""
                    INSERT INTO migration_plan_items
                        (plan_id, table_name, mode, batch_order, sort_order,
                         overrides_json, migration_id, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'PENDING')
                    RETURNING item_id
                """, (
                    plan_id, table_name, "BULK" if not strategy.has_cdc else "CDC",
                    batch_order, idx, json.dumps(overrides), mid,
                ))
                item_id = cur.fetchone()[0]
                created.append({
                    "item_id": item_id,
                    "table": table_name,
                    "migration_id": mid,
                    "batch_order": batch_order,
                })

            cur.execute("""
                UPDATE migration_plans
                SET    status = CASE
                           WHEN status IN ('DONE', 'FAILED', 'CANCELLED') THEN 'READY'
                           ELSE status
                       END
                WHERE  plan_id = %s
            """, (plan_id,))

        conn.commit()
        if strategy.has_cdc:
            try:
                from services.connector_groups import refresh_connector_tables
                refresh_connector_tables(connector_group_id)
            except Exception as exc:
                print(f"[schema_migrations.add_plan_items] refresh_connector_tables warning: {exc}")
        _state["broadcast"]({
            "type": "schema_migration.plan_items_added",
            "id": sm_id,
            "plan_id": plan_id,
            "count": len(created),
        })
        return jsonify({"plan_id": plan_id, "items": created}), 201
    except Exception as exc:
        conn.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.post("/api/schema-migrations/<sm_id>/rollback")
def rollback(sm_id: str):
    """Mark all child migrations as CANCELLING. Best-effort: orchestrator
    drives them to CANCELLED."""
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE migrations m
                SET phase = 'CANCELLING', state_changed_at = NOW(), updated_at = NOW()
                FROM migration_plan_items mpi, schema_migrations sm
                WHERE m.migration_id = mpi.migration_id
                  AND mpi.plan_id = sm.plan_id
                  AND sm.schema_migration_id = %s
                  AND m.phase NOT IN ('COMPLETED', 'CANCELLED', 'FAILED')
            """, (sm_id,))
            affected = cur.rowcount
            conn.commit()
        _state["broadcast"]({"type": "schema_migration.rollback", "id": sm_id, "affected": affected})
        return jsonify({"ok": True, "affected": affected})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()
