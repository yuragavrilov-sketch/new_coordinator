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


def init(*, get_conn_fn, db_available_ref, broadcast_fn, load_configs_fn=None):
    _state["get_conn"]     = get_conn_fn
    _state["db_available"] = db_available_ref
    _state["broadcast"]    = broadcast_fn
    _state["load_configs"] = load_configs_fn


def _db_ok() -> bool:
    return _state["db_available"]["value"]


def _source_oracle_conn():
    load_configs = _state.get("load_configs")
    if load_configs is None:
        raise RuntimeError("load_configs not wired")
    from db.oracle_browser import get_oracle_conn
    return get_oracle_conn("source", load_configs(), prefer_owner=True)


def _derive_cdc_key_info(info: dict) -> tuple[str, str, list[str], bool, bool]:
    pk_columns = info.get("pk_columns") or []
    uk_constraints = info.get("uk_constraints") or []
    if pk_columns:
        return "PRIMARY_KEY", "PK", pk_columns, True, bool(uk_constraints)
    if uk_constraints:
        return "UNIQUE_KEY", "UK", uk_constraints[0].get("columns") or [], False, True
    return "NONE", "NONE", [], False, False


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
    connector_group_id = payload.get("connector_group_id") or None

    sequential = True if strategy.has_cdc else bool(payload.get("sequential", True))
    chunk_size = int(payload.get("chunk_size") or 1_000_000)
    max_workers = int(payload.get("max_parallel_workers") or 1)
    baseline_pd = int(payload.get("baseline_parallel_degree") or 4)
    stage_tablespace = (payload.get("stage_tablespace") or "PAYSTAGE").strip().upper()

    conn = _state["get_conn"]()
    src_oconn = None
    now = datetime.now(timezone.utc).isoformat()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT schema_migration_id, name, src_schema, tgt_schema, plan_id, group_id
                FROM   schema_migrations
                WHERE  schema_migration_id = %s
                FOR UPDATE
            """, (sm_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "schema_migration not found"}), 404
            _, sm_name, src_schema, tgt_schema, plan_id, sm_group_id = row
            src_schema = (src_schema or "").strip().upper()
            tgt_schema = (tgt_schema or "").strip().upper()
            if not src_schema or not tgt_schema:
                return jsonify({"error": "schema migration src_schema/tgt_schema required"}), 400
            if strategy.has_cdc:
                connector_group_id = sm_group_id or connector_group_id
                if connector_group_id is None:
                    connector_group_id = str(uuid.uuid4())
                    suffix = connector_group_id.split("-")[0]
                    base = f"{src_schema.lower()}_{tgt_schema.lower()}_{suffix}"
                    cur.execute("""
                        INSERT INTO connector_groups
                            (group_id, group_name, source_connection_id,
                             connector_name, topic_prefix, consumer_group_prefix)
                        VALUES (%s, %s, 'oracle_source', %s, %s, %s)
                    """, (
                        connector_group_id,
                        f"{src_schema}->{tgt_schema} CDC",
                        f"sm_{base}_connector",
                        f"sm.{src_schema.lower()}.{tgt_schema.lower()}.{suffix}",
                        f"sm.{src_schema.lower()}.{tgt_schema.lower()}.{suffix}",
                    ))
                if sm_group_id is None:
                    cur.execute("""
                        UPDATE schema_migrations
                        SET    group_id = %s,
                               updated_at = NOW()
                        WHERE  schema_migration_id = %s
                    """, (connector_group_id, sm_id))

                cur.execute("""
                    SELECT status, topic_prefix, run_id
                    FROM   connector_groups
                    WHERE  group_id = %s
                """, (connector_group_id,))
                group_row = cur.fetchone()
                if not group_row:
                    return jsonify({"error": "connector group not found"}), 400
                _group_status, group_topic_prefix, group_run_id = group_row
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
            seen_tables: set[str] = set()
            for idx, table in enumerate(tables):
                manual_key_columns = []
                if isinstance(table, dict):
                    table_name = (table.get("source_table") or table.get("table") or "").strip().upper()
                    target_table = (table.get("target_table") or table_name).strip().upper()
                    raw_manual_key_columns = table.get("effective_key_columns") or table.get("key_columns") or []
                    if isinstance(raw_manual_key_columns, str):
                        manual_key_columns = [
                            c.strip().upper()
                            for c in raw_manual_key_columns.split(",")
                            if c.strip()
                        ]
                    elif isinstance(raw_manual_key_columns, list):
                        manual_key_columns = [
                            str(c).strip().upper()
                            for c in raw_manual_key_columns
                            if str(c).strip()
                        ]
                else:
                    table_name = str(table).strip().upper()
                    target_table = table_name
                if not table_name:
                    continue

                if table_name in seen_tables:
                    raise ValueError(f"Table {src_schema}.{table_name} is selected more than once")
                seen_tables.add(table_name)

                cur.execute("""
                    SELECT i.status, m.phase
                    FROM   migration_plan_items i
                    LEFT JOIN migrations m ON m.migration_id = i.migration_id
                    WHERE  i.plan_id = %s
                      AND  UPPER(i.table_name) = %s
                      AND  i.status NOT IN ('DONE', 'FAILED', 'CANCELLED')
                      AND  COALESCE(m.phase, '') NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
                    LIMIT  1
                """, (plan_id, table_name))
                duplicate = cur.fetchone()
                if duplicate:
                    dup_status, dup_phase = duplicate
                    raise ValueError(
                        f"Table {src_schema}.{table_name} is already in this plan "
                        f"({dup_status}, {dup_phase or 'no migration phase'})"
                    )

                effective_key_type = "NONE"
                effective_key_source = "NONE"
                effective_key_columns: list[str] = []
                source_pk_exists = False
                source_uk_exists = False
                if strategy.has_cdc:
                    if manual_key_columns:
                        effective_key_type = "USER_DEFINED"
                        effective_key_source = "USER"
                        effective_key_columns = manual_key_columns
                    else:
                        if src_oconn is None:
                            src_oconn = _source_oracle_conn()
                        from db.oracle_browser import get_table_info
                        info = get_table_info(src_oconn, src_schema, table_name)
                        (effective_key_type,
                         effective_key_source,
                         effective_key_columns,
                         source_pk_exists,
                         source_uk_exists) = _derive_cdc_key_info(info)
                    if not source_pk_exists and not source_uk_exists and not effective_key_columns:
                        raise ValueError(
                            f"CDC table {src_schema}.{table_name} has no PK/UK and no key columns. "
                            "Add it to the regular pack or provide CDC key columns."
                        )

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
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (group_id, source_schema, source_table) DO UPDATE
                        SET    target_schema = EXCLUDED.target_schema,
                               target_table = EXCLUDED.target_table,
                               effective_key_type = EXCLUDED.effective_key_type,
                               effective_key_columns_json = EXCLUDED.effective_key_columns_json,
                               source_pk_exists = EXCLUDED.source_pk_exists,
                               source_uk_exists = EXCLUDED.source_uk_exists,
                               topic_name = EXCLUDED.topic_name
                    """, (
                        str(uuid.uuid4()), connector_group_id,
                        src_schema, table_name,
                        tgt_schema, target_table,
                        effective_key_type,
                        json.dumps(effective_key_columns),
                        source_pk_exists,
                        source_uk_exists,
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
                        source_pk_exists, source_uk_exists,
                        effective_key_type, effective_key_source, effective_key_columns_json,
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
                        %s, %s,
                        %s, %s, %s,
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
                    source_pk_exists, source_uk_exists,
                    effective_key_type, effective_key_source, json.dumps(effective_key_columns),
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
        connector_start = None
        connector_start_error = None
        plan_start = None
        plan_starts: list[dict] = []
        plan_start_error = None
        if strategy.has_cdc:
            try:
                from services.connector_groups import refresh_connector_tables, request_start
                refresh_connector_tables(connector_group_id)
                connector_start = request_start(connector_group_id)
                _state["broadcast"]({
                    "type": "connector_group_status",
                    "group_id": connector_group_id,
                    "status": connector_start.get("status"),
                })
                try:
                    from routes.planner import _start_next_plan_batch
                    for batch_order in sorted({item["batch_order"] for item in created}):
                        started_batch = _start_next_plan_batch(
                            plan_id,
                            actor="SYSTEM",
                            batch_order=batch_order,
                            allow_cdc_queue_when_blocked=True,
                        )
                        plan_starts.append(started_batch)
                    plan_start = plan_starts[0] if plan_starts else None
                except Exception as start_exc:
                    plan_start_error = str(start_exc)
                    print(f"[schema_migrations.add_plan_items] CDC plan autostart warning: {start_exc}")
            except Exception as exc:
                connector_start_error = str(exc)
                print(f"[schema_migrations.add_plan_items] CDC connector autostart warning: {exc}")
        _state["broadcast"]({
            "type": "schema_migration.plan_items_added",
            "id": sm_id,
            "plan_id": plan_id,
            "count": len(created),
        })
        return jsonify({
            "plan_id": plan_id,
            "items": created,
            "connector_group_id": connector_group_id if strategy.has_cdc else None,
            "connector_start": connector_start,
            "connector_start_error": connector_start_error,
            "plan_start": plan_start,
            "plan_starts": plan_starts,
            "plan_start_error": plan_start_error,
        }), 201
    except ValueError as exc:
        conn.rollback()
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        conn.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        if src_oconn is not None:
            try:
                src_oconn.close()
            except Exception:
                pass
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
