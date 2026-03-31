"""Migrations CRUD, phase-transition, action, and monitoring routes."""

import json
import uuid
from datetime import datetime

from flask import Blueprint, jsonify, request

import services.debezium    as debezium
import services.job_queue   as job_queue
import services.kafka_lag   as kafka_lag_svc
import services.oracle_stage as oracle_stage

from routes.utils import db_required, validate_body
from schemas.migration_schemas import (
    ACTION_TRANSITIONS,
    DELETABLE_PHASES,
    CreateMigrationRequest,
    MigrationActionRequest,
    TransitionPhaseRequest,
    UpdateWorkersRequest,
)

bp = Blueprint("migrations", __name__)

_LIST_COLS = """
    migration_id, migration_name, phase, state_changed_at,
    source_connection_id, target_connection_id,
    source_schema, source_table, target_schema, target_table,
    created_at, updated_at,
    error_code, error_text, failed_phase, retry_count,
    description, created_by,
    total_rows, total_chunks, chunks_done, chunks_failed, rows_loaded,
    migration_strategy, migration_mode, group_id
"""

_state: dict = {}


def init(get_conn_fn, row_to_dict_fn, db_available_ref, broadcast_fn,
         load_configs_fn=None, enable_indexes_fn=None):
    _state["get_conn"]        = get_conn_fn
    _state["row_to_dict"]     = row_to_dict_fn
    _state["db_available"]    = db_available_ref
    _state["broadcast"]       = broadcast_fn
    _state["load_configs"]    = load_configs_fn
    _state["enable_indexes"]  = enable_indexes_fn


@bp.get("/api/migrations")
@db_required(_state)
def list_migrations():
    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT {_LIST_COLS} FROM migrations ORDER BY state_changed_at DESC")
                return jsonify([_state["row_to_dict"](cur, r) for r in cur.fetchall()])
        finally:
            conn.close()
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.get("/api/migrations/<migration_id>")
@db_required(_state)
def get_migration(migration_id: str):
    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM migrations WHERE migration_id = %s", (migration_id,))
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Not found"}), 404
                result = _state["row_to_dict"](cur, row)
                cur.execute("""
                    SELECT id, migration_id, from_phase, to_phase,
                           transition_status, transition_reason, message,
                           actor_type, actor_id, correlation_id, created_at
                    FROM migration_state_history
                    WHERE migration_id = %s
                    ORDER BY created_at DESC
                """, (migration_id,))
                result["history"] = [_state["row_to_dict"](cur, r) for r in cur.fetchall()]
            return jsonify(result)
        finally:
            conn.close()
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.post("/api/migrations")
@db_required(_state)
def create_migration():
    data = validate_body(CreateMigrationRequest)

    mid = str(uuid.uuid4())
    now = datetime.utcnow()

    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                # Group-based migration: derive connector fields from group
                group_id = data.group_id
                connector_name = ""
                topic_prefix = ""
                consumer_group = ""

                if group_id and data.migration_mode == "CDC":
                    from services.connector_groups import get_group as _get_group
                    group = _get_group(group_id)
                    if not group:
                        return jsonify({"error": f"Группа {group_id} не найдена"}), 404
                    from services.connector_groups import _active_topic_prefix
                    connector_name = group["connector_name"]
                    topic_prefix = _active_topic_prefix(group)
                    src_schema = data.source_schema.upper()
                    src_table = data.source_table.upper()
                    prefix = group.get("consumer_group_prefix") or group["topic_prefix"]
                    consumer_group = f"{prefix}_{src_schema}_{src_table}"
                elif data.migration_mode == "CDC":
                    connector_name = data.connector_name
                    topic_prefix = data.topic_prefix
                    consumer_group = data.consumer_group

                cur.execute("""
                    INSERT INTO migrations (
                        migration_id, migration_name, phase, state_changed_at,
                        source_connection_id, target_connection_id,
                        source_schema, source_table, target_schema, target_table,
                        stage_table_name, stage_tablespace,
                        connector_name, topic_prefix, consumer_group,
                        chunk_size, max_parallel_workers, baseline_parallel_degree,
                        validate_hash_sample,
                        source_pk_exists, source_uk_exists,
                        effective_key_type, effective_key_source, effective_key_columns_json,
                        migration_strategy, migration_mode,
                        group_id,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s,
                        %s, %s
                    )
                """, (
                    mid, data.migration_name, data.initial_phase, now,
                    data.source_connection_id,
                    data.target_connection_id,
                    data.source_schema, data.source_table,
                    data.target_schema, data.target_table,
                    data.stage_table_name,
                    data.stage_tablespace,
                    connector_name,
                    topic_prefix,
                    consumer_group,
                    data.chunk_size,
                    data.max_parallel_workers,
                    data.baseline_parallel_degree,
                    data.validate_hash_sample,
                    data.source_pk_exists, data.source_uk_exists,
                    data.effective_key_type, data.effective_key_source,
                    data.effective_key_columns_json,
                    data.migration_strategy, data.migration_mode,
                    group_id,
                    now, now,
                ))
                cur.execute("""
                    INSERT INTO migration_state_history
                        (migration_id, from_phase, to_phase, message, actor_type)
                    VALUES (%s, NULL, %s, %s, 'USER')
                """, (mid, data.initial_phase, "Migration created"))
            conn.commit()
        finally:
            conn.close()

        # If group-based, update Debezium connector table list
        if group_id and data.migration_mode == "CDC":
            try:
                from services.connector_groups import refresh_connector_tables
                refresh_connector_tables(group_id)
            except Exception as exc:
                print(f"[migrations] refresh_connector_tables warning: {exc}")

        _state["broadcast"]({
            "type":         "migration_phase",
            "migration_id": mid,
            "phase":        data.initial_phase,
            "ts":           now.isoformat() + "Z",
        })
        return jsonify({"ok": True, "migration_id": mid}), 201

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.patch("/api/migrations/<migration_id>/phase")
@db_required(_state)
def transition_phase(migration_id: str):
    data = validate_body(TransitionPhaseRequest)

    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT phase FROM migrations WHERE migration_id = %s FOR UPDATE",
                    (migration_id,),
                )
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Not found"}), 404
                from_phase = row[0]
                now = datetime.utcnow()

                update_fields: dict = {
                    "phase":            data.to_phase,
                    "state_changed_at": now,
                    "updated_at":       now,
                }
                if data.to_phase == "FAILED":
                    if data.error_code:  update_fields["error_code"] = data.error_code
                    if data.error_text:  update_fields["error_text"] = data.error_text
                    update_fields["failed_phase"] = from_phase
                if data.retry_count is not None:
                    update_fields["retry_count"] = data.retry_count

                set_clause = ", ".join(f"{k} = %s" for k in update_fields)
                cur.execute(
                    f"UPDATE migrations SET {set_clause} WHERE migration_id = %s",
                    [*update_fields.values(), migration_id],
                )
                cur.execute("""
                    INSERT INTO migration_state_history
                        (migration_id, from_phase, to_phase,
                         transition_status, transition_reason, message,
                         actor_type, actor_id, correlation_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    migration_id, from_phase, data.to_phase,
                    data.transition_status,
                    data.transition_reason,
                    data.message,
                    data.actor_type,
                    data.actor_id,
                    data.correlation_id,
                ))
            conn.commit()
        finally:
            conn.close()

        _state["broadcast"]({
            "type":         "migration_phase",
            "migration_id": migration_id,
            "from_phase":   from_phase,
            "phase":        data.to_phase,
            "ts":           now.isoformat() + "Z",
        })
        return jsonify({"ok": True, "from_phase": from_phase, "to_phase": data.to_phase})

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.delete("/api/migrations/<migration_id>")
@db_required(_state)
def delete_migration(migration_id: str):
    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT phase FROM migrations WHERE migration_id = %s FOR UPDATE",
                    (migration_id,),
                )
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Not found"}), 404
                phase = row[0]
                if phase not in DELETABLE_PHASES:
                    return jsonify({
                        "error": f"Нельзя удалить миграцию в фазе {phase}. "
                                 f"Допустимо: {', '.join(sorted(DELETABLE_PHASES))}"
                    }), 409
                cur.execute(
                    "SELECT connector_name, target_connection_id, "
                    "       target_schema, stage_table_name "
                    "FROM   migrations WHERE migration_id = %s",
                    (migration_id,),
                )
                crow = cur.fetchone()
                connector_name      = crow[0] if crow else None
                target_conn_id      = crow[1] if crow else None
                target_schema       = crow[2] if crow else None
                stage_table_name    = crow[3] if crow else None
                cur.execute(
                    "DELETE FROM migration_state_history WHERE migration_id = %s",
                    (migration_id,),
                )
                cur.execute(
                    "DELETE FROM migrations WHERE migration_id = %s",
                    (migration_id,),
                )
            conn.commit()
        finally:
            conn.close()
        # Delete Debezium connector best-effort (after DB commit so row is gone)
        if connector_name:
            try:
                debezium.delete_connector(connector_name)
            except Exception as exc:
                print(f"[delete_migration] connector delete failed (ignored): {exc}")
        # Drop stage table on target Oracle best-effort
        load_configs = _state.get("load_configs")
        if load_configs and target_conn_id and target_schema and stage_table_name:
            try:
                dst_cfg = load_configs().get(target_conn_id, {})
                oracle_stage.drop_stage_table(dst_cfg, target_schema, stage_table_name)
            except Exception as exc:
                print(f"[delete_migration] stage table drop failed (ignored): {exc}")
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Action endpoint (user-triggered transitions)
# ---------------------------------------------------------------------------

@bp.post("/api/migrations/<migration_id>/action")
@db_required(_state)
def migration_action(migration_id: str):
    data = validate_body(MigrationActionRequest)
    action = data.action

    required_from, to_phase = ACTION_TRANSITIONS[action]

    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT phase FROM migrations WHERE migration_id = %s FOR UPDATE",
                    (migration_id,),
                )
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Not found"}), 404
                current_phase = row[0]

            if required_from and current_phase != required_from:
                return jsonify({
                    "error": f"Action '{action}' requires phase '{required_from}', "
                             f"current phase is '{current_phase}'"
                }), 409

            now = datetime.utcnow()
            if action == "cancel":
                # stop Debezium connector async (best-effort)
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT connector_name FROM migrations WHERE migration_id = %s",
                            (migration_id,),
                        )
                        crow = cur.fetchone()
                    if crow and crow[0]:
                        debezium.delete_connector(crow[0])
                except Exception as exc:
                    print(f"[action/cancel] connector delete failed: {exc}")

            if action == "retry_verify":
                # Clear old data_compare task reference so orchestrator creates a new one
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE migrations SET data_compare_task_id = NULL "
                        "WHERE migration_id = %s",
                        (migration_id,))

            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE migrations SET phase=%s, state_changed_at=%s, updated_at=%s "
                    "WHERE migration_id=%s",
                    (to_phase, now, now, migration_id),
                )
                cur.execute("""
                    INSERT INTO migration_state_history
                        (migration_id, from_phase, to_phase, message, actor_type, actor_id)
                    VALUES (%s, %s, %s, %s, 'USER', %s)
                """, (migration_id, current_phase, to_phase,
                      data.message or f"Action: {action}",
                      data.actor_id))
            conn.commit()
        finally:
            conn.close()

        _state["broadcast"]({
            "type":         "migration_phase",
            "migration_id": migration_id,
            "from_phase":   current_phase,
            "phase":        to_phase,
            "ts":           now.isoformat() + "Z",
        })
        return jsonify({"ok": True, "from_phase": current_phase, "to_phase": to_phase})

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Monitoring endpoints
# ---------------------------------------------------------------------------

@bp.get("/api/migrations/<migration_id>/chunks")
@db_required(_state)
def get_migration_chunks(migration_id: str):
    chunk_type = request.args.get("chunk_type", "BULK").strip().upper()
    if chunk_type not in ("BULK", "BASELINE"):
        chunk_type = "BULK"
    page      = max(1, int(request.args.get("page", 1)))
    page_size = max(1, min(500, int(request.args.get("page_size", 100))))
    status_filter = request.args.get("status", "").strip().upper()
    if status_filter and status_filter not in ("PENDING", "CLAIMED", "RUNNING", "DONE", "FAILED"):
        status_filter = ""
    try:
        conn = _state["get_conn"]()
        try:
            result = job_queue.list_chunks(
                conn, migration_id, chunk_type,
                page=page, page_size=page_size,
                status_filter=status_filter,
            )
            stats  = job_queue.get_chunk_stats(conn, migration_id, chunk_type)
        finally:
            conn.close()
        return jsonify({
            "stats":      stats,
            "chunks":     result["chunks"],
            "total":      result["total"],
            "page":       result["page"],
            "page_size":  result["page_size"],
            "chunk_type": chunk_type,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.get("/api/migrations/<migration_id>/connector")
@db_required(_state)
def get_connector_status(migration_id: str):
    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT connector_name FROM migrations WHERE migration_id = %s",
                    (migration_id,),
                )
                row = cur.fetchone()
        finally:
            conn.close()

        if not row:
            return jsonify({"error": "Not found"}), 404

        connector_name = row[0]
        if not connector_name:
            return jsonify({"connector_name": None, "status": "NOT_CONFIGURED"})

        status = debezium.get_connector_status(connector_name)
        return jsonify({"connector_name": connector_name, "status": status})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.get("/api/migrations/<migration_id>/lag")
@db_required(_state)
def get_migration_lag(migration_id: str):
    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT total_lag, lag_by_partition, worker_id,
                           worker_heartbeat, updated_at, rows_applied
                    FROM   migration_cdc_state
                    WHERE  migration_id = %s
                """, (migration_id,))
                row = cur.fetchone()
        finally:
            conn.close()

        if not row:
            return jsonify({"total_lag": None, "message": "CDC state not yet initialised"})

        total_lag, lag_by_partition, worker_id, heartbeat, updated_at, rows_applied = row
        return jsonify({
            "total_lag":        int(total_lag or 0),
            "lag_by_partition": lag_by_partition,
            "worker_id":        worker_id,
            "worker_heartbeat": heartbeat.isoformat() + "Z" if heartbeat else None,
            "updated_at":       updated_at.isoformat() + "Z" if updated_at else None,
            "rows_applied":     int(rows_applied or 0),
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.patch("/api/migrations/<migration_id>/workers")
@db_required(_state)
def update_workers(migration_id: str):
    """Update max_parallel_workers and/or baseline_parallel_degree on the fly."""
    data = validate_body(UpdateWorkersRequest)

    fields: dict = {}
    if data.max_parallel_workers is not None:
        fields["max_parallel_workers"] = data.max_parallel_workers
    if data.baseline_parallel_degree is not None:
        fields["baseline_parallel_degree"] = data.baseline_parallel_degree

    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT phase FROM migrations WHERE migration_id = %s FOR UPDATE",
                    (migration_id,),
                )
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Not found"}), 404

                fields["updated_at"] = datetime.utcnow()
                set_clause = ", ".join(f"{k} = %s" for k in fields)
                cur.execute(
                    f"UPDATE migrations SET {set_clause} WHERE migration_id = %s",
                    [*fields.values(), migration_id],
                )
            conn.commit()
        finally:
            conn.close()
        return jsonify({"ok": True, **{k: v for k, v in fields.items() if k != "updated_at"}})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.post("/api/migrations/<migration_id>/retry-chunks")
@db_required(_state)
def retry_failed_chunks(migration_id: str):
    """Reset FAILED chunks back to PENDING so workers will retry them.

    Optional query param: chunk_type=BULK|BASELINE (default: reset all failed chunks).
    Allowed phases: BULK_LOADING, BASELINE_LOADING, FAILED.
    """
    chunk_type = request.args.get("chunk_type", "").strip().upper()
    if chunk_type not in ("BULK", "BASELINE"):
        chunk_type = ""  # reset all types

    _ALLOWED = {"BULK_LOADING", "BASELINE_LOADING", "FAILED"}

    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT phase FROM migrations WHERE migration_id = %s",
                    (migration_id,),
                )
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Not found"}), 404
                if row[0] not in _ALLOWED:
                    return jsonify({
                        "error": f"Повтор чанков недоступен в фазе {row[0]}. "
                                 f"Допустимо: {', '.join(sorted(_ALLOWED))}"
                    }), 409

                if chunk_type:
                    cur.execute("""
                        UPDATE migration_chunks
                        SET    status        = 'PENDING',
                               worker_id    = NULL,
                               claimed_at   = NULL,
                               started_at   = NULL,
                               completed_at = NULL,
                               error_text   = NULL,
                               retry_count  = 0
                        WHERE  migration_id = %s
                          AND  status       = 'FAILED'
                          AND  COALESCE(chunk_type, 'BULK') = %s
                    """, (migration_id, chunk_type))
                else:
                    cur.execute("""
                        UPDATE migration_chunks
                        SET    status        = 'PENDING',
                               worker_id    = NULL,
                               claimed_at   = NULL,
                               started_at   = NULL,
                               completed_at = NULL,
                               error_text   = NULL,
                               retry_count  = 0
                        WHERE  migration_id = %s
                          AND  status       = 'FAILED'
                    """, (migration_id,))
                reset_count = cur.rowcount

                # Reset the migration's failed-chunk counter
                cur.execute("""
                    UPDATE migrations
                    SET    chunks_failed = 0,
                           updated_at   = NOW()
                    WHERE  migration_id = %s
                """, (migration_id,))
            conn.commit()
        finally:
            conn.close()
        return jsonify({"ok": True, "reset": reset_count})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.post("/api/migrations/<migration_id>/enable-indexes")
@db_required(_state)
def enable_indexes(migration_id: str):
    """Manually trigger INDEXES_ENABLING work (rebuild indexes, re-enable constraints
    and triggers).  Migration must be in INDEXES_ENABLING phase."""
    fn = _state.get("enable_indexes")
    if fn is None:
        return jsonify({"error": "enable_indexes not wired"}), 500
    try:
        fn(migration_id)
        return jsonify({"ok": True})
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 409
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.post("/api/migrations/<migration_id>/enable-triggers")
@db_required(_state)
def enable_triggers(migration_id: str):
    """Manually re-enable DISABLED triggers on the target table.
    Only allowed once CDC apply is running (CDC_CATCHING_UP / CDC_CAUGHT_UP / STEADY_STATE)."""
    fn = _state.get("enable_triggers")
    if fn is None:
        return jsonify({"error": "enable_triggers not wired"}), 500
    try:
        fn(migration_id)
        return jsonify({"ok": True})
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 409
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.post("/api/migrations/<migration_id>/restart-baseline")
@db_required(_state)
def restart_baseline(migration_id: str):
    """Restart the baseline phase: delete old BASELINE chunks, TRUNCATE target,
    rebuild unique indexes, re-chunk and re-load."""
    fn = _state.get("restart_baseline")
    if fn is None:
        return jsonify({"error": "restart_baseline not wired"}), 500
    try:
        fn(migration_id)
        return jsonify({"ok": True})
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 409
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.get("/api/migrations/<migration_id>/validation")
@db_required(_state)
def get_validation_result(migration_id: str):
    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT validation_result FROM migrations WHERE migration_id = %s",
                    (migration_id,),
                )
                row = cur.fetchone()
        finally:
            conn.close()

        if not row:
            return jsonify({"error": "Not found"}), 404

        raw = row[0]
        if raw is None:
            return jsonify({"result": None, "message": "Validation not yet run"})

        result = raw if isinstance(raw, dict) else json.loads(raw)
        return jsonify({"result": result})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
