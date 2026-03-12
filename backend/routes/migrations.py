"""Migrations CRUD and phase-transition routes."""

import uuid
from datetime import datetime

from flask import Blueprint, jsonify, request

bp = Blueprint("migrations", __name__)

_VALID_PHASES = {
    "DRAFT", "NEW", "PREPARING", "SCN_FIXED",
    "CONNECTOR_STARTING", "CDC_BUFFERING",
    "CHUNKING", "BULK_LOADING", "BULK_LOADED",
    "STAGE_VALIDATING", "STAGE_VALIDATED",
    "BASELINE_PUBLISHING", "BASELINE_PUBLISHED",
    "CDC_APPLY_STARTING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP",
    "STEADY_STATE", "PAUSED",
    "CANCELLING", "CANCELLED",
    "COMPLETED", "FAILED",
}

_LIST_COLS = """
    migration_id, migration_name, phase, state_changed_at,
    source_connection_id, target_connection_id,
    source_schema, source_table, target_schema, target_table,
    created_at, updated_at,
    error_code, error_text, failed_phase, retry_count,
    description, created_by
"""

_state: dict = {}


def init(get_conn_fn, row_to_dict_fn, db_available_ref, broadcast_fn):
    _state["get_conn"]      = get_conn_fn
    _state["row_to_dict"]   = row_to_dict_fn
    _state["db_available"]  = db_available_ref
    _state["broadcast"]     = broadcast_fn


def _db_ok() -> bool:
    return _state["db_available"]["value"]


@bp.get("/api/migrations")
def list_migrations():
    if not _db_ok():
        return jsonify([])
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
def get_migration(migration_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
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
def create_migration():
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    body = request.get_json(force=True) or {}
    if not body.get("migration_name", "").strip():
        return jsonify({"error": "migration_name is required"}), 400

    initial_phase = body.get("initial_phase", "DRAFT").strip().upper()
    if initial_phase not in _VALID_PHASES:
        return jsonify({"error": f"Invalid initial_phase: {initial_phase}"}), 400

    mid = str(uuid.uuid4())
    now = datetime.utcnow()

    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO migrations (
                        migration_id, migration_name, phase, state_changed_at,
                        source_connection_id, target_connection_id,
                        source_schema, source_table, target_schema, target_table,
                        stage_table_name, connector_name, topic_prefix, consumer_group,
                        chunk_size,
                        source_pk_exists, source_uk_exists,
                        effective_key_type, effective_key_source, effective_key_columns_json,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s
                    )
                """, (
                    mid, body["migration_name"], initial_phase, now,
                    body.get("source_connection_id", ""),
                    body.get("target_connection_id", ""),
                    body.get("source_schema", ""), body.get("source_table", ""),
                    body.get("target_schema", ""), body.get("target_table", ""),
                    body.get("stage_table_name", ""), body.get("connector_name", ""),
                    body.get("topic_prefix", ""), body.get("consumer_group", ""),
                    body.get("chunk_size", 1_000_000),
                    body.get("source_pk_exists", False), body.get("source_uk_exists", False),
                    body.get("effective_key_type", ""), body.get("effective_key_source", ""),
                    body.get("effective_key_columns_json", "[]"),
                    now, now,
                ))
                cur.execute("""
                    INSERT INTO migration_state_history
                        (migration_id, from_phase, to_phase, message, actor_type)
                    VALUES (%s, NULL, %s, %s, 'USER')
                """, (mid, initial_phase, "Migration created"))
            conn.commit()
        finally:
            conn.close()

        _state["broadcast"]({
            "type":         "migration_phase",
            "migration_id": mid,
            "phase":        initial_phase,
            "ts":           now.isoformat() + "Z",
        })
        return jsonify({"ok": True, "migration_id": mid}), 201

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.patch("/api/migrations/<migration_id>/phase")
def transition_phase(migration_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    body = request.get_json(force=True) or {}
    to_phase = body.get("to_phase", "").strip().upper()
    if to_phase not in _VALID_PHASES:
        return jsonify({"error": f"Invalid phase: {to_phase}"}), 400

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
                    "phase":            to_phase,
                    "state_changed_at": now,
                    "updated_at":       now,
                }
                if to_phase == "FAILED":
                    if body.get("error_code"):  update_fields["error_code"] = body["error_code"]
                    if body.get("error_text"):  update_fields["error_text"] = body["error_text"]
                    update_fields["failed_phase"] = from_phase
                if body.get("retry_count") is not None:
                    update_fields["retry_count"] = body["retry_count"]

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
                    migration_id, from_phase, to_phase,
                    body.get("transition_status", "SUCCESS"),
                    body.get("transition_reason"),
                    body.get("message"),
                    body.get("actor_type", "SYSTEM"),
                    body.get("actor_id"),
                    body.get("correlation_id"),
                ))
            conn.commit()
        finally:
            conn.close()

        _state["broadcast"]({
            "type":         "migration_phase",
            "migration_id": migration_id,
            "from_phase":   from_phase,
            "phase":        to_phase,
            "ts":           now.isoformat() + "Z",
        })
        return jsonify({"ok": True, "from_phase": from_phase, "to_phase": to_phase})

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
