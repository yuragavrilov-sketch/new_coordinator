"""Worker-facing HTTP endpoints.

Bulk workers and CDC apply workers communicate exclusively through this API.
"""

import json
from datetime import datetime

from flask import Blueprint, jsonify, request

import services.job_queue as job_queue

bp = Blueprint("workers", __name__)

_state: dict = {}


def init(get_conn_fn, row_to_dict_fn, db_available_ref, broadcast_fn) -> None:
    _state["get_conn"]     = get_conn_fn
    _state["row_to_dict"]  = row_to_dict_fn
    _state["db_available"] = db_available_ref
    _state["broadcast"]    = broadcast_fn


def _db_ok() -> bool:
    return _state["db_available"]["value"]


# ---------------------------------------------------------------------------
# Bulk-worker chunk endpoints
# ---------------------------------------------------------------------------

@bp.post("/api/worker/chunks/claim")
def claim_chunk():
    """Claim the next available PENDING chunk. Returns 204 if nothing to do."""
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503

    body      = request.get_json(force=True) or {}
    worker_id = body.get("worker_id", "unknown")

    conn = _state["get_conn"]()
    try:
        chunk = job_queue.claim_chunk(conn, worker_id)
    finally:
        conn.close()

    if chunk is None:
        return "", 204  # nothing to claim

    return jsonify(chunk), 200


@bp.post("/api/worker/chunks/<chunk_id>/progress")
def chunk_progress(chunk_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503

    body       = request.get_json(force=True) or {}
    rows_loaded = int(body.get("rows_loaded", 0))

    conn = _state["get_conn"]()
    try:
        job_queue.update_chunk_progress(conn, chunk_id, rows_loaded)
    finally:
        conn.close()

    return jsonify({"ok": True})


@bp.post("/api/worker/chunks/<chunk_id>/complete")
def chunk_complete(chunk_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503

    body       = request.get_json(force=True) or {}
    rows_loaded = int(body.get("rows_loaded", 0))

    conn = _state["get_conn"]()
    try:
        job_queue.complete_chunk(conn, chunk_id, rows_loaded)
    finally:
        conn.close()

    return jsonify({"ok": True})


@bp.post("/api/worker/chunks/<chunk_id>/fail")
def chunk_fail(chunk_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503

    body       = request.get_json(force=True) or {}
    error_text = body.get("error_text", "unknown error")

    conn = _state["get_conn"]()
    try:
        job_queue.fail_chunk(conn, chunk_id, error_text)
    finally:
        conn.close()

    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# CDC apply-worker endpoints
# ---------------------------------------------------------------------------

@bp.post("/api/worker/cdc/checkin")
def cdc_checkin():
    """
    CDC apply-worker heartbeat + lag report.
    Body: { migration_id, worker_id, lag, rows_applied, last_event_ts }
    """
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503

    body         = request.get_json(force=True) or {}
    migration_id = body.get("migration_id", "").strip()
    if not migration_id:
        return jsonify({"error": "migration_id required"}), 400

    worker_id     = body.get("worker_id", "unknown")
    lag           = int(body.get("lag", 0))
    rows_applied  = int(body.get("rows_applied", 0))
    last_event_ts = body.get("last_event_ts")  # ISO string or None
    now           = datetime.utcnow()

    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO migration_cdc_state
                    (migration_id, consumer_group, topic,
                     total_lag, worker_id, worker_heartbeat, updated_at)
                SELECT migration_id, consumer_group, topic_prefix,
                       %s, %s, NOW(), NOW()
                FROM   migrations
                WHERE  migration_id = %s
                ON CONFLICT (migration_id) DO UPDATE
                    SET total_lag        = EXCLUDED.total_lag,
                        worker_id        = EXCLUDED.worker_id,
                        worker_heartbeat = NOW(),
                        updated_at       = NOW()
            """, (lag, worker_id, migration_id))
        conn.commit()
    finally:
        conn.close()

    _state["broadcast"]({
        "type":         "kafka_lag",
        "migration_id": migration_id,
        "total_lag":    lag,
        "rows_applied": rows_applied,
        "updated_at":   now.isoformat() + "Z",
        "ts":           now.isoformat() + "Z",
    })

    return jsonify({"ok": True})


@bp.post("/api/worker/cdc/claim")
def cdc_claim():
    """
    Claim a CDC migration that needs a worker.
    Returns migration details (200) or 204 if nothing needs a worker.

    A migration is claimable when:
    - phase IN (CDC_APPLY_STARTING, CDC_CATCHING_UP, STEADY_STATE)
    - no worker heartbeat in the last 2 minutes (stale or never started)
    """
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503

    body      = request.get_json(force=True) or {}
    worker_id = body.get("worker_id", "unknown")

    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT m.migration_id,
                       m.target_connection_id, m.target_schema, m.target_table,
                       m.source_schema, m.source_table,
                       m.topic_prefix, m.consumer_group,
                       m.effective_key_columns_json
                FROM   migrations m
                LEFT JOIN migration_cdc_state cs ON cs.migration_id = m.migration_id
                WHERE  m.phase IN ('CDC_APPLY_STARTING', 'CDC_CATCHING_UP', 'STEADY_STATE')
                  AND  (
                         cs.worker_heartbeat IS NULL
                      OR cs.worker_heartbeat < NOW() - INTERVAL '2 minutes'
                  )
                ORDER BY m.state_changed_at
                LIMIT 1
            """)
            row = cur.fetchone()
            if row is None:
                return "", 204

            keys = [
                "migration_id",
                "target_connection_id", "target_schema", "target_table",
                "source_schema", "source_table",
                "topic_prefix", "consumer_group",
                "effective_key_columns_json",
            ]
            migration = dict(zip(keys, row))

        # Immediately register heartbeat so another worker won't claim it
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO migration_cdc_state
                    (migration_id, consumer_group, topic,
                     total_lag, worker_id, worker_heartbeat, updated_at)
                SELECT migration_id, consumer_group, topic_prefix,
                       0, %s, NOW(), NOW()
                FROM   migrations
                WHERE  migration_id = %s
                ON CONFLICT (migration_id) DO UPDATE
                    SET worker_id        = EXCLUDED.worker_id,
                        worker_heartbeat = NOW(),
                        updated_at       = NOW()
            """, (worker_id, migration["migration_id"]))
        conn.commit()
    finally:
        conn.close()

    return jsonify(migration), 200
