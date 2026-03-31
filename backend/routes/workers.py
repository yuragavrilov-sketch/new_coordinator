"""Worker-facing HTTP endpoints.

Bulk workers and CDC apply workers communicate exclusively through this API.
"""

import json
from datetime import datetime

from flask import Blueprint, jsonify, request

import os

import services.job_queue as job_queue

bp = Blueprint("workers", __name__)

# Must match workers/common.py CDC_HEARTBEAT_STALE_MINUTES
_CDC_HEARTBEAT_STALE_MINUTES = int(os.environ.get("CDC_HEARTBEAT_STALE_MINUTES", "2"))

_state: dict = {}


def init(get_conn_fn, row_to_dict_fn, db_available_ref, broadcast_fn,
         load_configs_fn=None) -> None:
    _state["get_conn"]      = get_conn_fn
    _state["row_to_dict"]   = row_to_dict_fn
    _state["db_available"]  = db_available_ref
    _state["broadcast"]     = broadcast_fn
    _state["load_configs"]  = load_configs_fn


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
# Compare-worker chunk endpoints
# ---------------------------------------------------------------------------

@bp.post("/api/worker/compare/claim")
def compare_claim():
    """Claim the next available PENDING compare chunk. Returns 204 if nothing to do."""
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503

    body = request.get_json(force=True) or {}
    worker_id = body.get("worker_id", "unknown")

    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT dc.chunk_id, dc.task_id, dc.chunk_seq, dc.rowid_start, dc.rowid_end,
                       dc.side,
                       dt.source_schema, dt.source_table, dt.target_schema, dt.target_table
                FROM data_compare_chunks dc
                JOIN data_compare_tasks dt ON dt.task_id = dc.task_id
                WHERE dc.status = 'PENDING' AND dt.status = 'RUNNING'
                ORDER BY dc.created_at, dc.chunk_seq
                LIMIT 1
                FOR UPDATE OF dc SKIP LOCKED
            """)
            row = cur.fetchone()
            if not row:
                conn.rollback()
                return "", 204

            chunk_id = str(row[0])
            cur.execute("""
                UPDATE data_compare_chunks
                SET status='CLAIMED', worker_id=%s, claimed_at=NOW()
                WHERE chunk_id = %s
            """, (worker_id, chunk_id))
        conn.commit()

        # Resolve schema/table/connection_id based on side
        side = row[5]
        src_schema, src_table = row[6], row[7]
        tgt_schema, tgt_table = row[8], row[9]

        if side == "source":
            schema, table, connection_id = src_schema, src_table, "oracle_source"
        else:
            schema, table, connection_id = tgt_schema, tgt_table, "oracle_target"

        # Get connection configs for source and target
        configs = {}
        if _state.get("load_configs"):
            configs = _state["load_configs"]()
        src_cfg = configs.get("oracle_source", {})
        tgt_cfg = configs.get("oracle_target", {})

        return jsonify({"chunk": {
            "chunk_id": chunk_id,
            "task_id": str(row[1]),
            "chunk_seq": row[2],
            "side": side,
            "rowid_start": row[3],
            "rowid_end": row[4],
            "schema": schema,
            "table": table,
            "connection_id": connection_id,
            "source_schema": src_schema,
            "source_table": src_table,
            "target_schema": tgt_schema,
            "target_table": tgt_table,
            "source_connection": src_cfg,
            "target_connection": tgt_cfg,
        }}), 200
    finally:
        conn.close()


@bp.post("/api/worker/compare/<chunk_id>/complete")
def compare_complete(chunk_id: str):
    """Mark a compare chunk as DONE with count/hash results."""
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503

    body = request.get_json(force=True) or {}
    row_count = int(body.get("row_count", 0))
    hash_sum = body.get("hash_sum")

    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE data_compare_chunks
                SET status='DONE', completed_at=NOW(),
                    row_count=%s, hash_sum=%s
                WHERE chunk_id = %s
                RETURNING task_id
            """, (row_count,
                  str(hash_sum) if hash_sum is not None else None,
                  chunk_id))
            crow = cur.fetchone()
            if not crow:
                conn.rollback()
                return jsonify({"error": "chunk not found"}), 404
            task_id = str(crow[0])

            # Update task chunks_done counter
            cur.execute("""
                UPDATE data_compare_tasks
                SET chunks_done = (
                    SELECT COUNT(*) FROM data_compare_chunks
                    WHERE task_id = %s AND status = 'DONE'
                )
                WHERE task_id = %s
            """, (task_id, task_id))

            # Check if all chunks are done
            cur.execute("""
                SELECT t.chunks_total,
                       COUNT(*) FILTER (WHERE c.status = 'DONE')   AS done,
                       COUNT(*) FILTER (WHERE c.status = 'FAILED') AS failed,
                       COUNT(*) FILTER (WHERE c.status IN ('PENDING', 'CLAIMED')) AS active
                FROM data_compare_tasks t
                JOIN data_compare_chunks c ON c.task_id = t.task_id
                WHERE t.task_id = %s
                GROUP BY t.chunks_total
            """, (task_id,))
            task_row = cur.fetchone()
            if task_row:
                total, done, failed, active = task_row
                if active == 0:
                    # All chunks processed — aggregate results
                    if failed > 0:
                        cur.execute("""
                            UPDATE data_compare_tasks
                            SET status='FAILED', error_text=%s, completed_at=NOW()
                            WHERE task_id = %s
                        """, (f"{failed} chunk(s) failed", task_id))
                    else:
                        # Aggregate per side
                        cur.execute("""
                            SELECT side,
                                   SUM(COALESCE(row_count, 0)),
                                   SUM(COALESCE(hash_sum::bigint, 0))
                            FROM data_compare_chunks
                            WHERE task_id = %s AND status = 'DONE'
                            GROUP BY side
                        """, (task_id,))
                        side_data = {}
                        for side, rc, hs in cur.fetchall():
                            side_data[side] = {"count": int(rc), "hash": hs}

                        src = side_data.get("source", {"count": 0, "hash": 0})
                        tgt = side_data.get("target", {"count": 0, "hash": 0})
                        counts_match = src["count"] == tgt["count"]
                        hash_match = src["hash"] == tgt["hash"]

                        cur.execute("""
                            UPDATE data_compare_tasks
                            SET status='DONE', completed_at=NOW(),
                                source_count=%s, target_count=%s,
                                source_hash=%s, target_hash=%s,
                                counts_match=%s, hash_match=%s
                            WHERE task_id = %s
                        """, (src["count"], tgt["count"],
                              str(src["hash"]), str(tgt["hash"]),
                              counts_match, hash_match, task_id))
        conn.commit()
        return jsonify({"ok": True})
    finally:
        conn.close()


@bp.post("/api/worker/compare/<chunk_id>/fail")
def compare_fail(chunk_id: str):
    """Mark a compare chunk as FAILED."""
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503

    body = request.get_json(force=True) or {}
    error_text = body.get("error_text", "unknown error")

    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE data_compare_chunks
                SET status='FAILED', error_text=%s, completed_at=NOW()
                WHERE chunk_id = %s
            """, (error_text[:2000], chunk_id))
        conn.commit()
        return jsonify({"ok": True})
    finally:
        conn.close()


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
                      OR cs.worker_heartbeat < NOW() - make_interval(mins => %s)
                  )
                ORDER BY m.state_changed_at
                LIMIT 1
            """, (_CDC_HEARTBEAT_STALE_MINUTES,))
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
