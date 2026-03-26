"""
Shared utilities for workers: PostgreSQL state DB access, Oracle connections,
config loading.  Workers operate directly on the state DB — no HTTP to Flask.
"""

import json
import os
import socket
from datetime import datetime
from typing import Optional

import re
import time

import psycopg2
import psycopg2.extras

STATE_DB_DSN = os.environ.get(
    "STATE_DB_DSN",
    "postgresql://postgres:postgres@localhost:5432/migration_state",
)
WORKER_ID = os.environ.get("WORKER_ID", f"{socket.gethostname()}:{os.getpid()}")


def _masked_dsn(dsn: str) -> str:
    return re.sub(r"(://[^:]+:)[^@]+(@)", r"\1***\2", dsn)


print(f"[state_db] DSN = {_masked_dsn(STATE_DB_DSN)}")

_MAX_RETRIES   = 3
# How long without a heartbeat before a CDC worker is considered stale.
# Must match the value used in backend/routes/workers.py.
CDC_HEARTBEAT_STALE_MINUTES = int(os.environ.get("CDC_HEARTBEAT_STALE_MINUTES", "2"))
_STALE_MINUTES = 10


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------

def get_pg_conn():
    """Open a new PostgreSQL connection to the state DB."""
    try:
        conn = psycopg2.connect(STATE_DB_DSN)
        return conn
    except Exception as exc:
        print(f"[state_db] connection FAILED ({_masked_dsn(STATE_DB_DSN)}): {exc}")
        raise


def get_pg_conn_with_retry(retries: int = 0) -> "psycopg2.connection":
    """Connect to state DB, retrying indefinitely (retries=0) or N times.

    Logs each failed attempt so container logs show progress.
    """
    attempt = 1
    while True:
        try:
            conn = psycopg2.connect(STATE_DB_DSN)
            if attempt > 1:
                print(f"[state_db] connected after {attempt} attempt(s)")
            return conn
        except Exception as exc:
            print(f"[state_db] attempt #{attempt} FAILED ({_masked_dsn(STATE_DB_DSN)}): {exc}")
            if retries and attempt >= retries:
                raise
            attempt += 1
            time.sleep(5)


# ---------------------------------------------------------------------------
# Service configs (oracle_source / oracle_target / kafka / kafka_connect)
# ---------------------------------------------------------------------------

def load_configs(conn) -> dict:
    """Read all service configs from service_configs table."""
    with conn.cursor() as cur:
        cur.execute("SELECT service_name, config FROM service_configs")
        return {row[0]: row[1] for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# Oracle connection
# ---------------------------------------------------------------------------

def open_oracle(connection_id: str, configs: dict):
    """
    Open an Oracle connection using the service config keyed by connection_id.
    connection_id = 'oracle_source' | 'oracle_target'
    """
    try:
        import oracledb
    except ImportError:
        raise ImportError("oracledb not installed: pip install oracledb")

    cfg          = configs.get(connection_id) or {}
    host         = (cfg.get("host") or "").strip()
    port         = int(cfg.get("port") or 1521)
    service_name = (cfg.get("service_name") or "").strip()
    user         = (cfg.get("user") or "").strip()
    password     = cfg.get("password") or ""

    if not host or not service_name or not user:
        raise ValueError(
            f"Oracle '{connection_id}' not configured — "
            "check Settings in the UI."
        )
    conn = oracledb.connect(
        user=user,
        password=password,
        dsn=f"{host}:{port}/{service_name}",
    )

    # By default oracledb binds Python datetime as Oracle DATE which has no
    # fractional seconds.  Override to TIMESTAMP so .microsecond is preserved.
    def _input_type_handler(cursor, value, arraysize):
        if isinstance(value, datetime):
            return cursor.var(oracledb.DB_TYPE_TIMESTAMP, arraysize=arraysize)

    conn.inputtypehandler = _input_type_handler
    return conn


# ---------------------------------------------------------------------------
# Chunk job queue  (mirrors backend/services/job_queue.py but for workers)
# ---------------------------------------------------------------------------

def claim_chunk(conn) -> Optional[dict]:
    """
    Atomically claim one PENDING chunk (BULK or BASELINE).

    Respects per-type parallelism limits:
      BULK     chunks → max_parallel_workers
      BASELINE chunks → baseline_parallel_degree
    Skips any migration that already has >= limit chunks in CLAIMED/RUNNING state.

    Fair scheduling: among eligible migrations prefers the one with the
    fewest active chunks, then earliest state_changed_at, then chunk_seq.

    Returns a dict with chunk + migration context, or None if nothing available.
    """
    with conn.cursor() as cur:
        cur.execute("""
            WITH candidate AS (
                SELECT c.chunk_id
                FROM   migration_chunks c
                JOIN   migrations m ON m.migration_id = c.migration_id
                WHERE  c.status = 'PENDING'
                  AND  (
                      (COALESCE(c.chunk_type, 'BULK') = 'BULK'     AND m.phase = 'BULK_LOADING')
                   OR (c.chunk_type = 'BASELINE' AND m.phase = 'BASELINE_LOADING')
                  )
                  AND  (
                      SELECT COUNT(*)
                      FROM   migration_chunks c2
                      WHERE  c2.migration_id = c.migration_id
                        AND  c2.chunk_type   = c.chunk_type
                        AND  c2.status IN ('CLAIMED', 'RUNNING')
                  ) < GREATEST(
                      CASE WHEN COALESCE(c.chunk_type, 'BULK') = 'BASELINE'
                           THEN COALESCE(m.baseline_parallel_degree, 1)
                           ELSE COALESCE(m.max_parallel_workers, 1)
                      END, 1)
                ORDER BY (
                      SELECT COUNT(*)
                      FROM   migration_chunks c3
                      WHERE  c3.migration_id = c.migration_id
                        AND  c3.chunk_type   = c.chunk_type
                        AND  c3.status IN ('CLAIMED', 'RUNNING')
                  ) ASC,
                  m.state_changed_at ASC,
                  c.chunk_seq ASC
                FOR UPDATE OF c SKIP LOCKED
                LIMIT 1
            )
            UPDATE migration_chunks
            SET    status     = 'CLAIMED',
                   worker_id  = %s,
                   claimed_at = NOW(),
                   started_at = NOW()
            WHERE  chunk_id = (SELECT chunk_id FROM candidate)
            RETURNING chunk_id, migration_id, chunk_seq, rowid_start, rowid_end,
                      COALESCE(chunk_type, 'BULK')
        """, (WORKER_ID,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None

        chunk_id, migration_id, chunk_seq, rowid_start, rowid_end, chunk_type = row

        cur.execute("""
            SELECT source_connection_id, target_connection_id,
                   source_schema, source_table,
                   target_schema, target_table, stage_table_name,
                   start_scn, migration_strategy
            FROM   migrations
            WHERE  migration_id = %s
        """, (migration_id,))
        mrow = cur.fetchone()
        if not mrow:
            conn.rollback()
            return None

        (src_conn_id, dst_conn_id,
         src_schema, src_table,
         tgt_schema, tgt_table, stage_table,
         start_scn, migration_strategy) = mrow

    conn.commit()
    return {
        "chunk_id":             str(chunk_id),
        "migration_id":         str(migration_id),
        "chunk_seq":            chunk_seq,
        "chunk_type":           chunk_type,
        "rowid_start":          rowid_start,
        "rowid_end":            rowid_end,
        "source_connection_id": src_conn_id,
        "target_connection_id": dst_conn_id,
        "source_schema":        src_schema,
        "source_table":         src_table,
        "target_schema":        tgt_schema,
        "target_table":         tgt_table,
        "stage_table":          stage_table,
        "start_scn":            str(start_scn) if start_scn is not None else None,
        "migration_strategy":   (migration_strategy or "STAGE").upper(),
    }


def update_chunk_progress(conn, chunk_id: str, rows_loaded: int) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE migration_chunks
            SET    rows_loaded = %s, status = 'RUNNING'
            WHERE  chunk_id   = %s
        """, (rows_loaded, chunk_id))
    conn.commit()


def complete_chunk(conn, chunk_id: str, rows_loaded: int) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE migration_chunks
            SET    status       = 'DONE',
                   rows_loaded  = %s,
                   completed_at = NOW()
            WHERE  chunk_id     = %s
            RETURNING migration_id, COALESCE(chunk_type, 'BULK')
        """, (rows_loaded, chunk_id))
        row = cur.fetchone()
        if row:
            migration_id, chunk_type = row
            if chunk_type == 'BASELINE':
                cur.execute("""
                    UPDATE migrations
                    SET    baseline_chunks_done = baseline_chunks_done + 1,
                           rows_loaded          = rows_loaded + %s,
                           updated_at           = NOW()
                    WHERE  migration_id = %s
                """, (rows_loaded, migration_id))
            else:
                cur.execute("""
                    UPDATE migrations
                    SET    chunks_done  = chunks_done + 1,
                           rows_loaded  = rows_loaded + %s,
                           updated_at   = NOW()
                    WHERE  migration_id = %s
                """, (rows_loaded, migration_id))
    conn.commit()


def fail_chunk_permanent(conn, chunk_id: str, error_text: str) -> None:
    """Mark a chunk FAILED immediately, skipping retry logic.

    Use for errors where retrying with the same parameters is pointless
    (e.g. ORA-01555: snapshot too old — the SCN won't get newer on retry).
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE migration_chunks
            SET    status       = 'FAILED',
                   error_text   = %s,
                   completed_at = NOW()
            WHERE  chunk_id     = %s
            RETURNING migration_id
        """, (error_text[:2000], chunk_id))
        row = cur.fetchone()
        if row:
            cur.execute("""
                UPDATE migrations
                SET    chunks_failed = chunks_failed + 1,
                       updated_at    = NOW()
                WHERE  migration_id  = %s
            """, (row[0],))
    conn.commit()


def fail_chunk(conn, chunk_id: str, error_text: str) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT retry_count, migration_id
            FROM   migration_chunks
            WHERE  chunk_id = %s
            FOR UPDATE
        """, (chunk_id,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return
        retry_count, migration_id = row

        if retry_count < _MAX_RETRIES:
            cur.execute("""
                UPDATE migration_chunks
                SET    status      = 'PENDING',
                       worker_id   = NULL,
                       claimed_at  = NULL,
                       started_at  = NULL,
                       error_text  = %s,
                       retry_count = retry_count + 1
                WHERE  chunk_id    = %s
            """, (error_text[:2000], chunk_id))
        else:
            cur.execute("""
                UPDATE migration_chunks
                SET    status       = 'FAILED',
                       error_text   = %s,
                       completed_at = NOW()
                WHERE  chunk_id     = %s
            """, (error_text[:2000], chunk_id))
            cur.execute("""
                UPDATE migrations
                SET    chunks_failed = chunks_failed + 1,
                       updated_at    = NOW()
                WHERE  migration_id  = %s
            """, (migration_id,))
    conn.commit()


# ---------------------------------------------------------------------------
# CDC state
# ---------------------------------------------------------------------------

def claim_cdc_migration(conn) -> Optional[dict]:
    """
    Claim a CDC migration that has no active worker (heartbeat stale / absent).
    Immediately writes our heartbeat to prevent double-claiming.
    Returns migration dict or None.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT m.migration_id,
                   m.target_connection_id, m.target_schema, m.target_table,
                   m.source_schema, m.source_table,
                   m.topic_prefix, m.consumer_group,
                   m.effective_key_columns_json
            FROM   migrations m
            LEFT JOIN migration_cdc_state cs ON cs.migration_id = m.migration_id
            WHERE  m.phase IN ('CDC_APPLY_STARTING', 'CDC_APPLYING', 'CDC_CATCHING_UP', 'STEADY_STATE')
              AND  (
                     cs.worker_heartbeat IS NULL
                  OR cs.worker_heartbeat < NOW() - make_interval(mins => %s)
              )
            ORDER BY m.state_changed_at
            LIMIT 1
            FOR UPDATE OF m SKIP LOCKED
        """, (CDC_HEARTBEAT_STALE_MINUTES,))
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            return None

        keys = [
            "migration_id",
            "target_connection_id", "target_schema", "target_table",
            "source_schema", "source_table",
            "topic_prefix", "consumer_group",
            "effective_key_columns_json",
        ]
        migration = dict(zip(keys, row))

        # Reserve: write our heartbeat immediately
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
        """, (WORKER_ID, migration["migration_id"]))

    conn.commit()
    return migration


def cdc_checkin(conn, migration_id: str, total_lag: int,
                rows_applied: int, last_event_ts: Optional[str] = None) -> None:
    """Update CDC heartbeat + lag in state DB."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO migration_cdc_state
                (migration_id, consumer_group, topic,
                 total_lag, rows_applied, worker_id, worker_heartbeat, updated_at)
            SELECT migration_id, consumer_group, topic_prefix,
                   %s, %s, %s, NOW(), NOW()
            FROM   migrations
            WHERE  migration_id = %s
            ON CONFLICT (migration_id) DO UPDATE
                SET total_lag        = EXCLUDED.total_lag,
                    rows_applied     = EXCLUDED.rows_applied,
                    worker_id        = EXCLUDED.worker_id,
                    worker_heartbeat = NOW(),
                    updated_at       = NOW()
        """, (total_lag, rows_applied, WORKER_ID, migration_id))

        # Mirror lag onto migrations table so UI can read it
        cur.execute("""
            UPDATE migrations
            SET    kafka_lag            = %s,
                   kafka_lag_checked_at = NOW(),
                   updated_at          = NOW()
            WHERE  migration_id = %s
        """, (total_lag, migration_id))
    conn.commit()


# ---------------------------------------------------------------------------
# Data comparison chunks
# ---------------------------------------------------------------------------

_MAX_COMPARE_RETRIES = 3


def claim_compare_chunk(conn) -> Optional[dict]:
    """Atomically claim one PENDING data-compare chunk.

    Returns chunk dict with task context, or None.
    """
    with conn.cursor() as cur:
        cur.execute("""
            WITH candidate AS (
                SELECT c.chunk_id
                FROM   data_compare_chunks c
                JOIN   data_compare_tasks  t ON t.task_id = c.task_id
                WHERE  c.status = 'PENDING'
                  AND  t.status = 'RUNNING'
                ORDER BY c.created_at, c.chunk_seq
                FOR UPDATE OF c SKIP LOCKED
                LIMIT 1
            )
            UPDATE data_compare_chunks
            SET    status     = 'CLAIMED',
                   worker_id  = %s,
                   claimed_at = NOW()
            WHERE  chunk_id = (SELECT chunk_id FROM candidate)
            RETURNING chunk_id, task_id, side, chunk_seq, rowid_start, rowid_end
        """, (WORKER_ID,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None

        chunk_id, task_id, side, chunk_seq, rowid_start, rowid_end = row

        cur.execute("""
            SELECT source_schema, source_table, target_schema, target_table
            FROM   data_compare_tasks
            WHERE  task_id = %s
        """, (task_id,))
        trow = cur.fetchone()
        if not trow:
            conn.rollback()
            return None

        src_schema, src_table, tgt_schema, tgt_table = trow

    conn.commit()

    schema = src_schema if side == "source" else tgt_schema
    table = src_table if side == "source" else tgt_table
    conn_id = "oracle_source" if side == "source" else "oracle_target"

    return {
        "chunk_id":      str(chunk_id),
        "task_id":       str(task_id),
        "side":          side,
        "chunk_seq":     chunk_seq,
        "rowid_start":   rowid_start,
        "rowid_end":     rowid_end,
        "schema":        schema,
        "table":         table,
        "connection_id": conn_id,
    }


def complete_compare_chunk(conn, chunk_id: str, row_count: int, hash_sum) -> str:
    """Mark a compare chunk DONE and increment task.chunks_done.
    Returns the task_id.
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE data_compare_chunks
            SET    status       = 'DONE',
                   row_count    = %s,
                   hash_sum     = %s,
                   completed_at = NOW()
            WHERE  chunk_id     = %s
            RETURNING task_id
        """, (row_count, str(hash_sum) if hash_sum is not None else None, chunk_id))
        row = cur.fetchone()
        task_id = str(row[0]) if row else None

        if task_id:
            cur.execute("""
                UPDATE data_compare_tasks
                SET    chunks_done = chunks_done + 1
                WHERE  task_id = %s
            """, (task_id,))
    conn.commit()
    return task_id


def fail_compare_chunk(conn, chunk_id: str, error_text: str) -> Optional[str]:
    """Handle compare chunk failure with retry logic. Returns task_id."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT retry_count, task_id
            FROM   data_compare_chunks
            WHERE  chunk_id = %s
            FOR UPDATE
        """, (chunk_id,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None
        retry_count, task_id = row

        if retry_count < _MAX_COMPARE_RETRIES:
            cur.execute("""
                UPDATE data_compare_chunks
                SET    status      = 'PENDING',
                       worker_id   = NULL,
                       claimed_at  = NULL,
                       error_text  = %s,
                       retry_count = retry_count + 1
                WHERE  chunk_id    = %s
            """, (error_text[:2000], chunk_id))
        else:
            cur.execute("""
                UPDATE data_compare_chunks
                SET    status       = 'FAILED',
                       error_text   = %s,
                       completed_at = NOW()
                WHERE  chunk_id     = %s
            """, (error_text[:2000], chunk_id))
    conn.commit()
    return str(task_id)


def trigger_lag_zero(conn, migration_id: str) -> None:
    """
    Transition migration CDC_CATCHING_UP → CDC_CAUGHT_UP when lag reaches 0.
    Only acts if migration is currently in CDC_CATCHING_UP.
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE migrations
            SET    phase            = 'CDC_CAUGHT_UP',
                   state_changed_at = NOW(),
                   updated_at       = NOW()
            WHERE  migration_id = %s
              AND  phase = 'CDC_CATCHING_UP'
        """, (migration_id,))
    conn.commit()
