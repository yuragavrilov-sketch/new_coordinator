"""
Chunk job-queue backed by the migration_chunks PostgreSQL table.

Workers claim chunks via SELECT … FOR UPDATE SKIP LOCKED so multiple
workers can safely pick jobs in parallel without double-claiming.
"""

import json
from datetime import datetime, timedelta
from typing import Optional

_MAX_RETRIES = 3
_STALE_AFTER_MINUTES = 10


# ---------------------------------------------------------------------------
# Worker-facing operations
# ---------------------------------------------------------------------------

def claim_chunk(conn, worker_id: str) -> Optional[dict]:
    """
    Atomically claim one PENDING chunk for *worker_id*.

    Uses a single CTE to select candidate + update in one round-trip so
    there is no window where a chunk can be left in CLAIMED without a worker.
    Also respects max_parallel_workers per migration.

    Returns a dict with chunk + migration context, or None if nothing available.
    """
    with conn.cursor() as cur:
        cur.execute("""
            WITH candidate AS (
                SELECT c.chunk_id
                FROM   migration_chunks c
                JOIN   migrations m ON m.migration_id = c.migration_id
                WHERE  c.status = 'PENDING'
                  AND  m.phase  = 'BULK_LOADING'
                  AND  (
                      SELECT COUNT(*)
                      FROM   migration_chunks c2
                      WHERE  c2.migration_id = c.migration_id
                        AND  c2.status IN ('CLAIMED', 'RUNNING')
                  ) < GREATEST(COALESCE(m.max_parallel_workers, 1), 1)
                ORDER BY c.created_at
                FOR UPDATE OF c SKIP LOCKED
                LIMIT 1
            ),
            updated AS (
                UPDATE migration_chunks
                SET    status     = 'CLAIMED',
                       worker_id  = %s,
                       claimed_at = NOW(),
                       started_at = NOW()
                WHERE  chunk_id = (SELECT chunk_id FROM candidate)
                RETURNING chunk_id, migration_id, chunk_seq, rowid_start, rowid_end
            )
            SELECT u.chunk_id, u.migration_id, u.chunk_seq, u.rowid_start, u.rowid_end,
                   m.source_connection_id, m.target_connection_id,
                   m.source_schema, m.source_table,
                   m.target_schema, m.stage_table_name,
                   m.start_scn
            FROM   updated u
            JOIN   migrations m ON m.migration_id = u.migration_id
        """, (worker_id,))
        row = cur.fetchone()
        if not row:
            conn.commit()
            return None
        (chunk_id, migration_id, chunk_seq, rowid_start, rowid_end,
         src_conn_id, dst_conn_id,
         src_schema, src_table,
         tgt_schema, stage_table,
         start_scn) = row

    conn.commit()
    return {
        "chunk_id":             str(chunk_id),
        "migration_id":         str(migration_id),
        "chunk_seq":            chunk_seq,
        "rowid_start":          rowid_start,
        "rowid_end":            rowid_end,
        "source_connection_id": src_conn_id,
        "target_connection_id": dst_conn_id,
        "source_schema":        src_schema,
        "source_table":         src_table,
        "target_schema":        tgt_schema,
        "stage_table":          stage_table,
        "start_scn":            str(start_scn),
    }


def update_chunk_progress(conn, chunk_id: str, rows_loaded: int) -> None:
    """Update in-progress row count for a running chunk."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE migration_chunks
            SET    rows_loaded = %s,
                   status      = 'RUNNING'
            WHERE  chunk_id    = %s
        """, (rows_loaded, chunk_id))
    conn.commit()


def complete_chunk(conn, chunk_id: str, rows_loaded: int) -> None:
    """Mark a chunk DONE and increment migration.chunks_done."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE migration_chunks
            SET    status       = 'DONE',
                   rows_loaded  = %s,
                   completed_at = NOW()
            WHERE  chunk_id     = %s
            RETURNING migration_id
        """, (rows_loaded, chunk_id))
        row = cur.fetchone()
        if row:
            cur.execute("""
                UPDATE migrations
                SET    chunks_done = chunks_done + 1,
                       rows_loaded = rows_loaded + %s,
                       updated_at  = NOW()
                WHERE  migration_id = %s
            """, (rows_loaded, row[0],))
    conn.commit()


def fail_chunk(conn, chunk_id: str, error_text: str) -> None:
    """
    Mark chunk as failed.
    If retry_count < MAX_RETRIES → reset to PENDING (will be retried).
    Otherwise → permanently FAILED, increment migration.chunks_failed.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT retry_count, migration_id
            FROM   migration_chunks
            WHERE  chunk_id = %s
            FOR UPDATE
        """, (chunk_id,))
        row = cur.fetchone()
        if not row:
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
# Orchestrator helpers
# ---------------------------------------------------------------------------

def get_chunk_stats(conn, migration_id: str, chunk_type: str = "BULK") -> dict:
    """
    Return counts per status for the migration's chunks.
    {total, pending, claimed, running, done, failed}
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT status, COUNT(*)
            FROM   migration_chunks
            WHERE  migration_id = %s
              AND  COALESCE(chunk_type, 'BULK') = %s
            GROUP BY status
        """, (migration_id, chunk_type))
        counts = {row[0]: row[1] for row in cur.fetchall()}
    return {
        "total":   sum(counts.values()),
        "pending": counts.get("PENDING",  0),
        "claimed": counts.get("CLAIMED",  0),
        "running": counts.get("RUNNING",  0),
        "done":    counts.get("DONE",     0),
        "failed":  counts.get("FAILED",   0),
    }


def reset_stale_chunks(conn, stale_minutes: int = _STALE_AFTER_MINUTES) -> int:
    """
    Reset CLAIMED/RUNNING chunks whose claimed_at is older than *stale_minutes*
    back to PENDING.  Returns the number of chunks reset.
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE migration_chunks c
            SET    status     = 'PENDING',
                   worker_id  = NULL,
                   claimed_at = NULL,
                   started_at = NULL
            WHERE  c.status IN ('CLAIMED', 'RUNNING')
              AND  c.claimed_at < NOW() - INTERVAL '%s minutes'
              AND  EXISTS (
                  SELECT 1 FROM migrations m
                  WHERE  m.migration_id = c.migration_id
                    AND  m.phase = 'BULK_LOADING'
              )
        """, (stale_minutes,))
        count = cur.rowcount
    conn.commit()
    if count:
        print(f"[job_queue] reset {count} stale chunk(s) to PENDING")
    return count


def save_chunks(conn, migration_id: str, chunks: list) -> None:
    """
    Bulk-insert chunk ranges into migration_chunks.
    *chunks* is a list of ChunkRange (from oracle_chunker) or dicts with
    chunk_seq, rowid_start, rowid_end.
    """
    with conn.cursor() as cur:
        for ch in chunks:
            seq   = ch.chunk_seq   if hasattr(ch, "chunk_seq")   else ch["chunk_seq"]
            start = ch.rowid_start if hasattr(ch, "rowid_start") else ch["rowid_start"]
            end   = ch.rowid_end   if hasattr(ch, "rowid_end")   else ch["rowid_end"]
            cur.execute("""
                INSERT INTO migration_chunks
                    (migration_id, chunk_seq, rowid_start, rowid_end)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (migration_id, chunk_seq) DO NOTHING
            """, (migration_id, seq, start, end))
    conn.commit()


def list_chunks(conn, migration_id: str, chunk_type: str = "BULK") -> list[dict]:
    """Return chunks for a migration filtered by chunk_type, ordered by chunk_seq."""
    from db.state_db import row_to_dict
    with conn.cursor() as cur:
        cur.execute("""
            SELECT chunk_id, migration_id, chunk_seq,
                   rowid_start, rowid_end, status,
                   rows_loaded, worker_id,
                   claimed_at, started_at, completed_at,
                   error_text, retry_count, created_at,
                   COALESCE(chunk_type, 'BULK') AS chunk_type
            FROM   migration_chunks
            WHERE  migration_id = %s
              AND  COALESCE(chunk_type, 'BULK') = %s
            ORDER BY chunk_seq
        """, (migration_id, chunk_type))
        return [row_to_dict(cur, r) for r in cur.fetchall()]
