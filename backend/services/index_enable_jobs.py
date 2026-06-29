"""Orchestrator-side helpers for index_enable_jobs (worker-claimed)."""


def ensure_pending_job(conn, migration_id: str) -> None:
    """Create a PENDING job unless an active one already exists.

    The partial unique index idx_iej_active makes the insert a no-op when a
    PENDING/CLAIMED/RUNNING job is already present.
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO index_enable_jobs (migration_id, state)
            SELECT %s, 'PENDING'
            WHERE NOT EXISTS (
                SELECT 1 FROM index_enable_jobs
                WHERE  migration_id = %s
                  AND  state IN ('PENDING', 'CLAIMED', 'RUNNING')
            )
        """, (migration_id, migration_id))
    conn.commit()


def latest_job_state(conn, migration_id: str):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT state, result_json, error_text
            FROM   index_enable_jobs
            WHERE  migration_id = %s
            ORDER BY created_at DESC
            LIMIT 1
        """, (migration_id,))
        row = cur.fetchone()
    return (row[0], row[1], row[2]) if row else (None, None, None)


def reset_stale_jobs(conn, stale_minutes: int = 15) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE index_enable_jobs
            SET    state = 'PENDING', worker_id = NULL, claimed_at = NULL, started_at = NULL
            WHERE  state IN ('CLAIMED', 'RUNNING')
              AND  claimed_at < NOW() - make_interval(mins => %s)
        """, (stale_minutes,))
        n = cur.rowcount
    conn.commit()
    return n
