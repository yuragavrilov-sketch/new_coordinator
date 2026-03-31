"""Queue gating — controls concurrency for migrations in heavy phases."""

import os

from orchestrator.helpers import get_conn, update

# Max concurrent BULK_ONLY migrations in heavy phases.
# CDC is still limited to 1 (to control Kafka backlog).
MAX_BULK_CONCURRENT = int(os.environ.get("MAX_BULK_CONCURRENT", "5"))

_HEAVY_PHASES = frozenset({
    "PREPARING", "SCN_FIXED",
    "CONNECTOR_STARTING", "CDC_BUFFERING",
    "TOPIC_CREATING",
    "TARGET_CLEARING",
    "CHUNKING",
    "BULK_LOADING", "BULK_LOADED",
    "STAGE_VALIDATING", "STAGE_VALIDATED",
    "BASELINE_PUBLISHING", "BASELINE_LOADING", "BASELINE_PUBLISHED",
    "STAGE_DROPPING",
})


def update_queue_positions() -> None:
    """Recalculate queue_position for all migrations waiting in NEW."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE migrations m
                SET    queue_position = sub.pos
                FROM (
                    SELECT migration_id,
                           ROW_NUMBER() OVER (ORDER BY state_changed_at ASC) AS pos
                    FROM   migrations
                    WHERE  phase = 'NEW'
                ) sub
                WHERE m.migration_id = sub.migration_id
                  AND  m.phase = 'NEW'
            """)
            cur.execute("""
                UPDATE migrations
                SET    queue_position = NULL
                WHERE  phase != 'NEW'
                  AND  queue_position IS NOT NULL
            """)
        conn.commit()
    finally:
        conn.close()


def check_loading_slot(mid: str) -> bool:
    """Check if the loading slot is free and it's this migration's turn.

    Returns True if the migration may proceed, False if it should wait.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(_HEAVY_PHASES))
            cur.execute(
                f"""SELECT 1 FROM migrations
                    WHERE  phase IN ({placeholders})
                      AND  migration_id != %s
                    LIMIT 1""",
                (*_HEAVY_PHASES, mid),
            )
            slot_busy = cur.fetchone() is not None
    finally:
        conn.close()

    if slot_busy:
        update_queue_positions()
        return False

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT migration_id FROM migrations
                WHERE  phase = 'NEW'
                ORDER BY state_changed_at ASC
                LIMIT 1
            """)
            first = cur.fetchone()
    finally:
        conn.close()

    if first and first[0] != mid:
        update_queue_positions()
        return False

    return True


def check_bulk_slot(mid: str) -> bool:
    """Check if there's room for another BULK_ONLY migration.

    Unlike CDC (1 at a time), BULK_ONLY allows up to MAX_BULK_CONCURRENT
    migrations in heavy phases simultaneously.
    Returns True if the migration may proceed.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(_HEAVY_PHASES))
            cur.execute(
                f"""SELECT COUNT(*) FROM migrations
                    WHERE  phase IN ({placeholders})
                      AND  migration_id != %s
                      AND  migration_mode = 'BULK_ONLY'""",
                (*_HEAVY_PHASES, mid),
            )
            active_bulk = cur.fetchone()[0]
    finally:
        conn.close()

    if active_bulk >= MAX_BULK_CONCURRENT:
        update_queue_positions()
        return False

    return True
