from datetime import datetime

from orchestrator.helpers import get_conn, transition, update, broadcast


def handle_cdc_apply_starting(mid: str, m: dict) -> None:
    """Wait for heartbeat from cdc_apply_worker (written via /api/worker/cdc/checkin)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT worker_heartbeat
                FROM   migration_cdc_state
                WHERE  migration_id = %s
            """, (m["migration_id"],))
            row = cur.fetchone()
    finally:
        conn.close()

    if row and row[0]:
        transition(mid, "CDC_CATCHING_UP",
                   message="CDC apply-worker подключился")


def handle_cdc_applying(mid: str, m: dict) -> None:
    """Wait for CDC worker heartbeat (group-based variant of CDC_APPLY_STARTING)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT worker_heartbeat
                FROM   migration_cdc_state
                WHERE  migration_id = %s
            """, (m["migration_id"],))
            row = cur.fetchone()
    finally:
        conn.close()

    if row and row[0]:
        transition(mid, "CDC_CATCHING_UP",
                   message="CDC apply-worker подключился")


def handle_cdc_catching_up(mid: str, m: dict) -> None:
    """Sync lag from migration_cdc_state; lag=0 → CDC_CAUGHT_UP."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT total_lag, updated_at
                FROM   migration_cdc_state
                WHERE  migration_id = %s
            """, (m["migration_id"],))
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return

    lag, updated_at = int(row[0] or 0), row[1]
    update(mid, {"kafka_lag": lag, "kafka_lag_checked_at": updated_at})
    broadcast({
        "type":         "kafka_lag",
        "migration_id": mid,
        "total_lag":    lag,
        "updated_at":   updated_at.isoformat() + "Z" if updated_at else None,
        "ts":           datetime.utcnow().isoformat() + "Z",
    })

    if lag == 0:
        transition(mid, "CDC_CAUGHT_UP",
                   message="Kafka consumer group lag = 0")


def handle_cdc_caught_up(mid: str, m: dict) -> None:
    transition(mid, "STEADY_STATE",
               message="Миграция догнала источник")


def handle_steady_state(mid: str, m: dict) -> None:
    """Propagate lag updates to the UI while in steady state."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT total_lag, worker_heartbeat, updated_at
                FROM   migration_cdc_state
                WHERE  migration_id = %s
            """, (m["migration_id"],))
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return

    lag, heartbeat, updated_at = row
    lag = int(lag or 0)
    update(mid, {"kafka_lag": lag, "kafka_lag_checked_at": updated_at})
    broadcast({
        "type":         "kafka_lag",
        "migration_id": mid,
        "total_lag":    lag,
        "updated_at":   updated_at.isoformat() + "Z" if updated_at else None,
        "ts":           datetime.utcnow().isoformat() + "Z",
    })
