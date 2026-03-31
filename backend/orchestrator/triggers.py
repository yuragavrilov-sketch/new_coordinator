from db.state_db import row_to_dict
import services.oracle_scn as oracle_scn
import db.oracle_browser as oracle_browser

from orchestrator.helpers import get_conn, transition, oracle_cfg
from orchestrator.phases.cleanup import handle_indexes_enabling


def trigger_indexes_enabling(migration_id: str) -> None:
    """Called by the API endpoint when the user clicks 'Enable Indexes'.

    Accepts migrations in INDEXES_ENABLING phase.  Also accepts FAILED
    migrations whose error_code is INDEXES_ENABLE_ERROR so the user can
    recover a stuck migration without manual DB intervention — the migration
    is transitioned back to INDEXES_ENABLING before the handler runs.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM migrations WHERE migration_id = %s",
                (migration_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Migration {migration_id} not found")
            m = row_to_dict(cur, row)
    finally:
        conn.close()

    phase = m["phase"]
    if phase == "FAILED" and m.get("error_code") == "INDEXES_ENABLE_ERROR":
        # Recovery path: transition back to INDEXES_ENABLING, then re-fetch
        transition(migration_id, "INDEXES_ENABLING",
                   message="Повторный запуск пересчёта индексов")
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM migrations WHERE migration_id = %s",
                    (migration_id,),
                )
                m = row_to_dict(cur, cur.fetchone())
        finally:
            conn.close()
    elif phase != "INDEXES_ENABLING":
        raise ValueError(
            f"Migration is in phase {phase}, expected INDEXES_ENABLING"
        )
    handle_indexes_enabling(migration_id, m)


def trigger_enable_triggers(migration_id: str) -> None:
    """Called by the API endpoint when the user clicks 'Enable Triggers'.

    Accepts migrations in CDC_CATCHING_UP, CDC_CAUGHT_UP, or STEADY_STATE —
    i.e. only after CDC apply has started and indexes are rebuilt.
    """
    _ALLOWED = {"CDC_CATCHING_UP", "CDC_CAUGHT_UP", "STEADY_STATE"}
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM migrations WHERE migration_id = %s",
                (migration_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Migration {migration_id} not found")
            m = row_to_dict(cur, row)
    finally:
        conn.close()

    if m["phase"] not in _ALLOWED:
        raise ValueError(
            f"Migration is in phase {m['phase']}, "
            f"expected one of {', '.join(sorted(_ALLOWED))}"
        )

    dst_cfg = oracle_cfg(m["target_connection_id"])
    conn = oracle_scn.open_oracle_conn(dst_cfg)
    try:
        result = oracle_browser.enable_triggers(
            conn, m["target_schema"], m["target_table"],
        )
    finally:
        conn.close()

    if result["errors"]:
        names = [e["name"] for e in result["errors"]]
        raise RuntimeError(
            f"Не удалось включить триггеры: {', '.join(names)}. "
            + str(result["errors"])
        )

    n = len(result["enabled"])
    print(f"[orchestrator] {migration_id}: enabled {n} triggers")


def trigger_baseline_restart(migration_id: str) -> None:
    """Restart the baseline phase from scratch.

    Allowed from FAILED (baseline-related errors) or BASELINE_LOADING
    (e.g. when chunks are stuck or ORA-26026 occurred).

    1. Cancel PENDING chunks (prevent new claims).
    2. Transition to BASELINE_PUBLISHING (workers won't claim from this phase).
    3. _handle_baseline_publishing will wait for in-flight chunks to drain,
       delete old chunks, TRUNCATE, rebuild unique indexes, re-chunk, and load.
    """
    _ALLOWED_ERRORS = {"BASELINE_PUBLISH_ERROR", "BASELINE_LOAD_FAILED"}
    _ALLOWED_PHASES = {"BASELINE_LOADING"}

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM migrations WHERE migration_id = %s",
                (migration_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Migration {migration_id} not found")
            m = row_to_dict(cur, row)
    finally:
        conn.close()

    phase = m["phase"]
    ok = (
        phase in _ALLOWED_PHASES
        or (phase == "FAILED" and m.get("error_code") in _ALLOWED_ERRORS)
    )
    if not ok:
        raise ValueError(
            f"Нельзя перезапустить baseline из фазы {phase}"
            + (f" (error_code={m.get('error_code')})" if phase == "FAILED" else "")
        )

    # Cancel PENDING chunks immediately so workers don't pick them up
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE migration_chunks
                SET    status = 'CANCELLED'
                WHERE  migration_id = %s
                  AND  chunk_type = 'BASELINE'
                  AND  status = 'PENDING'
            """, (migration_id,))
        conn.commit()
    finally:
        conn.close()

    # Transition away from BASELINE_LOADING — workers won't claim new chunks.
    # _handle_baseline_publishing will drain in-flight chunks before TRUNCATE.
    transition(
        migration_id, "BASELINE_PUBLISHING",
        message="Перезапуск baseline: ожидание завершения активных воркеров…",
        extra_fields={"error_code": None, "error_text": None},
    )
    print(f"[orchestrator] {migration_id}: baseline restart triggered")
