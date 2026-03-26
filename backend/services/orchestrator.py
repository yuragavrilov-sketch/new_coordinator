"""
Migration phase orchestrator — background thread that drives state transitions.

Each tick (every TICK_INTERVAL seconds) the orchestrator:
  1. Resets stale claimed/running chunks back to PENDING.
  2. Fetches all migrations in active phases.
  3. For each migration calls the appropriate phase handler.

Long-running operations (STAGE_VALIDATING, BASELINE_PUBLISHING) are executed
in per-migration daemon threads to avoid blocking the orchestrator loop.
"""

import json
import threading
import time
from datetime import datetime

# DB helpers
from db.state_db import (
    get_active_migrations,
    row_to_dict,
    transition_phase,
    update_migration_fields,
)

# Services
import services.debezium        as debezium
import services.oracle_scn      as oracle_scn
import services.oracle_stage    as oracle_stage
import services.oracle_chunker  as oracle_chunker
import services.oracle_baseline as oracle_baseline
import services.kafka_lag       as kafka_lag_svc
import services.validator       as validator
import services.job_queue       as job_queue
import services.kafka_topics    as kafka_topics
import services.connector_groups as connector_groups_svc
import db.oracle_browser        as oracle_browser

TICK_INTERVAL = 5  # seconds

# Phases that occupy the "loading slot".  Only ONE migration at a time is
# allowed in these phases; the rest wait in NEW (before SCN fixation so
# Kafka doesn't accumulate a growing CDC backlog while waiting).
_HEAVY_PHASES = frozenset({
    "PREPARING", "SCN_FIXED",
    "CONNECTOR_STARTING", "CDC_BUFFERING",
    "TOPIC_CREATING",
    "CHUNKING",
    "BULK_LOADING", "BULK_LOADED",
    "STAGE_VALIDATING", "STAGE_VALIDATED",
    "BASELINE_PUBLISHING", "BASELINE_LOADING", "BASELINE_PUBLISHED",
    "STAGE_DROPPING",
})

# Track migrations running in a dedicated thread (long-running phases)
_in_progress: set[str] = set()
_in_progress_lock = threading.Lock()

_state: dict = {}


# ---------------------------------------------------------------------------
# Init + start
# ---------------------------------------------------------------------------

def init(get_conn_fn, load_configs_fn, broadcast_fn) -> None:
    _state["get_conn"]      = get_conn_fn
    _state["load_configs"]  = load_configs_fn
    _state["broadcast"]     = broadcast_fn


_orchestrator_started = False


def is_running() -> bool:
    return _orchestrator_started


def start_orchestrator() -> None:
    global _orchestrator_started
    if _orchestrator_started:
        return
    _orchestrator_started = True

    def _run():
        time.sleep(3)
        while True:
            try:
                _tick()
            except Exception as exc:
                print(f"[orchestrator] tick error: {exc}")
            time.sleep(TICK_INTERVAL)

    threading.Thread(target=_run, daemon=True, name="orchestrator").start()
    print("[orchestrator] started")


# ---------------------------------------------------------------------------
# Main tick
# ---------------------------------------------------------------------------

def _tick() -> None:
    get_conn = _state["get_conn"]
    conn = get_conn()
    try:
        # Reset stale chunks first
        job_queue.reset_stale_chunks(conn)

        migrations = get_active_migrations(conn)
    finally:
        conn.close()

    for m in migrations:
        mid   = m["migration_id"]
        phase = m["phase"]
        try:
            _dispatch(mid, phase, m)
        except Exception as exc:
            print(f"[orchestrator] migration {mid} phase {phase} error: {exc}")
            _fail(mid, str(exc))

    # Drive connector group lifecycle
    _tick_groups()

    # Check connector group health
    _check_group_connectors()


def _dispatch(migration_id: str, phase: str, m: dict) -> None:
    # Group-based migrations use a simplified phase machine
    if m.get("group_id"):
        handler = _GROUP_HANDLERS.get(phase)
    else:
        handler = _LEGACY_HANDLERS.get(phase)
    if handler:
        handler(migration_id, m)


# Legacy handlers (per-migration connector, AS OF SCN)
_LEGACY_HANDLERS = {
    "NEW":                  lambda mid, m: _handle_new(mid, m),
    "PREPARING":            lambda mid, m: _handle_preparing(mid, m),
    "SCN_FIXED":            lambda mid, m: _handle_scn_fixed(mid, m),
    "CONNECTOR_STARTING":   lambda mid, m: _handle_connector_starting(mid, m),
    "CDC_BUFFERING":        lambda mid, m: _handle_cdc_buffering(mid, m),
    "CHUNKING":             lambda mid, m: _handle_chunking(mid, m),
    "BULK_LOADING":         lambda mid, m: _handle_bulk_loading(mid, m),
    "BULK_LOADED":          lambda mid, m: _handle_bulk_loaded(mid, m),
    "STAGE_VALIDATING":     lambda mid, m: _handle_stage_validating(mid, m),
    "STAGE_VALIDATED":      lambda mid, m: _handle_stage_validated(mid, m),
    "BASELINE_PUBLISHING":  lambda mid, m: _handle_baseline_publishing(mid, m),
    "BASELINE_LOADING":     lambda mid, m: _handle_baseline_loading(mid, m),
    "BASELINE_PUBLISHED":   lambda mid, m: _handle_baseline_published(mid, m),
    "STAGE_DROPPING":       lambda mid, m: _handle_stage_dropping(mid, m),
    "INDEXES_ENABLING":     lambda mid, m: _handle_indexes_enabling(mid, m),
    "CDC_APPLY_STARTING":   lambda mid, m: _handle_cdc_apply_starting(mid, m),
    "CDC_CATCHING_UP":      lambda mid, m: _handle_cdc_catching_up(mid, m),
    "CDC_CAUGHT_UP":        lambda mid, m: _handle_cdc_caught_up(mid, m),
    "STEADY_STATE":         lambda mid, m: _handle_steady_state(mid, m),
    "CANCELLING":           lambda mid, m: _handle_cancelling(mid, m),
}

# Group handlers (shared connector, no SCN, pre-created topics)
_GROUP_HANDLERS = {
    "NEW":                  lambda mid, m: _handle_new_group(mid, m),
    "TOPIC_CREATING":       lambda mid, m: _handle_topic_creating(mid, m),
    "CHUNKING":             lambda mid, m: _handle_chunking(mid, m),
    "BULK_LOADING":         lambda mid, m: _handle_bulk_loading(mid, m),
    "BULK_LOADED":          lambda mid, m: _handle_bulk_loaded(mid, m),
    "STAGE_VALIDATING":     lambda mid, m: _handle_stage_validating(mid, m),
    "STAGE_VALIDATED":      lambda mid, m: _handle_stage_validated(mid, m),
    "BASELINE_PUBLISHING":  lambda mid, m: _handle_baseline_publishing(mid, m),
    "BASELINE_LOADING":     lambda mid, m: _handle_baseline_loading(mid, m),
    "BASELINE_PUBLISHED":   lambda mid, m: _handle_baseline_published(mid, m),
    "STAGE_DROPPING":       lambda mid, m: _handle_stage_dropping(mid, m),
    "INDEXES_ENABLING":     lambda mid, m: _handle_indexes_enabling_group(mid, m),
    "CDC_APPLYING":         lambda mid, m: _handle_cdc_applying(mid, m),
    "CDC_CATCHING_UP":      lambda mid, m: _handle_cdc_catching_up(mid, m),
    "CDC_CAUGHT_UP":        lambda mid, m: _handle_cdc_caught_up(mid, m),
    "STEADY_STATE":         lambda mid, m: _handle_steady_state(mid, m),
    "CANCELLING":           lambda mid, m: _handle_cancelling(mid, m),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _transition(migration_id: str, to_phase: str,
                message: str | None = None,
                error_code: str | None = None,
                error_text: str | None = None,
                extra_fields: dict | None = None) -> None:
    conn = _state["get_conn"]()
    try:
        from_phase = transition_phase(
            conn, migration_id, to_phase,
            message=message,
            error_code=error_code,
            error_text=error_text,
            extra_fields=extra_fields,
        )
        conn.commit()
    finally:
        conn.close()

    _state["broadcast"]({
        "type":         "migration_phase",
        "migration_id": migration_id,
        "from_phase":   from_phase,
        "phase":        to_phase,
        "ts":           datetime.utcnow().isoformat() + "Z",
    })
    print(f"[orchestrator] {migration_id}: {from_phase} → {to_phase}"
          + (f" ({message})" if message else ""))


def _fail(migration_id: str, error_text: str, error_code: str = "ORCHESTRATOR_ERROR") -> None:
    try:
        _transition(
            migration_id, "FAILED",
            message=error_text[:500],
            error_code=error_code,
            error_text=error_text[:2000],
        )
    except Exception as exc:
        print(f"[orchestrator] could not set FAILED for {migration_id}: {exc}")


def _update(migration_id: str, fields: dict) -> None:
    conn = _state["get_conn"]()
    try:
        update_migration_fields(conn, migration_id, fields)
        conn.commit()
    finally:
        conn.close()


def _configs() -> dict:
    return _state["load_configs"]()


def _oracle_cfg(connection_id: str) -> dict:
    return _configs().get(connection_id, {})


def _current_phase(migration_id: str) -> str | None:
    """Read the current phase from the DB.  Used by threaded handlers to
    detect if the user cancelled while they were running."""
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT phase FROM migrations WHERE migration_id = %s",
                (migration_id,),
            )
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def _safe_transition(migration_id: str, expected_phase: str, to_phase: str,
                     **kwargs) -> bool:
    """Transition only if the migration is still in *expected_phase*.
    Returns True if transitioned, False if phase changed (e.g. cancelled)."""
    cur = _current_phase(migration_id)
    if cur != expected_phase:
        print(f"[orchestrator] {migration_id}: skip transition {expected_phase}→{to_phase}, "
              f"current phase is {cur} (cancelled?)")
        return False
    _transition(migration_id, to_phase, **kwargs)
    return True


def _in_prog(migration_id: str) -> bool:
    with _in_progress_lock:
        return migration_id in _in_progress


def _mark_in_prog(migration_id: str) -> None:
    with _in_progress_lock:
        _in_progress.add(migration_id)


def _unmark_in_prog(migration_id: str) -> None:
    with _in_progress_lock:
        _in_progress.discard(migration_id)


def _update_queue_positions() -> None:
    """Recalculate queue_position for all migrations waiting in NEW."""
    conn = _state["get_conn"]()
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
            # Clear stale queue_position on non-NEW migrations
            cur.execute("""
                UPDATE migrations
                SET    queue_position = NULL
                WHERE  phase != 'NEW'
                  AND  queue_position IS NOT NULL
            """)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Phase handlers
# ---------------------------------------------------------------------------

def _handle_new(mid: str, m: dict) -> None:
    """
    Validate key columns, then transition to PREPARING — but only if the
    loading slot is free.  The gate is here (before SCN fixation) so that
    queued migrations don't accumulate a growing Kafka CDC backlog.
    """
    pk = m.get("source_pk_exists", False)
    uk = m.get("source_uk_exists", False)
    key_cols = json.loads(m.get("effective_key_columns_json") or "[]")

    if not pk and not uk and not key_cols:
        _fail(mid,
              "Таблица не имеет PK/UK и ключевые колонки не заданы. "
              "Укажите ключевые колонки при создании миграции.",
              "NO_KEY_COLUMNS")
        return

    # ── Queue gate ────────────────────────────────────────────────────
    conn = _state["get_conn"]()
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
        _update_queue_positions()
        return  # wait — another migration is loading

    # FIFO: among multiple NEW migrations, let the oldest proceed first
    conn = _state["get_conn"]()
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
        _update_queue_positions()
        return  # not our turn

    _update(mid, {"queue_position": None})
    _transition(mid, "PREPARING", message="Ключевые колонки проверены")


def _handle_preparing(mid: str, m: dict) -> None:
    """Create stage table, fix SCN → SCN_FIXED."""
    if _in_prog(mid):
        return
    _mark_in_prog(mid)

    def _run():
        try:
            src_cfg  = _oracle_cfg(m["source_connection_id"])
            dst_cfg  = _oracle_cfg(m["target_connection_id"])
            strategy = (m.get("migration_strategy") or "STAGE").upper()

            # Check supplemental logging (warn, don't block) — CDC only
            mode = (m.get("migration_mode") or "CDC").upper()
            if mode != "BULK_ONLY":
                try:
                    has_supp = oracle_scn.check_supplemental_logging(
                        src_cfg, m["source_schema"], m["source_table"]
                    )
                    if not has_supp:
                        print(
                            f"[orchestrator] WARNING: {m['source_schema']}.{m['source_table']} "
                            "does not have ALL COLUMNS supplemental logging. "
                            "Debezium may not capture full row images."
                        )
                except Exception as exc:
                    print(f"[orchestrator] supplemental logging check failed: {exc}")

            if strategy == "STAGE":
                ts = m.get("stage_tablespace") or ""
                print(f"[orchestrator] stage_tablespace from DB = {ts!r}")
                oracle_stage.create_stage_table(
                    src_cfg, dst_cfg,
                    m["source_schema"], m["source_table"],
                    m["target_schema"], m["stage_table_name"],
                    tablespace=ts,
                )
                stage_msg = "Stage table создана, "
            else:
                # DIRECT strategy — no stage table
                stage_msg = "Прямая загрузка (без stage), "

            # Fix SCN
            scn = oracle_scn.get_current_scn(src_cfg)

            _update(mid, {
                "start_scn":   scn,
                "scn_fixed_at": datetime.utcnow(),
            })
            _safe_transition(mid, "PREPARING", "SCN_FIXED",
                            message=f"{stage_msg}start_scn={scn}")
        except Exception as exc:
            if _current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                _fail(mid, str(exc), "PREPARING_ERROR")
        finally:
            _unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def _handle_scn_fixed(mid: str, m: dict) -> None:
    """CDC: create Debezium connector → CONNECTOR_STARTING.
    BULK_ONLY: skip connector, create chunks directly → CHUNKING."""
    mode = (m.get("migration_mode") or "CDC").upper()

    if mode == "BULK_ONLY":
        # Skip Debezium entirely — go straight to chunk creation
        _create_chunks_and_transition(mid, m)
        return

    # CDC mode — create Debezium connector
    src_cfg = _oracle_cfg(m["source_connection_id"])
    try:
        debezium.create_connector(m, src_cfg)
    except Exception as exc:
        _fail(mid, str(exc), "CONNECTOR_CREATE_ERROR")
        return
    _update(mid, {"connector_status": "CREATING"})
    _transition(mid, "CONNECTOR_STARTING", message="Debezium connector создан")


def _handle_connector_starting(mid: str, m: dict) -> None:
    """Poll connector status; RUNNING → CDC_BUFFERING."""
    try:
        status = debezium.get_connector_status(m["connector_name"])
    except Exception as exc:
        print(f"[orchestrator] connector status error for {mid}: {exc}")
        return

    _update(mid, {"connector_status": status})
    _state["broadcast"]({
        "type":           "connector_status",
        "migration_id":   mid,
        "status":         status,
        "connector_name": m["connector_name"],
        "ts":             datetime.utcnow().isoformat() + "Z",
    })

    if status == "RUNNING":
        _transition(mid, "CDC_BUFFERING",
                    message="Debezium connector RUNNING")
    elif status == "FAILED":
        _fail(mid, "Debezium connector перешёл в статус FAILED",
              "CONNECTOR_FAILED")


def _create_chunks_and_transition(mid: str, m: dict) -> None:
    """Create ROWID chunks and transition to CHUNKING.
    Shared by CDC (via _handle_cdc_buffering) and BULK_ONLY (via _handle_scn_fixed)."""
    if _in_prog(mid):
        return

    # Check if chunks already created
    conn = _state["get_conn"]()
    try:
        stats = job_queue.get_chunk_stats(conn, mid)
    finally:
        conn.close()

    if stats["total"] > 0:
        _update(mid, {"total_chunks": stats["total"]})
        _transition(mid, "CHUNKING",
                    message=f"Чанки уже созданы ({stats['total']}), переход к нарезке")
        return

    _mark_in_prog(mid)

    def _run():
        try:
            src_cfg = _oracle_cfg(m["source_connection_id"])
            chunks = oracle_chunker.create_chunks(
                src_cfg,
                m["source_schema"],
                m["source_table"],
                int(m.get("chunk_size") or 100_000),
                mid,
            )
            if not chunks:
                # Table might genuinely be empty — check before failing
                src_conn = oracle_scn.open_oracle_conn(src_cfg)
                try:
                    with src_conn.cursor() as cur:
                        cur.execute(
                            f'SELECT 1 FROM "{m["source_schema"].upper()}"'
                            f'."{m["source_table"].upper()}" WHERE ROWNUM = 1'
                        )
                        has_rows = cur.fetchone() is not None
                finally:
                    src_conn.close()

                if has_rows:
                    _fail(mid,
                          "DBMS_PARALLEL_EXECUTE вернул 0 чанков, но таблица не пуста — "
                          "проверьте привилегии EXECUTE ON DBMS_PARALLEL_EXECUTE",
                          "NO_CHUNKS")
                    return

                # Source table is genuinely empty — skip bulk loading,
                # go straight to enabling indexes and then CDC listener
                _update(mid, {"total_chunks": 0})
                _transition(mid, "INDEXES_ENABLING",
                            message="Таблица-источник пуста (0 строк), "
                                    "пропуск bulk-загрузки — включение индексов и запуск CDC")
                return

            pg_conn = _state["get_conn"]()
            try:
                job_queue.save_chunks(pg_conn, mid, chunks)
            finally:
                pg_conn.close()

            _update(mid, {"total_chunks": len(chunks)})
            _transition(mid, "CHUNKING",
                        message=f"Создано {len(chunks)} чанков")
        except Exception as exc:
            _fail(mid, str(exc), "CHUNKING_ERROR")
        finally:
            _unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def _handle_cdc_buffering(mid: str, m: dict) -> None:
    """
    Create ROWID chunks via DBMS_PARALLEL_EXECUTE and store in migration_chunks.
    Idempotent: if chunks already exist, skip.
    """
    _create_chunks_and_transition(mid, m)


def _handle_chunking(mid: str, m: dict) -> None:
    """Chunks are written — transition to BULK_LOADING."""
    _transition(mid, "BULK_LOADING",
                message="Чанки записаны, запуск bulk-загрузки")


def _handle_bulk_loading(mid: str, m: dict) -> None:
    """Monitor chunk completion."""
    conn = _state["get_conn"]()
    try:
        stats = job_queue.get_chunk_stats(conn, mid)
    finally:
        conn.close()

    total   = stats["total"]
    done    = stats["done"]
    failed  = stats["failed"]
    active  = stats["claimed"] + stats["running"] + stats["pending"]

    _state["broadcast"]({
        "type":         "chunk_progress",
        "migration_id": mid,
        "chunks_done":  done,
        "total_chunks": total,
        "ts":           datetime.utcnow().isoformat() + "Z",
    })

    if total == 0:
        return  # Chunks not yet written

    if done == total:
        _transition(mid, "BULK_LOADED",
                    message=f"Все {total} чанков загружены")
        return

    if failed > 0 and active == 0 and (done + failed) == total:
        _fail(mid,
              f"Bulk load завершился с ошибками: {failed} чанков не удалось загрузить",
              "BULK_LOAD_FAILED")


def _handle_bulk_loaded(mid: str, m: dict) -> None:
    strategy = (m.get("migration_strategy") or "STAGE").upper()
    if strategy == "DIRECT":
        # Skip stage validate / publish / drop — data is already in the target table
        _transition(mid, "INDEXES_ENABLING",
                    message="DIRECT стратегия: данные загружены напрямую, включение индексов")
    else:
        _transition(mid, "STAGE_VALIDATING")


def _handle_stage_validating(mid: str, m: dict) -> None:
    """Run validation in a separate thread."""
    if _in_prog(mid):
        return
    _mark_in_prog(mid)

    def _run():
        try:
            src_cfg = _oracle_cfg(m["source_connection_id"])
            dst_cfg = _oracle_cfg(m["target_connection_id"])
            result  = validator.validate_stage(m, src_cfg, dst_cfg)
            _update(mid, {"validation_result": json.dumps(result.to_dict())})

            if result.ok:
                _safe_transition(mid, "STAGE_VALIDATING", "STAGE_VALIDATED",
                                 message=result.message)
            else:
                if _current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                    _fail(mid, result.message, "VALIDATION_FAILED")
        except Exception as exc:
            if _current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                _fail(mid, str(exc), "VALIDATION_ERROR")
        finally:
            _unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def _handle_stage_validated(mid: str, m: dict) -> None:
    _transition(mid, "BASELINE_PUBLISHING")


def _handle_baseline_publishing(mid: str, m: dict) -> None:
    """
    TRUNCATE target, chunk the stage table on the target Oracle, store
    chunks (chunk_type='BASELINE') in migration_chunks, then move to
    BASELINE_LOADING so workers pick them up.
    """
    if _in_prog(mid):
        return
    _mark_in_prog(mid)

    def _run():
        try:
            dst_cfg    = _oracle_cfg(m["target_connection_id"])
            tgt_schema = m["target_schema"]
            tgt_table  = m["target_table"]
            stg_table  = m["stage_table_name"]
            chunk_size = int(m.get("baseline_batch_size") or 500_000)

            # Wait for in-flight BASELINE chunks to drain (restart scenario).
            # Phase is already BASELINE_PUBLISHING so workers won't claim new
            # chunks; we just need active ones to finish before TRUNCATE.
            pg_conn = _state["get_conn"]()
            try:
                while True:
                    with pg_conn.cursor() as cur:
                        cur.execute("""
                            SELECT COUNT(*) FROM migration_chunks
                            WHERE  migration_id = %s
                              AND  chunk_type = 'BASELINE'
                              AND  status IN ('CLAIMED', 'RUNNING')
                        """, (mid,))
                        active = cur.fetchone()[0]
                    if active == 0:
                        break
                    print(f"[baseline_publishing] waiting for {active} in-flight chunks to drain…")
                    time.sleep(3)
            finally:
                pg_conn.close()

            # Delete old BASELINE chunks (DONE/FAILED/CANCELLED from previous run)
            pg_conn = _state["get_conn"]()
            try:
                with pg_conn.cursor() as cur:
                    cur.execute("""
                        DELETE FROM migration_chunks
                        WHERE  migration_id = %s AND chunk_type = 'BASELINE'
                    """, (mid,))
                pg_conn.commit()
            finally:
                pg_conn.close()

            # TRUNCATE target table before loading so retries start from a clean slate
            tgt_quoted = f'"{tgt_schema.upper()}"."{tgt_table.upper()}"'
            conn = oracle_scn.open_oracle_conn(dst_cfg)
            try:
                with conn.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE {tgt_quoted}")
                conn.commit()
                print(f"[baseline_publishing] truncated {tgt_quoted}")

                # Recover PK/UK-backing indexes left UNUSABLE by a previous failed
                # attempt.  ORA-26026 is raised on INSERT if such an index is UNUSABLE.
                rebuilt = oracle_browser.rebuild_unusable_constraint_indexes(
                    conn, tgt_schema, tgt_table,
                )
                if rebuilt:
                    print(f"[baseline_publishing] rebuilt UNUSABLE constraint indexes: {rebuilt}")

                # Mark secondary (non-unique) indexes UNUSABLE so Oracle skips
                # index maintenance during INSERT — rebuilt by INDEXES_ENABLING.
                marked = oracle_browser.mark_indexes_unusable(
                    conn, tgt_schema, tgt_table, skip_pk=True,
                )
                if marked:
                    print(f"[baseline_publishing] marked UNUSABLE: {marked}")

                # Disable triggers so they don't fire on every INSERT row during
                # baseline load.  Re-enabled manually via "Включить триггеры"
                # button (enable_triggers API) after CDC apply catches up.
                disabled_trg = oracle_browser.disable_triggers(
                    conn, tgt_schema, tgt_table,
                )
                if disabled_trg:
                    print(f"[baseline_publishing] disabled triggers: {disabled_trg}")

                # Switch to NOLOGGING so direct-path INSERTs (APPEND hint) skip
                # redo generation.  Restored to LOGGING in INDEXES_ENABLING.
                oracle_browser.set_table_logging(conn, tgt_schema, tgt_table, nologging=True)
                print(f"[baseline_publishing] set NOLOGGING on {tgt_quoted}")
            finally:
                conn.close()

            # Create BASELINE chunks from stage table on target
            task_id = f"BAS_{m['migration_id']}"
            chunks = oracle_chunker.create_chunks(
                dst_cfg, tgt_schema, stg_table, chunk_size, task_id
            )

            if not chunks:
                # Stage is empty — skip straight to BASELINE_PUBLISHED
                _update(mid, {"baseline_chunks_total": 0, "baseline_chunks_done": 0})
                _transition(mid, "BASELINE_PUBLISHED",
                            message="Stage таблица пуста — целевая таблица обнулена")
                return

            # 3. Store as BASELINE chunks in migration_chunks
            pg_conn = _state["get_conn"]()
            try:
                with pg_conn.cursor() as cur:
                    for ch in chunks:
                        cur.execute("""
                            INSERT INTO migration_chunks
                                (migration_id, chunk_seq, rowid_start, rowid_end, chunk_type)
                            VALUES (%s, %s, %s, %s, 'BASELINE')
                            ON CONFLICT (migration_id, chunk_type, chunk_seq) DO NOTHING
                        """, (mid, ch.chunk_seq, ch.rowid_start, ch.rowid_end))
                pg_conn.commit()
            finally:
                pg_conn.close()

            _update(mid, {
                "baseline_chunks_total": len(chunks),
                "baseline_chunks_done":  0,
            })
            _safe_transition(mid, "BASELINE_PUBLISHING", "BASELINE_LOADING",
                            message=f"Создано {len(chunks)} baseline-чанков, запуск воркеров")
        except Exception as exc:
            if _current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                _fail(mid, str(exc), "BASELINE_PUBLISH_ERROR")
        finally:
            _unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def _handle_baseline_loading(mid: str, m: dict) -> None:
    """Monitor BASELINE chunk completion — analogous to _handle_bulk_loading."""
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT status, COUNT(*)
                FROM   migration_chunks
                WHERE  migration_id = %s AND chunk_type = 'BASELINE'
                GROUP BY status
            """, (mid,))
            counts = {row[0]: row[1] for row in cur.fetchall()}
    finally:
        conn.close()

    total   = sum(counts.values())
    done    = counts.get("DONE",    0)
    failed  = counts.get("FAILED",  0)
    active  = counts.get("CLAIMED", 0) + counts.get("RUNNING", 0) + counts.get("PENDING", 0)

    _state["broadcast"]({
        "type":                  "baseline_progress",
        "migration_id":          mid,
        "baseline_chunks_done":  done,
        "baseline_chunks_total": total,
        "ts":                    datetime.utcnow().isoformat() + "Z",
    })

    if total == 0:
        return

    _update(mid, {"baseline_chunks_done": done})

    if done == total:
        _transition(mid, "BASELINE_PUBLISHED",
                    message=f"Все {total} baseline-чанков загружены")
        return

    if failed > 0 and active == 0 and (done + failed) == total:
        _fail(mid,
              f"Baseline loading завершился с ошибками: {failed}/{total} чанков не удалось",
              "BASELINE_LOAD_FAILED")


def _handle_baseline_published(mid: str, m: dict) -> None:
    _transition(mid, "STAGE_DROPPING",
                message="Удаление stage-таблицы")


def _handle_stage_dropping(mid: str, m: dict) -> None:
    """Drop the stage table — runs in a thread."""
    if _in_prog(mid):
        return
    _mark_in_prog(mid)

    def _run():
        try:
            dst_cfg = _oracle_cfg(m["target_connection_id"])
            oracle_stage.drop_stage_table(
                dst_cfg, m["target_schema"], m["stage_table_name"],
            )
            _safe_transition(mid, "STAGE_DROPPING", "INDEXES_ENABLING",
                            message="Stage-таблица удалена, включение индексов и прочих объектов")
        except Exception as exc:
            if _current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                _fail(mid, str(exc), "STAGE_DROP_ERROR")
        finally:
            _unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def _handle_indexes_enabling(mid: str, m: dict) -> None:
    """Enable all UNUSABLE indexes, DISABLED constraints and triggers — runs in a thread."""
    if _in_prog(mid):
        return
    _mark_in_prog(mid)

    def _run():
        try:
            dst_cfg = _oracle_cfg(m["target_connection_id"])
            conn = oracle_scn.open_oracle_conn(dst_cfg)
            try:
                # Restore LOGGING before rebuilding indexes — indexes themselves
                # are rebuilt NOLOGGING (inside enable_all_disabled_objects), but
                # the table should return to protected mode for ongoing CDC DML.
                oracle_browser.set_table_logging(
                    conn, m["target_schema"], m["target_table"], nologging=False,
                )
                result = oracle_browser.enable_all_disabled_objects(
                    conn, m["target_schema"], m["target_table"],
                )
            finally:
                conn.close()

            err_count = (
                len(result["errors"]["indexes"])
                + len(result["errors"]["constraints"])
            )
            if err_count:
                names = (
                    [e["name"] for e in result["errors"]["indexes"]]
                    + [e["name"] for e in result["errors"]["constraints"]]
                )
                err_detail = str(result["errors"])
                # Stay in INDEXES_ENABLING so the user can retry via the UI button.
                # Transitioning to FAILED would make recovery impossible without
                # manual DB intervention.
                _transition(
                    mid, "INDEXES_ENABLING",
                    message=(
                        f"Ошибка пересчёта: {', '.join(names)}. "
                        "Нажмите «Включить индексы» ещё раз для повторной попытки."
                    ),
                    extra_fields={
                        "error_code": "INDEXES_ENABLE_ERROR",
                        "error_text": err_detail[:2000],
                    },
                )
                return

            n_idx = len(result["enabled"]["indexes"])
            n_con = len(result["enabled"]["constraints"])

            mode = (m.get("migration_mode") or "CDC").upper()
            if mode == "BULK_ONLY":
                # No CDC phase — enable triggers immediately and complete
                try:
                    oracle_browser.enable_triggers(
                        oracle_scn.open_oracle_conn(dst_cfg),
                        m["target_schema"], m["target_table"],
                    )
                except Exception as exc:
                    print(f"[orchestrator] {mid}: enable triggers warning: {exc}")

                msg = (
                    f"Включено: индексов={n_idx}, констрейнтов={n_con}. "
                    "Режим BULK_ONLY — миграция завершена"
                )
                _safe_transition(
                    mid, "INDEXES_ENABLING", "COMPLETED",
                    message=msg,
                    extra_fields={"error_code": None, "error_text": None},
                )
            else:
                msg = (
                    f"Включено: индексов={n_idx}, констрейнтов={n_con}. "
                    "Триггеры остаются выключенными до завершения CDC. "
                    "Ожидание запуска CDC apply-worker"
                )
                # Clear leftover error_code/error_text from previous failed attempts.
                # Use _safe_transition so a concurrent cancel is respected.
                _safe_transition(
                    mid, "INDEXES_ENABLING", "CDC_APPLY_STARTING",
                    message=msg,
                    extra_fields={"error_code": None, "error_text": None},
                )
        except Exception as exc:
            if _current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                _fail(mid, str(exc), "INDEXES_ENABLE_ERROR")
        finally:
            _unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()

def _handle_cancelling(mid: str, m: dict) -> None:
    """Wait for any in-flight thread to finish, then transition to CANCELLED."""
    if _in_prog(mid):
        return  # thread still running — wait for next tick
    _transition(mid, "CANCELLED", message="Миграция отменена")


# ---------------------------------------------------------------------------
# Public API — manual triggers
# ---------------------------------------------------------------------------

def trigger_indexes_enabling(migration_id: str) -> None:
    """Called by the API endpoint when the user clicks 'Enable Indexes'.

    Accepts migrations in INDEXES_ENABLING phase.  Also accepts FAILED
    migrations whose error_code is INDEXES_ENABLE_ERROR so the user can
    recover a stuck migration without manual DB intervention — the migration
    is transitioned back to INDEXES_ENABLING before the handler runs.
    """
    get_conn = _state["get_conn"]
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
        _transition(migration_id, "INDEXES_ENABLING",
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
    _handle_indexes_enabling(migration_id, m)


def trigger_enable_triggers(migration_id: str) -> None:
    """Called by the API endpoint when the user clicks 'Enable Triggers'.

    Accepts migrations in CDC_CATCHING_UP, CDC_CAUGHT_UP, or STEADY_STATE —
    i.e. only after CDC apply has started and indexes are rebuilt.
    """
    _ALLOWED = {"CDC_CATCHING_UP", "CDC_CAUGHT_UP", "STEADY_STATE"}
    get_conn = _state["get_conn"]
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

    dst_cfg = _oracle_cfg(m["target_connection_id"])
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

    get_conn = _state["get_conn"]
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
    _transition(
        migration_id, "BASELINE_PUBLISHING",
        message="Перезапуск baseline: ожидание завершения активных воркеров…",
        extra_fields={"error_code": None, "error_text": None},
    )
    print(f"[orchestrator] {migration_id}: baseline restart triggered")


def _handle_cdc_apply_starting(mid: str, m: dict) -> None:
    """Wait for heartbeat from cdc_apply_worker (written via /api/worker/cdc/checkin)."""
    conn = _state["get_conn"]()
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
        _transition(mid, "CDC_CATCHING_UP",
                    message="CDC apply-worker подключился")


def _handle_cdc_catching_up(mid: str, m: dict) -> None:
    """Sync lag from migration_cdc_state; lag=0 → CDC_CAUGHT_UP."""
    conn = _state["get_conn"]()
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
    _update(mid, {"kafka_lag": lag, "kafka_lag_checked_at": updated_at})
    _state["broadcast"]({
        "type":         "kafka_lag",
        "migration_id": mid,
        "total_lag":    lag,
        "updated_at":   updated_at.isoformat() + "Z" if updated_at else None,
        "ts":           datetime.utcnow().isoformat() + "Z",
    })

    if lag == 0:
        _transition(mid, "CDC_CAUGHT_UP",
                    message="Kafka consumer group lag = 0")


def _handle_cdc_caught_up(mid: str, m: dict) -> None:
    _transition(mid, "STEADY_STATE",
                message="Миграция догнала источник")


def _handle_steady_state(mid: str, m: dict) -> None:
    """Propagate lag updates to the UI while in steady state."""
    conn = _state["get_conn"]()
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
    _update(mid, {"kafka_lag": lag, "kafka_lag_checked_at": updated_at})
    _state["broadcast"]({
        "type":         "kafka_lag",
        "migration_id": mid,
        "total_lag":    lag,
        "updated_at":   updated_at.isoformat() + "Z" if updated_at else None,
        "ts":           datetime.utcnow().isoformat() + "Z",
    })


# ---------------------------------------------------------------------------
# Group-based handlers (shared connector, no SCN)
# ---------------------------------------------------------------------------

def _handle_new_group(mid: str, m: dict) -> None:
    """Group migration: validate keys, queue gate, create stage, → TOPIC_CREATING.

    Unlike legacy NEW:
    - No SCN fixation
    - Connector already managed at group level
    """
    pk = m.get("source_pk_exists", False)
    uk = m.get("source_uk_exists", False)
    key_cols = json.loads(m.get("effective_key_columns_json") or "[]")

    if not pk and not uk and not key_cols:
        _fail(mid,
              "Таблица не имеет PK/UK и ключевые колонки не заданы.",
              "NO_KEY_COLUMNS")
        return

    # Queue gate (same as legacy)
    conn = _state["get_conn"]()
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
        _update_queue_positions()
        return

    conn = _state["get_conn"]()
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
        _update_queue_positions()
        return

    # Verify group connector is RUNNING (for CDC mode)
    mode = (m.get("migration_mode") or "CDC").upper()
    if mode != "BULK_ONLY":
        group = connector_groups_svc.get_group(m["group_id"])
        if not group:
            _fail(mid, "Группа коннектора не найдена", "GROUP_NOT_FOUND")
            return
        if group["status"] != "RUNNING":
            _fail(mid,
                  f"Коннектор группы не запущен (status={group['status']}). "
                  "Запустите коннектор группы перед миграцией.",
                  "GROUP_NOT_RUNNING")
            return

    _update(mid, {"queue_position": None})

    # Create stage table (if STAGE strategy) — same as legacy PREPARING
    # but without SCN fixation
    if _in_prog(mid):
        return
    _mark_in_prog(mid)

    def _run():
        try:
            src_cfg = _oracle_cfg(m["source_connection_id"])
            dst_cfg = _oracle_cfg(m["target_connection_id"])
            strategy = (m.get("migration_strategy") or "STAGE").upper()

            if mode != "BULK_ONLY":
                try:
                    has_supp = oracle_scn.check_supplemental_logging(
                        src_cfg, m["source_schema"], m["source_table"]
                    )
                    if not has_supp:
                        print(
                            f"[orchestrator] WARNING: {m['source_schema']}.{m['source_table']} "
                            "does not have ALL COLUMNS supplemental logging."
                        )
                except Exception as exc:
                    print(f"[orchestrator] supplemental logging check failed: {exc}")

            if strategy == "STAGE":
                ts = m.get("stage_tablespace") or ""
                oracle_stage.create_stage_table(
                    src_cfg, dst_cfg,
                    m["source_schema"], m["source_table"],
                    m["target_schema"], m["stage_table_name"],
                    tablespace=ts,
                )
                stage_msg = "Stage table создана"
            else:
                stage_msg = "Прямая загрузка (без stage)"

            if mode == "BULK_ONLY":
                # Skip topic creation — go straight to chunking
                _safe_transition(mid, "NEW", "CHUNKING",
                                 message=f"{stage_msg}, BULK_ONLY → нарезка чанков")
                # Create chunks inline
                _unmark_in_prog(mid)
                _create_chunks_and_transition(mid, m)
                return
            else:
                _safe_transition(mid, "NEW", "TOPIC_CREATING",
                                 message=f"{stage_msg}, создание топика Kafka")
        except Exception as exc:
            if _current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                _fail(mid, str(exc), "PREPARING_ERROR")
        finally:
            _unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def _handle_topic_creating(mid: str, m: dict) -> None:
    """Pre-create the Kafka topic for this table, then create chunks."""
    if _in_prog(mid):
        return
    _mark_in_prog(mid)

    def _run():
        try:
            topic_prefix = m.get("topic_prefix", "")
            src_schema = m["source_schema"].upper()
            src_table = m["source_table"].upper()
            topic_name = f"{topic_prefix}.{src_schema}.{src_table}".replace("#", "_")

            # Get Kafka bootstrap servers
            configs = _configs()
            kafka_cfg = configs.get("kafka", {})
            bootstrap = [
                s.strip()
                for s in (kafka_cfg.get("bootstrap_servers") or "kafka:9092").split(",")
            ]

            kafka_topics.create_topic(
                bootstrap_servers=bootstrap,
                topic_name=topic_name,
            )

            # Transition to chunking
            _safe_transition(mid, "TOPIC_CREATING", "CHUNKING",
                             message=f"Топик {topic_name} создан, нарезка чанков")

            # Create chunks
            _unmark_in_prog(mid)
            _create_chunks_and_transition(mid, m)
            return
        except Exception as exc:
            if _current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                _fail(mid, str(exc), "TOPIC_CREATE_ERROR")
        finally:
            _unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def _handle_indexes_enabling_group(mid: str, m: dict) -> None:
    """Same as legacy _handle_indexes_enabling but routes to CDC_APPLYING
    instead of CDC_APPLY_STARTING for group-based migrations."""
    if _in_prog(mid):
        return
    _mark_in_prog(mid)

    def _run():
        try:
            dst_cfg = _oracle_cfg(m["target_connection_id"])
            conn = oracle_scn.open_oracle_conn(dst_cfg)
            try:
                oracle_browser.set_table_logging(
                    conn, m["target_schema"], m["target_table"], nologging=False,
                )
                result = oracle_browser.enable_all_disabled_objects(
                    conn, m["target_schema"], m["target_table"],
                )
            finally:
                conn.close()

            err_count = (
                len(result["errors"]["indexes"])
                + len(result["errors"]["constraints"])
            )
            if err_count:
                names = (
                    [e["name"] for e in result["errors"]["indexes"]]
                    + [e["name"] for e in result["errors"]["constraints"]]
                )
                err_detail = str(result["errors"])
                _transition(
                    mid, "INDEXES_ENABLING",
                    message=(
                        f"Ошибка пересчёта: {', '.join(names)}. "
                        "Нажмите «Включить индексы» ещё раз для повторной попытки."
                    ),
                    extra_fields={
                        "error_code": "INDEXES_ENABLE_ERROR",
                        "error_text": err_detail[:2000],
                    },
                )
                return

            n_idx = len(result["enabled"]["indexes"])
            n_con = len(result["enabled"]["constraints"])

            mode = (m.get("migration_mode") or "CDC").upper()
            if mode == "BULK_ONLY":
                try:
                    oracle_browser.enable_triggers(
                        oracle_scn.open_oracle_conn(dst_cfg),
                        m["target_schema"], m["target_table"],
                    )
                except Exception as exc:
                    print(f"[orchestrator] {mid}: enable triggers warning: {exc}")

                _safe_transition(
                    mid, "INDEXES_ENABLING", "COMPLETED",
                    message=(
                        f"Включено: индексов={n_idx}, констрейнтов={n_con}. "
                        "Режим BULK_ONLY — миграция завершена"
                    ),
                    extra_fields={"error_code": None, "error_text": None},
                )
            else:
                # Group-based CDC → CDC_APPLYING (not CDC_APPLY_STARTING)
                _safe_transition(
                    mid, "INDEXES_ENABLING", "CDC_APPLYING",
                    message=(
                        f"Включено: индексов={n_idx}, констрейнтов={n_con}. "
                        "Ожидание CDC apply-worker"
                    ),
                    extra_fields={"error_code": None, "error_text": None},
                )
        except Exception as exc:
            if _current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                _fail(mid, str(exc), "INDEXES_ENABLE_ERROR")
        finally:
            _unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def _handle_cdc_applying(mid: str, m: dict) -> None:
    """Wait for CDC worker heartbeat (group-based variant of CDC_APPLY_STARTING)."""
    conn = _state["get_conn"]()
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
        _transition(mid, "CDC_CATCHING_UP",
                    message="CDC apply-worker подключился")


# ---------------------------------------------------------------------------
# Group lifecycle tick (TOPICS_CREATING → CONNECTOR_STARTING → RUNNING)
# ---------------------------------------------------------------------------

_group_in_progress: set[str] = set()
_group_in_progress_lock = threading.Lock()


def _tick_groups() -> None:
    """Drive connector-group lifecycle phases."""
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT group_id, status, connector_name
                FROM   connector_groups
                WHERE  status IN ('TOPICS_CREATING', 'CONNECTOR_STARTING', 'STOPPING')
            """)
            groups = [{"group_id": r[0], "status": r[1], "connector_name": r[2]}
                      for r in cur.fetchall()]
    except Exception:
        return
    finally:
        conn.close()

    for g in groups:
        gid = g["group_id"]
        status = g["status"]
        try:
            if status == "TOPICS_CREATING":
                _handle_group_topics_creating(gid)
            elif status == "CONNECTOR_STARTING":
                _handle_group_connector_starting(gid)
            elif status == "STOPPING":
                _handle_group_stopping(gid)
        except Exception as exc:
            print(f"[orchestrator] group {gid} status {status} error: {exc}")
            connector_groups_svc.transition_group(gid, "FAILED", str(exc))
            _broadcast({
                "type": "connector_group_status",
                "group_id": gid,
                "status": "FAILED",
            })


def _handle_group_topics_creating(group_id: str) -> None:
    """Create Kafka topics, then move to CONNECTOR_STARTING."""
    with _group_in_progress_lock:
        if group_id in _group_in_progress:
            return
        _group_in_progress.add(group_id)

    def _run():
        try:
            results = connector_groups_svc.do_create_topics(group_id)
            errors = [r for r in results if r.get("status") == "error"]
            if errors:
                msg = "; ".join(f"{r['topic_name']}: {r.get('error','?')}" for r in errors)
                connector_groups_svc.transition_group(
                    group_id, "FAILED", f"Ошибка создания топиков: {msg}")
                _broadcast({
                    "type": "connector_group_status",
                    "group_id": group_id, "status": "FAILED",
                })
                return

            ok_topics = [r["topic_name"] for r in results if r.get("status") == "ok"]
            connector_groups_svc.transition_group(
                group_id, "CONNECTOR_STARTING",
                f"Создано {len(ok_topics)} топиков")
            _broadcast({
                "type": "connector_group_status",
                "group_id": group_id, "status": "CONNECTOR_STARTING",
            })
        except Exception as exc:
            connector_groups_svc.transition_group(
                group_id, "FAILED", str(exc))
            _broadcast({
                "type": "connector_group_status",
                "group_id": group_id, "status": "FAILED",
            })
        finally:
            with _group_in_progress_lock:
                _group_in_progress.discard(group_id)

    threading.Thread(target=_run, daemon=True).start()


def _handle_group_connector_starting(group_id: str) -> None:
    """Create and start Debezium connector, then move to RUNNING."""
    with _group_in_progress_lock:
        if group_id in _group_in_progress:
            return
        _group_in_progress.add(group_id)

    def _run():
        try:
            result = connector_groups_svc.do_start_connector(group_id)
            connector_groups_svc.transition_group(
                group_id, "RUNNING",
                f"Коннектор запущен: {result.get('name', '?')}")
            _broadcast({
                "type": "connector_group_status",
                "group_id": group_id, "status": "RUNNING",
            })
        except Exception as exc:
            connector_groups_svc.transition_group(
                group_id, "FAILED", str(exc))
            _broadcast({
                "type": "connector_group_status",
                "group_id": group_id, "status": "FAILED",
            })
        finally:
            with _group_in_progress_lock:
                _group_in_progress.discard(group_id)

    threading.Thread(target=_run, daemon=True).start()


def _handle_group_stopping(group_id: str) -> None:
    """Stop and delete Debezium connector, then move to STOPPED."""
    with _group_in_progress_lock:
        if group_id in _group_in_progress:
            return
        _group_in_progress.add(group_id)

    def _run():
        try:
            connector_groups_svc.do_stop_connector(group_id)
            connector_groups_svc.transition_group(
                group_id, "STOPPED", "Коннектор остановлен")
            _broadcast({
                "type": "connector_group_status",
                "group_id": group_id, "status": "STOPPED",
            })
        except Exception as exc:
            connector_groups_svc.transition_group(
                group_id, "FAILED", str(exc))
            _broadcast({
                "type": "connector_group_status",
                "group_id": group_id, "status": "FAILED",
            })
        finally:
            with _group_in_progress_lock:
                _group_in_progress.discard(group_id)

    threading.Thread(target=_run, daemon=True).start()


def _broadcast(event: dict) -> None:
    """Send SSE event."""
    _state["broadcast"](event)


# ---------------------------------------------------------------------------
# Group connector health check (runs every tick)
# ---------------------------------------------------------------------------

def _check_group_connectors() -> None:
    """Poll all RUNNING connector groups.  If a group connector failed,
    fail all active CDC migrations in that group."""
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT group_id, connector_name, COALESCE(run_id, '') as run_id
                FROM   connector_groups
                WHERE  status = 'RUNNING'
            """)
            groups = cur.fetchall()
    except Exception:
        return  # table may not exist yet on first run
    finally:
        conn.close()

    for group_id, connector_name, run_id in groups:
        active_name = f"{connector_name}_{run_id}" if run_id else connector_name
        try:
            status = debezium.get_connector_status(active_name)
        except Exception as exc:
            print(f"[orchestrator] group {group_id} connector check error: {exc}")
            continue

        if status == "FAILED":
            print(f"[orchestrator] group connector {active_name} FAILED — failing group migrations")
            connector_groups_svc.update_group_status(group_id, "FAILED", "Connector FAILED")

            # Fail all active CDC migrations in this group
            conn = _state["get_conn"]()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT migration_id FROM migrations
                        WHERE  group_id = %s
                          AND  phase NOT IN ('DRAFT', 'COMPLETED', 'CANCELLED', 'FAILED')
                          AND  migration_mode != 'BULK_ONLY'
                    """, (group_id,))
                    for row in cur.fetchall():
                        _fail(row[0],
                              f"Коннектор группы {connector_name} перешёл в FAILED",
                              "GROUP_CONNECTOR_FAILED")
            finally:
                conn.close()
