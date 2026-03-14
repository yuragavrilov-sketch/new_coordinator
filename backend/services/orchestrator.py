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
import db.oracle_browser        as oracle_browser

TICK_INTERVAL = 5  # seconds

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


def _dispatch(migration_id: str, phase: str, m: dict) -> None:
    handlers = {
        "NEW":                  _handle_new,
        "PREPARING":            _handle_preparing,
        "SCN_FIXED":            _handle_scn_fixed,
        "CONNECTOR_STARTING":   _handle_connector_starting,
        "CDC_BUFFERING":        _handle_cdc_buffering,
        "CHUNKING":             _handle_chunking,
        "BULK_LOADING":         _handle_bulk_loading,
        "BULK_LOADED":          _handle_bulk_loaded,
        "STAGE_VALIDATING":     _handle_stage_validating,
        "STAGE_VALIDATED":      _handle_stage_validated,
        "BASELINE_PUBLISHING":  _handle_baseline_publishing,
        "BASELINE_PUBLISHED":   _handle_baseline_published,
        "STAGE_DROPPING":       _handle_stage_dropping,
        "INDEXES_ENABLING":     _handle_indexes_enabling,
        "CDC_APPLY_STARTING":   _handle_cdc_apply_starting,
        "CDC_CATCHING_UP":      _handle_cdc_catching_up,
        "CDC_CAUGHT_UP":        _handle_cdc_caught_up,
        "STEADY_STATE":         _handle_steady_state,
    }
    handler = handlers.get(phase)
    if handler:
        handler(migration_id, m)


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


def _in_prog(migration_id: str) -> bool:
    with _in_progress_lock:
        return migration_id in _in_progress


def _mark_in_prog(migration_id: str) -> None:
    with _in_progress_lock:
        _in_progress.add(migration_id)


def _unmark_in_prog(migration_id: str) -> None:
    with _in_progress_lock:
        _in_progress.discard(migration_id)


# ---------------------------------------------------------------------------
# Phase handlers
# ---------------------------------------------------------------------------

def _handle_new(mid: str, m: dict) -> None:
    """
    Validate:
    - key columns defined (if no PK/UK)
    - effective_key_columns_json not empty
    Then transition to PREPARING.
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

    _transition(mid, "PREPARING", message="Ключевые колонки проверены")


def _handle_preparing(mid: str, m: dict) -> None:
    """Create stage table, fix SCN → SCN_FIXED."""
    if _in_prog(mid):
        return
    _mark_in_prog(mid)

    def _run():
        try:
            src_cfg = _oracle_cfg(m["source_connection_id"])
            dst_cfg = _oracle_cfg(m["target_connection_id"])

            # Check supplemental logging (warn, don't block)
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

            # Create stage table (idempotent)
            oracle_stage.create_stage_table(
                src_cfg, dst_cfg,
                m["source_schema"], m["source_table"],
                m["target_schema"], m["stage_table_name"],
            )

            # Fix SCN
            scn = oracle_scn.get_current_scn(src_cfg)

            _update(mid, {
                "start_scn":   scn,
                "scn_fixed_at": datetime.utcnow(),
            })
            _transition(mid, "SCN_FIXED",
                        message=f"Stage table создана, start_scn={scn}")
        except Exception as exc:
            _fail(mid, str(exc), "PREPARING_ERROR")
        finally:
            _unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def _handle_scn_fixed(mid: str, m: dict) -> None:
    """Create Debezium connector → CONNECTOR_STARTING."""
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


def _handle_cdc_buffering(mid: str, m: dict) -> None:
    """
    Create ROWID chunks via DBMS_PARALLEL_EXECUTE and store in migration_chunks.
    Idempotent: if chunks already exist, skip.
    """
    if _in_prog(mid):
        return

    # Check if chunks already created
    conn = _state["get_conn"]()
    try:
        stats = job_queue.get_chunk_stats(conn, mid)
    finally:
        conn.close()

    if stats["total"] > 0:
        # Chunks already written — move on
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
                _fail(mid,
                      "DBMS_PARALLEL_EXECUTE вернул 0 чанков — таблица пуста или нет прав",
                      "NO_CHUNKS")
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
                _transition(mid, "STAGE_VALIDATED", message=result.message)
            else:
                _fail(mid, result.message, "VALIDATION_FAILED")
        except Exception as exc:
            _fail(mid, str(exc), "VALIDATION_ERROR")
        finally:
            _unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def _handle_stage_validated(mid: str, m: dict) -> None:
    _transition(mid, "BASELINE_PUBLISHING")


def _handle_baseline_publishing(mid: str, m: dict) -> None:
    """TRUNCATE + INSERT APPEND in a dedicated thread."""
    if _in_prog(mid):
        return
    _mark_in_prog(mid)

    def _run():
        try:
            dst_cfg = _oracle_cfg(m["target_connection_id"])
            rows = oracle_baseline.publish_baseline(
                dst_cfg,
                m["target_schema"],
                m["target_table"],
                m["stage_table_name"],
                migration_id=m["migration_id"],
                parallel_degree=int(m.get("baseline_parallel_degree") or 4),
                chunk_size=int(m.get("baseline_batch_size") or 500_000),
            )
            _transition(mid, "BASELINE_PUBLISHED",
                        message=f"Вставлено {rows} строк в целевую таблицу")
        except Exception as exc:
            _fail(mid, str(exc), "BASELINE_PUBLISH_ERROR")
        finally:
            _unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


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
            _transition(mid, "INDEXES_ENABLING",
                        message="Stage-таблица удалена, включение индексов и прочих объектов")
        except Exception as exc:
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
                _fail(
                    mid,
                    f"Не удалось включить объекты: {', '.join(names)}. "
                    + str(result["errors"]),
                    "INDEXES_ENABLE_ERROR",
                )
                return

            n_idx = len(result["enabled"]["indexes"])
            n_con = len(result["enabled"]["constraints"])
            msg = (
                f"Включено: индексов={n_idx}, констрейнтов={n_con}. "
                "Ожидание запуска CDC apply-worker"
            )
            _transition(mid, "CDC_APPLY_STARTING", message=msg)
        except Exception as exc:
            _fail(mid, str(exc), "INDEXES_ENABLE_ERROR")
        finally:
            _unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


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
