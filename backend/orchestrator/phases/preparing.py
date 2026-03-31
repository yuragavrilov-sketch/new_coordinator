import json
import threading
from datetime import datetime

import services.debezium as debezium
import services.oracle_scn as oracle_scn
import services.oracle_stage as oracle_stage
import services.oracle_chunker as oracle_chunker
import services.job_queue as job_queue

from orchestrator.helpers import (
    oracle_cfg, transition, fail, update, safe_transition,
    current_phase, in_prog, mark_in_prog, unmark_in_prog, get_conn, broadcast,
)
from orchestrator.queue import check_loading_slot, check_bulk_slot


def handle_new(mid: str, m: dict) -> None:
    """
    Validate key columns, then transition to PREPARING — but only if the
    loading slot is free.  The gate is here (before SCN fixation) so that
    queued migrations don't accumulate a growing Kafka CDC backlog.
    """
    mode = (m.get("migration_mode") or "CDC").upper()
    pk = m.get("source_pk_exists", False)
    uk = m.get("source_uk_exists", False)
    key_cols = json.loads(m.get("effective_key_columns_json") or "[]")

    if mode != "BULK_ONLY" and not pk and not uk and not key_cols:
        fail(mid,
             "Таблица не имеет PK/UK и ключевые колонки не заданы. "
             "Укажите ключевые колонки при создании миграции.",
             "NO_KEY_COLUMNS")
        return

    # CDC: one at a time (Kafka backlog concern)
    # BULK_ONLY: up to MAX_BULK_CONCURRENT at a time
    if mode == "BULK_ONLY":
        if not check_bulk_slot(mid):
            return
    else:
        if not check_loading_slot(mid):
            return

    update(mid, {"queue_position": None})
    transition(mid, "PREPARING", message="Ключевые колонки проверены")


def handle_preparing(mid: str, m: dict) -> None:
    """Create stage table, fix SCN → SCN_FIXED."""
    if in_prog(mid):
        return
    mark_in_prog(mid)

    def _run():
        try:
            src_cfg  = oracle_cfg(m["source_connection_id"])
            dst_cfg  = oracle_cfg(m["target_connection_id"])
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

            if mode == "BULK_ONLY":
                # BULK_ONLY — skip SCN fixation, go to structure check
                safe_transition(mid, "PREPARING", "STRUCTURE_READY",
                                message=f"{stage_msg}переход к проверке структуры")
                return

            # Fix SCN (CDC mode only)
            scn = oracle_scn.get_current_scn(src_cfg)

            update(mid, {
                "start_scn":   scn,
                "scn_fixed_at": datetime.utcnow(),
            })
            safe_transition(mid, "PREPARING", "SCN_FIXED",
                            message=f"{stage_msg}start_scn={scn}")
        except Exception as exc:
            if current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                fail(mid, str(exc), "PREPARING_ERROR")
        finally:
            unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def handle_scn_fixed(mid: str, m: dict) -> None:
    """CDC: create Debezium connector → CONNECTOR_STARTING.
    BULK_ONLY: skip connector, create chunks directly → CHUNKING."""
    mode = (m.get("migration_mode") or "CDC").upper()

    if mode == "BULK_ONLY":
        # Skip Debezium entirely — go straight to chunk creation
        create_chunks_and_transition(mid, m)
        return

    # CDC mode — create Debezium connector
    src_cfg = oracle_cfg(m["source_connection_id"])
    try:
        debezium.create_connector(m, src_cfg)
    except Exception as exc:
        fail(mid, str(exc), "CONNECTOR_CREATE_ERROR")
        return
    update(mid, {"connector_status": "CREATING"})
    transition(mid, "CONNECTOR_STARTING", message="Debezium connector создан")


def handle_connector_starting(mid: str, m: dict) -> None:
    """Poll connector status; RUNNING → CDC_BUFFERING."""
    try:
        status = debezium.get_connector_status(m["connector_name"])
    except Exception as exc:
        print(f"[orchestrator] connector status error for {mid}: {exc}")
        return

    update(mid, {"connector_status": status})
    broadcast({
        "type":           "connector_status",
        "migration_id":   mid,
        "status":         status,
        "connector_name": m["connector_name"],
        "ts":             datetime.utcnow().isoformat() + "Z",
    })

    if status == "RUNNING":
        transition(mid, "CDC_BUFFERING",
                   message="Debezium connector RUNNING")
    elif status == "FAILED":
        fail(mid, "Debezium connector перешёл в статус FAILED",
             "CONNECTOR_FAILED")


def create_chunks_and_transition(mid: str, m: dict) -> None:
    """Create ROWID chunks and transition to CHUNKING.
    Shared by CDC (via handle_cdc_buffering) and BULK_ONLY (via handle_scn_fixed)."""
    if in_prog(mid):
        return

    # Check if chunks already created
    conn = get_conn()
    try:
        stats = job_queue.get_chunk_stats(conn, mid)
    finally:
        conn.close()

    if stats["total"] > 0:
        update(mid, {"total_chunks": stats["total"]})
        transition(mid, "CHUNKING",
                   message=f"Чанки уже созданы ({stats['total']}), переход к нарезке")
        return

    mark_in_prog(mid)

    def _run():
        try:
            src_cfg = oracle_cfg(m["source_connection_id"])
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
                    fail(mid,
                         "DBMS_PARALLEL_EXECUTE вернул 0 чанков, но таблица не пуста — "
                         "проверьте привилегии EXECUTE ON DBMS_PARALLEL_EXECUTE",
                         "NO_CHUNKS")
                    return

                # Source table is genuinely empty — skip bulk loading,
                # go straight to enabling indexes and then CDC listener
                update(mid, {"total_chunks": 0})
                transition(mid, "INDEXES_ENABLING",
                           message="Таблица-источник пуста (0 строк), "
                                   "пропуск bulk-загрузки — включение индексов и запуск CDC")
                return

            pg_conn = get_conn()
            try:
                job_queue.save_chunks(pg_conn, mid, chunks)
            finally:
                pg_conn.close()

            update(mid, {"total_chunks": len(chunks)})
            transition(mid, "CHUNKING",
                       message=f"Создано {len(chunks)} чанков")
        except Exception as exc:
            fail(mid, str(exc), "CHUNKING_ERROR")
        finally:
            unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def handle_cdc_buffering(mid: str, m: dict) -> None:
    """
    Create ROWID chunks via DBMS_PARALLEL_EXECUTE and store in migration_chunks.
    Idempotent: if chunks already exist, skip.
    """
    create_chunks_and_transition(mid, m)
