import json
import threading
import time
from datetime import datetime

import services.oracle_scn as oracle_scn
import services.oracle_stage as oracle_stage
import services.oracle_chunker as oracle_chunker
import services.validator as validator
import services.job_queue as job_queue
import db.oracle_browser as oracle_browser

from orchestrator.helpers import (
    oracle_cfg, get_conn, transition, fail, update, safe_transition,
    current_phase, in_prog, mark_in_prog, unmark_in_prog, broadcast,
)


def handle_stage_validating(mid: str, m: dict) -> None:
    """Run validation in a separate thread."""
    if in_prog(mid):
        return
    mark_in_prog(mid)

    def _run():
        try:
            src_cfg = oracle_cfg(m["source_connection_id"])
            dst_cfg = oracle_cfg(m["target_connection_id"])
            result  = validator.validate_stage(m, src_cfg, dst_cfg)
            update(mid, {"validation_result": json.dumps(result.to_dict())})

            if result.ok:
                safe_transition(mid, "STAGE_VALIDATING", "STAGE_VALIDATED",
                                message=result.message)
            else:
                if current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                    fail(mid, result.message, "VALIDATION_FAILED")
        except Exception as exc:
            if current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                fail(mid, str(exc), "VALIDATION_ERROR")
        finally:
            unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def handle_stage_validated(mid: str, m: dict) -> None:
    transition(mid, "BASELINE_PUBLISHING")


def handle_baseline_publishing(mid: str, m: dict) -> None:
    """
    TRUNCATE target, chunk the stage table on the target Oracle, store
    chunks (chunk_type='BASELINE') in migration_chunks, then move to
    BASELINE_LOADING so workers pick them up.
    """
    if in_prog(mid):
        return
    mark_in_prog(mid)

    def _run():
        try:
            dst_cfg    = oracle_cfg(m["target_connection_id"])
            tgt_schema = m["target_schema"]
            tgt_table  = m["target_table"]
            stg_table  = m["stage_table_name"]
            chunk_size = int(m.get("baseline_batch_size") or 500_000)

            # Wait for in-flight BASELINE chunks to drain (restart scenario).
            # Phase is already BASELINE_PUBLISHING so workers won't claim new
            # chunks; we just need active ones to finish before TRUNCATE.
            pg_conn = get_conn()
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
            pg_conn = get_conn()
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
                # Disable referencing FKs before TRUNCATE (ORA-02266)
                disabled_fks = oracle_browser.disable_referencing_fks(
                    conn, tgt_schema, tgt_table)
                if disabled_fks:
                    print(f"[baseline_publishing] disabled {len(disabled_fks)} referencing FKs")

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
                update(mid, {"baseline_chunks_total": 0, "baseline_chunks_done": 0})
                transition(mid, "BASELINE_PUBLISHED",
                           message="Stage таблица пуста — целевая таблица обнулена")
                return

            # 3. Store as BASELINE chunks in migration_chunks
            pg_conn = get_conn()
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

            update(mid, {
                "baseline_chunks_total": len(chunks),
                "baseline_chunks_done":  0,
            })
            safe_transition(mid, "BASELINE_PUBLISHING", "BASELINE_LOADING",
                            message=f"Создано {len(chunks)} baseline-чанков, запуск воркеров")
        except Exception as exc:
            if current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                fail(mid, str(exc), "BASELINE_PUBLISH_ERROR")
        finally:
            unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def handle_baseline_loading(mid: str, m: dict) -> None:
    """Monitor BASELINE chunk completion — analogous to handle_bulk_loading."""
    conn = get_conn()
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

    broadcast({
        "type":                  "baseline_progress",
        "migration_id":          mid,
        "baseline_chunks_done":  done,
        "baseline_chunks_total": total,
        "ts":                    datetime.utcnow().isoformat() + "Z",
    })

    if total == 0:
        return

    update(mid, {"baseline_chunks_done": done})

    if done == total:
        transition(mid, "BASELINE_PUBLISHED",
                   message=f"Все {total} baseline-чанков загружены")
        return

    if failed > 0 and active == 0 and (done + failed) == total:
        fail(mid,
             f"Baseline loading завершился с ошибками: {failed}/{total} чанков не удалось",
             "BASELINE_LOAD_FAILED")


def handle_baseline_published(mid: str, m: dict) -> None:
    transition(mid, "STAGE_DROPPING",
               message="Удаление stage-таблицы")


def handle_stage_dropping(mid: str, m: dict) -> None:
    """Drop the stage table — runs in a thread."""
    if in_prog(mid):
        return
    mark_in_prog(mid)

    def _run():
        try:
            dst_cfg = oracle_cfg(m["target_connection_id"])
            oracle_stage.drop_stage_table(
                dst_cfg, m["target_schema"], m["stage_table_name"],
            )
            safe_transition(mid, "STAGE_DROPPING", "INDEXES_ENABLING",
                            message="Stage-таблица удалена, включение индексов и прочих объектов")
        except Exception as exc:
            if current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                fail(mid, str(exc), "STAGE_DROP_ERROR")
        finally:
            unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()
