"""BULK_ONLY pipeline — new phase handlers."""

import threading
from datetime import datetime

import services.oracle_scn as oracle_scn
import services.oracle_chunker as oracle_chunker
import db.oracle_browser as oracle_browser

from orchestrator.helpers import (
    oracle_cfg, get_conn, transition, fail, update, safe_transition,
    current_phase, in_prog, mark_in_prog, unmark_in_prog, broadcast, configs,
)


def handle_structure_ready(mid: str, m: dict) -> None:
    """Immediate transition to DATA_COMPARING."""
    transition(mid, "DATA_COMPARING", message="Структура проверена, запуск сравнения данных")


def handle_data_comparing(mid: str, m: dict) -> None:
    """Create compare task + chunks on first tick, monitor completion on subsequent ticks."""
    task_id = m.get("data_compare_task_id")

    if not task_id:
        # First tick — create compare task and chunks
        if in_prog(mid):
            return
        mark_in_prog(mid)

        def _run():
            try:
                src_cfg = oracle_cfg(m["source_connection_id"])
                chunk_size = int(m.get("chunk_size") or 500_000)

                conn = get_conn()
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO data_compare_tasks
                                (source_schema, source_table, target_schema, target_table,
                                 compare_mode, chunk_size, status)
                            VALUES (%s, %s, %s, %s, 'full', %s, 'PENDING')
                            RETURNING task_id
                        """, (m["source_schema"], m["source_table"],
                              m["target_schema"], m["target_table"],
                              chunk_size))
                        new_task_id = str(cur.fetchone()[0])

                        cur.execute(
                            "UPDATE migrations SET data_compare_task_id = %s, updated_at = NOW() "
                            "WHERE migration_id = %s",
                            (new_task_id, mid))
                    conn.commit()
                finally:
                    conn.close()

                # Create ROWID chunks on source
                chunks = oracle_chunker.create_chunks(
                    src_cfg,
                    m["source_schema"],
                    m["source_table"],
                    chunk_size,
                    f"CMP_{mid[:8]}",
                )

                if not chunks:
                    # Empty table — mark as matching
                    conn = get_conn()
                    try:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE data_compare_tasks SET status='DONE', "
                                "counts_match=true, hash_match=true, "
                                "source_count=0, target_count=0 "
                                "WHERE task_id = %s",
                                (new_task_id,))
                        conn.commit()
                    finally:
                        conn.close()
                    # Empty source — check if target is also empty
                    safe_transition(mid, "DATA_COMPARING", "COMPLETED",
                                    message="Таблица-источник пуста, миграция не требуется")
                    return

                # Store compare chunks
                conn = get_conn()
                try:
                    with conn.cursor() as cur:
                        for ch in chunks:
                            cur.execute("""
                                INSERT INTO data_compare_chunks
                                    (task_id, side, chunk_seq, rowid_start, rowid_end, status)
                                VALUES (%s, 'both', %s, %s, %s, 'PENDING')
                                ON CONFLICT DO NOTHING
                            """, (new_task_id, ch.chunk_seq, ch.rowid_start, ch.rowid_end))
                        cur.execute(
                            "UPDATE data_compare_tasks SET status='RUNNING', "
                            "chunks_total=%s, started_at=NOW() WHERE task_id=%s",
                            (len(chunks), new_task_id))
                    conn.commit()
                finally:
                    conn.close()

                print(f"[orchestrator] {mid}: data compare task {new_task_id}, {len(chunks)} chunks")

            except Exception as exc:
                if current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                    fail(mid, f"Ошибка создания сравнения: {exc}", "DATA_COMPARE_ERROR")
            finally:
                unmark_in_prog(mid)

        threading.Thread(target=_run, daemon=True, name=f"dc-init-{mid[:8]}").start()
        return

    # Subsequent ticks — monitor compare task
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, chunks_done, chunks_total, error_text "
                "FROM data_compare_tasks WHERE task_id = %s",
                (task_id,))
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        fail(mid, f"data_compare task {task_id} not found", "DATA_COMPARE_ERROR")
        return

    status, done, total, err_text = row

    if status == "FAILED":
        fail(mid, f"Сравнение данных ошибка: {err_text or 'unknown'}", "DATA_COMPARE_ERROR")
        return

    if status not in ("DONE", "COMPLETED"):
        # Broadcast progress
        broadcast({
            "type": "data_compare_progress",
            "migration_id": mid,
            "chunks_done": done or 0,
            "chunks_total": total or 0,
            "ts": datetime.utcnow().isoformat() + "Z",
        })
        return

    # Compare complete — check results
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM data_compare_chunks
                WHERE task_id = %s AND status = 'DONE'
                  AND (NOT COALESCE(counts_match, false) OR NOT COALESCE(hash_match, false))
            """, (task_id,))
            mismatches = cur.fetchone()[0]
    finally:
        conn.close()

    if mismatches == 0:
        safe_transition(mid, "DATA_COMPARING", "COMPLETED",
                        message="Данные source и target идентичны, миграция не требуется",
                        extra_fields={"error_code": None, "error_text": None})
    else:
        safe_transition(mid, "DATA_COMPARING", "TARGET_CLEARING",
                        message=f"Обнаружены расхождения ({mismatches} чанков), начинаем перенос")


def handle_target_clearing(mid: str, m: dict) -> None:
    """TRUNCATE target, disable triggers, mark indexes unusable, set NOLOGGING."""
    if in_prog(mid):
        return
    mark_in_prog(mid)

    def _run():
        try:
            dst_cfg = oracle_cfg(m["target_connection_id"])
            tgt_schema = m["target_schema"]
            tgt_table = m["target_table"]

            conn = oracle_scn.open_oracle_conn(dst_cfg)
            try:
                tgt_quoted = f'"{tgt_schema.upper()}"."{tgt_table.upper()}"'

                # 1. TRUNCATE
                with conn.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE {tgt_quoted}")
                conn.commit()
                print(f"[orchestrator] {mid}: truncated {tgt_quoted}")

                # 2. Disable triggers
                disabled_trg = oracle_browser.disable_triggers(conn, tgt_schema, tgt_table)
                if disabled_trg:
                    print(f"[orchestrator] {mid}: disabled triggers: {disabled_trg}")

                # 3. Mark non-PK indexes UNUSABLE
                marked = oracle_browser.mark_indexes_unusable(conn, tgt_schema, tgt_table, skip_pk=True)
                if marked:
                    print(f"[orchestrator] {mid}: marked UNUSABLE: {marked}")

                # 4. Set NOLOGGING
                oracle_browser.set_table_logging(conn, tgt_schema, tgt_table, nologging=True)
                print(f"[orchestrator] {mid}: set NOLOGGING on {tgt_quoted}")
            finally:
                conn.close()

            # Clear old data_compare_task_id so DATA_VERIFYING creates a fresh one
            update(mid, {"data_compare_task_id": None})

            safe_transition(mid, "TARGET_CLEARING", "CHUNKING",
                            message="Target очищен, триггеры отключены, индексы UNUSABLE — нарезка чанков")
        except Exception as exc:
            if current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                fail(mid, str(exc), "TARGET_CLEARING_ERROR")
        finally:
            unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()
