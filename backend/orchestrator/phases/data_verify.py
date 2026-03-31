import threading

from orchestrator.helpers import (
    get_conn, fail, safe_transition, current_phase,
    in_prog, mark_in_prog, unmark_in_prog, configs,
)


def handle_data_verifying(mid: str, m: dict) -> None:
    """Create data_compare task on first tick, then monitor its completion."""
    task_id = m.get("data_compare_task_id")

    if not task_id:
        # First tick — create the data_compare task in a daemon thread
        if in_prog(mid):
            return
        mark_in_prog(mid)

        def _run():
            try:
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
                              m.get("chunk_size") or 100_000))
                        new_task_id = str(cur.fetchone()[0])

                        cur.execute(
                            "UPDATE migrations SET data_compare_task_id = %s, updated_at = NOW() "
                            "WHERE migration_id = %s",
                            (new_task_id, mid))
                    conn.commit()
                finally:
                    conn.close()

                # Launch chunking in background (reuse data_compare logic)
                from routes.data_compare import _create_chunks_and_start
                cfg = configs()
                threading.Thread(
                    target=_create_chunks_and_start,
                    args=(new_task_id, cfg,
                          m["source_schema"], m["source_table"],
                          m["target_schema"], m["target_table"],
                          m.get("chunk_size") or 100_000),
                    daemon=True,
                    name=f"dv-chunk-{mid[:8]}",
                ).start()

                print(f"[orchestrator] {mid}: data_compare task created: {new_task_id}")

            except Exception as exc:
                if current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                    fail(mid, f"Ошибка создания сверки: {exc}", "DATA_VERIFY_ERROR")
            finally:
                unmark_in_prog(mid)

        threading.Thread(target=_run, daemon=True, name=f"dv-init-{mid[:8]}").start()
        return

    # Subsequent ticks — check data_compare task status
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, counts_match, hash_match, "
                "       source_count, target_count, chunks_done, chunks_total, error_text "
                "FROM data_compare_tasks WHERE task_id = %s",
                (task_id,))
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        fail(mid, f"data_compare task {task_id} not found", "DATA_VERIFY_ERROR")
        return

    status, counts_match, hash_match, src_count, tgt_count, done, total, err_text = row

    if status == "FAILED":
        fail(mid, f"Сверка данных завершилась ошибкой: {err_text or 'unknown'}", "DATA_VERIFY_ERROR")
        return

    if status not in ("DONE", "COMPLETED"):
        return  # Still running

    # Verification complete — check results
    if counts_match and hash_match:
        safe_transition(
            mid, "DATA_VERIFYING", "COMPLETED",
            message=(
                f"Сверка данных пройдена. Source: {src_count}, Target: {tgt_count}. "
                "COUNT и HASH совпадают."
            ),
            extra_fields={"error_code": None, "error_text": None},
        )
    else:
        details = []
        if not counts_match:
            details.append(f"COUNT mismatch: source={src_count}, target={tgt_count}")
        if not hash_match:
            details.append("HASH mismatch")
        safe_transition(
            mid, "DATA_VERIFYING", "DATA_MISMATCH",
            message=f"Сверка выявила расхождения: {'; '.join(details)}",
            extra_fields={
                "error_code": "DATA_MISMATCH",
                "error_text": f"source_count={src_count}, target_count={tgt_count}, "
                              f"counts_match={counts_match}, hash_match={hash_match}",
            },
        )


def handle_data_mismatch(mid: str, m: dict) -> None:
    """Idle phase — wait for user action (retry_verify, force_complete, cancel)."""
    pass
