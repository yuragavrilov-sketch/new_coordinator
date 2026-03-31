from datetime import datetime

import services.job_queue as job_queue

from orchestrator.helpers import get_conn, transition, fail, update, broadcast


def handle_chunking(mid: str, m: dict) -> None:
    """Create chunks if not yet created, then transition to BULK_LOADING."""
    # Check if chunks already exist
    conn = get_conn()
    try:
        stats = job_queue.get_chunk_stats(conn, mid)
    finally:
        conn.close()

    if stats["total"] > 0:
        # Chunks already created — go to loading
        transition(mid, "BULK_LOADING",
                   message=f"Чанки записаны ({stats['total']}), запуск bulk-загрузки")
        return

    # No chunks yet — create them (used by BULK_ONLY pipeline after TARGET_CLEARING)
    from orchestrator.phases.preparing import create_chunks_and_transition
    create_chunks_and_transition(mid, m)


def handle_bulk_loading(mid: str, m: dict) -> None:
    """Monitor chunk completion."""
    conn = get_conn()
    try:
        stats = job_queue.get_chunk_stats(conn, mid)
    finally:
        conn.close()

    total   = stats["total"]
    done    = stats["done"]
    failed  = stats["failed"]
    active  = stats["claimed"] + stats["running"] + stats["pending"]

    broadcast({
        "type":         "chunk_progress",
        "migration_id": mid,
        "chunks_done":  done,
        "total_chunks": total,
        "ts":           datetime.utcnow().isoformat() + "Z",
    })

    if total == 0:
        return  # Chunks not yet written

    if done == total:
        transition(mid, "BULK_LOADED",
                   message=f"Все {total} чанков загружены")
        return

    if failed > 0 and active == 0 and (done + failed) == total:
        fail(mid,
             f"Bulk load завершился с ошибками: {failed} чанков не удалось загрузить",
             "BULK_LOAD_FAILED")


def handle_bulk_loaded(mid: str, m: dict) -> None:
    strategy = (m.get("migration_strategy") or "STAGE").upper()
    if strategy == "DIRECT":
        # Skip stage validate / publish / drop — data is already in the target table
        transition(mid, "INDEXES_ENABLING",
                   message="DIRECT стратегия: данные загружены напрямую, включение индексов")
    else:
        transition(mid, "STAGE_VALIDATING")
