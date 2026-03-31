from datetime import datetime

import services.job_queue as job_queue

from orchestrator.helpers import get_conn, transition, fail, update, broadcast


def handle_chunking(mid: str, m: dict) -> None:
    """Chunks are written — transition to BULK_LOADING."""
    transition(mid, "BULK_LOADING",
               message="Чанки записаны, запуск bulk-загрузки")


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
