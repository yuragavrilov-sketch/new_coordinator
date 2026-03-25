"""
Bulk Worker — reads ROWID chunks from the job queue and loads them into
the stage table on the target Oracle database.

Usage:
    python bulk_worker.py

Environment variables:
    API_URL      Flask backend URL (default: http://localhost:5000)
    WORKER_ID    Unique worker identifier (default: hostname:pid)
    BATCH_SIZE   Rows per INSERT batch (default: 5000)
    POLL_INTERVAL  Seconds to wait when no chunk available (default: 5)
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from common import api_get, api_post, get_configs, open_oracle, WORKER_ID

BATCH_SIZE    = int(os.environ.get("BATCH_SIZE",    5_000))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", 5))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_insert(cursor_description, target_schema: str, stage_table: str) -> str:
    col_names = [d[0] for d in cursor_description]
    cols      = ", ".join(f'"{c}"' for c in col_names)
    params    = ", ".join(f":{i + 1}" for i in range(len(col_names)))
    return (
        f'INSERT INTO "{target_schema.upper()}"."{stage_table.upper()}" '
        f'({cols}) VALUES ({params})'
    )


def _insert_batch(dst_conn, insert_sql: str, batch: list) -> None:
    with dst_conn.cursor() as cur:
        cur.executemany(insert_sql, batch)
    dst_conn.commit()


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------

def process_chunk(chunk: dict) -> None:
    chunk_id   = chunk["chunk_id"]
    migration_id = chunk["migration_id"]
    src_id     = chunk["source_connection_id"]
    dst_id     = chunk["target_connection_id"]
    src_schema = chunk["source_schema"]
    src_table  = chunk["source_table"]
    tgt_schema = chunk["target_schema"]
    stage      = chunk["stage_table"]
    raw_scn    = chunk.get("start_scn")
    start_scn  = int(raw_scn) if raw_scn else None
    rowid_start = chunk["rowid_start"]
    rowid_end   = chunk["rowid_end"]

    print(f"[bulk_worker] chunk {chunk_id} ({rowid_start}..{rowid_end}) scn={start_scn}")

    configs = get_configs(force=True)
    src_conn = open_oracle(src_id, configs)
    dst_conn = open_oracle(dst_id, configs)
    rows_loaded = 0
    insert_sql: str = ""

    try:
        with src_conn.cursor() as cur:
            if start_scn:
                cur.execute(
                    f'SELECT * FROM "{src_schema.upper()}"."{src_table.upper()}" '
                    f'AS OF SCN :scn '
                    f'WHERE ROWID BETWEEN CHARTOROWID(:start) AND CHARTOROWID(:end)',
                    {"scn": start_scn, "start": rowid_start, "end": rowid_end},
                )
            else:
                cur.execute(
                    f'SELECT * FROM "{src_schema.upper()}"."{src_table.upper()}" '
                    f'WHERE ROWID BETWEEN CHARTOROWID(:start) AND CHARTOROWID(:end)',
                    {"start": rowid_start, "end": rowid_end},
                )

            # Build INSERT from cursor metadata
            insert_sql = _build_insert(cur.description, tgt_schema, stage)

            batch: list = []
            for row in cur:
                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    _insert_batch(dst_conn, insert_sql, batch)
                    rows_loaded += len(batch)
                    batch = []
                    api_post(f"/api/worker/chunks/{chunk_id}/progress",
                             {"rows_loaded": rows_loaded})
                    print(f"  → {rows_loaded} строк загружено")

            if batch:
                _insert_batch(dst_conn, insert_sql, batch)
                rows_loaded += len(batch)

        api_post(f"/api/worker/chunks/{chunk_id}/complete", {"rows_loaded": rows_loaded})
        print(f"[bulk_worker] chunk {chunk_id} DONE — {rows_loaded} строк")

    except Exception as exc:
        error_text = str(exc)
        print(f"[bulk_worker] chunk {chunk_id} FAILED: {error_text}")
        try:
            api_post(f"/api/worker/chunks/{chunk_id}/fail", {"error_text": error_text})
        except Exception as report_err:
            print(f"[bulk_worker] failed to report failure: {report_err}")
        raise
    finally:
        try:
            src_conn.close()
        except Exception:
            pass
        try:
            dst_conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    print(f"[bulk_worker] started worker_id={WORKER_ID}")
    while True:
        try:
            resp = api_post("/api/worker/chunks/claim", {"worker_id": WORKER_ID})
            if resp is None:
                # 204 No Content — nothing to claim
                time.sleep(POLL_INTERVAL)
                continue

            process_chunk(resp)

        except KeyboardInterrupt:
            print("[bulk_worker] interrupted")
            break
        except Exception as exc:
            print(f"[bulk_worker] error: {exc}")
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
