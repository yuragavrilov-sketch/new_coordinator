"""
Universal Worker — handles both bulk chunk loading and CDC apply tasks.

A single process that:
  * polls for PENDING bulk chunks   → processes them one by one
  * polls for CDC migrations needing a worker → runs a Kafka consumer
    thread per migration (auto-restarts if thread dies)

Usage:
    python worker.py

Environment variables (see .env.example):
    API_URL            Flask backend URL            (default: http://localhost:5000)
    WORKER_ID          Unique identifier            (default: hostname:pid)
    BULK_BATCH_SIZE    Rows per INSERT batch        (default: 5000)
    BULK_POLL_INTERVAL Seconds between claim polls  (default: 5)
    CDC_BATCH_SIZE     Kafka records per poll cycle (default: 500)
    CDC_CHECKIN_SEC    Seconds between CDC checkins (default: 30)
    CDC_POLL_MS        Kafka poll timeout ms        (default: 1000)
    CDC_SCAN_INTERVAL  Seconds between CDC claim polls (default: 15)
"""

import json
import os
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Optional

sys.path.insert(0, os.path.dirname(__file__))
from common import api_get, api_post, get_configs, open_oracle, WORKER_ID

# ── Tunables ──────────────────────────────────────────────────────────────────

BULK_BATCH_SIZE    = int(os.environ.get("BULK_BATCH_SIZE",    5_000))
BULK_POLL_INTERVAL = int(os.environ.get("BULK_POLL_INTERVAL", 5))
CDC_BATCH_SIZE     = int(os.environ.get("CDC_BATCH_SIZE",     500))
CDC_CHECKIN_SEC    = int(os.environ.get("CDC_CHECKIN_SEC",    30))
CDC_POLL_MS        = int(os.environ.get("CDC_POLL_MS",        1_000))
CDC_SCAN_INTERVAL  = int(os.environ.get("CDC_SCAN_INTERVAL",  15))


# ══════════════════════════════════════════════════════════════════════════════
# BULK LOADING
# ══════════════════════════════════════════════════════════════════════════════

def _build_insert(cursor_description, target_schema: str, stage_table: str) -> str:
    col_names = [d[0] for d in cursor_description]
    cols      = ", ".join(f'"{c}"' for c in col_names)
    params    = ", ".join(f":{i + 1}" for i in range(len(col_names)))
    return (
        f'INSERT INTO "{target_schema.upper()}"."{stage_table.upper()}" '
        f'({cols}) VALUES ({params})'
    )


def process_bulk_chunk(chunk: dict) -> None:
    chunk_id    = chunk["chunk_id"]
    src_id      = chunk["source_connection_id"]
    dst_id      = chunk["target_connection_id"]
    src_schema  = chunk["source_schema"]
    src_table   = chunk["source_table"]
    tgt_schema  = chunk["target_schema"]
    stage       = chunk["stage_table"]
    start_scn   = int(chunk["start_scn"])
    rowid_start = chunk["rowid_start"]
    rowid_end   = chunk["rowid_end"]

    print(f"[bulk] chunk {chunk_id} ({rowid_start}..{rowid_end}) scn={start_scn}")

    configs  = get_configs(force=True)
    src_conn = open_oracle(src_id, configs)
    dst_conn = open_oracle(dst_id, configs)
    rows_loaded = 0
    insert_sql  = ""

    try:
        with src_conn.cursor() as cur:
            cur.execute(
                f'SELECT * FROM "{src_schema.upper()}"."{src_table.upper()}" '
                f'AS OF SCN :scn '
                f'WHERE ROWID BETWEEN CHARTOROWID(:start) AND CHARTOROWID(:end)',
                {"scn": start_scn, "start": rowid_start, "end": rowid_end},
            )
            insert_sql = _build_insert(cur.description, tgt_schema, stage)

            batch: list = []
            for row in cur:
                batch.append(row)
                if len(batch) >= BULK_BATCH_SIZE:
                    with dst_conn.cursor() as ic:
                        ic.executemany(insert_sql, batch)
                    dst_conn.commit()
                    rows_loaded += len(batch)
                    batch = []
                    api_post(f"/api/worker/chunks/{chunk_id}/progress",
                             {"rows_loaded": rows_loaded})
                    print(f"  → {rows_loaded} строк")

            if batch:
                with dst_conn.cursor() as ic:
                    ic.executemany(insert_sql, batch)
                dst_conn.commit()
                rows_loaded += len(batch)

        api_post(f"/api/worker/chunks/{chunk_id}/complete", {"rows_loaded": rows_loaded})
        print(f"[bulk] chunk {chunk_id} DONE — {rows_loaded} строк")

    except Exception as exc:
        err = str(exc)
        print(f"[bulk] chunk {chunk_id} FAILED: {err}")
        try:
            api_post(f"/api/worker/chunks/{chunk_id}/fail", {"error_text": err})
        except Exception:
            pass
        raise
    finally:
        for c in (src_conn, dst_conn):
            try:
                c.close()
            except Exception:
                pass


def bulk_loop() -> None:
    """Runs in the main thread: continuously claim + process bulk chunks."""
    print(f"[bulk] loop started (worker_id={WORKER_ID})")
    while True:
        try:
            resp = api_post("/api/worker/chunks/claim", {"worker_id": WORKER_ID})
            if resp is None:
                time.sleep(BULK_POLL_INTERVAL)
                continue
            process_bulk_chunk(resp)
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            print(f"[bulk] error: {exc}")
            time.sleep(BULK_POLL_INTERVAL)


# ══════════════════════════════════════════════════════════════════════════════
# CDC APPLY
# ══════════════════════════════════════════════════════════════════════════════

def _cdc_checkin(migration_id: str, consumer, rows_applied: int) -> None:
    from kafka import KafkaAdminClient

    configs   = get_configs()
    kafka_cfg = configs.get("kafka", {})
    bootstrap = [s.strip() for s in kafka_cfg.get("bootstrap_servers", "").split(",")]
    migration = api_get(f"/api/migrations/{migration_id}")

    consumer_group = migration["consumer_group"]
    total_lag      = 0

    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=5_000)
        try:
            offsets  = admin.list_consumer_group_offsets(consumer_group)
            committed = {tp: om.offset for tp, om in offsets.items() if om.offset >= 0}
            end      = consumer.end_offsets(list(committed.keys()))
            for tp, off in committed.items():
                total_lag += max(0, end.get(tp, off) - off)
        finally:
            admin.close()
    except Exception as exc:
        print(f"[cdc:{migration_id[:8]}] lag error: {exc}")

    api_post("/api/worker/cdc/checkin", {
        "migration_id": migration_id,
        "worker_id":    WORKER_ID,
        "lag":          total_lag,
        "rows_applied": rows_applied,
        "last_event_ts": datetime.now(timezone.utc).isoformat(),
    })
    print(f"[cdc:{migration_id[:8]}] checkin lag={total_lag} rows_applied={rows_applied}")

    if total_lag == 0:
        try:
            api_post(f"/api/migrations/{migration_id}/action",
                     {"action": "lag_zero", "actor_id": WORKER_ID})
        except Exception:
            pass


def _merge_upsert(conn, schema: str, table: str, row: dict, key_cols: list) -> None:
    columns  = list(row.keys())
    non_keys = [c for c in columns if c not in key_cols]
    key_conds   = " AND ".join(f't."{c}" = s."{c}"' for c in key_cols)
    src_cols    = ", ".join(f':{i + 1} "{c}"' for i, c in enumerate(columns))
    insert_cols = ", ".join(f'"{c}"' for c in columns)
    insert_vals = ", ".join(f's."{c}"' for c in columns)
    update_set  = ", ".join(f't."{c}" = s."{c}"' for c in non_keys)

    if update_set:
        sql = (
            f'MERGE INTO "{schema.upper()}"."{table.upper()}" t '
            f'USING (SELECT {src_cols} FROM DUAL) s ON ({key_conds}) '
            f'WHEN MATCHED THEN UPDATE SET {update_set} '
            f'WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})'
        )
    else:
        sql = (
            f'MERGE INTO "{schema.upper()}"."{table.upper()}" t '
            f'USING (SELECT {src_cols} FROM DUAL) s ON ({key_conds}) '
            f'WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})'
        )
    with conn.cursor() as cur:
        cur.execute(sql, list(row.values()))


def _delete_row(conn, schema: str, table: str, key_data: dict, key_cols: list) -> None:
    where  = " AND ".join(f'"{c}" = :{i + 1}' for i, c in enumerate(key_cols))
    values = [key_data.get(c) for c in key_cols]
    with conn.cursor() as cur:
        cur.execute(
            f'DELETE FROM "{schema.upper()}"."{table.upper()}" WHERE {where}',
            values,
        )


def _apply_event(conn, event: dict, target_schema: str,
                 target_table: str, key_cols: list) -> None:
    op = event["op"]
    if op in ("c", "r"):
        row = event.get("after") or {}
        if row:
            _merge_upsert(conn, target_schema, target_table, row, key_cols)
    elif op == "u":
        row = event.get("after") or {}
        if row:
            _merge_upsert(conn, target_schema, target_table, row, key_cols)
    elif op == "d":
        row = event.get("before") or {}
        if row and key_cols:
            _delete_row(conn, target_schema, target_table, row, key_cols)


def _parse_debezium(msg_value: bytes) -> Optional[dict]:
    if msg_value is None:
        return None
    try:
        doc = json.loads(msg_value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    payload = doc.get("payload")
    if not payload:
        return None
    op = payload.get("op")
    if op not in ("c", "u", "d", "r"):
        return None
    return {"op": op, "before": payload.get("before"),
            "after": payload.get("after"), "ts_ms": payload.get("ts_ms")}


def cdc_thread(migration: dict, stop_event: threading.Event) -> None:
    """
    Long-running thread: consume Kafka events for one migration and apply
    them to the target Oracle table.  Exits when stop_event is set.
    """
    try:
        from kafka import KafkaConsumer
    except ImportError:
        print("[cdc] kafka-python not installed — CDC disabled")
        return

    migration_id   = migration["migration_id"]
    target_schema  = migration["target_schema"]
    target_table   = migration["target_table"]
    source_schema  = migration["source_schema"]
    source_table   = migration["source_table"]
    topic_prefix   = migration["topic_prefix"]
    consumer_group = migration["consumer_group"]
    key_cols       = json.loads(migration.get("effective_key_columns_json") or "[]")

    topic = f"{topic_prefix}.{source_schema.upper()}.{source_table.upper()}"
    tag   = migration_id[:8]

    print(f"[cdc:{tag}] thread started topic={topic} group={consumer_group}")

    configs         = get_configs(force=True)
    kafka_cfg       = configs.get("kafka", {})
    bootstrap       = [s.strip() for s in kafka_cfg.get("bootstrap_servers", "localhost:9092").split(",")]

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=consumer_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=None,
        consumer_timeout_ms=CDC_POLL_MS,
        max_poll_records=CDC_BATCH_SIZE,
    )

    dst_conn        = open_oracle(migration["target_connection_id"], configs)
    last_checkin_ts = time.time()
    rows_applied    = 0

    try:
        while not stop_event.is_set():
            try:
                raw_msgs = consumer.poll(timeout_ms=CDC_POLL_MS)
            except Exception as exc:
                print(f"[cdc:{tag}] poll error: {exc}")
                time.sleep(5)
                continue

            for _tp, messages in raw_msgs.items():
                for msg in messages:
                    event = _parse_debezium(msg.value)
                    if event is None:
                        continue
                    try:
                        _apply_event(dst_conn, event,
                                     target_schema, target_table, key_cols)
                        rows_applied += 1
                    except Exception as exc:
                        print(f"[cdc:{tag}] apply error: {exc}")
                        raise

                dst_conn.commit()
                consumer.commit()

            # Periodic checkin / heartbeat
            if time.time() - last_checkin_ts >= CDC_CHECKIN_SEC:
                try:
                    _cdc_checkin(migration_id, consumer, rows_applied)
                except Exception as exc:
                    print(f"[cdc:{tag}] checkin error: {exc}")
                last_checkin_ts = time.time()

    except Exception as exc:
        print(f"[cdc:{tag}] fatal error: {exc}")
    finally:
        print(f"[cdc:{tag}] thread stopping")
        for obj in (dst_conn, consumer):
            try:
                obj.close()
            except Exception:
                pass


# ══════════════════════════════════════════════════════════════════════════════
# CDC MANAGER — background thread that claims CDC migrations
# ══════════════════════════════════════════════════════════════════════════════

def cdc_manager(stop_event: threading.Event) -> None:
    """
    Periodically scans for CDC migrations needing a worker and starts
    a cdc_thread for each one.  Cleans up threads that have finished.
    """
    # migration_id → (thread, stop_event)
    active: dict[str, tuple[threading.Thread, threading.Event]] = {}

    print(f"[cdc_manager] started (scan every {CDC_SCAN_INTERVAL}s)")

    while not stop_event.is_set():
        # Reap finished threads
        for mid in list(active.keys()):
            t, _ = active[mid]
            if not t.is_alive():
                print(f"[cdc_manager] thread for {mid[:8]} exited")
                del active[mid]

        # Try to claim a new CDC migration
        try:
            resp = api_post("/api/worker/cdc/claim", {"worker_id": WORKER_ID})
            if resp:  # 200 with body
                mid = resp["migration_id"]
                if mid not in active:
                    se = threading.Event()
                    t  = threading.Thread(
                        target=cdc_thread,
                        args=(resp, se),
                        name=f"cdc-{mid[:8]}",
                        daemon=True,
                    )
                    t.start()
                    active[mid] = (t, se)
                    print(f"[cdc_manager] started thread for migration {mid[:8]}")
        except Exception as exc:
            print(f"[cdc_manager] claim error: {exc}")

        time.sleep(CDC_SCAN_INTERVAL)

    # Graceful shutdown: signal all CDC threads
    for mid, (t, se) in active.items():
        se.set()
    for mid, (t, _) in active.items():
        t.join(timeout=10)
        print(f"[cdc_manager] joined thread {mid[:8]}")


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    print(f"[worker] universal worker started  worker_id={WORKER_ID}")
    print(f"[worker] bulk_batch={BULK_BATCH_SIZE}  cdc_batch={CDC_BATCH_SIZE}"
          f"  cdc_scan={CDC_SCAN_INTERVAL}s")

    main_stop = threading.Event()

    # Start CDC manager in a background daemon thread
    mgr = threading.Thread(
        target=cdc_manager,
        args=(main_stop,),
        name="cdc-manager",
        daemon=True,
    )
    mgr.start()

    try:
        bulk_loop()  # blocks; ctrl-c raises KeyboardInterrupt
    except KeyboardInterrupt:
        print("[worker] shutting down…")
        main_stop.set()
        mgr.join(timeout=15)
        print("[worker] stopped")


if __name__ == "__main__":
    main()
