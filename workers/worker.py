"""
Universal Worker — bulk loading + CDC apply, both via direct PostgreSQL access.

No HTTP calls to Flask.  Workers read configs and job queue straight from the
state DB using SELECT … FOR UPDATE SKIP LOCKED.

Usage:
    python worker.py

Environment variables (see .env.example):
    STATE_DB_DSN       PostgreSQL DSN           (default: postgres://postgres:postgres@localhost:5432/migration_state)
    WORKER_ID          Unique identifier        (default: hostname:pid)
    BULK_BATCH_SIZE    Rows per INSERT batch    (default: 5000)
    BULK_POLL_INTERVAL Seconds between polls    (default: 5)
    CDC_BATCH_SIZE     Kafka records per cycle  (default: 500)
    CDC_CHECKIN_SEC    Seconds between checkins (default: 30)
    CDC_POLL_MS        Kafka poll timeout ms    (default: 1000)
    CDC_SCAN_INTERVAL  Seconds between CDC scans (default: 15)
"""

import json
import os
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Optional

sys.path.insert(0, os.path.dirname(__file__))
import common as db
from common import WORKER_ID

BULK_BATCH_SIZE    = int(os.environ.get("BULK_BATCH_SIZE",    5_000))
BULK_POLL_INTERVAL = int(os.environ.get("BULK_POLL_INTERVAL", 5))
CDC_BATCH_SIZE     = int(os.environ.get("CDC_BATCH_SIZE",     500))
CDC_CHECKIN_SEC    = int(os.environ.get("CDC_CHECKIN_SEC",    30))
CDC_POLL_MS        = int(os.environ.get("CDC_POLL_MS",        1_000))
CDC_SCAN_INTERVAL  = int(os.environ.get("CDC_SCAN_INTERVAL",  15))


# ══════════════════════════════════════════════════════════════════════════════
# BULK LOADING
# ══════════════════════════════════════════════════════════════════════════════

def _build_insert(cursor_description, target_schema: str,
                  stage_table: str) -> tuple:
    """
    Returns (sql, bind_names).

    Bind variable names are :c0, :c1, … — safe prefixed form that avoids
    ORA-01745 (names must start with a letter) and reserved-word collisions.
    executemany must be called with a list of dicts keyed by bind_names.
    """
    col_names  = [d[0] for d in cursor_description]
    bind_names = [f"c{i}" for i in range(len(col_names))]
    cols   = ", ".join(f'"{c}"' for c in col_names)
    params = ", ".join(f":{b}" for b in bind_names)
    sql = (
        f'INSERT INTO "{target_schema.upper()}"."{stage_table.upper()}" '
        f'({cols}) VALUES ({params})'
    )
    return sql, bind_names


def process_bulk_chunk(chunk: dict, pg_conn, configs: dict) -> None:
    chunk_id    = chunk["chunk_id"]
    src_schema  = chunk["source_schema"]
    src_table   = chunk["source_table"]
    tgt_schema  = chunk["target_schema"]
    stage       = chunk["stage_table"]
    start_scn   = int(chunk["start_scn"])
    rowid_start = chunk["rowid_start"]
    rowid_end   = chunk["rowid_end"]

    print(f"[bulk] chunk {chunk_id} seq={chunk['chunk_seq']}"
          f" ({rowid_start}..{rowid_end}) scn={start_scn}")

    src_conn = db.open_oracle(chunk["source_connection_id"], configs)
    dst_conn = db.open_oracle(chunk["target_connection_id"], configs)
    rows_loaded = 0
    insert_sql  = ""
    bind_names: list = []

    try:
        with src_conn.cursor() as cur:
            cur.execute(
                f'SELECT * FROM "{src_schema.upper()}"."{src_table.upper()}" '
                f'AS OF SCN :scn '
                f'WHERE ROWID BETWEEN CHARTOROWID(:start) AND CHARTOROWID(:end)',
                {"scn": start_scn, "start": rowid_start, "end": rowid_end},
            )
            insert_sql, bind_names = _build_insert(cur.description, tgt_schema, stage)

            batch: list = []
            for row in cur:
                batch.append(dict(zip(bind_names, row)))
                if len(batch) >= BULK_BATCH_SIZE:
                    with dst_conn.cursor() as ic:
                        ic.executemany(insert_sql, batch)
                    dst_conn.commit()
                    rows_loaded += len(batch)
                    batch = []
                    db.update_chunk_progress(pg_conn, chunk_id, rows_loaded)
                    print(f"  → {rows_loaded} rows")

            if batch:
                with dst_conn.cursor() as ic:
                    ic.executemany(insert_sql, batch)
                dst_conn.commit()
                rows_loaded += len(batch)

        db.complete_chunk(pg_conn, chunk_id, rows_loaded)
        print(f"[bulk] chunk {chunk_id} DONE — {rows_loaded} rows")

    except Exception as exc:
        err = str(exc)
        print(f"[bulk] chunk {chunk_id} FAILED: {err}")
        try:
            db.fail_chunk(pg_conn, chunk_id, err)
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
    """Main thread: continuously claim + process bulk chunks."""
    print(f"[bulk] loop started (worker_id={WORKER_ID})")
    pg = db.get_pg_conn()
    try:
        while True:
            try:
                chunk = db.claim_chunk(pg)
                if chunk is None:
                    time.sleep(BULK_POLL_INTERVAL)
                    continue
                configs = db.load_configs(pg)
                process_bulk_chunk(chunk, pg, configs)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                print(f"[bulk] error: {exc}")
                # Reconnect on connection errors
                try:
                    pg.close()
                except Exception:
                    pass
                pg = db.get_pg_conn()
                time.sleep(BULK_POLL_INTERVAL)
    finally:
        try:
            pg.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# CDC APPLY
# ══════════════════════════════════════════════════════════════════════════════

def _calc_lag(consumer, consumer_group: str, bootstrap: list) -> int:
    from kafka import KafkaAdminClient
    total_lag = 0
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=5_000)
        try:
            offsets   = admin.list_consumer_group_offsets(consumer_group)
            committed = {tp: om.offset for tp, om in offsets.items() if om.offset >= 0}
            end       = consumer.end_offsets(list(committed.keys()))
            for tp, off in committed.items():
                total_lag += max(0, end.get(tp, off) - off)
        finally:
            admin.close()
    except Exception as exc:
        print(f"[cdc] lag error: {exc}")
    return total_lag


def _merge_upsert(conn, schema: str, table: str, row: dict, key_cols: list) -> None:
    columns  = list(row.keys())
    non_keys = [c for c in columns if c not in key_cols]
    key_conds   = " AND ".join(f't."{c}" = s."{c}"' for c in key_cols)
    src_cols    = ", ".join(f':{i + 1} "{c}"' for i, c in enumerate(columns))
    insert_cols = ", ".join(f'"{c}"' for c in columns)
    insert_vals = ", ".join(f's."{c}"' for c in columns)
    update_set  = ", ".join(f't."{c}" = s."{c}"' for c in non_keys)

    sql = (
        f'MERGE INTO "{schema.upper()}"."{table.upper()}" t '
        f'USING (SELECT {src_cols} FROM DUAL) s ON ({key_conds}) '
        + (f'WHEN MATCHED THEN UPDATE SET {update_set} ' if update_set else '')
        + f'WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})'
    )
    with conn.cursor() as cur:
        cur.execute(sql, list(row.values()))


def _delete_row(conn, schema: str, table: str, key_data: dict, key_cols: list) -> None:
    where  = " AND ".join(f'"{c}" = :{i + 1}' for i, c in enumerate(key_cols))
    with conn.cursor() as cur:
        cur.execute(
            f'DELETE FROM "{schema.upper()}"."{table.upper()}" WHERE {where}',
            [key_data.get(c) for c in key_cols],
        )


def _apply_event(oracle_conn, event: dict, target_schema: str,
                 target_table: str, key_cols: list) -> None:
    op = event["op"]
    if op in ("c", "r", "u"):
        row = event.get("after") or {}
        if row:
            _merge_upsert(oracle_conn, target_schema, target_table, row, key_cols)
    elif op == "d":
        row = event.get("before") or {}
        if row and key_cols:
            _delete_row(oracle_conn, target_schema, target_table, row, key_cols)


def _parse_debezium(msg_value: bytes) -> Optional[dict]:
    if msg_value is None:
        return None
    try:
        payload = json.loads(msg_value.decode("utf-8")).get("payload")
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
        return None
    if not payload:
        return None
    op = payload.get("op")
    if op not in ("c", "u", "d", "r"):
        return None
    return {"op": op, "before": payload.get("before"), "after": payload.get("after")}


def cdc_thread(migration: dict, stop_event: threading.Event) -> None:
    """Long-running thread: apply Debezium events for one migration."""
    try:
        from kafka import KafkaConsumer
    except ImportError:
        print("[cdc] kafka-python not installed")
        return

    migration_id   = migration["migration_id"]
    target_schema  = migration["target_schema"]
    target_table   = migration["target_table"]
    source_schema  = migration["source_schema"]
    source_table   = migration["source_table"]
    topic_prefix   = migration["topic_prefix"]
    consumer_group = migration["consumer_group"]
    key_cols       = json.loads(migration.get("effective_key_columns_json") or "[]")
    topic          = f"{topic_prefix}.{source_schema.upper()}.{source_table.upper()}"
    tag            = migration_id[:8]

    print(f"[cdc:{tag}] thread started  topic={topic}  group={consumer_group}")

    pg      = db.get_pg_conn()
    configs = db.load_configs(pg)
    kafka_cfg   = configs.get("kafka") or {}
    bootstrap   = [s.strip() for s in (kafka_cfg.get("bootstrap_servers") or "localhost:9092").split(",")]

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
    oracle_conn     = db.open_oracle(migration["target_connection_id"], configs)
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
                    _apply_event(oracle_conn, event,
                                 target_schema, target_table, key_cols)
                    rows_applied += 1

                oracle_conn.commit()
                consumer.commit()

            # Periodic checkin
            if time.time() - last_checkin_ts >= CDC_CHECKIN_SEC:
                try:
                    total_lag = _calc_lag(consumer, consumer_group, bootstrap)
                    db.cdc_checkin(pg, migration_id, total_lag, rows_applied)
                    print(f"[cdc:{tag}] checkin lag={total_lag} rows={rows_applied}")
                    if total_lag == 0:
                        db.trigger_lag_zero(pg, migration_id)
                except Exception as exc:
                    print(f"[cdc:{tag}] checkin error: {exc}")
                    # Reconnect pg on error
                    try:
                        pg.close()
                    except Exception:
                        pass
                    pg = db.get_pg_conn()
                last_checkin_ts = time.time()

    except Exception as exc:
        print(f"[cdc:{tag}] fatal: {exc}")
    finally:
        print(f"[cdc:{tag}] thread stopping")
        for obj in (oracle_conn, consumer):
            try:
                obj.close()
            except Exception:
                pass
        try:
            pg.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# CDC MANAGER — background thread that claims CDC migrations
# ══════════════════════════════════════════════════════════════════════════════

def cdc_manager(stop_event: threading.Event) -> None:
    """
    Periodically scans for CDC migrations needing a worker and starts
    a cdc_thread for each one.  Reaps finished threads.
    """
    # migration_id → (thread, stop_event)
    active: dict = {}
    print(f"[cdc_manager] started (scan every {CDC_SCAN_INTERVAL}s)")

    pg = db.get_pg_conn()
    try:
        while not stop_event.is_set():
            # Reap dead threads
            for mid in list(active):
                t, _ = active[mid]
                if not t.is_alive():
                    print(f"[cdc_manager] thread {mid[:8]} exited")
                    del active[mid]

            # Claim a new CDC migration if available
            try:
                migration = db.claim_cdc_migration(pg)
                if migration:
                    mid = migration["migration_id"]
                    if mid not in active:
                        se = threading.Event()
                        t  = threading.Thread(
                            target=cdc_thread,
                            args=(migration, se),
                            name=f"cdc-{mid[:8]}",
                            daemon=True,
                        )
                        t.start()
                        active[mid] = (t, se)
                        print(f"[cdc_manager] started thread for {mid[:8]}")
            except Exception as exc:
                print(f"[cdc_manager] scan error: {exc}")
                try:
                    pg.close()
                except Exception:
                    pass
                pg = db.get_pg_conn()

            time.sleep(CDC_SCAN_INTERVAL)
    finally:
        # Signal all CDC threads to stop
        for mid, (t, se) in active.items():
            se.set()
        for mid, (t, _) in active.items():
            t.join(timeout=10)
            print(f"[cdc_manager] joined {mid[:8]}")
        try:
            pg.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    print(f"[worker] started  worker_id={WORKER_ID}")
    print(f"[worker] state_db={db.STATE_DB_DSN}")
    print(f"[worker] bulk_batch={BULK_BATCH_SIZE}  cdc_batch={CDC_BATCH_SIZE}"
          f"  cdc_scan={CDC_SCAN_INTERVAL}s")

    main_stop = threading.Event()
    mgr = threading.Thread(
        target=cdc_manager, args=(main_stop,),
        name="cdc-manager", daemon=True,
    )
    mgr.start()

    try:
        bulk_loop()
    except KeyboardInterrupt:
        print("[worker] shutting down…")
        main_stop.set()
        mgr.join(timeout=15)
        print("[worker] stopped")


if __name__ == "__main__":
    main()
