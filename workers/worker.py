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
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Optional

# Load .env from the workers directory (if it exists) before anything else
_HERE = Path(__file__).parent
_env_file = _HERE / ".env"
if _env_file.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_file, override=False)   # override=False: OS env takes priority
        print(f"[worker] loaded env from {_env_file}")
    except ImportError:
        # dotenv not installed — parse manually (key=value, skip comments/blanks)
        with open(_env_file) as _f:
            for _line in _f:
                _line = _line.strip()
                if not _line or _line.startswith("#") or "=" not in _line:
                    continue
                _k, _, _v = _line.partition("=")
                _k = _k.strip()
                _v = _v.strip().strip('"').strip("'")
                if _k and _k not in os.environ:   # don't override OS env
                    os.environ[_k] = _v
        print(f"[worker] loaded env from {_env_file} (manual parser)")

sys.path.insert(0, str(_HERE))
import common as db
from common import WORKER_ID

# Fetch LOBs (CLOB/BLOB/NCLOB) as Python str/bytes directly — LOB locators
# from AS OF SCN flashback cursors are invalid outside the fetching cursor,
# which causes ORA-64219.  Setting this globally is safe for bulk copy.
try:
    import oracledb
    oracledb.defaults.fetch_lobs = False
except ImportError:
    pass

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

    Uses APPEND_VALUES hint for direct-path (no undo) insert and
    safe :c0, :c1 bind names (oracledb requires names to start with a letter).
    executemany is called with a list of dicts keyed by bind_names.
    """
    col_names  = [d[0] for d in cursor_description]
    bind_names = [f"c{i}" for i in range(len(col_names))]
    cols   = ", ".join(f'"{c}"' for c in col_names)
    params = ", ".join(f":{b}" for b in bind_names)
    sql = (
        f'INSERT /*+ APPEND_VALUES */ INTO '
        f'"{target_schema.upper()}"."{stage_table.upper()}" '
        f'({cols}) VALUES ({params})'
    )
    return sql, bind_names


def _flush_batch(dst_conn, insert_sql: str, batch: list) -> None:
    """Execute one batch insert and commit."""
    with dst_conn.cursor() as ic:
        ic.executemany(insert_sql, batch)
    dst_conn.commit()


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

    try:
        with src_conn.cursor() as cur:
            # arraysize controls how many rows are fetched per network round-trip
            cur.arraysize = BULK_BATCH_SIZE
            cur.prefetchrows = BULK_BATCH_SIZE + 1

            cur.execute(
                f'SELECT * FROM "{src_schema.upper()}"."{src_table.upper()}" '
                f'AS OF SCN :p_scn '
                f'WHERE ROWID BETWEEN CHARTOROWID(:p_start) AND CHARTOROWID(:p_end)',
                {"p_scn": start_scn, "p_start": rowid_start, "p_end": rowid_end},
            )
            insert_sql, bind_names = _build_insert(cur.description, tgt_schema, stage)

            while True:
                rows = cur.fetchmany(BULK_BATCH_SIZE)
                if not rows:
                    break
                batch = [dict(zip(bind_names, row)) for row in rows]
                _flush_batch(dst_conn, insert_sql, batch)
                rows_loaded += len(batch)
                db.update_chunk_progress(pg_conn, chunk_id, rows_loaded)
                print(f"  → {rows_loaded} rows")

        db.complete_chunk(pg_conn, chunk_id, rows_loaded)
        print(f"[bulk] chunk {chunk_id} DONE — {rows_loaded} rows")

    except Exception as exc:
        err = str(exc)
        print(f"[bulk] chunk {chunk_id} FAILED: {err}")
        try:
            # ORA-01555 ("snapshot too old") is non-retriable: retrying with the
            # same SCN will always fail because undo data won't come back.
            # Mark permanently failed so we don't waste retry attempts.
            if "ORA-01555" in err:
                print(f"[bulk] chunk {chunk_id} permanent fail (ORA-01555 — undo retention too short)")
                db.fail_chunk_permanent(pg_conn, chunk_id, err)
            else:
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
    pg = db.get_pg_conn_with_retry()
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


# Fields injected by ExtractNewRecordState — not real table columns
_DEBEZIUM_META = frozenset({"__op", "__table", "__source_ts_ms", "__deleted",
                            "__db", "__schema"})

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

# Debezium / Kafka Connect logical type names for temporal fields
# (appear as "name" inside the field schema)
_LTYPE_TS_MS = frozenset({          # int64 → milliseconds since epoch → datetime
    "org.apache.kafka.connect.data.Timestamp",
    "io.debezium.time.Timestamp",
})
_LTYPE_TS_US = frozenset({          # int64 → microseconds since epoch → datetime
    "io.debezium.time.MicroTimestamp",
})
_LTYPE_TS_NS = frozenset({          # int64 → nanoseconds since epoch → datetime
    "io.debezium.time.NanoTimestamp",
})
_LTYPE_DATE = frozenset({           # int32 → days since epoch → date
    "org.apache.kafka.connect.data.Date",
    "io.debezium.time.Date",
})
_LTYPE_TIME_MS = frozenset({        # int32/int64 → time-of-day ms → skip (pass as-is)
    "org.apache.kafka.connect.data.Time",
    "io.debezium.time.Time",
})


def _build_type_map(schema: dict) -> dict:
    """
    Walk a Debezium struct schema and return {field_name: logical_type_name}
    for every field that has a logical type.  Fields without a 'name' (logical
    type) are omitted — they need no special coercion.
    """
    if not schema or schema.get("type") != "struct":
        return {}
    return {
        f["field"]: f["name"]
        for f in schema.get("fields", [])
        if f.get("field") and f.get("name")
    }


def _coerce_row(row: dict, type_map: dict) -> dict:
    """
    Convert Debezium-encoded temporal values to Python datetime / date objects
    so that oracledb accepts them for Oracle DATE / TIMESTAMP columns.
    Values that are None, or whose logical type is unknown, pass through unchanged.
    """
    if not type_map:
        return row
    out: dict = {}
    for col, val in row.items():
        ltype = type_map.get(col)
        if val is None or ltype is None:
            out[col] = val
        elif ltype in _LTYPE_TS_MS:
            out[col] = _EPOCH + timedelta(milliseconds=int(val))
        elif ltype in _LTYPE_TS_US:
            out[col] = _EPOCH + timedelta(microseconds=int(val))
        elif ltype in _LTYPE_TS_NS:
            out[col] = _EPOCH + timedelta(microseconds=int(val) // 1000)
        elif ltype in _LTYPE_DATE:
            out[col] = (_EPOCH + timedelta(days=int(val))).date()
        else:
            out[col] = val
    return out


def _parse_debezium(msg_value: bytes) -> Optional[dict]:
    """
    Parse a message produced by the ExtractNewRecordState (unwrap) transform.

    With value.converter.schemas.enable=true the wire format is:
        { "schema": {...}, "payload": { col1: v1, ..., "__op": "c"|"u"|"d"|"r",
                                        "__deleted": "true"|"false" } }

    Temporal columns (Oracle DATE / TIMESTAMP) arrive as integers
    (ms / µs / days since epoch).  _coerce_row converts them to Python
    datetime / date so that oracledb accepts them without ORA-00932.

    Delete events use delete.handling.mode=rewrite: the value payload contains
    the *before* record with __deleted=true and __op=d.
    Tombstones (null value) are skipped.
    """
    if msg_value is None:
        return None  # tombstone — skip
    try:
        envelope = json.loads(msg_value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
        return None

    # schemas.enable=true → {schema:…, payload:{…}}; =false → payload IS the root
    has_schema = isinstance(envelope, dict) and "schema" in envelope
    payload    = envelope.get("payload") if has_schema else envelope
    if not isinstance(payload, dict):
        return None

    type_map = _build_type_map(envelope.get("schema") if has_schema else {})

    op      = payload.get("__op")
    deleted = payload.get("__deleted") == "true"

    # Strip meta fields → actual table columns only
    row = _coerce_row(
        {k: v for k, v in payload.items() if k not in _DEBEZIUM_META},
        type_map,
    )

    if deleted or op == "d":
        return {"op": "d", "before": row, "after": None}
    if op in ("c", "r", "u"):
        return {"op": op, "before": None, "after": row}
    return None


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

    pg = db.get_pg_conn_with_retry()
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
