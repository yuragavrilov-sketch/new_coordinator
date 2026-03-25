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
CMP_POLL_INTERVAL  = int(os.environ.get("CMP_POLL_INTERVAL",  5))


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


def _process_bulk_chunk(chunk: dict, pg_conn, configs: dict) -> None:
    """BULK/DIRECT: read from source, write to stage or target.

    If start_scn is set (legacy per-migration connector): read AS OF SCN.
    If start_scn is NULL (group-based connector): read current data.
    """
    chunk_id    = chunk["chunk_id"]
    src_schema  = chunk["source_schema"]
    src_table   = chunk["source_table"]
    tgt_schema  = chunk["target_schema"]
    strategy    = chunk.get("migration_strategy", "STAGE")
    dest_table  = chunk["target_table"] if strategy == "DIRECT" else chunk["stage_table"]
    raw_scn     = chunk.get("start_scn")
    start_scn   = int(raw_scn) if raw_scn else None
    rowid_start = chunk["rowid_start"]
    rowid_end   = chunk["rowid_end"]

    src_conn = db.open_oracle(chunk["source_connection_id"], configs)
    dst_conn = db.open_oracle(chunk["target_connection_id"], configs)
    rows_loaded = 0
    try:
        with src_conn.cursor() as cur:
            cur.arraysize = BULK_BATCH_SIZE
            cur.prefetchrows = BULK_BATCH_SIZE + 1
            if start_scn:
                # Legacy mode: consistent snapshot via flashback query
                cur.execute(
                    f'SELECT * FROM "{src_schema.upper()}"."{src_table.upper()}" '
                    f'AS OF SCN :p_scn '
                    f'WHERE ROWID BETWEEN CHARTOROWID(:p_start) AND CHARTOROWID(:p_end)',
                    {"p_scn": start_scn, "p_start": rowid_start, "p_end": rowid_end},
                )
            else:
                # Group mode: read current data (no flashback, CDC handles consistency)
                cur.execute(
                    f'SELECT * FROM "{src_schema.upper()}"."{src_table.upper()}" '
                    f'WHERE ROWID BETWEEN CHARTOROWID(:p_start) AND CHARTOROWID(:p_end)',
                    {"p_start": rowid_start, "p_end": rowid_end},
                )
            insert_sql, bind_names = _build_insert(cur.description, tgt_schema, dest_table)
            while True:
                rows = cur.fetchmany(BULK_BATCH_SIZE)
                if not rows:
                    break
                batch = [dict(zip(bind_names, row)) for row in rows]
                _flush_batch(dst_conn, insert_sql, batch)
                rows_loaded += len(batch)
                db.update_chunk_progress(pg_conn, chunk_id, rows_loaded)
                print(f"  → {rows_loaded} rows")
    finally:
        for c in (src_conn, dst_conn):
            try: c.close()
            except Exception: pass
    return rows_loaded


def _process_baseline_chunk(chunk: dict, pg_conn, configs: dict) -> int:
    """BASELINE: INSERT INTO target SELECT * FROM stage WHERE ROWID BETWEEN ... (no SCN).

    Oracle commit and PG progress update are intentionally separated:
    if PG fails after Oracle commits, the exception propagates without
    attempting to rollback an already-committed Oracle transaction.
    If the chunk is retried after a partial commit, ORA-00001 is caught
    and treated as idempotent success (rows already present from prior attempt).
    """
    chunk_id    = chunk["chunk_id"]
    tgt_schema  = chunk["target_schema"]
    tgt_table   = chunk["target_table"]
    stg_table   = chunk["stage_table"]
    rowid_start = chunk["rowid_start"]
    rowid_end   = chunk["rowid_end"]

    tgt = f'"{tgt_schema.upper()}"."{tgt_table.upper()}"'
    stg = f'"{tgt_schema.upper()}"."{stg_table.upper()}"'

    dst_conn = db.open_oracle(chunk["target_connection_id"], configs)
    rows_loaded = 0
    try:
        with dst_conn.cursor() as cur:
            # Enable parallel DML for this session so the PARALLEL hint is honoured.
            cur.execute("ALTER SESSION ENABLE PARALLEL DML")
            # APPEND   — direct-path insert (skips buffer cache, skips redo when
            #            the table is in NOLOGGING mode set by baseline_publishing).
            # PARALLEL — Oracle may spawn parallel servers for both the INSERT and
            #            the SELECT scan of the stage table.
            cur.execute(
                f'INSERT /*+ APPEND PARALLEL(tgt, DEFAULT) */ INTO {tgt} tgt '
                f'SELECT /*+ PARALLEL(stg, DEFAULT) */ * FROM {stg} stg '
                f'WHERE stg.ROWID BETWEEN CHARTOROWID(:rs) AND CHARTOROWID(:re)',
                {"rs": rowid_start, "re": rowid_end},
            )
            rows_loaded = cur.rowcount if cur.rowcount >= 0 else 0
        dst_conn.commit()
    except Exception as exc:
        if "ORA-00001" in str(exc):
            # Rows already committed by a previous attempt — count from stage and treat as done
            try:
                with dst_conn.cursor() as cur:
                    cur.execute(
                        f'SELECT COUNT(*) FROM {stg} stg '
                        f'WHERE stg.ROWID BETWEEN CHARTOROWID(:rs) AND CHARTOROWID(:re)',
                        {"rs": rowid_start, "re": rowid_end},
                    )
                    rows_loaded = cur.fetchone()[0]
                print(f"[worker] chunk {chunk_id} ORA-00001 — already committed, "
                      f"treating as done ({rows_loaded} rows)")
            except Exception:
                rows_loaded = 0
        else:
            try: dst_conn.rollback()
            except Exception: pass
            raise
    finally:
        try: dst_conn.close()
        except Exception: pass

    # Oracle committed — update PG outside try/except so a PG failure here
    # does NOT trigger rollback of the already-committed Oracle transaction.
    db.update_chunk_progress(pg_conn, chunk_id, rows_loaded)
    return rows_loaded


def process_chunk(chunk: dict, pg_conn, configs: dict) -> None:
    chunk_id   = chunk["chunk_id"]
    chunk_type = chunk.get("chunk_type", "BULK")

    print(f"[worker] chunk {chunk_id} seq={chunk['chunk_seq']}"
          f" type={chunk_type} ({chunk['rowid_start']}..{chunk['rowid_end']})")

    try:
        if chunk_type == "BASELINE":
            rows_loaded = _process_baseline_chunk(chunk, pg_conn, configs)
        else:
            rows_loaded = _process_bulk_chunk(chunk, pg_conn, configs)

        db.complete_chunk(pg_conn, chunk_id, rows_loaded)
        print(f"[worker] chunk {chunk_id} DONE — {rows_loaded} rows")

    except Exception as exc:
        err = str(exc)
        print(f"[worker] chunk {chunk_id} FAILED: {err}")
        try:
            if "ORA-01555" in err:
                print(f"[worker] chunk {chunk_id} permanent fail (ORA-01555)")
                db.fail_chunk_permanent(pg_conn, chunk_id, err)
            else:
                db.fail_chunk(pg_conn, chunk_id, err)
        except Exception:
            pass
        raise


def bulk_loop() -> None:
    """Main thread: continuously claim + process bulk/baseline chunks."""
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
                process_chunk(chunk, pg, configs)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                print(f"[bulk] error: {exc}")
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
# DATA COMPARISON
# ══════════════════════════════════════════════════════════════════════════════

# Column types to skip in hash computation (must match routes/data_compare.py)
_CMP_SKIP_TYPES = frozenset({
    "BLOB", "CLOB", "NCLOB", "BFILE", "LONG", "LONG RAW",
    "XMLTYPE", "SDO_GEOMETRY", "ANYDATA", "URITYPE",
})


def _cmp_col_expr(col_name: str, col_type: str) -> str:
    q = f'"{col_name}"'
    if col_type == "DATE":
        return f"NVL(TO_CHAR({q}, 'YYYY-MM-DD HH24:MI:SS'), CHR(0))"
    if col_type.startswith("TIMESTAMP"):
        return f"NVL(TO_CHAR({q}, 'YYYY-MM-DD HH24:MI:SS.FF6'), CHR(0))"
    return f"NVL(TO_CHAR({q}), CHR(0))"


def _get_comparable_columns(conn, schema: str, table: str) -> list:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM   all_tab_columns
            WHERE  owner = :s AND table_name = :t
            ORDER BY column_id
        """, {"s": schema, "t": table})
        return [
            {"name": r[0], "data_type": r[1]}
            for r in cur.fetchall()
            if r[1] not in _CMP_SKIP_TYPES
        ]


def process_compare_chunk(chunk: dict, pg_conn, configs: dict) -> None:
    """Process one data-compare chunk: COUNT(*) + SUM(hash) for a ROWID range."""
    chunk_id    = chunk["chunk_id"]
    side        = chunk["side"]
    schema      = chunk["schema"]
    table       = chunk["table"]
    rowid_start = chunk["rowid_start"]
    rowid_end   = chunk["rowid_end"]

    print(f"[compare] chunk {chunk_id[:8]} side={side} seq={chunk['chunk_seq']}"
          f" ({rowid_start}..{rowid_end})")

    try:
        ora_conn = db.open_oracle(chunk["connection_id"], configs)
        try:
            columns = _get_comparable_columns(ora_conn, schema, table)
            hash_parts = [f"ORA_HASH({_cmp_col_expr(c['name'], c['data_type'])})"
                          for c in columns]
            row_hash = " + ".join(hash_parts) if hash_parts else "0"

            sql = (
                f'SELECT COUNT(*) AS cnt, SUM({row_hash}) AS hash_sum '
                f'FROM "{schema}"."{table}" '
                f'WHERE ROWID BETWEEN CHARTOROWID(:rs) AND CHARTOROWID(:re)'
            )
            with ora_conn.cursor() as cur:
                cur.execute(sql, {"rs": rowid_start, "re": rowid_end})
                row_count, hash_sum = cur.fetchone()
        finally:
            try:
                ora_conn.close()
            except Exception:
                pass

        task_id = db.complete_compare_chunk(pg_conn, chunk_id, row_count or 0, hash_sum)
        print(f"[compare] chunk {chunk_id[:8]} DONE — {row_count} rows")

        # Try to aggregate (check if all chunks are done)
        if task_id:
            _try_aggregate_from_worker(task_id, pg_conn)

    except Exception as exc:
        err = str(exc)
        print(f"[compare] chunk {chunk_id[:8]} FAILED: {err}")
        try:
            db.fail_compare_chunk(pg_conn, chunk_id, err)
        except Exception:
            pass
        raise


def _try_aggregate_from_worker(task_id: str, pg_conn) -> None:
    """Worker-side aggregation: check if all chunks done and finalize task."""
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT status, chunks_total FROM data_compare_tasks "
                "WHERE task_id = %s FOR UPDATE", (task_id,))
            row = cur.fetchone()
            if not row or row[0] != 'RUNNING':
                pg_conn.rollback()
                return

            # Count statuses
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'DONE')    AS done,
                    COUNT(*) FILTER (WHERE status = 'FAILED')  AS failed,
                    COUNT(*) FILTER (WHERE status IN ('PENDING', 'CLAIMED')) AS active
                FROM data_compare_chunks
                WHERE task_id = %s
            """, (task_id,))
            done, failed, active = cur.fetchone()

            cur.execute(
                "UPDATE data_compare_tasks SET chunks_done = %s WHERE task_id = %s",
                (done, task_id))

            if active > 0:
                pg_conn.commit()
                return

            if failed > 0:
                cur.execute("""
                    UPDATE data_compare_tasks
                    SET    status = 'FAILED', error_text = %s, completed_at = NOW()
                    WHERE  task_id = %s
                """, (f"{failed} chunk(s) failed", task_id))
                pg_conn.commit()
                return

            # All done — aggregate per side
            cur.execute("""
                SELECT side, SUM(COALESCE(row_count, 0)), SUM(COALESCE(hash_sum, 0))
                FROM   data_compare_chunks
                WHERE  task_id = %s AND status = 'DONE'
                GROUP BY side
            """, (task_id,))
            side_data = {}
            for side, rc, hs in cur.fetchall():
                side_data[side] = {"count": int(rc), "hash": hs}

            src = side_data.get("source", {"count": 0, "hash": 0})
            tgt = side_data.get("target", {"count": 0, "hash": 0})

            counts_match = src["count"] == tgt["count"]
            hash_match = src["hash"] == tgt["hash"]

            cur.execute("""
                UPDATE data_compare_tasks
                SET    status = 'DONE',
                       source_count = %s, target_count = %s,
                       source_hash  = %s, target_hash  = %s,
                       counts_match = %s, hash_match   = %s,
                       chunks_done  = %s,
                       completed_at = NOW()
                WHERE  task_id = %s
            """, (src["count"], tgt["count"],
                  str(src["hash"]), str(tgt["hash"]),
                  counts_match, hash_match, done, task_id))
        pg_conn.commit()

        print(f"[compare] task {task_id[:8]} DONE: "
              f"src={src['count']} tgt={tgt['count']} "
              f"counts={'OK' if counts_match else 'MISMATCH'} "
              f"hash={'OK' if hash_match else 'MISMATCH'}")

    except Exception as exc:
        print(f"[compare] aggregate error: {exc}")
        try:
            pg_conn.rollback()
        except Exception:
            pass


def compare_loop(stop_event: threading.Event) -> None:
    """Background thread: continuously claim + process data-compare chunks."""
    print(f"[compare] loop started (worker_id={WORKER_ID})")
    pg = db.get_pg_conn_with_retry()
    try:
        while not stop_event.is_set():
            try:
                chunk = db.claim_compare_chunk(pg)
                if chunk is None:
                    time.sleep(CMP_POLL_INTERVAL)
                    continue
                configs = db.load_configs(pg)
                process_compare_chunk(chunk, pg, configs)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                print(f"[compare] error: {exc}")
                try:
                    pg.close()
                except Exception:
                    pass
                pg = db.get_pg_conn()
                time.sleep(CMP_POLL_INTERVAL)
    finally:
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

    cmp = threading.Thread(
        target=compare_loop, args=(main_stop,),
        name="compare-loop", daemon=True,
    )
    cmp.start()

    try:
        bulk_loop()
    except KeyboardInterrupt:
        print("[worker] shutting down…")
        main_stop.set()
        mgr.join(timeout=15)
        cmp.join(timeout=10)
        print("[worker] stopped")


if __name__ == "__main__":
    main()
