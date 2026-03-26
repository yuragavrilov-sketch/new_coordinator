"""
CDC Apply Worker — reads Debezium change events from Kafka and applies
INSERT / UPDATE / DELETE to the target Oracle final table.

Usage:
    python cdc_apply_worker.py --migration-id <uuid>

Environment variables:
    API_URL          Flask backend URL (default: http://localhost:5000)
    WORKER_ID        Unique worker identifier (default: hostname:pid)
    BATCH_SIZE       Events per commit cycle (default: 500)
    CHECKIN_INTERVAL Seconds between heartbeat calls (default: 30)
    POLL_TIMEOUT_MS  Kafka poll timeout ms (default: 1000)
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

sys.path.insert(0, os.path.dirname(__file__))
from common import api_get, api_post, get_configs, open_oracle, WORKER_ID

BATCH_SIZE       = int(os.environ.get("BATCH_SIZE",        500))
CHECKIN_INTERVAL = int(os.environ.get("CHECKIN_INTERVAL",  30))
POLL_TIMEOUT_MS  = int(os.environ.get("POLL_TIMEOUT_MS",   1000))


# ---------------------------------------------------------------------------
# Debezium event parsing
# ---------------------------------------------------------------------------

def parse_event(msg_value: bytes) -> Optional[dict]:
    """
    Parse a Debezium Oracle change event.
    Returns None for schema/tombstone messages.
    """
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

    return {
        "op":     op,
        "before": payload.get("before"),
        "after":  payload.get("after"),
        "ts_ms":  payload.get("ts_ms"),
    }


# ---------------------------------------------------------------------------
# Oracle DML helpers
# ---------------------------------------------------------------------------

def _merge_upsert(conn, schema: str, table: str, row: dict,
                  key_cols: list[str]) -> None:
    """MERGE upsert: UPDATE when key exists, INSERT when not."""
    columns  = list(row.keys())
    non_keys = [c for c in columns if c not in key_cols]

    key_conds   = " AND ".join(f't."{c}" = s."{c}"' for c in key_cols)
    src_cols    = ", ".join(f':{i + 1} "{c}"' for i, c in enumerate(columns))
    insert_cols = ", ".join(f'"{c}"' for c in columns)
    insert_vals = ", ".join(f's."{c}"' for c in columns)
    update_set  = ", ".join(f't."{c}" = s."{c}"' for c in non_keys)

    if update_set:
        merge_sql = (
            f'MERGE INTO "{schema.upper()}"."{table.upper()}" t '
            f'USING (SELECT {src_cols} FROM DUAL) s '
            f'ON ({key_conds}) '
            f'WHEN MATCHED THEN UPDATE SET {update_set} '
            f'WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})'
        )
    else:
        # All columns are key columns — only INSERT when not matched
        merge_sql = (
            f'MERGE INTO "{schema.upper()}"."{table.upper()}" t '
            f'USING (SELECT {src_cols} FROM DUAL) s '
            f'ON ({key_conds}) '
            f'WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})'
        )

    values = list(row.values())
    with conn.cursor() as cur:
        cur.execute(merge_sql, values)


def _delete_row(conn, schema: str, table: str, key_data: dict,
                key_cols: list[str]) -> None:
    """DELETE a row by its key columns."""
    where  = " AND ".join(f'"{c}" = :{i + 1}' for i, c in enumerate(key_cols))
    values = [key_data.get(c) for c in key_cols]
    with conn.cursor() as cur:
        cur.execute(
            f'DELETE FROM "{schema.upper()}"."{table.upper()}" WHERE {where}',
            values,
        )


def apply_event(conn, event: dict, target_schema: str, target_table: str,
                key_cols: list[str]) -> None:
    op = event["op"]
    if op in ("c", "r"):
        # create / read(snapshot) → upsert
        row = event.get("after") or {}
        if row:
            _merge_upsert(conn, target_schema, target_table, row, key_cols)
    elif op == "u":
        # update → upsert (after state)
        row = event.get("after") or {}
        if row:
            _merge_upsert(conn, target_schema, target_table, row, key_cols)
    elif op == "d":
        # delete → delete by key from before state
        row = event.get("before") or {}
        if row and key_cols:
            _delete_row(conn, target_schema, target_table, row, key_cols)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(migration_id: str) -> None:
    try:
        from kafka import KafkaConsumer
        from kafka.structs import TopicPartition
    except ImportError:
        raise ImportError("kafka-python не установлен: pip install kafka-python")

    print(f"[cdc_apply_worker] started migration_id={migration_id} worker_id={WORKER_ID}")

    # Load migration details
    migration = api_get(f"/api/migrations/{migration_id}")

    target_schema = migration["target_schema"]
    target_table  = migration["target_table"]
    topic_prefix  = migration["topic_prefix"]
    source_schema = migration["source_schema"]
    source_table  = migration["source_table"]
    consumer_group = migration["consumer_group"]
    key_cols = json.loads(migration.get("effective_key_columns_json") or "[]")

    # Kafka topic: {topic_prefix}.{source_schema}.{source_table}
    topic = f"{topic_prefix}.{source_schema.upper()}.{source_table.upper()}".replace("#", "_")

    configs         = get_configs(force=True)
    kafka_cfg       = configs.get("kafka", {})
    bootstrap       = kafka_cfg.get("bootstrap_servers", "localhost:9092")
    bootstrap_list  = [s.strip() for s in bootstrap.split(",")]

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_list,
        group_id=consumer_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=None,   # raw bytes; we parse manually
        consumer_timeout_ms=-1,
        max_poll_records=BATCH_SIZE,
    )

    dst_conn        = open_oracle(migration["target_connection_id"], configs)
    last_checkin_ts = time.time()
    rows_applied    = 0

    try:
        while True:
            raw_msgs = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)

            for tp, messages in raw_msgs.items():
                for msg in messages:
                    event = parse_event(msg.value)
                    if event is None:
                        continue
                    try:
                        apply_event(
                            dst_conn, event,
                            target_schema, target_table,
                            key_cols,
                        )
                        rows_applied += 1
                    except Exception as exc:
                        print(f"[cdc_apply_worker] apply error: {exc} | event={event}")
                        raise

                dst_conn.commit()
                consumer.commit()

            # Periodic heartbeat + lag report
            now = time.time()
            if now - last_checkin_ts >= CHECKIN_INTERVAL:
                try:
                    _checkin(migration_id, consumer, rows_applied)
                except Exception as exc:
                    print(f"[cdc_apply_worker] checkin error: {exc}")
                last_checkin_ts = now

    except KeyboardInterrupt:
        print("[cdc_apply_worker] interrupted")
    finally:
        try:
            dst_conn.close()
        except Exception:
            pass
        try:
            consumer.close()
        except Exception:
            pass


def _checkin(migration_id: str, consumer, rows_applied: int) -> None:
    """Report heartbeat and lag to Flask API."""
    from kafka import KafkaAdminClient

    configs = get_configs()
    kafka_cfg = configs.get("kafka", {})
    bootstrap = [s.strip() for s in kafka_cfg.get("bootstrap_servers", "").split(",")]
    migration = api_get(f"/api/migrations/{migration_id}")

    consumer_group = migration["consumer_group"]
    total_lag = 0

    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=5000)
        try:
            offsets = admin.list_consumer_group_offsets(consumer_group)
            committed_map = {tp: om.offset for tp, om in offsets.items() if om.offset >= 0}
            end_offsets = consumer.end_offsets(list(committed_map.keys()))
            for tp, committed in committed_map.items():
                total_lag += max(0, end_offsets.get(tp, committed) - committed)
        finally:
            admin.close()
    except Exception as exc:
        print(f"[cdc_apply_worker] lag calculation error: {exc}")

    api_post("/api/worker/cdc/checkin", {
        "migration_id": migration_id,
        "worker_id":    WORKER_ID,
        "lag":          total_lag,
        "rows_applied": rows_applied,
        "last_event_ts": datetime.now(timezone.utc).isoformat(),
    })

    print(f"[cdc_apply_worker] checkin: lag={total_lag} rows_applied={rows_applied}")

    if total_lag == 0:
        api_post(f"/api/migrations/{migration_id}/action",
                 {"action": "lag_zero", "actor_id": WORKER_ID})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CDC Apply Worker")
    parser.add_argument("--migration-id", required=True, help="Migration UUID")
    args = parser.parse_args()
    main(args.migration_id)
