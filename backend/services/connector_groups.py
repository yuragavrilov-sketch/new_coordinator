"""Connector-group lifecycle management.

A connector group is a single Debezium connector that captures CDC events
for multiple tables.  One LogMiner session serves all tables in the group.

Tables are stored in the `group_tables` table, decoupled from migrations.
"""

import json
import os
import random
import string
import uuid

from . import debezium


# ---------------------------------------------------------------------------
# Initialisation (called once from app.py)
# ---------------------------------------------------------------------------

_state: dict = {}


def init(*, get_conn_fn, row_to_dict_fn, load_configs_fn) -> None:
    _state["get_conn"] = get_conn_fn
    _state["row_to_dict"] = row_to_dict_fn
    _state["load_configs"] = load_configs_fn


def _conn():
    return _state["get_conn"]()


def _r2d(cur, row):
    return _state["row_to_dict"](cur, row)


def _gen_run_id() -> str:
    """Short random id for each start cycle (e.g. 'a7x9b2')."""
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=6))


def _active_connector_name(group: dict) -> str:
    """Debezium connector name for the current run: base_name + '_' + run_id."""
    run_id = group.get("run_id") or ""
    base = group["connector_name"]
    return f"{base}_{run_id}" if run_id else base


def _schema_topic_name(group: dict) -> str:
    """Schema history topic for the current run."""
    return f"schema-changes.{_active_connector_name(group)}"


# ---------------------------------------------------------------------------
# CRUD — groups
# ---------------------------------------------------------------------------

def create_group(
    group_name: str,
    source_connection_id: str,
    connector_name: str,
    topic_prefix: str,
    consumer_group_prefix: str = "",
) -> dict:
    gid = str(uuid.uuid4())
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO connector_groups
                    (group_id, group_name, source_connection_id,
                     connector_name, topic_prefix, consumer_group_prefix)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING *
            """, (gid, group_name, source_connection_id,
                  connector_name, topic_prefix, consumer_group_prefix))
            row = _r2d(cur, cur.fetchone())
        conn.commit()
        return row
    finally:
        conn.close()


def get_group(group_id: str) -> dict | None:
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM connector_groups WHERE group_id = %s", (group_id,))
            row = cur.fetchone()
            return _r2d(cur, row) if row else None
    finally:
        conn.close()


def list_groups() -> list[dict]:
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM connector_groups ORDER BY created_at DESC")
            return [_r2d(cur, r) for r in cur.fetchall()]
    finally:
        conn.close()


def delete_group(group_id: str) -> None:
    """Delete group record only. Connector and topics must be stopped first."""
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM migrations
                WHERE  group_id = %s
                  AND  phase NOT IN ('DRAFT', 'COMPLETED', 'CANCELLED', 'FAILED')
            """, (group_id,))
            active = cur.fetchone()[0]
            if active:
                raise ValueError(
                    f"Нельзя удалить группу: {active} активных миграций"
                )
            # group_tables + group_state_history deleted by ON DELETE CASCADE
            cur.execute("DELETE FROM connector_groups WHERE group_id = %s", (group_id,))
        conn.commit()
    finally:
        conn.close()


def transition_group(group_id: str, to_status: str,
                     message: str | None = None,
                     error_text: str | None = None) -> None:
    """Atomically update group status and record history."""
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM connector_groups WHERE group_id = %s",
                (group_id,))
            row = cur.fetchone()
            from_status = row[0] if row else None

            cur.execute("""
                UPDATE connector_groups
                SET    status = %s, error_text = %s, updated_at = NOW()
                WHERE  group_id = %s
            """, (to_status, error_text, group_id))

            cur.execute("""
                INSERT INTO group_state_history
                    (group_id, from_status, to_status, message)
                VALUES (%s, %s, %s, %s)
            """, (group_id, from_status, to_status, message))
        conn.commit()
    finally:
        conn.close()


def get_group_history(group_id: str) -> list[dict]:
    """Return state transition history for a group."""
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, group_id, from_status, to_status, message, created_at
                FROM   group_state_history
                WHERE  group_id = %s
                ORDER BY created_at DESC
                LIMIT 50
            """, (group_id,))
            return [_r2d(cur, r) for r in cur.fetchall()]
    finally:
        conn.close()


# Backward compat alias
def update_group_status(group_id: str, status: str, error_text: str | None = None) -> None:
    transition_group(group_id, status, error_text=error_text)


# ---------------------------------------------------------------------------
# CRUD — group_tables
# ---------------------------------------------------------------------------

def add_tables(group_id: str, tables: list[dict]) -> list[dict]:
    """Add tables to a group.  Returns inserted rows."""
    group = get_group(group_id)
    if not group:
        raise ValueError(f"Группа {group_id} не найдена")

    topic_prefix = group["topic_prefix"]
    conn = _conn()
    try:
        rows = []
        with conn.cursor() as cur:
            for t in tables:
                tid = str(uuid.uuid4())
                src_schema = t["source_schema"]
                src_table = t["source_table"]
                tgt_schema = t.get("target_schema", src_schema)
                tgt_table = t.get("target_table", src_table)
                ekt = t.get("effective_key_type", "NONE")
                ekc = json.dumps(t.get("effective_key_columns", []))
                pk = t.get("source_pk_exists", False)
                uk = t.get("source_uk_exists", False)
                topic = f"{topic_prefix}.{src_schema}.{src_table}".upper()

                cur.execute("""
                    INSERT INTO group_tables
                        (id, group_id, source_schema, source_table,
                         target_schema, target_table,
                         effective_key_type, effective_key_columns_json,
                         source_pk_exists, source_uk_exists, topic_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (group_id, source_schema, source_table) DO NOTHING
                    RETURNING *
                """, (tid, group_id, src_schema, src_table,
                      tgt_schema, tgt_table,
                      ekt, ekc, pk, uk, topic))
                row = cur.fetchone()
                if row:
                    rows.append(_r2d(cur, row))
        conn.commit()
        return rows
    finally:
        conn.close()


def get_group_tables(group_id: str) -> list[dict]:
    """Return all tables in a group."""
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM group_tables
                WHERE  group_id = %s
                ORDER BY source_schema, source_table
            """, (group_id,))
            return [_r2d(cur, r) for r in cur.fetchall()]
    finally:
        conn.close()


def remove_table(group_id: str, source_schema: str, source_table: str) -> None:
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM group_tables
                WHERE group_id = %s AND source_schema = %s AND source_table = %s
            """, (group_id, source_schema, source_table))
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Legacy: group members via migrations (for backward compat)
# ---------------------------------------------------------------------------

def get_group_migrations(group_id: str) -> list[dict]:
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM migrations
                WHERE  group_id = %s
                ORDER BY created_at
            """, (group_id,))
            return [_r2d(cur, r) for r in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Debezium connector helpers
# ---------------------------------------------------------------------------

def _build_table_include_list(group_id: str) -> str:
    """Collect SCHEMA.TABLE from group_tables."""
    tables = get_group_tables(group_id)
    entries = []
    for t in tables:
        entry = f"{t['source_schema'].upper()}.{t['source_table'].upper()}"
        if entry not in entries:
            entries.append(entry)
    return ",".join(entries)


def _build_key_columns(group_id: str) -> str:
    """Build message.key.columns for tables without PK/UK.

    Format: "SCHEMA.T1:col1,col2;SCHEMA.T2:colA,colB"
    Tables WITH PK/UK are omitted — Debezium auto-detects their keys.
    """
    tables = get_group_tables(group_id)
    parts = []
    for t in tables:
        if t.get("source_pk_exists") or t.get("source_uk_exists"):
            continue
        key_cols_json = t.get("effective_key_columns_json") or "[]"
        key_cols = json.loads(key_cols_json) if isinstance(key_cols_json, str) else key_cols_json
        if key_cols:
            schema = t["source_schema"].upper()
            table = t["source_table"].upper()
            cols_csv = ",".join(c.upper() for c in key_cols)
            parts.append(f"{schema}.{table}:{cols_csv}")
    return ";".join(parts)


def _build_topic_names(group_id: str) -> list[str]:
    """Return list of topic names for all tables in the group."""
    tables = get_group_tables(group_id)
    return [t["topic_name"] for t in tables if t.get("topic_name")]


def _oracle_cfg(source_connection_id: str) -> dict:
    configs = _state["load_configs"]()
    return configs.get(source_connection_id, {})


# ---------------------------------------------------------------------------
# Config preview (no side-effects)
# ---------------------------------------------------------------------------

def build_connector_config(group_id: str) -> dict:
    """Build the Debezium connector config JSON without sending it anywhere."""
    group = get_group(group_id)
    if not group:
        raise ValueError(f"Группа {group_id} не найдена")

    oracle_cfg = _oracle_cfg(group["source_connection_id"])
    table_list = _build_table_include_list(group_id)
    key_columns = _build_key_columns(group_id)

    connector_name = _active_connector_name(group)
    topic_prefix = group["topic_prefix"]
    bootstrap = ""
    try:
        from . import debezium as _deb
        bootstrap = _deb._kafka_bootstrap()
    except Exception:
        bootstrap = "(not configured)"

    lob_enabled = os.getenv("DEBEZIUM_LOB_ENABLED", "true").lower() == "true"

    config = {
        "connector.class":              "io.debezium.connector.oracle.OracleConnector",
        "tasks.max":                    "1",
        "snapshot.mode":                "no_data",
        "snapshot.locking.mode":        "none",
        "log.mining.strategy":          "online_catalog",
        "database.connection.adapter":  "logminer",
        "log.mining.continuous.mine":   "true",
        "heartbeat.interval.ms":        "30000",

        "topic.prefix":      topic_prefix,
        "database.hostname": oracle_cfg.get("host", ""),
        "database.port":     str(oracle_cfg.get("port", 1521)),
        "database.user":     oracle_cfg.get("user", ""),
        "database.password": "********",
        "database.dbname":   oracle_cfg.get("service_name", ""),

        "table.include.list": table_list,

        "topic.creation.default.replication.factor": os.getenv("DEBEZIUM_TOPIC_REPLICATION_FACTOR", "1"),
        "topic.creation.default.partitions":         os.getenv("DEBEZIUM_TOPIC_PARTITIONS", "1"),
        "topic.creation.default.cleanup.policy":     os.getenv("DEBEZIUM_TOPIC_CLEANUP_POLICY", "delete"),
        "topic.creation.default.retention.ms":       os.getenv("DEBEZIUM_TOPIC_RETENTION_MS", "604800000"),
        "topic.creation.default.compression.type":   os.getenv("DEBEZIUM_TOPIC_COMPRESSION_TYPE", "snappy"),

        "lob.enabled":           "true" if lob_enabled else "false",
        "lob.fetch.size":        os.getenv("DEBEZIUM_LOB_FETCH_SIZE", "0"),
        "lob.fetch.buffer.size": os.getenv("DEBEZIUM_LOB_FETCH_BUFFER_SIZE", "0"),

        "schema.history.internal.kafka.bootstrap.servers": bootstrap,
        "schema.history.internal.kafka.topic":             f"schema-changes.{connector_name}",
        "include.schema.changes": "false",

        "key.converter":                 "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable":  "true",
        "value.converter":               "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "true",

        "decimal.handling.mode": "double",
        "time.precision.mode":   "connect",

        "provide.transaction.metadata": "false",
        "tombstones.on.delete":          "true",
        "skipped.operations":            "none",

        "transforms":                              "unwrap,route",
        "transforms.unwrap.type":                  "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.delete.handling.mode":  "rewrite",
        "transforms.unwrap.add.fields":            "op,table,source.ts_ms",
        "transforms.unwrap.add.fields.prefix":     "__",
        "transforms.route.type":                   "io.debezium.transforms.ByLogicalTableRouter",
        "transforms.route.topic.regex":            f"({topic_prefix}\\..*)",
        "transforms.route.topic.replacement":      "$1",
    }

    if key_columns:
        config["message.key.columns"] = key_columns

    return {"name": connector_name, "config": config}


# ---------------------------------------------------------------------------
# Kafka topics — pre-create and monitor
# ---------------------------------------------------------------------------

def create_group_topics(group_id: str) -> list[dict]:
    """Pre-create Kafka topics for all tables in the group.
    Returns list of {topic_name, status} dicts.
    """
    from . import kafka_topics

    topics = _build_topic_names(group_id)
    results = []
    for topic in topics:
        try:
            kafka_topics.create_topic(topic_name=topic)
            results.append({"topic_name": topic, "status": "ok"})
        except Exception as exc:
            results.append({"topic_name": topic, "status": "error", "error": str(exc)})
    return results


def _delete_group_topics(group: dict) -> None:
    """Delete all Kafka topics for a group: data topics + schema-changes topic."""
    from . import kafka_topics

    group_id = group["group_id"]
    # Data topics
    topics = _build_topic_names(group_id)
    for topic in topics:
        try:
            kafka_topics.delete_topic(topic_name=topic)
        except Exception as exc:
            print(f"[connector_groups] failed to delete topic {topic}: {exc}")

    # Schema-changes topic
    schema_topic = _schema_topic_name(group)
    try:
        kafka_topics.delete_topic(topic_name=schema_topic)
    except Exception as exc:
        print(f"[connector_groups] failed to delete schema topic {schema_topic}: {exc}")


def get_topic_message_counts(group_id: str) -> list[dict]:
    """Return message counts for each topic in the group."""
    from kafka import KafkaConsumer
    from kafka.structs import TopicPartition

    topics = _build_topic_names(group_id)
    if not topics:
        return []

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",")

    results = []
    consumer = None
    try:
        consumer = KafkaConsumer(bootstrap_servers=bootstrap)
        for topic_name in topics:
            try:
                partitions = consumer.partitions_for_topic(topic_name)
                if partitions is None:
                    results.append({"topic_name": topic_name, "count": -1, "exists": False})
                    continue
                tps = [TopicPartition(topic_name, p) for p in partitions]
                end_offsets = consumer.end_offsets(tps)
                begin_offsets = consumer.beginning_offsets(tps)
                total = sum(end_offsets[tp] - begin_offsets[tp] for tp in tps)
                results.append({"topic_name": topic_name, "count": total, "exists": True})
            except Exception:
                results.append({"topic_name": topic_name, "count": -1, "exists": False})
    except Exception:
        for topic_name in topics:
            results.append({"topic_name": topic_name, "count": -1, "exists": False})
    finally:
        if consumer:
            consumer.close()
    return results


# ---------------------------------------------------------------------------
# Connector lifecycle
# ---------------------------------------------------------------------------

def request_start(group_id: str) -> dict:
    """Validate and initiate the async start lifecycle.

    Generates a unique run_id for this start cycle so that connector name
    and schema-changes topic don't collide with previous runs.
    Sets group to TOPICS_CREATING — the orchestrator picks it up from there.
    """
    group = get_group(group_id)
    if not group:
        raise ValueError(f"Группа {group_id} не найдена")

    table_list = _build_table_include_list(group_id)
    if not table_list:
        raise ValueError("В группе нет таблиц — добавьте таблицы перед запуском")

    oracle_cfg = _oracle_cfg(group["source_connection_id"])
    if not oracle_cfg.get("host"):
        raise ValueError("Oracle source не настроен — проверьте Настройки")

    # Generate unique run_id for this start cycle
    run_id = _gen_run_id()
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE connector_groups SET run_id = %s, updated_at = NOW()
                WHERE group_id = %s
            """, (run_id, group_id))
        conn.commit()
    finally:
        conn.close()

    transition_group(group_id, "TOPICS_CREATING",
                     f"Запуск инициирован (run_id={run_id})")
    return {"group_id": group_id, "status": "TOPICS_CREATING", "run_id": run_id}


def do_create_topics(group_id: str) -> list[dict]:
    """Phase handler: create Kafka topics for group tables."""
    return create_group_topics(group_id)


def do_start_connector(group_id: str) -> dict:
    """Phase handler: create Debezium connector with run_id in name."""
    group = get_group(group_id)
    if not group:
        raise ValueError(f"Группа {group_id} не найдена")

    connector_name = _active_connector_name(group)
    oracle_cfg = _oracle_cfg(group["source_connection_id"])
    table_list = _build_table_include_list(group_id)
    key_columns = _build_key_columns(group_id)

    return debezium.create_group_connector(
        connector_name=connector_name,
        topic_prefix=group["topic_prefix"],
        oracle_cfg=oracle_cfg,
        table_include_list=table_list,
        key_columns=key_columns,
    )


def request_stop(group_id: str) -> None:
    """Initiate async stop — orchestrator handles the actual stop."""
    group = get_group(group_id)
    if not group:
        raise ValueError(f"Группа {group_id} не найдена")
    transition_group(group_id, "STOPPING", "Остановка инициирована пользователем")


def do_stop_connector(group_id: str) -> None:
    """Phase handler: delete Debezium connector + all Kafka topics."""
    group = get_group(group_id)
    if not group:
        raise ValueError(f"Группа {group_id} не найдена")

    connector_name = _active_connector_name(group)

    # 1. Delete Debezium connector
    debezium.delete_connector(connector_name)

    # 2. Delete all data topics + schema-changes topic
    _delete_group_topics(group)


def refresh_connector_tables(group_id: str) -> None:
    """Re-read group_tables and update Debezium table.include.list + key columns.

    Called when tables are added/removed from the group while
    the connector is already running.
    """
    group = get_group(group_id)
    if not group:
        raise ValueError(f"Группа {group_id} не найдена")

    connector_name = _active_connector_name(group)
    status = debezium.get_connector_status(connector_name)
    if status == "NOT_FOUND":
        return  # connector not running, nothing to update

    table_list = _build_table_include_list(group_id)
    key_columns = _build_key_columns(group_id)

    debezium.update_connector_tables(
        connector_name=connector_name,
        table_include_list=table_list,
        key_columns=key_columns,
    )


def get_connector_status(group_id: str) -> str:
    """Return current Debezium connector status for the group."""
    group = get_group(group_id)
    if not group:
        return "NOT_FOUND"
    try:
        status = debezium.get_connector_status(_active_connector_name(group))
        update_group_status(group_id, status if status != "NOT_FOUND" else "STOPPED")
        return status
    except Exception:
        return "UNKNOWN"
