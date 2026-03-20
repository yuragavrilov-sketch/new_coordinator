"""Connector-group lifecycle management.

A connector group is a single Debezium connector that captures CDC events
for multiple tables.  One LogMiner session serves all tables in the group.
"""

import json
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


# ---------------------------------------------------------------------------
# CRUD
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
    """Delete group.  Raises if any non-terminal migrations reference it."""
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
            cur.execute("DELETE FROM connector_groups WHERE group_id = %s", (group_id,))
        conn.commit()
    finally:
        conn.close()


def update_group_status(group_id: str, status: str, error_text: str | None = None) -> None:
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE connector_groups
                SET    status = %s, error_text = %s, updated_at = NOW()
                WHERE  group_id = %s
            """, (status, error_text, group_id))
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Group members (migrations)
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
    """Collect SCHEMA.TABLE for all CDC migrations in the group."""
    migrations = get_group_migrations(group_id)
    tables = []
    for m in migrations:
        mode = (m.get("migration_mode") or "CDC").upper()
        if mode == "BULK_ONLY":
            continue
        schema = m["source_schema"].upper()
        table = m["source_table"].upper()
        entry = f"{schema}.{table}"
        if entry not in tables:
            tables.append(entry)
    return ",".join(tables)


def _build_key_columns(group_id: str) -> str:
    """Build message.key.columns for tables without PK/UK.

    Format: "SCHEMA.T1:col1,col2;SCHEMA.T2:colA,colB"
    Tables WITH PK/UK are omitted — Debezium auto-detects their keys.
    """
    migrations = get_group_migrations(group_id)
    parts = []
    for m in migrations:
        if m.get("source_pk_exists") or m.get("source_uk_exists"):
            continue
        key_cols_json = m.get("effective_key_columns_json") or "[]"
        key_cols = json.loads(key_cols_json) if isinstance(key_cols_json, str) else key_cols_json
        if key_cols:
            schema = m["source_schema"].upper()
            table = m["source_table"].upper()
            cols_csv = ",".join(c.upper() for c in key_cols)
            parts.append(f"{schema}.{table}:{cols_csv}")
    return ";".join(parts)


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

    import os
    connector_name = group["connector_name"]
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
# Connector lifecycle
# ---------------------------------------------------------------------------

def start_connector(group_id: str) -> dict:
    """Create and start the Debezium connector for a group.

    Unlike per-migration connectors, group connectors do NOT use
    log.mining.start.scn — they mine from the current redo position.
    """
    group = get_group(group_id)
    if not group:
        raise ValueError(f"Группа {group_id} не найдена")

    connector_name = group["connector_name"]

    # Already exists?
    status = debezium.get_connector_status(connector_name)
    if status not in ("NOT_FOUND",):
        update_group_status(group_id, status)
        return {"name": connector_name, "already_existed": True, "state": status}

    oracle_cfg = _oracle_cfg(group["source_connection_id"])
    if not oracle_cfg.get("host"):
        raise ValueError("Oracle source не настроен — проверьте Настройки")

    table_list = _build_table_include_list(group_id)
    key_columns = _build_key_columns(group_id)

    update_group_status(group_id, "STARTING")
    try:
        result = debezium.create_group_connector(
            connector_name=connector_name,
            topic_prefix=group["topic_prefix"],
            oracle_cfg=oracle_cfg,
            table_include_list=table_list,
            key_columns=key_columns,
        )
        update_group_status(group_id, "RUNNING")
        return result
    except Exception as exc:
        update_group_status(group_id, "FAILED", str(exc))
        raise


def stop_connector(group_id: str) -> None:
    """Stop and delete the Debezium connector for a group."""
    group = get_group(group_id)
    if not group:
        raise ValueError(f"Группа {group_id} не найдена")
    update_group_status(group_id, "STOPPING")
    try:
        debezium.delete_connector(group["connector_name"])
        update_group_status(group_id, "STOPPED")
    except Exception as exc:
        update_group_status(group_id, "FAILED", str(exc))
        raise


def refresh_connector_tables(group_id: str) -> None:
    """Re-read migrations and update Debezium table.include.list + key columns.

    Called when a new migration is added or removed from the group while
    the connector is already running.
    """
    group = get_group(group_id)
    if not group:
        raise ValueError(f"Группа {group_id} не найдена")

    status = debezium.get_connector_status(group["connector_name"])
    if status == "NOT_FOUND":
        return  # connector not running, nothing to update

    table_list = _build_table_include_list(group_id)
    key_columns = _build_key_columns(group_id)

    debezium.update_connector_tables(
        connector_name=group["connector_name"],
        table_include_list=table_list,
        key_columns=key_columns,
    )


def get_connector_status(group_id: str) -> str:
    """Return current Debezium connector status for the group."""
    group = get_group(group_id)
    if not group:
        return "NOT_FOUND"
    try:
        status = debezium.get_connector_status(group["connector_name"])
        update_group_status(group_id, status if status != "NOT_FOUND" else "STOPPED")
        return status
    except Exception:
        return "UNKNOWN"
