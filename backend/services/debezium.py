"""Kafka Connect REST API client for Debezium Oracle connector management."""

import json
import os

import requests

_state: dict = {}


class DebeziumError(Exception):
    pass


def init(load_configs_fn) -> None:
    _state["load_configs"] = load_configs_fn


def _base_url() -> str:
    cfg = _state["load_configs"]()
    url = cfg.get("kafka_connect", {}).get("url", "").strip()
    if not url:
        raise DebeziumError("Kafka Connect URL не настроен — проверьте Настройки")
    return url.rstrip("/")


def _kafka_bootstrap() -> str:
    cfg = _state["load_configs"]()
    servers = cfg.get("kafka", {}).get("bootstrap_servers", "").strip()
    if not servers:
        raise DebeziumError("Kafka bootstrap_servers не настроен — проверьте Настройки")
    return servers


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def create_connector(migration: dict, oracle_cfg: dict) -> dict:
    """
    Create a Debezium Oracle connector for the migration.
    oracle_cfg is the oracle_source service config dict.
    Raises DebeziumError on failure.
    """
    connector_name = migration["connector_name"]

    # Check if already exists — idempotent
    status = get_connector_status(connector_name)
    if status not in ("NOT_FOUND",):
        return {"name": connector_name, "already_existed": True, "state": status}

    bootstrap    = _kafka_bootstrap()
    topic_prefix = migration["topic_prefix"]
    src_schema   = migration["source_schema"].upper()
    src_table    = migration["source_table"].upper()
    lob_enabled  = migration.get("lob_enabled", True)

    # User-defined key columns (for tables without PK/UK)
    key_cols_json = migration.get("effective_key_columns_json") or "[]"
    key_cols = json.loads(key_cols_json) if isinstance(key_cols_json, str) else key_cols_json

    config = {
        "connector.class":              "io.debezium.connector.oracle.OracleConnector",
        "tasks.max":                    "1",
        "snapshot.mode":                "no_data",
        "snapshot.locking.mode":        "none",
        "log.mining.strategy":          "online_catalog",
        "database.connection.adapter":  "logminer",
        "log.mining.continuous.mine":   "true",
        "heartbeat.interval.ms":        "30000",

        # Oracle source connection
        "topic.prefix":      topic_prefix,
        "database.hostname": oracle_cfg.get("host", ""),
        "database.port":     str(oracle_cfg.get("port", 1521)),
        "database.user":     oracle_cfg.get("user", ""),
        "database.password": oracle_cfg.get("password", ""),
        "database.dbname":   oracle_cfg.get("service_name", ""),

        # Which table to capture
        "table.include.list":    f"{src_schema}.{src_table}",

        # Start position
        "log.mining.start.scn": str(int(migration["start_scn"])),

        # Topic auto-creation
        "topic.creation.default.replication.factor": os.getenv("DEBEZIUM_TOPIC_REPLICATION_FACTOR", "1"),
        "topic.creation.default.partitions":         os.getenv("DEBEZIUM_TOPIC_PARTITIONS", "1"),
        "topic.creation.default.cleanup.policy":     os.getenv("DEBEZIUM_TOPIC_CLEANUP_POLICY", "delete"),
        "topic.creation.default.retention.ms":       os.getenv("DEBEZIUM_TOPIC_RETENTION_MS", "604800000"),
        "topic.creation.default.compression.type":   os.getenv("DEBEZIUM_TOPIC_COMPRESSION_TYPE", "snappy"),

        # LOB handling
        "lob.enabled":           "true" if lob_enabled else "false",
        "lob.fetch.size":        os.getenv("DEBEZIUM_LOB_FETCH_SIZE", "0"),
        "lob.fetch.buffer.size": os.getenv("DEBEZIUM_LOB_FETCH_BUFFER_SIZE", "0"),

        # Schema history (internal Kafka topic, one per connector)
        "schema.history.internal.kafka.bootstrap.servers": bootstrap,
        "schema.history.internal.kafka.topic":             f"schema-changes.{connector_name}",
        "include.schema.changes": "false",

        # Message converters — JSON with schema envelope
        "key.converter":                 "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable":  "true",
        "value.converter":               "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "true",

        # Type handling
        "decimal.handling.mode": "double",
        "time.precision.mode":   "connect",

        # Misc
        "provide.transaction.metadata": "false",
        "tombstones.on.delete":          "true",
        "skipped.operations":            "none",

        # Transforms: unwrap Debezium envelope → flat record, then route to canonical topic
        "transforms":                              "unwrap,route",
        "transforms.unwrap.type":                  "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.delete.handling.mode":  "rewrite",
        "transforms.unwrap.add.fields":            "op,table,source.ts_ms",
        "transforms.unwrap.add.fields.prefix":     "__",
        "transforms.route.type":                   "io.debezium.transforms.ByLogicalTableRouter",
        "transforms.route.topic.regex":            f"({topic_prefix}\\..*)",
        "transforms.route.topic.replacement":      "$1",
    }

    # Tell Debezium which columns form the message key when the table has
    # no PK/UK.  Format: "SCHEMA.TABLE:col1,col2"
    if key_cols:
        cols_csv = ",".join(c.upper() for c in key_cols)
        config["message.key.columns"] = f"{src_schema}.{src_table}:{cols_csv}"

    url = f"{_base_url()}/connectors"
    try:
        resp = requests.post(
            url,
            headers={"Content-Type": "application/json"},
            data=json.dumps({"name": connector_name, "config": config}),
            timeout=15,
        )
    except requests.RequestException as exc:
        raise DebeziumError(f"Kafka Connect недоступен: {exc}") from exc

    if resp.status_code in (200, 201):
        return resp.json()
    raise DebeziumError(
        f"Не удалось создать коннектор (HTTP {resp.status_code}): {resp.text[:300]}"
    )


def delete_connector(connector_name: str) -> None:
    """Delete connector; no error if it does not exist."""
    try:
        resp = requests.delete(
            f"{_base_url()}/connectors/{connector_name}",
            timeout=10,
        )
    except requests.RequestException as exc:
        raise DebeziumError(f"Kafka Connect недоступен: {exc}") from exc

    if resp.status_code not in (204, 404):
        raise DebeziumError(
            f"Не удалось удалить коннектор (HTTP {resp.status_code}): {resp.text[:200]}"
        )


def get_connector_status(connector_name: str) -> str:
    """
    Return connector state string: RUNNING | FAILED | PAUSED | UNASSIGNED | NOT_FOUND.
    """
    try:
        resp = requests.get(
            f"{_base_url()}/connectors/{connector_name}/status",
            timeout=10,
        )
    except requests.RequestException as exc:
        raise DebeziumError(f"Kafka Connect недоступен: {exc}") from exc

    if resp.status_code == 404:
        return "NOT_FOUND"
    if not resp.ok:
        raise DebeziumError(
            f"Ошибка статуса коннектора (HTTP {resp.status_code}): {resp.text[:200]}"
        )
    data = resp.json()
    # data = { "name": "...", "connector": {"state": "RUNNING", ...},
    #           "tasks": [{"id": 0, "state": "RUNNING", ...}] }
    connector_state = data.get("connector", {}).get("state", "UNKNOWN")
    # If connector is RUNNING but task is FAILED, report task state
    tasks = data.get("tasks", [])
    if tasks:
        task_state = tasks[0].get("state", "UNKNOWN")
        if task_state == "FAILED":
            return "FAILED"
    return connector_state


def list_connectors() -> list[str]:
    """Return list of connector names."""
    try:
        resp = requests.get(f"{_base_url()}/connectors", timeout=10)
    except requests.RequestException as exc:
        raise DebeziumError(f"Kafka Connect недоступен: {exc}") from exc
    if not resp.ok:
        raise DebeziumError(f"HTTP {resp.status_code}: {resp.text[:200]}")
    return resp.json()


# ---------------------------------------------------------------------------
# Group connector API (one connector for multiple tables)
# ---------------------------------------------------------------------------

def create_group_connector(
    *,
    connector_name: str,
    topic_prefix: str,
    oracle_cfg: dict,
    table_include_list: str,
    key_columns: str = "",
) -> dict:
    """Create a Debezium connector for a connector group.

    Unlike per-migration connectors:
    - table.include.list contains multiple tables
    - NO log.mining.start.scn — mine from current redo position
    - message.key.columns aggregated across tables
    """
    status = get_connector_status(connector_name)
    if status not in ("NOT_FOUND",):
        return {"name": connector_name, "already_existed": True, "state": status}

    bootstrap = _kafka_bootstrap()
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

        # Oracle source connection
        "topic.prefix":      topic_prefix,
        "database.hostname": oracle_cfg.get("host", ""),
        "database.port":     str(oracle_cfg.get("port", 1521)),
        "database.user":     oracle_cfg.get("user", ""),
        "database.password": oracle_cfg.get("password", ""),
        "database.dbname":   oracle_cfg.get("service_name", ""),

        # Multiple tables
        "table.include.list": table_include_list,

        # NO log.mining.start.scn — mine from current position

        # Topic auto-creation
        "topic.creation.default.replication.factor": os.getenv("DEBEZIUM_TOPIC_REPLICATION_FACTOR", "1"),
        "topic.creation.default.partitions":         os.getenv("DEBEZIUM_TOPIC_PARTITIONS", "1"),
        "topic.creation.default.cleanup.policy":     os.getenv("DEBEZIUM_TOPIC_CLEANUP_POLICY", "delete"),
        "topic.creation.default.retention.ms":       os.getenv("DEBEZIUM_TOPIC_RETENTION_MS", "604800000"),
        "topic.creation.default.compression.type":   os.getenv("DEBEZIUM_TOPIC_COMPRESSION_TYPE", "snappy"),

        # LOB handling
        "lob.enabled":           "true" if lob_enabled else "false",
        "lob.fetch.size":        os.getenv("DEBEZIUM_LOB_FETCH_SIZE", "0"),
        "lob.fetch.buffer.size": os.getenv("DEBEZIUM_LOB_FETCH_BUFFER_SIZE", "0"),

        # Schema history
        "schema.history.internal.kafka.bootstrap.servers": bootstrap,
        "schema.history.internal.kafka.topic":             f"schema-changes.{connector_name}",
        "include.schema.changes": "false",

        # Message converters
        "key.converter":                 "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable":  "true",
        "value.converter":               "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "true",

        # Type handling
        "decimal.handling.mode": "double",
        "time.precision.mode":   "connect",

        # Misc
        "provide.transaction.metadata": "false",
        "tombstones.on.delete":          "true",
        "skipped.operations":            "none",

        # Transforms
        "transforms":                              "unwrap,route",
        "transforms.unwrap.type":                  "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.delete.handling.mode":  "rewrite",
        "transforms.unwrap.add.fields":            "op,table,source.ts_ms",
        "transforms.unwrap.add.fields.prefix":     "__",
        "transforms.route.type":                   "io.debezium.transforms.ByLogicalTableRouter",
        "transforms.route.topic.regex":            f"({topic_prefix}\\..*)",
        "transforms.route.topic.replacement":      "$1",
    }

    # Multi-table key columns: "SCHEMA.T1:col1,col2;SCHEMA.T2:colA"
    if key_columns:
        config["message.key.columns"] = key_columns

    url = f"{_base_url()}/connectors"
    try:
        resp = requests.post(
            url,
            headers={"Content-Type": "application/json"},
            data=json.dumps({"name": connector_name, "config": config}),
            timeout=15,
        )
    except requests.RequestException as exc:
        raise DebeziumError(f"Kafka Connect недоступен: {exc}") from exc

    if resp.status_code in (200, 201):
        return resp.json()
    raise DebeziumError(
        f"Не удалось создать групповой коннектор (HTTP {resp.status_code}): {resp.text[:300]}"
    )


def update_connector_tables(
    *,
    connector_name: str,
    table_include_list: str,
    key_columns: str = "",
) -> dict:
    """Update table.include.list on a running connector via PUT config.

    Debezium applies config changes without full restart.
    """
    # Read current config
    url = f"{_base_url()}/connectors/{connector_name}/config"
    try:
        resp = requests.get(url, timeout=10)
    except requests.RequestException as exc:
        raise DebeziumError(f"Kafka Connect недоступен: {exc}") from exc
    if not resp.ok:
        raise DebeziumError(
            f"Не удалось прочитать конфиг коннектора (HTTP {resp.status_code})"
        )

    config = resp.json()
    config["table.include.list"] = table_include_list
    if key_columns:
        config["message.key.columns"] = key_columns
    elif "message.key.columns" in config:
        del config["message.key.columns"]

    # PUT updated config
    try:
        resp = requests.put(
            url,
            headers={"Content-Type": "application/json"},
            data=json.dumps(config),
            timeout=15,
        )
    except requests.RequestException as exc:
        raise DebeziumError(f"Kafka Connect недоступен: {exc}") from exc

    if resp.ok:
        return resp.json()
    raise DebeziumError(
        f"Не удалось обновить конфиг коннектора (HTTP {resp.status_code}): {resp.text[:300]}"
    )
