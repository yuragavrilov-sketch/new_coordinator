"""Kafka Connect REST API client for Debezium Oracle connector management."""

import json

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

    bootstrap = _kafka_bootstrap()
    config = {
        "connector.class": "io.debezium.connector.oracle.OracleConnector",
        "tasks.max": "1",

        # Oracle source connection
        "database.hostname": oracle_cfg.get("host", ""),
        "database.port":     str(oracle_cfg.get("port", 1521)),
        "database.user":     oracle_cfg.get("user", ""),
        "database.password": oracle_cfg.get("password", ""),
        "database.dbname":   oracle_cfg.get("service_name", ""),

        # Kafka topic / snapshot
        "topic.prefix":      migration["topic_prefix"],
        "table.include.list": f"{migration['source_schema']}.{migration['source_table']}",
        "snapshot.mode":     "no_data",
        "log.mining.start.scn": str(int(migration["start_scn"])),

        # Schema history (internal Kafka topic)
        "schema.history.internal.kafka.bootstrap.servers": bootstrap,
        "schema.history.internal.kafka.topic":
            f"schema-changes.{connector_name}",

        # Heartbeat to keep connector alive
        "heartbeat.interval.ms": "10000",
    }

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
