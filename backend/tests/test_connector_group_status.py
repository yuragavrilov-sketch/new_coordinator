from __future__ import annotations

from services import connector_groups


def test_status_poll_preserves_start_lifecycle_when_connector_not_created():
    assert connector_groups._normalize_polled_connector_status(
        "TOPICS_CREATING",
        "NOT_FOUND",
    ) == ("TOPICS_CREATING", None)

    assert connector_groups._normalize_polled_connector_status(
        "CONNECTOR_STARTING",
        "NOT_FOUND",
    ) == ("CONNECTOR_STARTING", None)


def test_status_poll_preserves_stopping_lifecycle():
    assert connector_groups._normalize_polled_connector_status(
        "STOPPING",
        "RUNNING",
    ) == ("STOPPING", None)


def test_status_poll_marks_missing_stable_connector_stopped():
    assert connector_groups._normalize_polled_connector_status(
        "RUNNING",
        "NOT_FOUND",
    ) == ("STOPPED", "STOPPED")


def test_status_poll_writes_real_status_for_stable_group():
    assert connector_groups._normalize_polled_connector_status(
        "RUNNING",
        "FAILED",
    ) == ("FAILED", "FAILED")


def test_refresh_connector_tables_marks_missing_running_connector_stopped(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups,
        "get_group",
        lambda group_id: {
            "group_id": group_id,
            "status": "RUNNING",
            "connector_name": "cdc-main",
            "topic_prefix": "cdc.topic",
            "run_id": None,
        },
    )
    monkeypatch.setattr(
        connector_groups.debezium,
        "get_connector_status",
        lambda connector_name: "NOT_FOUND",
    )
    monkeypatch.setattr(
        connector_groups,
        "update_group_status",
        lambda group_id, status, error_text=None: calls.append((group_id, status, error_text)),
    )

    try:
        connector_groups.refresh_connector_tables("gid-1")
    except ValueError as exc:
        assert "marked RUNNING but is missing" in str(exc)
    else:
        raise AssertionError("expected missing Kafka Connect connector error")

    assert calls == [
        ("gid-1", "STOPPED", "CDC connector cdc-main is missing in Kafka Connect"),
    ]


class CursorStub:
    def __init__(self, row):
        self.row = row
        self.executed = []

    def execute(self, sql, params):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.row


def test_active_migration_for_group_table_finds_non_terminal_phase():
    cur = CursorStub(("mid-1", "CDC_APPLYING"))

    assert connector_groups._active_migration_for_group_table(
        cur, "gid", "tcbpay", "allorders",
    ) == ("mid-1", "CDC_APPLYING")
    assert "COALESCE(phase, '') NOT IN" in cur.executed[0][0]


def test_active_migration_for_group_table_allows_terminal_phase_absent():
    cur = CursorStub(None)

    assert connector_groups._active_migration_for_group_table(
        cur, "gid", "TCBPAY", "ALLORDERS",
    ) is None


def test_topic_count_consumer_uses_supported_timeout_configs(monkeypatch):
    captured = {}

    class FakeKafkaConsumer:
        def __init__(self, **configs):
            captured.update(configs)

    import kafka

    monkeypatch.setattr(kafka, "KafkaConsumer", FakeKafkaConsumer)

    assert isinstance(
        connector_groups._new_topic_count_consumer(["broker:9092"]),
        FakeKafkaConsumer,
    )
    assert captured == {
        "bootstrap_servers": ["broker:9092"],
        "request_timeout_ms": 5000,
        "connections_max_idle_ms": 8000,
    }
