from __future__ import annotations

import sys
from pathlib import Path


WORKERS_DIR = Path(__file__).resolve().parents[2] / "workers"
if str(WORKERS_DIR) not in sys.path:
    sys.path.insert(0, str(WORKERS_DIR))

import worker  # noqa: E402


class CursorStub:
    def __init__(self, calls):
        self.calls = calls

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        self.calls.append((sql, params or []))


class ConnStub:
    def __init__(self):
        self.calls = []

    def cursor(self):
        return CursorStub(self.calls)

    def close(self):
        self.calls.append(("close", []))


def test_cdc_apply_upsert_requires_key_columns_before_sql():
    conn = ConnStub()

    try:
        worker._apply_event(
            conn,
            {"op": "u", "after": {"AMOUNT": 100}},
            "TCBPAY",
            "ALLORDERS",
            ["ID"],
        )
    except ValueError as exc:
        assert "missing key columns: ID" in str(exc)
    else:
        raise AssertionError("expected missing CDC key error")

    assert conn.calls == []


def test_cdc_apply_delete_requires_key_columns_before_sql():
    conn = ConnStub()

    try:
        worker._apply_event(
            conn,
            {"op": "d", "before": {"AMOUNT": 100}},
            "TCBPAY",
            "ALLORDERS",
            ["ID"],
        )
    except ValueError as exc:
        assert "missing key columns: ID" in str(exc)
    else:
        raise AssertionError("expected missing CDC key error")

    assert conn.calls == []


def test_cdc_apply_upsert_uses_key_columns_when_present():
    conn = ConnStub()

    worker._apply_event(
        conn,
        {"op": "u", "after": {"ID": 7, "AMOUNT": 100}},
        "TCBPAY",
        "ALLORDERS",
        ["ID"],
    )

    assert len(conn.calls) == 1
    sql, params = conn.calls[0]
    assert 'MERGE INTO "TCBPAY"."ALLORDERS"' in sql
    assert 't."ID" = s."ID"' in sql
    assert params == [7, 100]


def _migration():
    return {
        "migration_id": "mid-1",
        "target_schema": "TCBPAY",
        "target_table": "ALLORDERS",
        "source_schema": "TCBPAY",
        "source_table": "ALLORDERS",
        "topic_prefix": "cdc.run1",
        "consumer_group": "cdc_group",
        "effective_key_columns_json": '["ID"]',
        "target_connection_id": "oracle_target",
    }


def test_cdc_thread_marks_failed_when_kafka_consumer_start_fails(monkeypatch):
    calls = []

    class PgConn:
        def close(self):
            calls.append(("pg-close",))

    class FailingKafkaConsumer:
        def __init__(self, *_args, **_kwargs):
            raise RuntimeError("kafka unavailable")

    import kafka

    monkeypatch.setattr(kafka, "KafkaConsumer", FailingKafkaConsumer)
    monkeypatch.setattr(worker.db, "get_pg_conn", lambda: calls.append(("pg",)) or PgConn())
    monkeypatch.setattr(worker.db, "load_configs", lambda _pg: {"kafka": {"bootstrap_servers": "broker:9092"}})
    monkeypatch.setattr(
        worker.db,
        "fail_cdc_migration",
        lambda pg, mid, code, detail: calls.append(("fail", mid, code, detail)),
    )

    worker.cdc_thread(_migration(), stop_event=type("Stop", (), {"is_set": lambda self: True})())

    assert ("fail", "mid-1", "CDC_WORKER_START_FAILED", "RuntimeError: kafka unavailable") in calls
    assert ("pg-close",) in calls


def test_cdc_thread_marks_failed_when_target_oracle_start_fails(monkeypatch):
    calls = []

    class PgConn:
        def close(self):
            calls.append(("pg-close",))

    class KafkaConsumer:
        def __init__(self, *_args, **_kwargs):
            pass

        def close(self):
            calls.append(("consumer-close",))

    import kafka

    monkeypatch.setattr(kafka, "KafkaConsumer", KafkaConsumer)
    monkeypatch.setattr(worker.db, "get_pg_conn", lambda: calls.append(("pg",)) or PgConn())
    monkeypatch.setattr(worker.db, "load_configs", lambda _pg: {"kafka": {"bootstrap_servers": "broker:9092"}})
    monkeypatch.setattr(worker.db, "open_oracle", lambda *_args: (_ for _ in ()).throw(RuntimeError("oracle down")))
    monkeypatch.setattr(
        worker.db,
        "fail_cdc_migration",
        lambda pg, mid, code, detail: calls.append(("fail", mid, code, detail)),
    )

    worker.cdc_thread(_migration(), stop_event=type("Stop", (), {"is_set": lambda self: True})())

    assert ("fail", "mid-1", "CDC_WORKER_START_FAILED", "RuntimeError: oracle down") in calls
    assert ("consumer-close",) in calls
    assert ("pg-close",) in calls
