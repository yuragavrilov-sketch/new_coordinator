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
