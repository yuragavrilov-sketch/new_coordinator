from __future__ import annotations

import sys
from pathlib import Path


WORKERS_DIR = Path(__file__).resolve().parents[2] / "workers"
if str(WORKERS_DIR) not in sys.path:
    sys.path.insert(0, str(WORKERS_DIR))

import common as worker_common  # noqa: E402


class CursorStub:
    def __init__(self, row=None):
        self.row = row
        self.executed: list[tuple[str, tuple]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params or ()))

    def fetchone(self):
        return self.row


class ConnStub:
    def __init__(self, row=None):
        self.cur = CursorStub(row)
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self.cur

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def test_cdc_topic_name_sanitizes_hash_table_names():
    assert (
        worker_common.cdc_topic_name("sm.tcbpay.pay.r123ab", "tcbpay", "merchants#orders")
        == "sm.tcbpay.pay.r123ab.TCBPAY.MERCHANTS_ORDERS"
    )


def test_claim_cdc_migration_persists_exact_worker_topic(monkeypatch):
    monkeypatch.setattr(worker_common, "WORKER_ID", "worker-1")
    row = (
        "mid-1",
        "oracle_target",
        "TCBPAY",
        "MERCHANTS#ORDERS",
        "TCBPAY",
        "MERCHANTS#ORDERS",
        "sm.tcbpay.pay.r123ab",
        "sm.tcbpay.pay_TCBPAY_MERCHANTS#ORDERS",
        '["ID"]',
    )
    conn = ConnStub(row)

    migration = worker_common.claim_cdc_migration(conn)

    assert migration is not None
    assert conn.committed is True
    reserve_sql, reserve_params = conn.cur.executed[1]
    assert "INSERT INTO migration_cdc_state" in reserve_sql
    assert reserve_params == (
        "sm.tcbpay.pay.r123ab.TCBPAY.MERCHANTS_ORDERS",
        "worker-1",
        "mid-1",
    )
    assert "topic            = EXCLUDED.topic" in reserve_sql


def test_cdc_checkin_recomputes_topic_from_migration_columns(monkeypatch):
    monkeypatch.setattr(worker_common, "WORKER_ID", "worker-1")
    conn = ConnStub()

    worker_common.cdc_checkin(conn, "mid-1", total_lag=7, rows_applied=3)

    sql, params = conn.cur.executed[0]
    assert "INSERT INTO migration_cdc_state" in sql
    assert "UPPER(source_schema) || '.' || UPPER(source_table)" in sql
    assert "topic            = EXCLUDED.topic" in sql
    assert params == (7, "{}", 3, "worker-1", "mid-1")
    assert conn.committed is True
