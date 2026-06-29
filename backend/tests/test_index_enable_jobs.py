from __future__ import annotations

import sys
from pathlib import Path

WORKERS_DIR = Path(__file__).resolve().parents[2] / "workers"
if str(WORKERS_DIR) not in sys.path:
    sys.path.insert(0, str(WORKERS_DIR))

import common as worker_common  # noqa: E402


class CursorStub:
    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.executed: list[tuple[str, tuple]] = []
        self._last = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params or ()))
        self._last = self.rows.pop(0) if self.rows else None

    def fetchone(self):
        return self._last


class ConnStub:
    def __init__(self, rows=None):
        self.cur = CursorStub(rows)
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self.cur

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def test_claim_returns_none_when_no_pending_job():
    conn = ConnStub(rows=[None])
    assert worker_common.claim_index_enable_job(conn) is None
    assert conn.rolled_back


def test_claim_returns_job_dict():
    conn = ConnStub(rows=[(
        "job-1", "mig-1", "tgt-conn", "TGT", "ORDERS", "CDC_STAGE",
    )])
    job = worker_common.claim_index_enable_job(conn)
    assert job["job_id"] == "job-1"
    assert job["migration_id"] == "mig-1"
    assert job["target_schema"] == "TGT"
    assert job["strategy"] == "CDC_STAGE"
    assert conn.committed


def test_complete_is_guarded_by_worker_id():
    conn = ConnStub()
    worker_common.complete_index_enable_job(conn, "job-1", {"enabled": {}})
    sql, params = conn.cur.executed[-1]
    assert "worker_id = %s" in sql
    assert worker_common.WORKER_ID in params
    assert conn.committed


def test_fail_is_guarded_by_worker_id():
    conn = ConnStub()
    worker_common.fail_index_enable_job(conn, "job-1", "boom")
    sql, params = conn.cur.executed[-1]
    assert "worker_id = %s" in sql
    assert worker_common.WORKER_ID in params
    assert conn.committed
