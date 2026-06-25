from __future__ import annotations

import pytest

from services import target_trigger_jobs


class CursorStub:
    def __init__(self, row):
        self.row = row
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.row


class ConnStub:
    def __init__(self, row):
        self.cursor_stub = CursorStub(row)
        self.committed = False

    def cursor(self):
        return self.cursor_stub

    def commit(self):
        self.committed = True


def test_trigger_job_rejects_cdc_catching_up_before_lag_zero():
    conn = ConnStub(("CDC_CATCHING_UP", "CDC_DIRECT"))

    with pytest.raises(ValueError) as exc:
        target_trigger_jobs.ensure_pending_job(conn, "mid-cdc")

    assert "Cannot create trigger job from phase CDC_CATCHING_UP" in str(exc.value)
    assert not conn.committed
