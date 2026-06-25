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
