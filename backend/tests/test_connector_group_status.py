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
