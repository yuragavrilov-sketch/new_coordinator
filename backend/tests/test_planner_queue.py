from __future__ import annotations

from routes import planner


def test_can_start_cdc_batch_while_cdc_is_running():
    assert planner._can_start_plan_batch(
        running_items=[("CDC", "CDC_DIRECT")],
        pending_items=[("CDC", "CDC_DIRECT")],
    )


def test_cdc_queue_rule_accepts_strategy_when_mode_is_missing():
    assert planner._can_start_plan_batch(
        running_items=[("", "CDC_STAGE")],
        pending_items=[("", "CDC_DIRECT")],
    )


def test_running_bulk_blocks_next_batch():
    assert not planner._can_start_plan_batch(
        running_items=[("BULK", "BULK_DIRECT")],
        pending_items=[("CDC", "CDC_DIRECT")],
    )


def test_running_cdc_does_not_allow_pending_bulk_batch():
    assert not planner._can_start_plan_batch(
        running_items=[("CDC", "CDC_DIRECT")],
        pending_items=[("BULK", "BULK_DIRECT")],
    )


def test_legacy_payload_detects_cdc_mode():
    assert planner._legacy_payload_has_cdc(
        batches=[{"tables": [{"table": "T1", "mode": "CDC", "overrides": {}}]}],
        defaults={},
    )


def test_legacy_payload_detects_default_cdc_strategy():
    assert planner._legacy_payload_has_cdc(
        batches=[{"tables": [{"table": "T1", "overrides": {}}]}],
        defaults={"strategy": "CDC_STAGE"},
    )


def test_legacy_payload_detects_implicit_cdc_defaults():
    assert planner._legacy_payload_has_cdc(
        batches=[{"tables": [{"table": "T1", "overrides": {}}]}],
        defaults={},
    )


def test_legacy_payload_allows_bulk():
    assert not planner._legacy_payload_has_cdc(
        batches=[{"tables": [{"table": "T1", "mode": "BULK", "overrides": {"strategy": "BULK_STAGE"}}]}],
        defaults={},
    )


def test_legacy_payload_allows_bulk_default_strategy_without_mode():
    assert not planner._legacy_payload_has_cdc(
        batches=[{"tables": [{"table": "T1", "overrides": {}}]}],
        defaults={"strategy": "BULK_STAGE"},
    )


def test_plan_item_status_for_active_phase_is_running():
    assert planner._plan_item_status_for_phase("NEW") == "RUNNING"
    assert planner._plan_item_status_for_phase("CDC_APPLYING") == "RUNNING"


def test_plan_item_status_for_terminal_phase():
    assert planner._plan_item_status_for_phase("COMPLETED") == "DONE"
    assert planner._plan_item_status_for_phase("FAILED") == "FAILED"
    assert planner._plan_item_status_for_phase("CANCELLED") == "CANCELLED"
