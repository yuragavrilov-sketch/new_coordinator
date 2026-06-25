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
