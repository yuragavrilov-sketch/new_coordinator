from __future__ import annotations

from routes import planner


# --- lane classification ------------------------------------------------------

def test_plan_item_lane_by_mode_and_strategy():
    assert planner._plan_item_lane("CDC", None) == "CDC"
    assert planner._plan_item_lane(None, "CDC_DIRECT") == "CDC"
    assert planner._plan_item_lane("BULK", "BULK_STAGE") == "BULK"
    assert planner._plan_item_lane(None, None) == "BULK"


# --- per-lane promotion decision ----------------------------------------------
# items: (lane, batch_order, status). A lane is "busy" while it has a RUNNING
# item; when free it promotes the lowest batch_order that still has a PENDING
# item IN THAT LANE. Lanes are independent.

def test_non_cdc_lane_starts_in_parallel_with_running_cdc():
    # The user's bug: CDC batch 1 is running, a non-CDC batch 2 is PENDING.
    # The non-CDC lane is free, so its lowest pending batch must promote now —
    # it must NOT wait for the CDC batch to finish.
    items = [("CDC", 1, "RUNNING"), ("BULK", 2, "PENDING")]
    assert planner._lanes_ready_to_promote(items) == {"BULK": 2}


def test_busy_lane_does_not_advance():
    # non-CDC lane has a RUNNING item → it is busy, promote nothing for it.
    items = [("BULK", 1, "RUNNING"), ("BULK", 2, "PENDING")]
    assert planner._lanes_ready_to_promote(items) == {}


def test_each_lane_promotes_its_own_lowest_pending_batch():
    items = [
        ("CDC", 1, "PENDING"),
        ("CDC", 2, "PENDING"),
        ("BULK", 2, "PENDING"),
        ("BULK", 3, "PENDING"),
    ]
    assert planner._lanes_ready_to_promote(items) == {"CDC": 1, "BULK": 2}


def test_lane_order_preserved_within_lane():
    # non-CDC batch 1 still running → batch 2 must wait (order preserved).
    items = [("BULK", 1, "RUNNING"), ("BULK", 2, "PENDING"), ("CDC", 1, "DONE")]
    assert planner._lanes_ready_to_promote(items) == {}


def test_done_items_do_not_block_next_batch():
    items = [("BULK", 1, "DONE"), ("BULK", 2, "PENDING")]
    assert planner._lanes_ready_to_promote(items) == {"BULK": 2}


def test_no_pending_means_nothing_to_promote():
    items = [("CDC", 1, "DONE"), ("BULK", 1, "RUNNING")]
    assert planner._lanes_ready_to_promote(items) == {}
