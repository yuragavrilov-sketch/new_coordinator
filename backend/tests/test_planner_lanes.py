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


# --- add-items driver: promote the non-CDC lane on add ------------------------

def _planner_conn_stub(plan_status, item_rows):
    events: list = []

    class CursorStub:
        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

        def execute(self, sql, params=()):
            self.sql = sql
            self.rowcount = 1 if ("UPDATE migrations" in sql and "phase = 'DRAFT'" in sql) else 0

        def fetchone(self):
            if "SELECT status FROM migration_plans" in self.sql:
                return (plan_status,)
            return None

        def fetchall(self):
            if "SELECT i.item_id, i.migration_id" in self.sql:
                return item_rows
            return []

    class ConnStub:
        def __init__(self):
            self.cur = CursorStub()

        def cursor(self):
            return self.cur

        def commit(self):
            events.append(("commit",))

        def rollback(self):
            events.append(("rollback",))

        def close(self):
            pass

    return ConnStub, events


def test_start_ready_lane_batches_promotes_bulk_when_running(monkeypatch):
    # (item_id, migration_id, batch_order, mode, strategy, status, phase, group_id)
    rows = [(11, "mid-bulk", 1, "BULK", "BULK_DIRECT", "PENDING", "DRAFT", None)]
    ConnStub, events = _planner_conn_stub("RUNNING", rows)
    monkeypatch.setitem(planner._state, "get_conn", lambda: ConnStub())
    monkeypatch.setitem(
        planner._state, "broadcast",
        lambda e: events.append(("broadcast", e["migration_id"], e["phase"])),
    )
    monkeypatch.setattr(planner, "_refresh_queue_positions_best_effort", lambda: None)

    started = planner._start_ready_lane_batches(42, only_lanes={"BULK"}, require_running=True)

    assert started == ["mid-bulk"]
    assert ("broadcast", "mid-bulk", "NEW") in events
    assert ("commit",) in events
    assert ("rollback",) not in events


def test_start_ready_lane_batches_skips_when_plan_not_running(monkeypatch):
    ConnStub, _events = _planner_conn_stub("READY", [])
    monkeypatch.setitem(planner._state, "get_conn", lambda: ConnStub())

    started = planner._start_ready_lane_batches(42, only_lanes={"BULK"}, require_running=True)

    assert started == []


def test_start_ready_lane_batches_starts_ready_plan_when_not_gated(monkeypatch):
    # require_running=False (schema-migration add-and-run) starts a READY plan.
    rows = [(11, "mid-bulk", 1, "BULK", "BULK_DIRECT", "PENDING", "DRAFT", None)]
    ConnStub, events = _planner_conn_stub("READY", rows)
    monkeypatch.setitem(planner._state, "get_conn", lambda: ConnStub())
    monkeypatch.setitem(
        planner._state, "broadcast",
        lambda e: events.append(("broadcast", e["migration_id"], e["phase"])),
    )
    monkeypatch.setattr(planner, "_refresh_queue_positions_best_effort", lambda: None)

    started = planner._start_ready_lane_batches(42, only_lanes={"BULK"}, require_running=False)

    assert started == ["mid-bulk"]
