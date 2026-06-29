from __future__ import annotations

import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import orchestrator as orch


def test_migration_lane_classification():
    assert orch._migration_lane("CDC_STAGE") == "CDC"
    assert orch._migration_lane("CDC_DIRECT") == "CDC"
    assert orch._migration_lane("BULK_STAGE") == "BULK"
    assert orch._migration_lane("BULK_DIRECT") == "BULK"
    assert orch._migration_lane("") == "BULK"
    assert orch._migration_lane(None) == "BULK"


def test_indexes_enabling_is_not_a_blocking_phase():
    assert "INDEXES_ENABLING" not in orch.BULK_LANE_PHASES
    assert "BULK_LOADING" in orch.BULK_LANE_PHASES


def test_cdc_lane_free_when_only_bulk_is_loading():
    # A BULK migration in BULK_LOADING must not block a CDC start.
    assert orch._lane_is_free("CDC", [("BULK_DIRECT", "BULK_LOADING")])


def test_bulk_lane_free_when_only_cdc_is_loading():
    assert orch._lane_is_free("BULK", [("CDC_DIRECT", "BULK_LOADING")])


def test_cdc_lane_blocked_by_other_cdc_in_bulk_phase():
    assert not orch._lane_is_free("CDC", [("CDC_STAGE", "CHUNKING")])


def test_lane_free_when_other_cdc_is_in_indexes_enabling():
    # INDEXES_ENABLING is the tail, not a blocking phase.
    assert orch._lane_is_free("CDC", [("CDC_STAGE", "INDEXES_ENABLING")])


def test_lane_free_when_no_other_migrations():
    assert orch._lane_is_free("CDC", [])
    assert orch._lane_is_free("BULK", [])


# --- head-of-line election must be per-lane, not global -----------------------

def test_non_cdc_is_lane_head_despite_older_blocked_cdc():
    # Regression: an older runnable-NEW CDC migration (blocked elsewhere by a
    # busy CDC lane) must NOT block a non-CDC migration whose own lane is free.
    runnable_new = [("cdc-old", "CDC_STAGE"), ("bulk-new", "BULK_DIRECT")]
    assert orch._is_lane_head("bulk-new", "BULK", runnable_new)
    # The CDC one is still the head of its OWN lane.
    assert orch._is_lane_head("cdc-old", "CDC", runnable_new)


def test_older_same_lane_blocks_head():
    runnable_new = [("bulk-old", "BULK_STAGE"), ("bulk-new", "BULK_DIRECT")]
    assert not orch._is_lane_head("bulk-new", "BULK", runnable_new)
    assert orch._is_lane_head("bulk-old", "BULK", runnable_new)


def test_lane_head_when_only_candidate_in_lane():
    runnable_new = [("cdc-1", "CDC_STAGE"), ("bulk-1", "BULK_DIRECT")]
    assert orch._is_lane_head("bulk-1", "BULK", runnable_new)
    assert orch._is_lane_head("cdc-1", "CDC", runnable_new)


def test_not_lane_head_when_absent_from_runnable_set():
    # Candidate not in the runnable set (e.g. not actually runnable) → not head.
    assert not orch._is_lane_head("ghost", "BULK", [("bulk-1", "BULK_STAGE")])
