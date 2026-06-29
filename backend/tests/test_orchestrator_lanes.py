from __future__ import annotations

import os
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
