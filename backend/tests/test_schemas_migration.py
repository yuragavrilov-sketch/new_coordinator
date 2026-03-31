import pytest
from pydantic import ValidationError
from schemas.migration_schemas import (
    CreateMigrationRequest,
    MigrationActionRequest,
    UpdateWorkersRequest,
    TransitionPhaseRequest,
)


def test_create_migration_minimal():
    req = CreateMigrationRequest(migration_name="test")
    assert req.migration_name == "test"
    assert req.migration_strategy == "STAGE"
    assert req.migration_mode == "CDC"
    assert req.chunk_size == 1_000_000
    assert req.max_parallel_workers == 1
    assert req.baseline_parallel_degree == 4


def test_create_migration_name_required():
    with pytest.raises(ValidationError):
        CreateMigrationRequest(migration_name="")


def test_create_migration_name_stripped():
    req = CreateMigrationRequest(migration_name="  test  ")
    assert req.migration_name == "test"


def test_create_migration_full():
    req = CreateMigrationRequest(
        migration_name="hr-employees",
        source_connection_id="oracle_source",
        target_connection_id="oracle_target",
        source_schema="HR",
        source_table="EMPLOYEES",
        target_schema="hr",
        target_table="employees",
        migration_strategy="DIRECT",
        migration_mode="BULK_ONLY",
        chunk_size=500_000,
        max_parallel_workers=4,
        baseline_parallel_degree=8,
        group_id="some-uuid",
    )
    assert req.migration_strategy == "DIRECT"
    assert req.migration_mode == "BULK_ONLY"


def test_create_migration_invalid_strategy():
    with pytest.raises(ValidationError):
        CreateMigrationRequest(migration_name="test", migration_strategy="INVALID")


def test_create_migration_workers_clamped():
    req = CreateMigrationRequest(migration_name="test", max_parallel_workers=0)
    assert req.max_parallel_workers == 1


def test_migration_action_valid():
    req = MigrationActionRequest(action="cancel")
    assert req.action == "cancel"


def test_migration_action_invalid():
    with pytest.raises(ValidationError):
        MigrationActionRequest(action="invalid_action")


def test_migration_action_with_actor():
    req = MigrationActionRequest(action="run", actor_id="admin", message="starting")
    assert req.actor_id == "admin"


def test_update_workers():
    req = UpdateWorkersRequest(max_parallel_workers=8)
    assert req.max_parallel_workers == 8
    assert req.baseline_parallel_degree is None


def test_update_workers_at_least_one():
    with pytest.raises(ValidationError):
        UpdateWorkersRequest()


def test_transition_phase():
    req = TransitionPhaseRequest(to_phase="PREPARING")
    assert req.to_phase == "PREPARING"


def test_transition_phase_invalid():
    with pytest.raises(ValidationError):
        TransitionPhaseRequest(to_phase="INVALID_PHASE")
