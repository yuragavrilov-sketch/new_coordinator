from models.enums import Phase, ChunkStatus, MigrationStrategy, MigrationMode, GroupStatus


def test_phase_values():
    assert Phase.NEW == "NEW"
    assert Phase.PREPARING == "PREPARING"
    assert Phase.COMPLETED == "COMPLETED"
    assert Phase.FAILED == "FAILED"
    assert Phase.CANCELLED == "CANCELLED"


def test_phase_from_string():
    assert Phase("NEW") == Phase.NEW
    assert Phase("COMPLETED") == Phase.COMPLETED


def test_phase_is_str():
    assert isinstance(Phase.NEW, str)
    assert Phase.NEW == "NEW"
    phases_set = {"NEW", "PREPARING"}
    assert Phase.NEW in phases_set


def test_phase_terminal():
    terminal = Phase.terminal()
    assert Phase.COMPLETED in terminal
    assert Phase.FAILED in terminal
    assert Phase.CANCELLED in terminal
    assert Phase.NEW not in terminal


def test_phase_active():
    active = Phase.active()
    assert Phase.NEW in active
    assert Phase.PREPARING in active
    assert Phase.BULK_LOADING in active
    assert Phase.COMPLETED not in active
    assert Phase.FAILED not in active


def test_phase_heavy():
    heavy = Phase.heavy()
    assert Phase.PREPARING in heavy
    assert Phase.BULK_LOADING in heavy
    assert Phase.STEADY_STATE not in heavy
    assert Phase.COMPLETED not in heavy


def test_phase_count():
    assert len(Phase) == 32


def test_chunk_status_values():
    assert ChunkStatus.PENDING == "PENDING"
    assert ChunkStatus.DONE == "DONE"
    assert ChunkStatus.FAILED == "FAILED"
    assert ChunkStatus.CANCELLED == "CANCELLED"


def test_migration_strategy():
    assert MigrationStrategy.STAGE == "STAGE"
    assert MigrationStrategy.DIRECT == "DIRECT"


def test_migration_mode():
    assert MigrationMode.CDC == "CDC"
    assert MigrationMode.BULK_ONLY == "BULK_ONLY"


def test_group_status():
    assert GroupStatus.PENDING == "PENDING"
    assert GroupStatus.RUNNING == "RUNNING"
    assert GroupStatus.FAILED == "FAILED"
