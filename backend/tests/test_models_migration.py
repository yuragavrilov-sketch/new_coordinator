from datetime import datetime, timezone
from uuid import uuid4

from models.migration import Migration, MigrationChunk, CdcState, StateHistoryEntry
from models.enums import Phase, ChunkStatus, MigrationStrategy, MigrationMode, ChunkType


def test_migration_from_db_row():
    mid = str(uuid4())
    now = datetime.now(timezone.utc)
    row = {
        "migration_id": mid,
        "migration_name": "test_migration",
        "phase": "NEW",
        "state_changed_at": now,
        "source_connection_id": "oracle_source",
        "target_connection_id": "oracle_target",
        "source_schema": "HR",
        "source_table": "EMPLOYEES",
        "target_schema": "hr",
        "target_table": "employees",
        "stage_table_name": "STG_EMPLOYEES",
        "stage_tablespace": "",
        "connector_name": "cdc-hr-employees",
        "topic_prefix": "cdc",
        "consumer_group": "cg-hr",
        "chunk_strategy": "",
        "chunk_size": 100000,
        "max_parallel_workers": 4,
        "apply_mode": "",
        "source_pk_exists": True,
        "source_uk_exists": False,
        "effective_key_type": "PK",
        "effective_key_source": "AUTO",
        "effective_key_columns_json": '["EMPLOYEE_ID"]',
        "key_uniqueness_validated": False,
        "key_validation_status": None,
        "key_validation_message": None,
        "start_scn": None,
        "scn_fixed_at": None,
        "created_by": "admin",
        "description": "Test migration",
        "created_at": now,
        "updated_at": now,
        "locked_by": None,
        "lock_until": None,
        "error_code": None,
        "error_text": None,
        "failed_phase": None,
        "retry_count": 0,
        "total_rows": None,
        "total_chunks": None,
        "chunks_done": 0,
        "chunks_failed": 0,
        "validate_hash_sample": False,
        "validation_result": None,
        "connector_status": None,
        "kafka_lag": None,
        "kafka_lag_checked_at": None,
        "rows_loaded": 0,
        "baseline_parallel_degree": 4,
        "baseline_batch_size": 500000,
        "migration_strategy": "STAGE",
        "baseline_chunks_total": None,
        "baseline_chunks_done": 0,
        "queue_position": None,
        "migration_mode": "CDC",
        "data_compare_task_id": None,
        "group_id": None,
    }
    m = Migration.model_validate(row)
    assert m.migration_id == mid
    assert m.phase == Phase.NEW
    assert m.migration_strategy == MigrationStrategy.STAGE
    assert m.migration_mode == MigrationMode.CDC
    assert m.source_pk_exists is True
    assert m.effective_key_columns == ["EMPLOYEE_ID"]


def test_migration_is_terminal():
    m = Migration.model_validate({
        "migration_id": str(uuid4()),
        "migration_name": "test",
        "phase": "COMPLETED",
        "state_changed_at": datetime.now(timezone.utc),
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    })
    assert m.is_terminal is True


def test_migration_chunk():
    chunk = MigrationChunk.model_validate({
        "chunk_id": str(uuid4()),
        "migration_id": str(uuid4()),
        "chunk_seq": 1,
        "rowid_start": "AAABcAAEAAAAIjAAA",
        "rowid_end": "AAABcAAEAAAAIjAAZ",
        "status": "PENDING",
        "rows_loaded": 0,
        "retry_count": 0,
        "chunk_type": "BULK",
        "created_at": datetime.now(timezone.utc),
    })
    assert chunk.status == ChunkStatus.PENDING
    assert chunk.chunk_type == ChunkType.BULK


def test_cdc_state():
    state = CdcState.model_validate({
        "migration_id": str(uuid4()),
        "consumer_group": "cg-test",
        "topic": "cdc.HR.EMPLOYEES",
        "total_lag": 1500,
        "rows_applied": 10000,
        "updated_at": datetime.now(timezone.utc),
    })
    assert state.total_lag == 1500
    assert state.rows_applied == 10000


def test_state_history_entry():
    entry = StateHistoryEntry.model_validate({
        "id": 1,
        "migration_id": str(uuid4()),
        "from_phase": "NEW",
        "to_phase": "PREPARING",
        "transition_status": "SUCCESS",
        "actor_type": "SYSTEM",
        "created_at": datetime.now(timezone.utc),
    })
    assert entry.from_phase == "NEW"
    assert entry.to_phase == "PREPARING"
