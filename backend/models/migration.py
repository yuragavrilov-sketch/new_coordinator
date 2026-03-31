from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator

from models.enums import (
    ChunkStatus,
    ChunkType,
    MigrationMode,
    MigrationStrategy,
    Phase,
)


class Migration(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    migration_id: str
    migration_name: str
    phase: Phase = Phase.DRAFT
    state_changed_at: datetime | None = None

    source_connection_id: str = ""
    target_connection_id: str = ""
    source_schema: str = ""
    source_table: str = ""
    target_schema: str = ""
    target_table: str = ""

    stage_table_name: str = ""
    stage_tablespace: str = ""
    connector_name: str = ""
    topic_prefix: str = ""
    consumer_group: str = ""

    chunk_strategy: str = ""
    chunk_size: int = 10000
    max_parallel_workers: int = 1
    apply_mode: str = ""

    source_pk_exists: bool = False
    source_uk_exists: bool = False
    effective_key_type: str = ""
    effective_key_source: str = ""
    effective_key_columns_json: str = "[]"

    key_uniqueness_validated: bool = False
    key_validation_status: str | None = None
    key_validation_message: str | None = None

    start_scn: Decimal | None = None
    scn_fixed_at: datetime | None = None

    created_by: str | None = None
    description: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    locked_by: str | None = None
    lock_until: datetime | None = None

    error_code: str | None = None
    error_text: str | None = None
    failed_phase: str | None = None
    retry_count: int = 0

    total_rows: int | None = None
    total_chunks: int | None = None
    chunks_done: int = 0
    chunks_failed: int = 0
    validate_hash_sample: bool = False
    validation_result: dict[str, Any] | None = None
    connector_status: str | None = None
    kafka_lag: int | None = None
    kafka_lag_checked_at: datetime | None = None
    rows_loaded: int = 0

    baseline_parallel_degree: int = 1
    baseline_batch_size: int = 500000
    migration_strategy: MigrationStrategy = MigrationStrategy.STAGE
    baseline_chunks_total: int | None = None
    baseline_chunks_done: int = 0
    queue_position: int | None = None
    migration_mode: MigrationMode = MigrationMode.CDC
    data_compare_task_id: str | None = None
    group_id: str | None = None

    @field_validator("validation_result", mode="before")
    @classmethod
    def _parse_validation_result(cls, v: Any) -> dict[str, Any] | None:
        if isinstance(v, str):
            return json.loads(v)
        return v

    @property
    def effective_key_columns(self) -> list[str]:
        return json.loads(self.effective_key_columns_json)

    @property
    def is_terminal(self) -> bool:
        return self.phase in Phase.terminal()


class MigrationChunk(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    chunk_id: str
    migration_id: str
    chunk_seq: int
    rowid_start: str
    rowid_end: str
    status: ChunkStatus = ChunkStatus.PENDING
    rows_loaded: int = 0
    worker_id: str | None = None
    claimed_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_text: str | None = None
    retry_count: int = 0
    created_at: datetime | None = None
    chunk_type: ChunkType = ChunkType.BULK


class CdcState(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    migration_id: str
    consumer_group: str = ""
    topic: str = ""
    total_lag: int = 0
    lag_by_partition: dict[str, Any] | None = None
    last_event_scn: Decimal | None = None
    last_event_ts: datetime | None = None
    apply_rate_rps: Decimal | None = None
    rows_applied: int = 0
    worker_id: str | None = None
    worker_heartbeat: datetime | None = None
    updated_at: datetime | None = None


class StateHistoryEntry(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    migration_id: str
    from_phase: str | None = None
    to_phase: str
    transition_status: str = "SUCCESS"
    transition_reason: str | None = None
    message: str | None = None
    actor_type: str = "SYSTEM"
    actor_id: str | None = None
    correlation_id: str | None = None
    created_at: datetime | None = None
