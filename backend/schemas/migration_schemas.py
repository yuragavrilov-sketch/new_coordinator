from __future__ import annotations
from pydantic import BaseModel, Field, field_validator, model_validator
from models.enums import MigrationMode, MigrationStrategy, Phase

VALID_ACTIONS = frozenset({
    "run", "pause", "resume", "cancel", "restart",
    "lag_zero", "retry_verify", "force_complete",
})

ACTION_TRANSITIONS: dict[str, tuple[str | None, str]] = {
    "run":            ("DRAFT",           "NEW"),
    "pause":          (None,              "PAUSED"),
    "resume":         ("PAUSED",          "BULK_LOADING"),
    "cancel":         (None,              "CANCELLING"),
    "restart":        ("CANCELLED",       "NEW"),
    "lag_zero":       ("CDC_CATCHING_UP", "CDC_CAUGHT_UP"),
    "retry_verify":   ("DATA_MISMATCH",   "DATA_VERIFYING"),
    "force_complete": ("DATA_MISMATCH",   "COMPLETED"),
}

DELETABLE_PHASES = None  # any phase — force-delete always allowed


class CreateMigrationRequest(BaseModel):
    migration_name: str = Field(min_length=1, max_length=255)
    initial_phase: str = "DRAFT"
    migration_strategy: MigrationStrategy = MigrationStrategy.STAGE
    migration_mode: MigrationMode = MigrationMode.CDC
    group_id: str | None = None
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
    chunk_size: int = 1_000_000
    max_parallel_workers: int = 1
    baseline_parallel_degree: int = 4
    baseline_batch_size: int = 500_000
    validate_hash_sample: bool = False
    source_pk_exists: bool = False
    source_uk_exists: bool = False
    effective_key_type: str = ""
    effective_key_source: str = ""
    effective_key_columns_json: str = "[]"
    source_filter: str | None = None
    created_by: str | None = None
    description: str | None = None

    @field_validator("migration_name", mode="before")
    @classmethod
    def _strip_name(cls, v: str) -> str:
        if isinstance(v, str):
            return v.strip()
        return v

    @field_validator("max_parallel_workers", "baseline_parallel_degree", mode="after")
    @classmethod
    def _clamp_min_1(cls, v: int) -> int:
        return max(1, v)

    @field_validator("stage_tablespace", mode="after")
    @classmethod
    def _upper_tablespace(cls, v: str) -> str:
        return v.upper() if v else v

    @field_validator("initial_phase", mode="after")
    @classmethod
    def _validate_phase(cls, v: str) -> str:
        try:
            Phase(v)
        except ValueError:
            raise ValueError(f"Invalid initial_phase: {v}")
        return v


class MigrationActionRequest(BaseModel):
    action: str
    message: str | None = None
    actor_id: str | None = None

    @field_validator("action", mode="after")
    @classmethod
    def _validate_action(cls, v: str) -> str:
        v = v.strip().lower()
        if v not in VALID_ACTIONS:
            raise ValueError(f"Unknown action: {v}. Valid: {sorted(VALID_ACTIONS)}")
        return v


class UpdateWorkersRequest(BaseModel):
    max_parallel_workers: int | None = None
    baseline_parallel_degree: int | None = None

    @model_validator(mode="after")
    def _at_least_one(self) -> UpdateWorkersRequest:
        if self.max_parallel_workers is None and self.baseline_parallel_degree is None:
            raise ValueError("At least one of max_parallel_workers or baseline_parallel_degree must be provided")
        return self

    @field_validator("max_parallel_workers", "baseline_parallel_degree", mode="after")
    @classmethod
    def _clamp_min_1(cls, v: int | None) -> int | None:
        if v is not None:
            return max(1, v)
        return v


class TransitionPhaseRequest(BaseModel):
    to_phase: str
    error_code: str | None = None
    error_text: str | None = None
    retry_count: int | None = None
    transition_status: str = "SUCCESS"
    transition_reason: str | None = None
    message: str | None = None
    actor_type: str = "SYSTEM"
    actor_id: str | None = None
    correlation_id: str | None = None

    @field_validator("to_phase", mode="after")
    @classmethod
    def _validate_phase(cls, v: str) -> str:
        try:
            Phase(v)
        except ValueError:
            raise ValueError(f"Invalid phase: {v}")
        return v
