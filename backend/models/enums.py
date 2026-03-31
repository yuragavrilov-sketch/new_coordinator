from __future__ import annotations

from enum import StrEnum


class Phase(StrEnum):
    DRAFT = "DRAFT"
    NEW = "NEW"
    PREPARING = "PREPARING"
    SCN_FIXED = "SCN_FIXED"
    CONNECTOR_STARTING = "CONNECTOR_STARTING"
    CDC_BUFFERING = "CDC_BUFFERING"
    TOPIC_CREATING = "TOPIC_CREATING"
    CHUNKING = "CHUNKING"
    BULK_LOADING = "BULK_LOADING"
    BULK_LOADED = "BULK_LOADED"
    STAGE_VALIDATING = "STAGE_VALIDATING"
    STAGE_VALIDATED = "STAGE_VALIDATED"
    BASELINE_PUBLISHING = "BASELINE_PUBLISHING"
    BASELINE_LOADING = "BASELINE_LOADING"
    BASELINE_PUBLISHED = "BASELINE_PUBLISHED"
    STAGE_DROPPING = "STAGE_DROPPING"
    INDEXES_ENABLING = "INDEXES_ENABLING"
    DATA_VERIFYING = "DATA_VERIFYING"
    DATA_MISMATCH = "DATA_MISMATCH"
    CDC_APPLY_STARTING = "CDC_APPLY_STARTING"
    CDC_APPLYING = "CDC_APPLYING"
    CDC_CATCHING_UP = "CDC_CATCHING_UP"
    CDC_CAUGHT_UP = "CDC_CAUGHT_UP"
    STEADY_STATE = "STEADY_STATE"
    PAUSED = "PAUSED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

    @classmethod
    def terminal(cls) -> frozenset[Phase]:
        return frozenset({cls.COMPLETED, cls.FAILED, cls.CANCELLED})

    @classmethod
    def active(cls) -> frozenset[Phase]:
        return frozenset(p for p in cls if p not in cls.terminal() and p != cls.DRAFT)

    @classmethod
    def heavy(cls) -> frozenset[Phase]:
        return frozenset({
            cls.PREPARING, cls.SCN_FIXED,
            cls.CONNECTOR_STARTING, cls.CDC_BUFFERING,
            cls.TOPIC_CREATING,
            cls.CHUNKING,
            cls.BULK_LOADING, cls.BULK_LOADED,
            cls.STAGE_VALIDATING, cls.STAGE_VALIDATED,
            cls.BASELINE_PUBLISHING, cls.BASELINE_LOADING, cls.BASELINE_PUBLISHED,
            cls.STAGE_DROPPING,
        })


class ChunkStatus(StrEnum):
    PENDING = "PENDING"
    CLAIMED = "CLAIMED"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ChunkType(StrEnum):
    BULK = "BULK"
    BASELINE = "BASELINE"


class MigrationStrategy(StrEnum):
    STAGE = "STAGE"
    DIRECT = "DIRECT"


class MigrationMode(StrEnum):
    CDC = "CDC"
    BULK_ONLY = "BULK_ONLY"


class GroupStatus(StrEnum):
    PENDING = "PENDING"
    TOPICS_CREATING = "TOPICS_CREATING"
    CONNECTOR_STARTING = "CONNECTOR_STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"
