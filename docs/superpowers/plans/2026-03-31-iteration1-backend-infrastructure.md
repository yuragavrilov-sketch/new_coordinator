# Iteration 1: Backend Infrastructure — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the typed foundation (enums, Pydantic models, API schemas, config, logging) that all subsequent iterations depend on.

**Architecture:** Add models/, schemas/ directories alongside existing code. New modules are additive — no existing code is modified in this iteration. Existing code continues to work with string literals; later iterations will migrate to these types.

**Tech Stack:** Python 3.11+, Pydantic v2, structlog, alembic, pytest

---

### Task 1: Install dependencies and create directory structure

**Files:**
- Modify: `backend/requirements.txt`
- Create: `backend/models/__init__.py`
- Create: `backend/schemas/__init__.py`
- Create: `backend/tests/__init__.py`
- Create: `backend/tests/conftest.py`

- [ ] **Step 1: Add new dependencies to requirements.txt**

```
flask==3.0.3
flask-cors==4.0.1
python-dotenv>=1.0
psycopg2-binary>=2.9
oracledb>=2.0
kafka-python>=2.0
requests>=2.31
gunicorn>=21.2
pydantic>=2.6
structlog>=24.1
alembic>=1.13
pytest>=8.0
```

- [ ] **Step 2: Install dependencies**

Run: `cd /mnt/c/work/database_migration/new/front/backend && pip install -r requirements.txt`
Expected: All packages install successfully.

- [ ] **Step 3: Create directory structure**

```bash
mkdir -p backend/models backend/schemas backend/tests
touch backend/models/__init__.py backend/schemas/__init__.py backend/tests/__init__.py
```

- [ ] **Step 4: Create test conftest.py**

```python
# backend/tests/conftest.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
```

- [ ] **Step 5: Verify pytest runs with no tests**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m pytest tests/ -v`
Expected: "no tests ran" or "0 items collected", exit code 0 or 5 (no tests).

- [ ] **Step 6: Commit**

```bash
git add backend/requirements.txt backend/models/__init__.py backend/schemas/__init__.py backend/tests/__init__.py backend/tests/conftest.py
git commit -m "chore: add pydantic, structlog, alembic, pytest dependencies and directory structure"
```

---

### Task 2: Config dataclass

**Files:**
- Create: `backend/config.py`
- Create: `backend/tests/test_config.py`

- [ ] **Step 1: Write the failing test**

```python
# backend/tests/test_config.py
from config import AppConfig


def test_defaults():
    cfg = AppConfig()
    assert cfg.state_db_dsn == "postgresql://postgres:postgres@localhost:5432/migration_state"
    assert cfg.pg_pool_min == 2
    assert cfg.pg_pool_max == 10
    assert cfg.flask_host == "0.0.0.0"
    assert cfg.flask_port == 5000
    assert cfg.flask_debug is True
    assert cfg.orchestrator_tick_interval == 5
    assert cfg.cdc_heartbeat_stale_minutes == 2
    assert cfg.status_poller_interval == 30
    assert cfg.kafka_bootstrap_servers == "kafka:9092"
    assert cfg.debezium_lob_enabled is True
    assert cfg.debezium_topic_replication_factor == "1"
    assert cfg.debezium_topic_partitions == "1"


def test_from_env(monkeypatch):
    monkeypatch.setenv("STATE_DB_DSN", "postgresql://test:test@db:5432/test")
    monkeypatch.setenv("PG_POOL_MIN", "5")
    monkeypatch.setenv("PG_POOL_MAX", "20")
    monkeypatch.setenv("FLASK_PORT", "8080")
    monkeypatch.setenv("FLASK_DEBUG", "false")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092")
    monkeypatch.setenv("DEBEZIUM_LOB_ENABLED", "false")

    cfg = AppConfig.from_env()
    assert cfg.state_db_dsn == "postgresql://test:test@db:5432/test"
    assert cfg.pg_pool_min == 5
    assert cfg.pg_pool_max == 20
    assert cfg.flask_port == 8080
    assert cfg.flask_debug is False
    assert cfg.kafka_bootstrap_servers == "kafka1:9092,kafka2:9092"
    assert cfg.debezium_lob_enabled is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m pytest tests/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'config'`

- [ ] **Step 3: Write minimal implementation**

```python
# backend/config.py
from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class AppConfig:
    """Centralized application configuration. All values sourced from environment."""

    # PostgreSQL
    state_db_dsn: str = "postgresql://postgres:postgres@localhost:5432/migration_state"
    pg_pool_min: int = 2
    pg_pool_max: int = 10

    # Flask
    flask_host: str = "0.0.0.0"
    flask_port: int = 5000
    flask_debug: bool = True

    # Orchestrator
    orchestrator_tick_interval: int = 5
    orchestrator_start_delay: int = 3

    # Workers / CDC
    cdc_heartbeat_stale_minutes: int = 2

    # Status poller
    status_poller_interval: int = 30
    status_poller_initial_delay: int = 5

    # Kafka
    kafka_bootstrap_servers: str = "kafka:9092"

    # Debezium
    debezium_lob_enabled: bool = True
    debezium_log_mining_batch_size_max: str = "5000"
    debezium_log_mining_sleep_time_increment_ms: str = "400"
    debezium_log_mining_sleep_time_max_ms: str = "1000"
    debezium_topic_replication_factor: str = "1"
    debezium_topic_partitions: str = "1"
    debezium_topic_cleanup_policy: str = "delete"
    debezium_topic_retention_ms: str = "604800000"
    debezium_topic_compression_type: str = "snappy"
    debezium_lob_fetch_size: str = "0"
    debezium_lob_fetch_buffer_size: str = "0"

    @classmethod
    def from_env(cls) -> AppConfig:
        """Build config from environment variables."""

        def _bool(key: str, default: str) -> bool:
            return os.environ.get(key, default).lower() in ("true", "1", "yes")

        return cls(
            state_db_dsn=os.environ.get("STATE_DB_DSN", cls.state_db_dsn),
            pg_pool_min=int(os.environ.get("PG_POOL_MIN", str(cls.pg_pool_min))),
            pg_pool_max=int(os.environ.get("PG_POOL_MAX", str(cls.pg_pool_max))),
            flask_host=os.environ.get("FLASK_HOST", cls.flask_host),
            flask_port=int(os.environ.get("FLASK_PORT", str(cls.flask_port))),
            flask_debug=_bool("FLASK_DEBUG", str(cls.flask_debug)),
            orchestrator_tick_interval=int(os.environ.get("ORCHESTRATOR_TICK_INTERVAL", str(cls.orchestrator_tick_interval))),
            orchestrator_start_delay=int(os.environ.get("ORCHESTRATOR_START_DELAY", str(cls.orchestrator_start_delay))),
            cdc_heartbeat_stale_minutes=int(os.environ.get("CDC_HEARTBEAT_STALE_MINUTES", str(cls.cdc_heartbeat_stale_minutes))),
            status_poller_interval=int(os.environ.get("STATUS_POLLER_INTERVAL", str(cls.status_poller_interval))),
            status_poller_initial_delay=int(os.environ.get("STATUS_POLLER_INITIAL_DELAY", str(cls.status_poller_initial_delay))),
            kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", cls.kafka_bootstrap_servers),
            debezium_lob_enabled=_bool("DEBEZIUM_LOB_ENABLED", str(cls.debezium_lob_enabled)),
            debezium_log_mining_batch_size_max=os.environ.get("DEBEZIUM_LOG_MINING_BATCH_SIZE_MAX", cls.debezium_log_mining_batch_size_max),
            debezium_log_mining_sleep_time_increment_ms=os.environ.get("DEBEZIUM_LOG_MINING_SLEEP_TIME_INCREMENT_MS", cls.debezium_log_mining_sleep_time_increment_ms),
            debezium_log_mining_sleep_time_max_ms=os.environ.get("DEBEZIUM_LOG_MINING_SLEEP_TIME_MAX_MS", cls.debezium_log_mining_sleep_time_max_ms),
            debezium_topic_replication_factor=os.environ.get("DEBEZIUM_TOPIC_REPLICATION_FACTOR", cls.debezium_topic_replication_factor),
            debezium_topic_partitions=os.environ.get("DEBEZIUM_TOPIC_PARTITIONS", cls.debezium_topic_partitions),
            debezium_topic_cleanup_policy=os.environ.get("DEBEZIUM_TOPIC_CLEANUP_POLICY", cls.debezium_topic_cleanup_policy),
            debezium_topic_retention_ms=os.environ.get("DEBEZIUM_TOPIC_RETENTION_MS", cls.debezium_topic_retention_ms),
            debezium_topic_compression_type=os.environ.get("DEBEZIUM_TOPIC_COMPRESSION_TYPE", cls.debezium_topic_compression_type),
            debezium_lob_fetch_size=os.environ.get("DEBEZIUM_LOB_FETCH_SIZE", cls.debezium_lob_fetch_size),
            debezium_lob_fetch_buffer_size=os.environ.get("DEBEZIUM_LOB_FETCH_BUFFER_SIZE", cls.debezium_lob_fetch_buffer_size),
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m pytest tests/test_config.py -v`
Expected: 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add backend/config.py backend/tests/test_config.py
git commit -m "feat: add AppConfig dataclass with env-based configuration"
```

---

### Task 3: Enums

**Files:**
- Create: `backend/models/enums.py`
- Create: `backend/tests/test_enums.py`

- [ ] **Step 1: Write the failing test**

```python
# backend/tests/test_enums.py
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
    """Phase values work as plain strings (for backward compat with existing code)."""
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
    """Ensure all 30 phases are defined."""
    assert len(Phase) == 30


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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m pytest tests/test_enums.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'models.enums'`

- [ ] **Step 3: Write minimal implementation**

```python
# backend/models/enums.py
from __future__ import annotations

from enum import StrEnum


class Phase(StrEnum):
    """Migration lifecycle phases."""

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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m pytest tests/test_enums.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add backend/models/enums.py backend/tests/test_enums.py
git commit -m "feat: add Phase, ChunkStatus, MigrationStrategy, MigrationMode, GroupStatus enums"
```

---

### Task 4: Logging setup

**Files:**
- Create: `backend/logging_setup.py`
- Create: `backend/tests/test_logging_setup.py`

- [ ] **Step 1: Write the failing test**

```python
# backend/tests/test_logging_setup.py
import structlog
from logging_setup import setup_logging


def test_setup_configures_structlog():
    setup_logging()
    log = structlog.get_logger()
    # Should not raise — structlog is configured
    assert log is not None


def test_logger_binds_context():
    setup_logging()
    log = structlog.get_logger().bind(migration_id="abc-123")
    assert log is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m pytest tests/test_logging_setup.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'logging_setup'`

- [ ] **Step 3: Write minimal implementation**

```python
# backend/logging_setup.py
from __future__ import annotations

import structlog


def setup_logging(*, json_output: bool = False) -> None:
    """Configure structlog for the application.

    Args:
        json_output: If True, render logs as JSON (for production).
                     If False, use colored console output (for development).
    """
    renderer = (
        structlog.processors.JSONRenderer()
        if json_output
        else structlog.dev.ConsoleRenderer()
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(0),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m pytest tests/test_logging_setup.py -v`
Expected: 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add backend/logging_setup.py backend/tests/test_logging_setup.py
git commit -m "feat: add structlog-based logging setup"
```

---

### Task 5: Pydantic models — Migration domain

**Files:**
- Create: `backend/models/migration.py`
- Create: `backend/tests/test_models_migration.py`

- [ ] **Step 1: Write the failing test**

```python
# backend/tests/test_models_migration.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m pytest tests/test_models_migration.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'models.migration'`

- [ ] **Step 3: Write minimal implementation**

```python
# backend/models/migration.py
from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from models.enums import (
    ChunkStatus,
    ChunkType,
    MigrationMode,
    MigrationStrategy,
    Phase,
)


class Migration(BaseModel):
    """Domain model for a migration row."""

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

    # Progress / monitoring
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
    """Domain model for a migration_chunks row."""

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
    """Domain model for migration_cdc_state row."""

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
    """Domain model for migration_state_history row."""

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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m pytest tests/test_models_migration.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add backend/models/migration.py backend/tests/test_models_migration.py
git commit -m "feat: add Pydantic models for Migration, MigrationChunk, CdcState, StateHistoryEntry"
```

---

### Task 6: Pydantic models — Connector groups, Catalog, Plans, DataCompare, Checklist

**Files:**
- Create: `backend/models/connector_group.py`
- Create: `backend/models/catalog.py`
- Create: `backend/models/plan.py`
- Create: `backend/models/data_compare.py`
- Create: `backend/models/checklist.py`
- Create: `backend/tests/test_models_other.py`

- [ ] **Step 1: Write the failing test**

```python
# backend/tests/test_models_other.py
from datetime import datetime, timezone
from uuid import uuid4

from models.connector_group import ConnectorGroup, GroupTable, GroupStateHistory
from models.catalog import DdlSnapshot, DdlObject, DdlCompareResult
from models.plan import MigrationPlan, MigrationPlanItem
from models.data_compare import DataCompareTask, DataCompareChunk
from models.checklist import ChecklistList, ChecklistItem
from models.enums import GroupStatus


def test_connector_group():
    g = ConnectorGroup.model_validate({
        "group_id": str(uuid4()),
        "group_name": "group-1",
        "source_connection_id": "oracle_source",
        "connector_name": "cdc-group-1",
        "topic_prefix": "cdc",
        "status": "RUNNING",
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    })
    assert g.status == GroupStatus.RUNNING


def test_group_table():
    t = GroupTable.model_validate({
        "id": str(uuid4()),
        "group_id": str(uuid4()),
        "source_schema": "HR",
        "source_table": "EMPLOYEES",
        "target_schema": "hr",
        "target_table": "employees",
        "created_at": datetime.now(timezone.utc),
    })
    assert t.source_schema == "HR"


def test_ddl_snapshot():
    s = DdlSnapshot.model_validate({
        "snapshot_id": 1,
        "src_schema": "HR",
        "tgt_schema": "hr",
        "loaded_at": datetime.now(timezone.utc),
    })
    assert s.snapshot_id == 1


def test_ddl_object():
    o = DdlObject.model_validate({
        "id": 1,
        "snapshot_id": 1,
        "db_side": "source",
        "object_type": "TABLE",
        "object_name": "EMPLOYEES",
    })
    assert o.object_type == "TABLE"


def test_ddl_compare_result():
    r = DdlCompareResult.model_validate({
        "id": 1,
        "snapshot_id": 1,
        "object_type": "TABLE",
        "object_name": "EMPLOYEES",
        "match_status": "MATCH",
    })
    assert r.match_status == "MATCH"


def test_migration_plan():
    p = MigrationPlan.model_validate({
        "plan_id": 1,
        "name": "plan-1",
        "src_schema": "HR",
        "tgt_schema": "hr",
        "status": "DRAFT",
    })
    assert p.status == "DRAFT"


def test_migration_plan_item():
    i = MigrationPlanItem.model_validate({
        "item_id": 1,
        "plan_id": 1,
        "table_name": "EMPLOYEES",
        "mode": "CDC",
        "batch_order": 1,
        "sort_order": 0,
        "status": "PENDING",
    })
    assert i.table_name == "EMPLOYEES"


def test_data_compare_task():
    t = DataCompareTask.model_validate({
        "task_id": str(uuid4()),
        "source_schema": "HR",
        "source_table": "EMPLOYEES",
        "target_schema": "hr",
        "target_table": "employees",
        "status": "PENDING",
        "created_at": datetime.now(timezone.utc),
    })
    assert t.status == "PENDING"


def test_data_compare_chunk():
    c = DataCompareChunk.model_validate({
        "chunk_id": str(uuid4()),
        "task_id": str(uuid4()),
        "side": "source",
        "chunk_seq": 1,
        "rowid_start": "AAABcAAEAAAAIjAAA",
        "rowid_end": "AAABcAAEAAAAIjAAZ",
        "status": "PENDING",
        "created_at": datetime.now(timezone.utc),
    })
    assert c.side == "source"


def test_checklist_list():
    cl = ChecklistList.model_validate({
        "list_id": 1,
        "name": "pre-migration",
        "created_at": datetime.now(timezone.utc),
    })
    assert cl.name == "pre-migration"


def test_checklist_item():
    ci = ChecklistItem.model_validate({
        "item_id": 1,
        "list_id": 1,
        "table_name": "EMPLOYEES",
        "decision": "migrate",
        "status": "pending",
        "created_at": datetime.now(timezone.utc),
    })
    assert ci.decision == "migrate"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m pytest tests/test_models_other.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write all model files**

```python
# backend/models/connector_group.py
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict

from models.enums import GroupStatus


class ConnectorGroup(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    group_id: str
    group_name: str
    source_connection_id: str = "oracle_source"
    connector_name: str
    topic_prefix: str
    consumer_group_prefix: str = ""
    status: GroupStatus = GroupStatus.PENDING
    error_text: str | None = None
    connector_config_json: dict[str, Any] | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    run_id: str = ""


class GroupTable(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    group_id: str
    source_schema: str
    source_table: str
    target_schema: str
    target_table: str
    effective_key_type: str = "NONE"
    effective_key_columns_json: str = "[]"
    source_pk_exists: bool = False
    source_uk_exists: bool = False
    topic_name: str = ""
    created_at: datetime | None = None


class GroupStateHistory(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    group_id: str
    from_status: str | None = None
    to_status: str
    message: str | None = None
    created_at: datetime | None = None
```

```python
# backend/models/catalog.py
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


class DdlSnapshot(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    snapshot_id: int
    src_schema: str
    tgt_schema: str
    loaded_at: datetime | None = None


class DdlObject(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    snapshot_id: int
    db_side: str
    object_type: str
    object_name: str
    oracle_status: str | None = None
    last_ddl_time: datetime | None = None
    metadata: dict[str, Any] | None = None


class DdlCompareResult(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    snapshot_id: int
    object_type: str
    object_name: str
    match_status: str = "UNKNOWN"
    diff: dict[str, Any] | None = None
```

```python
# backend/models/plan.py
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


class MigrationPlan(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    plan_id: int
    name: str
    src_schema: str
    tgt_schema: str
    connector_group_id: str | None = None
    defaults_json: dict[str, Any] | None = None
    status: str = "DRAFT"
    created_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


class MigrationPlanItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    item_id: int
    plan_id: int
    table_name: str
    mode: str = "CDC"
    batch_order: int = 1
    sort_order: int = 0
    overrides_json: dict[str, Any] | None = None
    migration_id: str | None = None
    status: str = "PENDING"
```

```python
# backend/models/data_compare.py
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class DataCompareTask(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    task_id: str
    source_schema: str
    source_table: str
    target_schema: str
    target_table: str
    compare_mode: str = "full"
    last_n: int | None = None
    order_column: str | None = None
    chunk_size: int = 100000
    status: str = "PENDING"
    source_count: int | None = None
    target_count: int | None = None
    source_hash: Decimal | None = None
    target_hash: Decimal | None = None
    counts_match: bool | None = None
    hash_match: bool | None = None
    chunks_total: int = 0
    chunks_done: int = 0
    error_text: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    created_at: datetime | None = None


class DataCompareChunk(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    chunk_id: str
    task_id: str
    side: str
    chunk_seq: int
    rowid_start: str
    rowid_end: str
    status: str = "PENDING"
    row_count: int | None = None
    hash_sum: Decimal | None = None
    worker_id: str | None = None
    claimed_at: datetime | None = None
    completed_at: datetime | None = None
    error_text: str | None = None
    retry_count: int = 0
    created_at: datetime | None = None
```

```python
# backend/models/checklist.py
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class ChecklistList(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    list_id: int
    name: str
    created_at: datetime | None = None


class ChecklistItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    item_id: int
    list_id: int
    schema_name: str = ""
    table_name: str
    decision: str = "migrate"
    status: str = "pending"
    created_at: datetime | None = None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m pytest tests/test_models_other.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add backend/models/connector_group.py backend/models/catalog.py backend/models/plan.py backend/models/data_compare.py backend/models/checklist.py backend/tests/test_models_other.py
git commit -m "feat: add Pydantic models for ConnectorGroup, Catalog, Plan, DataCompare, Checklist"
```

---

### Task 7: API Schemas — Common

**Files:**
- Create: `backend/schemas/common.py`
- Create: `backend/tests/test_schemas_common.py`

- [ ] **Step 1: Write the failing test**

```python
# backend/tests/test_schemas_common.py
import pytest
from pydantic import ValidationError
from schemas.common import PaginationParams, ErrorResponse, validate_request_data


def test_pagination_defaults():
    p = PaginationParams()
    assert p.page == 1
    assert p.page_size == 100


def test_pagination_clamps():
    p = PaginationParams(page=0, page_size=9999)
    assert p.page == 1
    assert p.page_size == 500


def test_pagination_valid():
    p = PaginationParams(page=3, page_size=50)
    assert p.page == 3
    assert p.page_size == 50


def test_error_response():
    e = ErrorResponse(error="something went wrong")
    assert e.error == "something went wrong"


def test_validate_request_data_valid():
    result = validate_request_data(PaginationParams, {"page": 2, "page_size": 50})
    assert result.page == 2


def test_validate_request_data_invalid():
    with pytest.raises(ValueError):
        validate_request_data(PaginationParams, "not a dict")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m pytest tests/test_schemas_common.py -v`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

```python
# backend/schemas/common.py
from __future__ import annotations

from pydantic import BaseModel, field_validator


class PaginationParams(BaseModel):
    page: int = 1
    page_size: int = 100

    @field_validator("page", mode="after")
    @classmethod
    def _clamp_page(cls, v: int) -> int:
        return max(1, v)

    @field_validator("page_size", mode="after")
    @classmethod
    def _clamp_page_size(cls, v: int) -> int:
        return max(1, min(500, v))


class ErrorResponse(BaseModel):
    error: str


def validate_request_data(schema_cls: type[BaseModel], data: dict | None) -> BaseModel:
    """Validate request data against a Pydantic schema.

    Raises ValueError if data is not a dict or validation fails.
    In Flask routes, catch ValueError and return 400.
    """
    if not isinstance(data, dict):
        raise ValueError("Request body must be a JSON object")
    return schema_cls.model_validate(data)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m pytest tests/test_schemas_common.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add backend/schemas/common.py backend/tests/test_schemas_common.py
git commit -m "feat: add common API schemas (PaginationParams, ErrorResponse, validate_request_data)"
```

---

### Task 8: API Schemas — Migrations

**Files:**
- Create: `backend/schemas/migration_schemas.py`
- Create: `backend/tests/test_schemas_migration.py`

- [ ] **Step 1: Write the failing test**

```python
# backend/tests/test_schemas_migration.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m pytest tests/test_schemas_migration.py -v`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

```python
# backend/schemas/migration_schemas.py
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from models.enums import MigrationMode, MigrationStrategy, Phase

# Valid actions for the action endpoint
VALID_ACTIONS = frozenset({
    "run", "pause", "resume", "cancel",
    "lag_zero", "retry_verify", "force_complete",
})

# Action transition table: action -> (required_from_phase or None, to_phase)
ACTION_TRANSITIONS: dict[str, tuple[str | None, str]] = {
    "run":            (None,              "NEW"),
    "pause":          (None,              "PAUSED"),
    "resume":         ("PAUSED",          "BULK_LOADING"),
    "cancel":         (None,              "CANCELLING"),
    "lag_zero":       ("CDC_CATCHING_UP", "CDC_CAUGHT_UP"),
    "retry_verify":   ("DATA_MISMATCH",   "DATA_VERIFYING"),
    "force_complete": ("DATA_MISMATCH",   "COMPLETED"),
}

# Phases that allow deletion
DELETABLE_PHASES = frozenset({"DRAFT", "CANCELLING", "CANCELLED", "FAILED"})


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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m pytest tests/test_schemas_migration.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add backend/schemas/migration_schemas.py backend/tests/test_schemas_migration.py
git commit -m "feat: add API schemas for migration endpoints"
```

---

### Task 9: API Schemas — Connector groups, Catalog

**Files:**
- Create: `backend/schemas/connector_schemas.py`
- Create: `backend/schemas/catalog_schemas.py`
- Create: `backend/tests/test_schemas_other.py`

- [ ] **Step 1: Write the failing test**

```python
# backend/tests/test_schemas_other.py
import pytest
from pydantic import ValidationError

from schemas.connector_schemas import CreateGroupRequest, AddGroupTableRequest
from schemas.catalog_schemas import SnapshotRequest


def test_create_group_minimal():
    req = CreateGroupRequest(
        group_name="group-1",
        connector_name="cdc-group-1",
        topic_prefix="cdc",
    )
    assert req.group_name == "group-1"
    assert req.source_connection_id == "oracle_source"


def test_create_group_name_required():
    with pytest.raises(ValidationError):
        CreateGroupRequest(
            group_name="",
            connector_name="cdc-group-1",
            topic_prefix="cdc",
        )


def test_add_group_table():
    req = AddGroupTableRequest(
        source_schema="HR",
        source_table="EMPLOYEES",
        target_schema="hr",
        target_table="employees",
    )
    assert req.source_schema == "HR"


def test_snapshot_request():
    req = SnapshotRequest(
        src_schema="HR",
        tgt_schema="hr",
    )
    assert req.src_schema == "HR"


def test_snapshot_request_required():
    with pytest.raises(ValidationError):
        SnapshotRequest(src_schema="HR")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m pytest tests/test_schemas_other.py -v`
Expected: FAIL

- [ ] **Step 3: Write implementations**

```python
# backend/schemas/connector_schemas.py
from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class CreateGroupRequest(BaseModel):
    group_name: str = Field(min_length=1, max_length=255)
    source_connection_id: str = "oracle_source"
    connector_name: str = Field(min_length=1, max_length=255)
    topic_prefix: str = Field(min_length=1, max_length=255)
    consumer_group_prefix: str = ""

    @field_validator("group_name", mode="before")
    @classmethod
    def _strip_name(cls, v: str) -> str:
        return v.strip() if isinstance(v, str) else v


class AddGroupTableRequest(BaseModel):
    source_schema: str = Field(min_length=1)
    source_table: str = Field(min_length=1)
    target_schema: str = Field(min_length=1)
    target_table: str = Field(min_length=1)
    effective_key_type: str = "NONE"
    effective_key_columns_json: str = "[]"
    source_pk_exists: bool = False
    source_uk_exists: bool = False
```

```python
# backend/schemas/catalog_schemas.py
from __future__ import annotations

from pydantic import BaseModel, Field


class SnapshotRequest(BaseModel):
    src_schema: str = Field(min_length=1)
    tgt_schema: str = Field(min_length=1)
    source_connection_id: str = "oracle_source"
    target_connection_id: str = "oracle_target"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m pytest tests/test_schemas_other.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add backend/schemas/connector_schemas.py backend/schemas/catalog_schemas.py backend/tests/test_schemas_other.py
git commit -m "feat: add API schemas for connector groups and catalog endpoints"
```

---

### Task 10: Export models and schemas from __init__.py

**Files:**
- Modify: `backend/models/__init__.py`
- Modify: `backend/schemas/__init__.py`

- [ ] **Step 1: Update models/__init__.py**

```python
# backend/models/__init__.py
from models.enums import (
    ChunkStatus,
    ChunkType,
    GroupStatus,
    MigrationMode,
    MigrationStrategy,
    Phase,
)
from models.migration import CdcState, Migration, MigrationChunk, StateHistoryEntry
from models.connector_group import ConnectorGroup, GroupStateHistory, GroupTable
from models.catalog import DdlCompareResult, DdlObject, DdlSnapshot
from models.plan import MigrationPlan, MigrationPlanItem
from models.data_compare import DataCompareChunk, DataCompareTask
from models.checklist import ChecklistItem, ChecklistList

__all__ = [
    "Phase", "ChunkStatus", "ChunkType", "MigrationStrategy", "MigrationMode", "GroupStatus",
    "Migration", "MigrationChunk", "CdcState", "StateHistoryEntry",
    "ConnectorGroup", "GroupTable", "GroupStateHistory",
    "DdlSnapshot", "DdlObject", "DdlCompareResult",
    "MigrationPlan", "MigrationPlanItem",
    "DataCompareTask", "DataCompareChunk",
    "ChecklistList", "ChecklistItem",
]
```

- [ ] **Step 2: Update schemas/__init__.py**

```python
# backend/schemas/__init__.py
from schemas.common import ErrorResponse, PaginationParams, validate_request_data
from schemas.migration_schemas import (
    ACTION_TRANSITIONS,
    DELETABLE_PHASES,
    CreateMigrationRequest,
    MigrationActionRequest,
    TransitionPhaseRequest,
    UpdateWorkersRequest,
)
from schemas.connector_schemas import AddGroupTableRequest, CreateGroupRequest
from schemas.catalog_schemas import SnapshotRequest

__all__ = [
    "PaginationParams", "ErrorResponse", "validate_request_data",
    "CreateMigrationRequest", "MigrationActionRequest", "TransitionPhaseRequest",
    "UpdateWorkersRequest", "ACTION_TRANSITIONS", "DELETABLE_PHASES",
    "CreateGroupRequest", "AddGroupTableRequest",
    "SnapshotRequest",
]
```

- [ ] **Step 3: Run full test suite**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m pytest tests/ -v`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add backend/models/__init__.py backend/schemas/__init__.py
git commit -m "feat: export all models and schemas from package __init__"
```

---

### Task 11: Alembic initialization

**Files:**
- Create: `backend/alembic.ini`
- Create: `backend/alembic/env.py`
- Create: `backend/alembic/script.py.mako`
- Create: `backend/alembic/versions/` (directory)

- [ ] **Step 1: Initialize Alembic**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m alembic init alembic`
Expected: Creates alembic/ directory and alembic.ini

- [ ] **Step 2: Configure alembic.ini**

Edit `backend/alembic.ini` to set the sqlalchemy.url:

Change:
```
sqlalchemy.url = driver://user:pass@localhost/dbname
```
To:
```
# Overridden by env.py from STATE_DB_DSN environment variable
sqlalchemy.url =
```

- [ ] **Step 3: Update alembic/env.py for dynamic DSN**

Replace the content of `backend/alembic/env.py` with:

```python
import os
from logging.config import fileConfig

from alembic import context

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Use STATE_DB_DSN env var for database URL
db_url = os.environ.get(
    "STATE_DB_DSN",
    "postgresql://postgres:postgres@localhost:5432/migration_state",
)
config.set_main_option("sqlalchemy.url", db_url)

target_metadata = None


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    from sqlalchemy import create_engine

    connectable = create_engine(config.get_main_option("sqlalchemy.url"))
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

- [ ] **Step 4: Verify Alembic configuration**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m alembic heads`
Expected: No errors (may show empty heads since no migrations yet)

- [ ] **Step 5: Commit**

```bash
git add backend/alembic.ini backend/alembic/
git commit -m "chore: initialize Alembic for database schema migrations"
```

---

### Task 12: Run full test suite and final verification

- [ ] **Step 1: Run all tests**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -m pytest tests/ -v --tb=short`
Expected: All tests PASS (approximately 25-30 tests)

- [ ] **Step 2: Verify imports work**

Run: `cd /mnt/c/work/database_migration/new/front/backend && python -c "from models import Phase, Migration, ConnectorGroup; from schemas import CreateMigrationRequest, PaginationParams; from config import AppConfig; from logging_setup import setup_logging; print('All imports OK')" `
Expected: "All imports OK"

- [ ] **Step 3: Verify directory structure**

Run: `find backend/models backend/schemas backend/tests backend/config.py backend/logging_setup.py -type f | sort`
Expected output:
```
backend/config.py
backend/logging_setup.py
backend/models/__init__.py
backend/models/catalog.py
backend/models/checklist.py
backend/models/connector_group.py
backend/models/data_compare.py
backend/models/enums.py
backend/models/migration.py
backend/models/plan.py
backend/schemas/__init__.py
backend/schemas/catalog_schemas.py
backend/schemas/common.py
backend/schemas/connector_schemas.py
backend/schemas/migration_schemas.py
backend/tests/__init__.py
backend/tests/conftest.py
backend/tests/test_config.py
backend/tests/test_enums.py
backend/tests/test_logging_setup.py
backend/tests/test_models_migration.py
backend/tests/test_models_other.py
backend/tests/test_schemas_common.py
backend/tests/test_schemas_migration.py
backend/tests/test_schemas_other.py
```
