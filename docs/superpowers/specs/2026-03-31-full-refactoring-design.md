# Full Project Refactoring — Design Spec

**Date:** 2026-03-31
**Scope:** Backend + Frontend
**Approach:** Iterative (6 iterations, each producing a working commit)

## Constraints

- API contract may change (frontend adapts in later iterations)
- DB schema may change (via Alembic migrations)
- New dependencies allowed: pydantic, structlog, alembic (backend); react-router-dom (frontend)
- Target file size: ≤300 LOC per file

---

## Iteration 1: Infrastructure (Backend)

### 1.1 Enums (models/enums.py)

All string-literal phases, statuses, and strategies become `StrEnum`:

- `Phase` — 25 values: NEW, PREPARING, SCN_FIXED, CONNECTOR_STARTING, CDC_BUFFERING, CHUNKING, BULK_LOADING, BULK_LOADED, STAGE_VALIDATING, STAGE_VALIDATED, BASELINE_PUBLISHING, BASELINE_LOADING, BASELINE_PUBLISHED, STAGE_DROPPING, INDEXES_ENABLING, CDC_APPLY_STARTING, CDC_APPLYING, CDC_CATCHING_UP, CDC_CAUGHT_UP, STEADY_STATE, DATA_VERIFYING, DATA_MISMATCH, COMPLETED, FAILED, CANCELLED
- `ChunkStatus` — PENDING, CLAIMED, RUNNING, DONE, FAILED
- `MigrationStrategy` — FULL, BULK_ONLY

### 1.2 Pydantic Models (models/)

Domain models for internal use (DB row → model):

- `models/migration.py` — Migration, MigrationChunk, CdcState
- `models/connector_group.py` — ConnectorGroup, GroupTable
- `models/catalog.py` — DdlSnapshot, DdlObject
- `models/plan.py` — MigrationPlan, PlanItem
- `models/data_compare.py` — DataCompareTask, DataCompareChunk
- `models/checklist.py` — ChecklistList, ChecklistItem

### 1.3 API Schemas (schemas/)

Pydantic schemas for request/response validation:

- `schemas/migration_schemas.py` — CreateMigrationRequest, MigrationActionRequest, etc.
- `schemas/connector_schemas.py` — CreateGroupRequest, etc.
- `schemas/catalog_schemas.py` — SnapshotRequest, etc.
- `schemas/common.py` — PaginationParams, ErrorResponse

### 1.4 Logging (logging_setup.py)

Replace all `print()` with structlog:

```python
import structlog

def setup_logging():
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.dev.ConsoleRenderer(),
        ],
    )
```

### 1.5 Config (config.py)

Centralize env-based config into a dataclass:

```python
@dataclass
class AppConfig:
    pg_dsn: str
    pg_pool_min: int = 2
    pg_pool_max: int = 10
    tick_interval: int = 5
    stale_worker_minutes: int = 10
    # ...
```

### 1.6 Alembic

Initialize Alembic for DB schema migrations. Initial migration captures the current schema as-is.

---

## Iteration 2: Orchestrator Decomposition (Backend)

Split `orchestrator.py` (1,811 LOC) into ~8 files:

### 2.1 orchestrator/engine.py (~150 LOC)

Core loop:
- `OrchestratorEngine` class with `start()`, `tick()`, `transition()`, `run_in_thread()`
- Holds `_in_progress` dict, lock, config, db reference, sse broadcaster
- Delegates phase handling to `PhaseDispatcher`

### 2.2 orchestrator/transition.py (~80 LOC)

- `transition()` — write phase change to DB + history + SSE broadcast
- `safe_transition()` — check current phase before transitioning (race protection)

### 2.3 orchestrator/queue.py (~60 LOC)

- `HEAVY_PHASES` set — phases requiring exclusive resources
- `ordered()` — sort migrations, gate heavy phases (one at a time)

### 2.4 Phase Handlers (~200-300 LOC each)

Each handler class receives `engine` reference for `transition()`, `run_in_thread()`, `db`, `sse`:

| File | Phases Covered |
|------|---------------|
| `phases/preparing.py` | NEW → PREPARING → SCN_FIXED → CONNECTOR_STARTING → CDC_BUFFERING |
| `phases/chunking.py` | CHUNKING → BULK_LOADING → BULK_LOADED |
| `phases/baseline.py` | STAGE_VALIDATING → BASELINE_PUBLISHING → BASELINE_PUBLISHED |
| `phases/cdc.py` | CDC_APPLY_STARTING → CDC_APPLYING → CDC_CATCHING_UP → CDC_CAUGHT_UP → STEADY_STATE |
| `phases/data_verify.py` | DATA_VERIFYING → DATA_MISMATCH |
| `phases/cleanup.py` | STAGE_DROPPING, INDEXES_ENABLING, trigger operations |

### 2.5 PhaseDispatcher

Dispatch table mapping `Phase` → handler method, replacing if/elif chains.

---

## Iteration 3: Data Layer (Backend)

Split `state_db.py` (748 LOC) into pool + repositories:

### 3.1 db/pool.py (~80 LOC)

- `DatabasePool` class wrapping `ThreadedConnectionPool`
- Context manager `connection()` with auto commit/rollback
- `available` property for health checks

### 3.2 db/base_repo.py (~30 LOC)

- `BaseRepo` with `_fetch_one()`, `_fetch_all()`, `_execute()` helpers

### 3.3 Domain Repositories

Each repo returns Pydantic models, not raw dicts:

| File | Tables | ~LOC |
|------|--------|------|
| `migrations_repo.py` | migrations, migration_chunks, migration_cdc_state, migration_state_history | 250 |
| `connector_groups_repo.py` | connector_groups, group_tables, group_state_history | 120 |
| `catalog_repo.py` | ddl_snapshots, ddl_objects | 100 |
| `plan_repo.py` | migration_plans, migration_plan_items | 80 |
| `data_compare_repo.py` | data_compare_tasks, data_compare_chunks | 80 |
| `checklist_repo.py` | checklist_lists, checklist_items | 60 |

### 3.4 db/schema.py (~100 LOC)

Table initialization (CREATE TABLE IF NOT EXISTS), extracted from state_db.

### 3.5 Database Facade

```python
class Database:
    def __init__(self, pool: DatabasePool):
        self.migrations = MigrationsRepo(pool)
        self.connector_groups = ConnectorGroupsRepo(pool)
        self.catalog = CatalogRepo(pool)
        self.plans = PlanRepo(pool)
        self.data_compare = DataCompareRepo(pool)
        self.checklists = ChecklistRepo(pool)
```

---

## Iteration 4: Routes Refactoring (Backend)

Thin routes: validate schema → call service → return response.

### 4.1 Request Validation Helper

```python
def validate_request(schema_cls):
    try:
        return schema_cls.model_validate(request.json or {})
    except ValidationError as e:
        abort(400, description=e.errors())
```

### 4.2 Unified Error Handlers

Global Flask error handlers for 400, 404, 500 with JSON responses and structured logging.

### 4.3 Route Slimming

| File | Before LOC | After LOC | What moves out |
|------|-----------|-----------|----------------|
| migrations.py | 783 | ~250 | SQL → repo, validation → schema, logic → service |
| target_prep.py | 590 | ~200 | DDL logic stays in services |
| data_compare.py | 561 | ~180 | Same pattern |
| connector_groups.py | 467 | ~150 | CRUD → repo |
| planner.py | 458 | ~150 | Sort/FK logic → service |
| catalog.py | 436 | ~150 | Snapshot logic → service |
| workers.py | 234 | ~100 | Chunk claiming → job_queue |
| checklist.py | 170 | ~80 | Direct CRUD |
| sse.py | 109 | ~80 | Minimal changes |
| config.py | 85 | ~60 | Minimal changes |
| oracle_db.py | 59 | ~50 | Minimal changes |

**Total routes: ~3,950 → ~1,450 LOC**

---

## Iteration 5: Frontend Core

### 5.1 API Client (src/api/)

- `api/client.ts` (~50 LOC) — typed HTTP client with error handling
- `api/migrations.ts` — migrationsApi: list, get, create, action, chunks
- `api/connectors.ts` — connectorsApi: list, get, create, tables, topics
- `api/catalog.ts` — catalogApi: snapshots, objects, compare, sync
- `api/planner.ts`, `api/targetPrep.ts`, `api/dataCompare.ts`, `api/checklist.ts`

### 5.2 Shared UI Components (src/components/ui/)

| Component | Purpose |
|-----------|---------|
| `SearchSelect.tsx` | Replaces 3+ duplicated implementations |
| `Modal.tsx` | Shared overlay with close, title |
| `Button.tsx` | Variants: primary, danger, outline, ghost |
| `Badge.tsx` | Unified phase/status badge |
| `Panel.tsx` | Card container |
| `Spinner.tsx` | Loading indicator |

### 5.3 Utilities (src/utils/)

- `utils/format.ts` — fmtTs, fmtNum, fmtDuration, fmtBytes (deduplicated)
- `utils/phases.ts` — BULK_PHASES, ACTIVE_PHASES, TERMINAL_PHASES sets

### 5.4 Custom Hooks (src/hooks/)

| Hook | Purpose |
|------|---------|
| `useApi.ts` | Fetch with loading/error/data + AbortController cancellation |
| `usePolling.ts` | Reusable polling with configurable interval |
| `useMigrations.ts` | Migration list + refresh |
| `useMigrationDetail.ts` | Single migration details + chunks + polling |

### 5.5 Theme (src/theme.ts)

Centralized color tokens:
- `bg` — primary, secondary, card
- `text` — primary, secondary, muted
- `border` — default, active
- `accent` — blue, green, red, yellow

---

## Iteration 6: Frontend Components

### 6.1 Component Decomposition

**MigrationDetail (1,728 LOC) →**
- `MigrationDetail.tsx` (~200) — container, phase-based section selection
- `MigrationProperties.tsx` (~150) — core properties, editing
- `MigrationStatistics.tsx` (~150) — statistics, counters
- `ChunksTable.tsx` (~200) — chunks table with pagination
- `PhaseActions.tsx` (~100) — action buttons per phase

**PlannerWizard (1,327 LOC) →**
- `PlannerWizard.tsx` (~150) — wizard steps
- `SchemaCompareStep.tsx` (~200) — schema comparison
- `KeySelectionStep.tsx` (~200) — table key selection
- `BatchManagement.tsx` (~200) — batch management
- `PlanReview.tsx` (~150) — final plan review

**TargetPrep (1,274 LOC) →**
- `TargetPrep.tsx` (~150) — container
- `DdlCompare.tsx` (~200) — DDL comparison
- `ColumnSync.tsx` (~200) — column synchronization
- `ConstraintSync.tsx` (~200) — constraint synchronization

**CreateMigrationModal (1,127 LOC) →**
- `CreateMigrationModal.tsx` (~150) — modal steps
- `ConnectionStep.tsx` (~150) — source/target selection
- `TableSelectionStep.tsx` (~200) — table selection
- `SettingsStep.tsx` (~150) — migration settings

### 6.2 React Router

Replace manual tab switching with react-router-dom:

| Route | Component |
|-------|-----------|
| `/` | MigrationList |
| `/migrations/:id` | MigrationDetail |
| `/catalog` | DDLCatalog |
| `/connector-groups` | ConnectorGroupsPanel |
| `/target-prep` | TargetPrep |
| `/data-compare` | DataCompare |
| `/checklist` | Checklist |

---

## Target Architecture Summary

### Backend

```
backend/
  app.py
  config.py
  logging_setup.py
  models/
    enums.py
    migration.py, connector_group.py, catalog.py, plan.py, data_compare.py, checklist.py
  schemas/
    migration_schemas.py, connector_schemas.py, catalog_schemas.py, common.py
  db/
    pool.py, base_repo.py, schema.py
    migrations_repo.py, connector_groups_repo.py, catalog_repo.py
    plan_repo.py, data_compare_repo.py, checklist_repo.py
    oracle_browser.py
    alembic/
  orchestrator/
    engine.py, transition.py, queue.py
    phases/
      preparing.py, chunking.py, baseline.py, cdc.py, data_verify.py, cleanup.py
  services/
    (16 existing files, refactored to use Database facade and Pydantic models)
  routes/
    (11 existing files, slimmed to validation → service → response)
```

### Frontend

```
frontend/src/
  main.tsx
  App.tsx (with React Router)
  theme.ts
  api/
    client.ts, migrations.ts, connectors.ts, catalog.ts, planner.ts,
    targetPrep.ts, dataCompare.ts, checklist.ts
  hooks/
    useSSE.ts, useApi.ts, usePolling.ts, useMigrations.ts, useMigrationDetail.ts
  utils/
    format.ts, phases.ts
  types/
    migration.ts (existing, extended)
  components/
    ui/
      SearchSelect.tsx, Modal.tsx, Button.tsx, Badge.tsx, Panel.tsx, Spinner.tsx
    MigrationList.tsx
    MigrationDetail/
      MigrationDetail.tsx, MigrationProperties.tsx, MigrationStatistics.tsx,
      ChunksTable.tsx, PhaseActions.tsx
    DDLCatalog/
      DDLCatalog.tsx, ObjectTabs.tsx, TablesTab.tsx, ViewsTab.tsx, CodeTab.tsx,
      OtherTab.tsx, ObjectActions.tsx, StatusBadges.tsx, Pagination.tsx
    PlannerWizard/
      PlannerWizard.tsx, SchemaCompareStep.tsx, KeySelectionStep.tsx,
      BatchManagement.tsx, PlanReview.tsx
    TargetPrep/
      TargetPrep.tsx, DdlCompare.tsx, ColumnSync.tsx, ConstraintSync.tsx
    CreateMigrationModal/
      CreateMigrationModal.tsx, ConnectionStep.tsx, TableSelectionStep.tsx,
      SettingsStep.tsx
    ConnectorGroupsPanel.tsx
    DataCompare.tsx
    Checklist.tsx
    EventTable.tsx
    MigrationPanels.tsx
    ServiceStatusBar.tsx
    SettingsModal.tsx
    Stats.tsx
```
