# DDL Catalog — Design Spec

**Date:** 2026-03-30
**Status:** Approved

## Summary

Replace the current MigrationPlanner wizard with a full DDL Catalog tab. The catalog displays all DDL objects from the source Oracle schema, organized by type with sub-tabs, cached in PostgreSQL, with target comparison statuses and migration statuses. The planner wizard (steps 2-4) launches inline for selected tables.

## Architecture

### Overall Layout

```
Tab "DDL Каталог"
├── SchemaSelector (source + target schema dropdowns)
├── CatalogToolbar (Load catalog / Refresh from source / Compare with target)
├── ObjectTabs (sub-tabs by object type)
│   ├── TablesTab — tables with columns/indexes/constraints/triggers
│   ├── ViewsTab — views + materialized views
│   ├── CodeTab — functions, procedures, packages
│   └── OtherTab — sequences, synonyms, types
├── ObjectTable — object list for current type
│   ├── checkboxes for selection
│   ├── statuses (target match / migration)
│   └── actions (compare, create, sync)
└── PlannerWizard (steps 2-4 of current wizard, inline)
```

### Frontend File Structure

```
components/
  DDLCatalog/
    DDLCatalog.tsx          — main component (replaces MigrationPlanner)
    SchemaSelector.tsx      — schema selection + load button
    ObjectTabs.tsx          — sub-tabs by object type
    TablesTab.tsx           — table list with expandable details
    ViewsTab.tsx            — views & materialized views
    CodeTab.tsx             — functions, procedures, packages
    OtherTab.tsx            — sequences, synonyms, types
    ObjectActions.tsx       — action buttons per object type
    StatusBadges.tsx        — status badges
    PlannerWizard.tsx       — steps 2-4 extracted from MigrationPlanner
```

### Backend

```
routes/catalog.py               — new blueprint, 6 endpoints
services/ddl_compare.py         — comparison logic per object type (new)
services/ddl_sync_extended.py   — sync-to-target for non-table objects (new)
db/oracle_browser.py            — extended with 6 new introspection functions
```

### PostgreSQL — New Tables

```sql
ddl_snapshots (
    id SERIAL PRIMARY KEY,
    src_schema TEXT NOT NULL,
    tgt_schema TEXT NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT now()
)

ddl_objects (
    id SERIAL PRIMARY KEY,
    snapshot_id INT REFERENCES ddl_snapshots(id) ON DELETE CASCADE,
    db_side TEXT NOT NULL,          -- 'source' | 'target'
    object_type TEXT NOT NULL,      -- TABLE, VIEW, MVIEW, FUNCTION, PROCEDURE, PACKAGE, SEQUENCE, SYNONYM, TYPE
    object_name TEXT NOT NULL,
    oracle_status TEXT,             -- VALID / INVALID
    last_ddl_time TIMESTAMPTZ,
    metadata JSONB                  -- type-specific details
)

ddl_compare_results (
    id SERIAL PRIMARY KEY,
    snapshot_id INT REFERENCES ddl_snapshots(id) ON DELETE CASCADE,
    object_type TEXT NOT NULL,
    object_name TEXT NOT NULL,
    match_status TEXT NOT NULL,     -- MATCH, DIFF, MISSING, EXTRA, UNKNOWN
    diff JSONB                     -- type-specific diff details
)
```

## Object Types and Metadata

| object_type | metadata (JSONB) |
|---|---|
| TABLE | columns, constraints, indexes, triggers, partitioning, row_count |
| VIEW | sql_text, columns, status |
| MVIEW | sql_text, columns, refresh_type, last_refresh |
| FUNCTION | source_code, status, argument_count |
| PROCEDURE | source_code, status, argument_count |
| PACKAGE | spec_source, body_source, status |
| SEQUENCE | min_value, max_value, increment_by, last_number, cache_size |
| SYNONYM | table_owner, table_name, db_link |
| TYPE | typecode, attributes, source |

## API Endpoints (`routes/catalog.py`)

### `GET /api/catalog/snapshots`
List all snapshots. Returns `[{id, src_schema, tgt_schema, loaded_at}]`.

### `POST /api/catalog/load`
Load full catalog for a schema pair into cache.

**Body:** `{ src_schema, tgt_schema }`

**Process:**
1. `list_all_objects()` for source and target
2. For each object — call corresponding `get_*_info()`
3. Batch insert into `ddl_objects`
4. Auto-compare: match/diff/missing for each object
5. Write `ddl_compare_results`

**Returns:** `{ snapshot_id, object_counts: {TABLE: 124, VIEW: 12, ...} }`

### `GET /api/catalog/objects`
List objects by type.

**Query params:** `snapshot_id`, `type` (TABLE, VIEW, etc.)

**Returns:** `[{object_name, oracle_status, last_ddl_time, match_status, migration_status, metadata}]`

`migration_status` computed via join with `migrations` table:
- No record → `NONE`
- DRAFT/NEW/PREPARING → `PLANNED`
- Active phases → `IN_PROGRESS`
- COMPLETED → `COMPLETED`
- FAILED → `FAILED`

### `GET /api/catalog/objects/<name>/detail`
Full metadata for a single object.

**Query params:** `snapshot_id`, `type`

**Returns:** Full JSONB metadata + source/target comparison diff.

### `POST /api/catalog/compare`
Re-compare specific objects with target.

**Body:** `{ src_schema, tgt_schema, objects: ["TABLE:USERS", "VIEW:V_ORDERS"] }`

**Returns:** Updated compare results.

### `POST /api/catalog/refresh`
Refresh metadata from source Oracle.

**Body:** `{ src_schema, objects: ["TABLE:USERS", ...] }`

**Returns:** Updated object metadata.

### `POST /api/catalog/sync-to-target`
Create or synchronize object on target.

**Body:** `{ src_schema, tgt_schema, object_type, object_name, action }`

**Actions by type:**

| Type | Actions |
|---|---|
| TABLE | `create`, `sync_cols`, `sync_objects` (delegates to existing target-prep API) |
| VIEW | `create` (CREATE OR REPLACE VIEW) |
| MVIEW | `create` (CREATE MATERIALIZED VIEW) |
| FUNCTION / PROCEDURE | `compile` (CREATE OR REPLACE from source_code) |
| PACKAGE | `compile` (spec then body) |
| SEQUENCE | `create` (CREATE SEQUENCE), `sync` (ALTER SEQUENCE) |
| SYNONYM | `create` (CREATE OR REPLACE SYNONYM) |
| TYPE | `compile` (CREATE OR REPLACE TYPE + body) |

## Oracle Introspection — New Functions in `oracle_browser.py`

```python
list_all_objects(conn, schema) -> list[dict]
    # all_objects WHERE object_type IN (TABLE, VIEW, MATERIALIZED VIEW,
    #   FUNCTION, PROCEDURE, PACKAGE, SEQUENCE, SYNONYM, TYPE)
    # NOTE: PACKAGE BODY and TYPE BODY are excluded from the list —
    #   their source is fetched and stored in the parent's metadata
    #   (spec_source + body_source for PACKAGE, source + body for TYPE)
    # Returns: object_name, object_type, status, last_ddl_time

get_view_info(conn, schema, name) -> dict
    # all_views + all_tab_columns
    # Returns: sql_text, columns, status

get_mview_info(conn, schema, name) -> dict
    # all_mviews + all_tab_columns
    # Returns: sql_text, columns, refresh_type, last_refresh

get_source_code(conn, schema, name, type) -> str
    # all_source WHERE type IN (FUNCTION, PROCEDURE, PACKAGE, PACKAGE BODY, TYPE, TYPE BODY)
    # Returns: concatenated source text

get_sequence_info(conn, schema, name) -> dict
    # all_sequences
    # Returns: min_value, max_value, increment_by, cache_size, last_number

get_synonym_info(conn, schema, name) -> dict
    # all_synonyms
    # Returns: table_owner, table_name, db_link
```

**Optimization:** Batch queries instead of per-object. E.g., fetch `all_source` for all functions in one query, `all_tab_columns` for all views in one query.

## Comparison Logic (`services/ddl_compare.py`)

| Type | Comparison |
|---|---|
| TABLE | Existing `_diff_summary()` — columns, indexes, constraints, triggers |
| VIEW | Normalized sql_text comparison + VALID/INVALID status |
| MVIEW | sql_text + refresh_type |
| FUNCTION / PROCEDURE / TYPE | Source code comparison (line-by-line, ignoring whitespace) |
| PACKAGE | Separate spec and body comparison |
| SEQUENCE | min, max, increment, cache (last_number ignored — it changes at runtime) |
| SYNONYM | table_owner + table_name + db_link |

## Sync to Target (`services/ddl_sync_extended.py`)

| Type | Create | Sync |
|---|---|---|
| TABLE | CREATE TABLE + columns (existing ensure-table) | sync_cols + sync_objects |
| VIEW | CREATE OR REPLACE VIEW AS (sql_text) | same — replace |
| MVIEW | CREATE MATERIALIZED VIEW AS (sql_text) | DROP + CREATE |
| FUNCTION / PROCEDURE | CREATE OR REPLACE from source_code | same — replace |
| PACKAGE | CREATE spec, then CREATE body | same |
| SEQUENCE | CREATE SEQUENCE with params | ALTER SEQUENCE |
| SYNONYM | CREATE OR REPLACE SYNONYM | same |
| TYPE | CREATE OR REPLACE TYPE + body | same |

## UI/UX

### Toolbar
- **Schema selectors** — source and target, same SearchSelect as current
- **"Загрузить каталог"** — full load into cache, shows progress
- **"Обновить из source"** — refresh selected objects
- **Last loaded timestamp** displayed

### Sub-tabs with Counts
`Таблицы(124) | Views & MViews(18) | Code(45) | Другое(32)`

### Object Table
- Search filter
- Status filter: All | Match | Diff | Missing on target
- Checkboxes for multi-select
- Expandable rows for details
- Per-row action buttons (type-specific)

### Statuses per Object

**Target match:**
- `Совпадает` (green) — MATCH
- `Отличается` (yellow) — DIFF
- `Нет на таргете` (red) — MISSING
- `Не проверено` (gray) — UNKNOWN

**Migration status (tables only):**
- `Нет` (gray)
- `Запланирована` (blue)
- `В процессе` (yellow)
- `Завершена` (green)
- `Ошибка` (red)

### Planner Wizard
- Button "Запустить визард для выбранных (N)" — active only when tables are selected
- Opens inline panel with steps 2-4 of current wizard:
  - Step 2: Table settings (mode, strategy, chunk_size, workers)
  - Step 3: Load order (FK dependencies, batches)
  - Step 4: Review and launch

## What Changes

| Action | Target |
|---|---|
| **Delete** | `MigrationPlanner.tsx` — replaced by DDLCatalog |
| **Delete** | `SchemaCompareStep` (step 1 of wizard) — replaced by catalog |
| **Modify** | `App.tsx` — change import and tab name to "DDL Каталог" |
| **Modify** | `oracle_browser.py` — add 6 new introspection functions |
| **Modify** | `state_db.py` — add 3 new tables |
| **Create** | `components/DDLCatalog/` — 10 new files |
| **Create** | `routes/catalog.py` — new blueprint |
| **Create** | `services/ddl_compare.py` — comparison module |
| **Create** | `services/ddl_sync_extended.py` — extended sync |

## What Stays the Same

- All other tabs (Migrations, Connector Groups, Target Prep, Data Compare, Checklist)
- All existing API endpoints
- Existing `oracle_ddl_sync.py` for table sync (reused)
- Existing `planner.py` endpoints (reused for wizard steps 2-4)
