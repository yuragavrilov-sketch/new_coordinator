# Dashboard — Main Page Design Spec

**Date:** 2026-03-31
**Route:** `/` (new default page, DDL Catalog stays at `/catalog`)

## Goal

Table-centric dashboard showing all source tables with their migration status. Allows creating individual or bulk migrations directly from the table list.

## Page Structure

### Toolbar (DashboardToolbar)

- **Schema selector:** SearchSelect for source schema (from snapshots or Oracle)
- **Refresh button:** "Обновить из Oracle" — calls `POST /api/catalog/snapshot` to refresh
- **Counters:** total tables, with migration, without migration, errors
- **Bulk action buttons** (visible when checkboxes selected):
  - "Создать миграции" — creates individual migrations (BULK_ONLY by default)
  - "Создать группу + миграции" — opens BulkCreateModal wizard

### Table List (TableList)

Columns: Checkbox | Table Name | Migration Status (badge) | Phase | Progress | Connector Group

- Rows without migration: gray "—" badge
- Rows with migration: colored phase badge (reuse existing PhaseBadge)
- Search/filter by table name
- Status filter: all / no migration / active / completed / errors

### Expandable Detail Panel (TableDetail)

Accordion-style panel below the clicked row:

- **If migration exists:** phase, chunk progress, rows loaded, duration, action buttons
- **If no migration:** "Создать миграцию" button with pre-filled source_schema/source_table

### Bulk Create Modal (BulkCreateModal)

Mini-wizard for mass creation:

- Step 1: group name, topic_prefix, mode (CDC / BULK_ONLY)
- Step 2: review selected tables
- Step 3: create group → create migrations with group_id

## Data Flow

### Table source

1. Load from latest `ddl_snapshot` for selected schema (if exists)
2. "Refresh" button → `POST /api/catalog/snapshot` → reload
3. No snapshot → empty state with "Загрузить каталог" button

### Table → Migration mapping

- Load `GET /api/migrations` → map by `source_schema + source_table`
- Poll every 5 seconds for status updates
- SSE events for real-time phase changes

### Bulk operations

**"Создать миграции" (without group):**
- For each selected table: `POST /api/migrations` with `migration_mode: "BULK_ONLY"`
- Show creation progress

**"Создать группу + миграции":**
- `POST /api/connector-groups` with selected tables
- For each table: `POST /api/migrations` with `group_id`

## API Endpoints Used (all existing, no new backend work)

- `GET /api/catalog/snapshots` — list snapshots
- `POST /api/catalog/snapshot` — create/update snapshot
- `GET /api/catalog/snapshot/:id/objects?object_type=TABLE&db_side=source` — source tables
- `GET /api/migrations` — all migrations
- `POST /api/migrations` — create migration
- `POST /api/connector-groups` — create connector group
- SSE `/api/events` — live phase updates

## Files

### New

```
frontend/src/components/Dashboard/
  Dashboard.tsx        — container: state, data loading, polling
  DashboardToolbar.tsx — schema select, refresh, counters, bulk actions
  TableList.tsx        — table with checkboxes, filters, sorting
  TableRow.tsx         — table row + expandable detail panel
  TableDetail.tsx      — expanded panel (migration info or "create")
  BulkCreateModal.tsx  — mini-wizard for bulk group+migration creation
  index.tsx            — re-export
```

### Modified

- `App.tsx` — add route `/` → `<Dashboard />`, remove redirect to `/catalog`

### Reused

- `SearchSelect` from `components/ui/SearchSelect`
- `PhaseBadge` from `components/PhaseBadge`
- `CreateMigrationModal` from `components/CreateMigrationModal`
- Phase colors from `types/migration.ts`
- Format utils from `utils/format.ts`
- Theme colors from `theme.ts`
