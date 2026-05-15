import React, { useCallback, useEffect, useMemo, useState, Suspense } from "react";
import { SchemaHeader } from "./SchemaHeader";
import { KpiRow, KpiCard } from "./KpiRow";
import { ObjectFilters, type SortKey, type StatusFilter } from "./ObjectFilters";
import { ObjectTable } from "./ObjectTable";
import { ObjectDrawer } from "./ObjectDrawer";
import { NewMigrationWizard } from "./NewMigrationWizard";
import { DashboardEmptyState } from "./EmptyState";
import { LoadSnapshotBanner } from "./LoadSnapshotBanner";
import { ProblemsSummary } from "./ProblemsSummary";
import { BulkCreateMigrationModal } from "./BulkCreateMigrationModal";
import { primaryActionStyle, secondaryActionStyle } from "./buttonStyles";
import { t } from "../theme";
import { fmtCompactNum } from "../utils/format";
import { useApi } from "../hooks/useApi";
import type { SSEEvent } from "../hooks/useSSE";
import { OBJECT_TYPES, type SchemaObject, type ObjectType, type MigrationEvent } from "./types";
import {
  type SchemaMigrationListItem,
  createSchemaMigration,
} from "./api";
import type { MigrationPrefill } from "../components/CreateMigrationModal/types";

const CreateMigrationModal = React.lazy(() =>
  import("../components/CreateMigrationModal").then(m => ({ default: m.CreateMigrationModal }))
);

interface Props {
  selectedId:        string | null;
  schema:            SchemaMigrationListItem | null;
  onCreated:         (newId: string) => void;
  /** When `true` and `schema` is null, render the empty state with CTA. */
  showEmptyState:    boolean;
  sseEvents:         SSEEvent[];
}

export function Dashboard({ selectedId, schema, onCreated, showEmptyState, sseEvents }: Props) {
  const [typeFilter,   setTypeFilter]   = useState<ObjectType | "all">("all");
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");
  const [search,       setSearch]       = useState("");
  const [sort,         setSort]         = useState<SortKey>("priority");
  const [openObject,   setOpenObject]   = useState<SchemaObject | null>(null);
  const [wizardOpen,   setWizardOpen]   = useState(false);
  const [page,         setPage]         = useState(1);
  const [pageSize,     setPageSize]     = useState(25);
  const [migrateModalPrefill, setMigrateModalPrefill] = useState<MigrationPrefill | null>(null);
  const [selectedIds,         setSelectedIds]         = useState<Set<string>>(() => new Set());
  const [bulkOpen,            setBulkOpen]            = useState(false);

  // Fetch objects and events for this schema migration (auto-poll 5s)
  const objectsApi = useApi<SchemaObject[]>(
    selectedId ? `/api/schema-migrations/${selectedId}/objects` : null,
    { intervalMs: 5000 },
  );
  const eventsApi = useApi<MigrationEvent[]>(
    selectedId ? `/api/schema-migrations/${selectedId}/events?limit=200` : null,
    { intervalMs: 5000 },
  );

  const objects = objectsApi.data || [];
  const events  = eventsApi.data  || [];

  // Aggregate KPIs from current objects (or use server-side KPI from schema header)
  const overall = useMemo(() => {
    const done       = objects.filter(o => o.status === "done" || o.status === "skipped").length;
    const err        = objects.reduce((a, o) => a + o.err, 0);
    const warn       = objects.reduce((a, o) => a + o.warn, 0);
    const rowsPerSec = objects.reduce((a, o) => a + (o.rowsPerSec || 0), 0);
    const mbPerSec   = objects.reduce((a, o) => a + (o.mbPerSec   || 0), 0);
    const totalRows  = objects.reduce((a, o) => a + (o.rows     || 0), 0);
    const doneRows   = objects.reduce((a, o) => a + (o.rowsDone || 0), 0);
    const progress   = totalRows ? (doneRows / totalRows) * 100 : 0;
    return { done, total: objects.length, err, warn, rowsPerSec, mbPerSec, totalRows, doneRows, progress };
  }, [objects]);

  const statusCounts = useMemo(() => {
    const c = { all: objects.length, running: 0, queued: 0, done: 0 };
    objects.forEach(o => {
      if (o.status === "running") c.running++;
      else if (o.status === "queued") c.queued++;
      else if (o.status === "done") c.done++;
    });
    return c;
  }, [objects]);

  // Categorize problem objects for the user's "what's broken" view
  const problems = useMemo(() => {
    const missing:     SchemaObject[] = [];          // нет в target
    const diff:        SchemaObject[] = [];          // DDL отличается
    const srcInvalid:  SchemaObject[] = [];          // INVALID в source
    const tgtInvalid:  SchemaObject[] = [];          // INVALID в target (но valid в source)
    const bothInvalid: SchemaObject[] = [];          // INVALID и там и там
    for (const o of objects) {
      const note = o.note || "";
      const si = (o.srcStatus || "").toUpperCase() === "INVALID";
      const ti = (o.tgtStatus || "").toUpperCase() === "INVALID";
      if (si && ti) bothInvalid.push(o);
      else if (si) srcInvalid.push(o);
      else if (ti) tgtInvalid.push(o);
      if (note.startsWith("нет в target")) missing.push(o);
      else if (note.startsWith("DDL отличается")) diff.push(o);
    }
    return { missing, diff, srcInvalid, tgtInvalid, bothInvalid };
  }, [objects]);

  // Filtered + sorted
  const filtered = useMemo(() => {
    let arr = objects;
    if (typeFilter !== "all") arr = arr.filter(o => o.type === typeFilter);
    if (statusFilter !== "all") {
      arr = arr.filter(o => {
        if (statusFilter === "issues") return o.err > 0 || o.warn > 0 || o.status === "error" || o.status === "warn";
        return o.status === statusFilter;
      });
    }
    if (search) {
      const q = search.toLowerCase();
      arr = arr.filter(o => o.name.toLowerCase().includes(q));
    }
    arr = [...arr].sort((a, b) => {
      if (sort === "priority") {
        const rank: Record<string, number> = {
          error: 0, warn: 1, running: 2, validating: 3, paused: 4, queued: 5, done: 6, skipped: 7,
        };
        const sa = rank[a.status] ?? 9;
        const sb = rank[b.status] ?? 9;
        if (sa !== sb) return sa - sb;
        return (b.sizeMb || 0) - (a.sizeMb || 0);
      }
      if (sort === "size")     return (b.sizeMb || 0) - (a.sizeMb || 0);
      if (sort === "progress") return b.progress - a.progress;
      if (sort === "name")     return a.name.localeCompare(b.name);
      if (sort === "type")     return OBJECT_TYPES[a.type].ord - OBJECT_TYPES[b.type].ord;
      return 0;
    });
    return arr;
  }, [objects, typeFilter, statusFilter, search, sort]);

  // Reset drawer when switching schemas
  useEffect(() => { setOpenObject(null); }, [selectedId]);
  // Reset to page 1 when filters change so the user always sees the matches
  useEffect(() => { setPage(1); }, [typeFilter, statusFilter, search, sort, pageSize, selectedId]);
  // Clear bulk-selection when switching schemas
  useEffect(() => { setSelectedIds(new Set()); }, [selectedId]);

  // Bulk-select: only TABLEs without a migration (status=queued).
  // Backend treats queued TABLE rows as "not yet migrated" — same rule.
  const selectableIds = useMemo(() => {
    const s = new Set<string>();
    for (const o of objects) {
      if (o.type === "TABLE" && o.status === "queued") s.add(o.id);
    }
    return s;
  }, [objects]);

  // Keep selection in sync if objects list changes (drop stale ids)
  useEffect(() => {
    setSelectedIds(prev => {
      let changed = false;
      const next = new Set<string>();
      prev.forEach(id => {
        if (selectableIds.has(id)) next.add(id);
        else changed = true;
      });
      return changed ? next : prev;
    });
  }, [selectableIds]);

  const toggleSelect = useCallback((id: string) => {
    setSelectedIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  const selectAllPage = useCallback((ids: string[], allSelected: boolean) => {
    setSelectedIds(prev => {
      const next = new Set(prev);
      if (allSelected) ids.forEach(id => next.delete(id));
      else             ids.forEach(id => next.add(id));
      return next;
    });
  }, []);

  // Resolve selected ids → bulk-modal payload
  const selectedTables = useMemo(() => {
    if (selectedIds.size === 0 || !schema) return [];
    const byId = new Map(objects.map(o => [o.id, o]));
    const src = schema.src_schema || "";
    const tgt = schema.tgt_schema || "";
    const out: { source_schema: string; source_table: string; target_schema: string; target_table: string }[] = [];
    selectedIds.forEach(id => {
      const o = byId.get(id);
      if (!o) return;
      out.push({
        source_schema: src,
        source_table:  o.name,
        target_schema: tgt,
        target_table:  o.name,
      });
    });
    return out;
  }, [selectedIds, objects, schema]);

  // Stable handlers (so React.memo on ObjectRow can skip re-renders)
  const handleOpen = useCallback((o: SchemaObject) => setOpenObject(o), []);
  const handleRowAction = useCallback(
    (o: SchemaObject, a: "pause" | "retry" | "more") => console.log("object action", a, o.name),
    [],
  );

  // Empty state
  if (!schema) {
    return (
      <>
        {showEmptyState && <DashboardEmptyState onCreate={() => setWizardOpen(true)}/>}
        {wizardOpen && (
          <NewMigrationWizard
            onClose={() => setWizardOpen(false)}
            onSubmit={async d => {
              const id = await createSchemaMigration({
                name:           d.sourceSchema || "—",
                src_schema:     d.sourceSchema,
                tgt_schema:     d.targetSchema,
                source_host:    d.sourceCluster,
                source_version: d.sourceVersion,
                target_host:    d.targetCluster,
                target_version: d.targetVersion,
              });
              setWizardOpen(false);
              onCreated(id);
            }}
          />
        )}
      </>
    );
  }

  return (
    <>
      <SchemaHeader
        schema={schema}
        progress={schema.kpi.progress}
      />

      <KpiRow>
        <KpiCard
          label="Прогресс схемы"
          value={`${schema.kpi.progress.toFixed(1)}%`}
          sub={`${schema.kpi.doneObjects}/${schema.kpi.totalObjects} объектов готово`}
          tone="info"
        />
        <KpiCard
          label="Объектов в работе"
          value={statusCounts.running}
          sub={`${statusCounts.queued} в очереди · ${statusCounts.done} готовы`}
          tone="info"
          mono={false}
        />
        <KpiCard
          label="Скорость"
          value={fmtCompactNum(overall.rowsPerSec)}
          sub={`rows/s · ${overall.mbPerSec} MB/s`}
          tone="ok"
        />
        <KpiCard
          label="Совместимость"
          value={`${(schema.schemaCompat || 100).toFixed(1)}%`}
          sub="по PL/SQL и view"
          tone="warn"
        />
        <KpiCard
          label="Ошибки"
          value={schema.kpi.errorObjects}
          sub={`${overall.warn} предупреждений`}
          tone="error"
          mono={false}
        />
      </KpiRow>

      <ObjectFilters
        objects={objects}
        filtered={filtered}
        typeFilter={typeFilter}     onTypeFilter={setTypeFilter}
        statusFilter={statusFilter} onStatusFilter={setStatusFilter}
        search={search}             onSearch={setSearch}
        sort={sort}                 onSort={setSort}
      />

      <LoadSnapshotBanner
        srcSchema={schema.src_schema || ""}
        tgtSchema={schema.tgt_schema || ""}
        sseEvents={sseEvents}
        onLoaded={() => objectsApi.reload()}
      />

      {selectedId && (
        <ProblemsSummary
          missing={problems.missing}
          diff={problems.diff}
          srcInvalid={problems.srcInvalid}
          tgtInvalid={problems.tgtInvalid}
          bothInvalid={problems.bothInvalid}
          schemaMigrationId={selectedId}
          srcSchema={schema.src_schema || ""}
          tgtSchema={schema.tgt_schema || ""}
          onOpen={handleOpen}
          onApplied={() => { objectsApi.reload(); eventsApi.reload(); }}
        />
      )}


      <ObjectTable
        objects={filtered}
        onOpen={handleOpen}
        onAction={handleRowAction}
        page={page}
        pageSize={pageSize}
        onPageChange={setPage}
        onPageSizeChange={setPageSize}
        selectableIds={selectableIds}
        selectedIds={selectedIds}
        onToggleSelect={toggleSelect}
        onSelectAllPage={selectAllPage}
      />

      {selectedIds.size > 0 && (
        <BulkSelectionBar
          count={selectedIds.size}
          onClear={() => setSelectedIds(new Set())}
          onCreate={() => setBulkOpen(true)}
        />
      )}

      {bulkOpen && selectedTables.length > 0 && (
        <BulkCreateMigrationModal
          tables={selectedTables}
          onClose={() => setBulkOpen(false)}
          onCreated={() => {
            setBulkOpen(false);
            setSelectedIds(new Set());
            objectsApi.reload();
            eventsApi.reload();
          }}
        />
      )}

      {openObject && selectedId && (
        <ObjectDrawer
          schemaMigrationId={selectedId}
          object={openObject}
          events={events}
          srcSchema={schema.src_schema || ""}
          tgtSchema={schema.tgt_schema || ""}
          onClose={() => setOpenObject(null)}
          onAction={(o, a) => console.log("drawer action", a, o.name)}
          onApplied={() => { objectsApi.reload(); eventsApi.reload(); }}
          onMigrate={setMigrateModalPrefill}
        />
      )}
      {migrateModalPrefill && (
        <Suspense fallback={null}>
          <CreateMigrationModal
            prefill={migrateModalPrefill}
            onClose={() => setMigrateModalPrefill(null)}
            onCreated={() => {
              setMigrateModalPrefill(null);
              objectsApi.reload();
              eventsApi.reload();
            }}
          />
        </Suspense>
      )}
      {wizardOpen && (
        <NewMigrationWizard
          onClose={() => setWizardOpen(false)}
          onSubmit={async d => {
            const id = await createSchemaMigration({
              name:           d.sourceSchema || "—",
              src_schema:     d.sourceSchema,
              tgt_schema:     d.targetSchema,
              source_host:    d.sourceCluster,
              source_version: d.sourceVersion,
              target_host:    d.targetCluster,
              target_version: d.targetVersion,
            });
            setWizardOpen(false);
            onCreated(id);
          }}
        />
      )}
    </>
  );
}

function BulkSelectionBar({ count, onClear, onCreate }: {
  count: number; onClear: () => void; onCreate: () => void;
}) {
  return (
    <div style={{
      position:   "fixed",
      bottom:     20,
      left:       "50%",
      transform:  "translateX(-50%)",
      zIndex:     900,
      display:    "flex",
      alignItems: "center",
      gap:        12,
      padding:    "10px 16px",
      background: t.bg.s1,
      border:     `1px solid ${t.border.base}`,
      borderRadius: t.radius.md,
      boxShadow:  "0 8px 24px rgba(0,0,0,.35)",
    }}>
      <span style={{ fontSize: 13, color: t.text.primary }}>
        Выбрано: <strong style={{ fontFamily: t.font.mono }}>{count}</strong>
      </span>
      <button onClick={onClear} style={secondaryActionStyle()}>Очистить</button>
      <button onClick={onCreate} style={primaryActionStyle(false)}>
        Создать миграции ({count})
      </button>
    </div>
  );
}
