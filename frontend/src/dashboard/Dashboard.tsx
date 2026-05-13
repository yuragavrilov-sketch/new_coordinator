import React, { useEffect, useMemo, useState } from "react";
import { SchemaHeader } from "./SchemaHeader";
import { KpiRow, KpiCard } from "./KpiRow";
import { ObjectFilters, type SortKey, type StatusFilter } from "./ObjectFilters";
import { ObjectTable } from "./ObjectTable";
import { ObjectDrawer } from "./ObjectDrawer";
import { NewMigrationWizard } from "./NewMigrationWizard";
import { DashboardEmptyState } from "./EmptyState";
import { LoadSnapshotBanner } from "./LoadSnapshotBanner";
import { ProblemsSummary } from "./ProblemsSummary";
import { fmtCompactNum } from "../utils/format";
import { useApi } from "../hooks/useApi";
import type { SSEEvent } from "../hooks/useSSE";
import { OBJECT_TYPES, type SchemaObject, type ObjectType, type MigrationEvent } from "./types";
import {
  type SchemaMigrationListItem,
  createSchemaMigration, pause as pauseApi, rollback as rollbackApi,
} from "./api";

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

  const onPause = async () => {
    if (!selectedId) return;
    try { await pauseApi(selectedId, !schema.paused); } catch (e) { console.error(e); }
  };
  const onRollback = async () => {
    if (!selectedId) return;
    if (!window.confirm("Откатить миграцию? Все активные таблицы будут CANCELLING.")) return;
    try { await rollbackApi(selectedId); } catch (e) { console.error(e); }
  };

  return (
    <>
      <SchemaHeader
        schema={schema}
        progress={schema.kpi.progress}
        onPause={onPause}
        onRollback={onRollback}
        onNew={() => setWizardOpen(true)}
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
          onOpen={o => setOpenObject(o)}
          onApplied={() => { objectsApi.reload(); eventsApi.reload(); }}
        />
      )}


      <ObjectTable
        objects={filtered}
        onOpen={o => setOpenObject(o)}
        onAction={(o, a) => console.log("object action", a, o.name)}
      />

      {openObject && selectedId && (
        <ObjectDrawer
          schemaMigrationId={selectedId}
          object={openObject}
          events={events}
          onClose={() => setOpenObject(null)}
          onAction={(o, a) => console.log("drawer action", a, o.name)}
          onApplied={() => { objectsApi.reload(); eventsApi.reload(); }}
        />
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
