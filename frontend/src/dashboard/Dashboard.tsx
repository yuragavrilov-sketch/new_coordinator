import React, { useCallback, useEffect, useMemo, useState, Suspense } from "react";
import { ObjectFilters, type SortKey, type StatusFilter, type KeyFilter, type SuppFilter } from "./ObjectFilters";
import { ObjectTable } from "./ObjectTable";
import { ObjectDrawer } from "./ObjectDrawer";
import { NewMigrationWizard } from "./NewMigrationWizard";
import { DashboardEmptyState } from "./EmptyState";
import { AddToPlanModal } from "./AddToPlanModal";
import { PlanPanel } from "./PlanPanel";
import { primaryActionStyle, secondaryActionStyle } from "./buttonStyles";
import { t } from "../theme";
import { useApi } from "../hooks/useApi";
import type { SSEEvent } from "../hooks/useSSE";
import { type SchemaObject, type ObjectType, type MigrationEvent } from "./types";
import {
  type SchemaMigrationListItem,
  type MigrationPlanDetail,
  type MigrationPlanCdcGroup,
  createSchemaMigration,
  startMigrationPlan,
} from "./api";
import type { MigrationPrefill } from "../components/CreateMigrationModal/types";

const CreateMigrationModal = React.lazy(() =>
  import("../components/CreateMigrationModal").then(m => ({ default: m.CreateMigrationModal }))
);

interface Props {
  selectedId:        string | null;
  schema:            SchemaMigrationListItem | null;
  planId:            number | null;
  onCreated:         (newId: string) => void;
  onPlanChanged:     (planId: number) => void;
  onOpenPlan:        () => void;
  /** When `true` and `schema` is null, render the empty state with CTA. */
  showEmptyState:    boolean;
  sseEvents:         SSEEvent[];
}

export function Dashboard({
  selectedId,
  schema,
  planId,
  onCreated,
  onPlanChanged,
  onOpenPlan,
  showEmptyState,
  sseEvents,
}: Props) {
  const [typeFilter,   setTypeFilter]   = useState<ObjectType | "all">("all");
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");
  const [keyFilter,    setKeyFilter]    = useState<KeyFilter>("all");
  const [suppFilter,   setSuppFilter]   = useState<SuppFilter>("all");
  const [search,       setSearch]       = useState("");
  const [sort,         setSort]         = useState<SortKey>("priority");
  const [openObject,   setOpenObject]   = useState<SchemaObject | null>(null);
  const [wizardOpen,   setWizardOpen]   = useState(false);
  const [page,         setPage]         = useState(1);
  const [pageSize,     setPageSize]     = useState(25);
  const [migrateModalPrefill, setMigrateModalPrefill] = useState<MigrationPrefill | null>(null);
  const [selectedIds,         setSelectedIds]         = useState<Set<string>>(() => new Set());
  const [planMode,            setPlanMode]            = useState<"historical" | "cdc" | null>(null);
  const [activePlanId,        setActivePlanId]        = useState<number | null>(planId ?? schema?.planId ?? null);
  const [planBusy,            setPlanBusy]            = useState(false);
  const [planErr,             setPlanErr]             = useState("");
  const [toast,               setToast]               = useState<string>("");

  // Fetch objects and events for this schema migration (auto-poll 5s)
  const objectsApi = useApi<SchemaObject[]>(
    selectedId ? `/api/schema-migrations/${selectedId}/objects` : null,
    { intervalMs: 5000 },
  );
  const eventsApi = useApi<MigrationEvent[]>(
    selectedId ? `/api/schema-migrations/${selectedId}/events?limit=200` : null,
    { intervalMs: 5000 },
  );
  const planApi = useApi<MigrationPlanDetail>(
    activePlanId ? `/api/planner/plans/${activePlanId}` : null,
    { intervalMs: 5000 },
  );
  const cdcGroupApi = useApi<MigrationPlanCdcGroup>(
    selectedId ? `/api/schema-migrations/${selectedId}/cdc-group` : null,
    { intervalMs: 5000 },
  );

  const objects = objectsApi.data || [];
  const events  = eventsApi.data  || [];
  const tableObjects = useMemo(() => objects.filter(o => o.type === "TABLE"), [objects]);
  const cdcGroup = planApi.data?.cdc_group || cdcGroupApi.data || null;

  useEffect(() => {
    setActivePlanId(planId ?? schema?.planId ?? null);
  }, [schema?.id, schema?.planId, planId]);

  useEffect(() => {
    const event = sseEvents[0];
    if (!event) return;

    if (event.type === "schema_migration.plan_items_added" && event.id === selectedId) {
      setActivePlanId(event.plan_id);
      objectsApi.reload();
      eventsApi.reload();
      cdcGroupApi.reload();
      planApi.reload();
      return;
    }

    if (event.type === "connector_group_status") {
      cdcGroupApi.reload();
      if (activePlanId) planApi.reload();
      return;
    }

    if (event.type === "migration_phase" && activePlanId) {
      objectsApi.reload();
      eventsApi.reload();
      cdcGroupApi.reload();
      planApi.reload();
    }
  }, [
    sseEvents,
    selectedId,
    activePlanId,
    objectsApi.reload,
    eventsApi.reload,
    cdcGroupApi.reload,
    planApi.reload,
  ]);

  // Filtered + sorted
  const filtered = useMemo(() => {
    let arr = tableObjects;
    if (statusFilter !== "all") {
      arr = arr.filter(o => {
        if (statusFilter === "issues") return o.err > 0 || o.warn > 0 || o.status === "error" || o.status === "warn";
        return o.status === statusFilter;
      });
    }
    // Фильтры PK/UK/NO KEY и SUPP/NO SUPP применяются только к таблицам;
    // DDL-объекты (INDEX, VIEW, PACKAGE...) при активном фильтре отсекаются
    // — иначе сегмент «NO KEY» показывал бы все view/package и т.п.
    if (keyFilter !== "all") {
      arr = arr.filter(o => {
        if (o.type !== "TABLE") return false;
        if (keyFilter === "pk")     return !!o.hasPk;
        if (keyFilter === "uk")     return !o.hasPk && !!o.hasUk;
        if (keyFilter === "no_key") return o.hasPk === false && o.hasUk === false;
        return true;
      });
    }
    if (suppFilter !== "all") {
      arr = arr.filter(o => {
        if (o.type !== "TABLE") return false;
        if (suppFilter === "supp")    return o.hasSuppLog === true;
        if (suppFilter === "no_supp") return o.hasSuppLog === false;
        return true;
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
      return 0;
    });
    return arr;
  }, [tableObjects, statusFilter, keyFilter, suppFilter, search, sort]);

  // Reset drawer when switching schemas
  useEffect(() => { setOpenObject(null); }, [selectedId]);
  // Reset to page 1 when filters change so the user always sees the matches
  useEffect(() => { setPage(1); }, [typeFilter, statusFilter, keyFilter, suppFilter, search, sort, pageSize, selectedId]);
  // Clear bulk-selection when switching schemas
  useEffect(() => { setSelectedIds(new Set()); }, [selectedId]);

  // Bulk-select: все TABLE-объекты без миграции. Backend в get_objects
  // отдельно отдаёт TABLE-строки из DDL-снэпшота только для тех таблиц, у
  // которых ещё нет migration-row (services/schema_migrations.get_objects),
  // и присваивает им синтетический id "ddl-TABLE-<NAME>". Поэтому
  // подходящий маркер «нет миграции» — id, а не status: расхождения по
  // индексам/констрейнтам делают status=warn, но миграцию для такой
  // таблицы создавать всё равно можно.
  const selectableIds = useMemo(() => {
    const s = new Set<string>();
    for (const o of objects) {
      if (o.type === "TABLE" && o.id.startsWith("ddl-TABLE-")) s.add(o.id);
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

  const handleStartPlan = useCallback(async () => {
    if (!activePlanId) return;
    setPlanBusy(true);
    setPlanErr("");
    try {
      await startMigrationPlan(activePlanId);
      planApi.reload();
      objectsApi.reload();
      eventsApi.reload();
    } catch (e) {
      setPlanErr(String(e instanceof Error ? e.message : e));
    } finally {
      setPlanBusy(false);
    }
  }, [activePlanId, planApi, objectsApi, eventsApi]);

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
      <ObjectFilters
        objects={tableObjects}
        filtered={filtered}
        typeFilter={typeFilter}     onTypeFilter={setTypeFilter}
        statusFilter={statusFilter} onStatusFilter={setStatusFilter}
        keyFilter={keyFilter}       onKeyFilter={setKeyFilter}
        suppFilter={suppFilter}     onSuppFilter={setSuppFilter}
        search={search}             onSearch={setSearch}
        sort={sort}                 onSort={setSort}
        tablesOnly
      />

      <PlanPanel
        plan={activePlanId ? (planApi.data || null) : null}
        loading={!!activePlanId && planApi.loading}
        cdcGroup={cdcGroup}
        onStart={handleStartPlan}
        onReload={() => {
          planApi.reload();
          cdcGroupApi.reload();
        }}
        onOpenDetails={onOpenPlan}
        busy={planBusy}
        error={planErr || planApi.error || ""}
        variant="overview"
      />

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
          onCdcPack={() => setPlanMode("cdc")}
          onBulkPack={() => setPlanMode("historical")}
        />
      )}

      {planMode && selectedTables.length > 0 && selectedId && (
        <AddToPlanModal
          schemaMigrationId={selectedId}
          tables={selectedTables}
          initialMode={planMode}
          cdcGroup={cdcGroup}
          cdcGroupLoading={planMode === "cdc" && !cdcGroup && (cdcGroupApi.loading || (!!activePlanId && planApi.loading && !planApi.data))}
          cdcGroupError={planMode === "cdc" ? (cdcGroupApi.error || (!!activePlanId && !planApi.data ? planApi.error : null)) : null}
          onClose={() => setPlanMode(null)}
          onReloadCdcGroup={() => {
            cdcGroupApi.reload();
            planApi.reload();
          }}
          onDone={async (planId, count, response) => {
            const target = planMode === "cdc" ? "CDC-коннектор" : "обычную пачку";
            let autoStartOk = true;
            if (response.connector_start_error) {
              autoStartOk = false;
              setPlanErr(`CDC-коннектор не стартовал: ${response.connector_start_error}`);
            }
            let startNote = "";
            if (planMode === "cdc") {
              const connectorCount = response.cdc_group?.tables?.length;
              const connectorStatus = String(response.connector_start?.status || response.cdc_group?.status || "").trim();
              if (response.plan_start_error) {
                autoStartOk = false;
                startNote = " · автозапуск не выполнен";
                setPlanErr(response.plan_start_error);
              } else if (response.plan_starts?.length) {
                const startedCount = response.plan_starts.reduce((sum, item) => sum + item.started.length, 0);
                startNote = startedCount
                  ? ` · очередь: ${count} таблиц / запущено: ${startedCount}`
                  : " · запуск уже обработан";
              } else if (response.plan_start) {
                const startedCount = response.plan_start.started.length;
                startNote = startedCount
                  ? ` · очередь: ${count} таблиц / запущено: ${startedCount}`
                  : " · запуск уже обработан";
              } else if (!response.connector_start_error) {
                try {
                  const started = await startMigrationPlan(planId);
                  const startedCount = started.started.length;
                  startNote = startedCount
                    ? ` · очередь: ${count} таблиц / запущено: ${startedCount}`
                    : " · запуск уже обработан";
                } catch (e) {
                  const msg = e instanceof Error ? e.message : String(e);
                  if (/already running/i.test(msg)) {
                    startNote = " · в очереди за текущей миграцией";
                  } else {
                    autoStartOk = false;
                    startNote = " · автозапуск не выполнен";
                    setPlanErr(msg);
                  }
                }
              } else {
                startNote = " · ожидает запуска CDC-коннектора";
              }
              if (connectorCount !== undefined) {
                startNote += ` · Debezium tables: ${connectorCount}`;
              }
              if (connectorStatus) {
                startNote += ` · коннектор: ${connectorStatus}`;
              }
            }
            setPlanMode(null);
            setSelectedIds(new Set());
            setActivePlanId(planId);
            cdcGroupApi.setData(response.cdc_group || null);
            onPlanChanged(planId);
            setToast(
              autoStartOk
                ? `Добавлено в ${target}: ${count}${startNote}`
                : `Добавлено в ${target}: ${count} · проверьте ошибку автозапуска`
            );
            objectsApi.reload();
            eventsApi.reload();
            cdcGroupApi.reload();
            planApi.reload();
            setTimeout(() => setToast(""), 5000);
          }}
        />
      )}

      {toast && (
        <div style={{
          position: "fixed", bottom: 80, left: "50%",
          transform: "translateX(-50%)", zIndex: 1100,
          background: t.bg.s1, color: t.text.primary,
          border: `1px solid ${t.green.dim}`, borderRadius: t.radius.md,
          padding: "8px 14px", fontSize: 13,
          boxShadow: "0 8px 24px rgba(0,0,0,.35)",
        }}>{toast}</div>
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

function BulkSelectionBar({ count, onClear, onCdcPack, onBulkPack }: {
  count: number; onClear: () => void;
  onCdcPack: () => void; onBulkPack: () => void;
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
      <button onClick={onCdcPack} style={secondaryActionStyle()}>
        В CDC-коннектор ({count})
      </button>
      <button onClick={onBulkPack} style={secondaryActionStyle()}>
        В обычную пачку ({count})
      </button>
    </div>
  );
}
