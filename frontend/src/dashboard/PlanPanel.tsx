import React, { useMemo } from "react";
import { t } from "../theme";
import type { SSEEvent } from "../hooks/useSSE";
import { ProgressBar } from "../components/ui";
import { primaryActionStyle, secondaryActionStyle } from "./buttonStyles";
import type {
  MigrationPlanCdcGroup,
  MigrationPlanCdcTable,
  MigrationPlanDetail,
  MigrationPlanItem,
  StartMigrationPlanResp,
} from "./api";

interface Props {
  plan: MigrationPlanDetail | null;
  loading: boolean;
  onStart: () => void;
  onReload: () => void;
  onOpenDetails?: () => void;
  busy: boolean;
  error: string;
  variant?: "overview" | "detail";
  cdcGroup?: MigrationPlanCdcGroup | null;
  sseEvents?: SSEEvent[];
}

const DONE = new Set(["DONE"]);
const BAD = new Set(["FAILED", "CANCELLED"]);
const ACTIVE_WORK_PHASES = new Set([
  "PREPARING",
  "SCN_FIXED",
  "CONNECTOR_STARTING",
  "CDC_BUFFERING",
  "TOPIC_CREATING",
  "CHUNKING",
  "BULK_LOADING",
  "BULK_LOADED",
  "STAGE_VALIDATING",
  "STAGE_VALIDATED",
  "BASELINE_PUBLISHING",
  "BASELINE_LOADING",
  "BASELINE_PUBLISHED",
  "STAGE_DROPPING",
  "INDEXES_ENABLING",
  "DATA_VERIFYING",
  "CDC_APPLY_STARTING",
  "CDC_APPLYING",
  "CDC_CATCHING_UP",
  "CDC_CAUGHT_UP",
]);

interface CdcConnectorActionResp {
  status?: string;
  error?: string;
  plan_starts?: StartMigrationPlanResp[];
  plan_start_error?: string | null;
  cdc_queue_kicked?: boolean;
}

interface DebeziumSyncStatus {
  connector_name: string;
  exists: boolean;
  in_sync: boolean;
  desired_table_include_list: string;
  actual_table_include_list: string | null;
  desired_message_key_columns: string;
  actual_message_key_columns: string | null;
  missing_tables: string[];
  extra_tables: string[];
  key_columns_match: boolean;
}

interface TargetTriggerJob {
  job_id: string;
  state: "PENDING" | "RUNNING" | "DONE" | "FAILED";
  enabled_count: number;
  error_text: string | null;
}

export function PlanPanel({
  plan,
  loading,
  onStart,
  onReload,
  onOpenDetails,
  busy,
  error,
  variant = "detail",
  cdcGroup: cdcGroupProp = null,
  sseEvents = [],
}: Props) {
  const batches = useMemo(() => {
    const map = new Map<number, MigrationPlanItem[]>();
    for (const item of plan?.items || []) {
      const key = item.batch_order || 1;
      if (!map.has(key)) map.set(key, []);
      map.get(key)!.push(item);
    }
    return Array.from(map.entries()).sort((a, b) => a[0] - b[0]);
  }, [plan]);
  const effectiveCdcGroup = plan?.cdc_group || cdcGroupProp || null;
  const [cdcActionBusy, setCdcActionBusy] = React.useState("");
  const [cdcActionErr, setCdcActionErr] = React.useState("");
  const [cdcActionInfo, setCdcActionInfo] = React.useState("");
  const [cdcSyncStatus, setCdcSyncStatus] = React.useState<DebeziumSyncStatus | null>(null);
  const [cdcSyncStatusErr, setCdcSyncStatusErr] = React.useState("");
  const [cdcSyncStatusLoading, setCdcSyncStatusLoading] = React.useState(false);

  const loadDebeziumSyncStatus = React.useCallback((groupId: string | null | undefined) => {
    if (!groupId) {
      setCdcSyncStatus(null);
      setCdcSyncStatusErr("");
      setCdcSyncStatusLoading(false);
      return;
    }
    setCdcSyncStatusErr("");
    setCdcSyncStatusLoading(true);
    fetch(`/api/connector-groups/${groupId}/debezium-sync-status`)
      .then(async r => {
        const body = await r.json().catch(() => ({}));
        if (!r.ok) throw new Error(body?.error || `HTTP ${r.status}`);
        return body as DebeziumSyncStatus;
      })
      .then(setCdcSyncStatus)
      .catch(e => {
        setCdcSyncStatus(null);
        setCdcSyncStatusErr(e instanceof Error ? e.message : String(e));
      })
      .finally(() => setCdcSyncStatusLoading(false));
  }, []);

  React.useEffect(() => {
    loadDebeziumSyncStatus(effectiveCdcGroup?.group_id);
  }, [effectiveCdcGroup?.group_id, loadDebeziumSyncStatus]);

  async function syncCdcGroup(group: MigrationPlanCdcGroup) {
    setCdcActionBusy("sync");
    setCdcActionErr("");
    setCdcActionInfo("");
    try {
      const res = await fetch(`/api/connector-groups/${group.group_id}/refresh-tables`, { method: "POST" });
      const body = await res.json().catch(() => ({})) as CdcConnectorActionResp;
      if (!res.ok) {
        throw new Error(body.error || `HTTP ${res.status}`);
      }
      onReload();
      loadDebeziumSyncStatus(group.group_id);
      const status = String(body.status || group.status || "").toUpperCase();
      const syncText = status && status !== "RUNNING"
        ? `CDC-коннектор ${status}; Debezium синхронизируется после запуска`
        : "Debezium синхронизирован";
      if (body.plan_start_error) {
        setCdcActionErr(`${syncText}, но CDC очередь не продолжена: ${body.plan_start_error}`);
      } else {
        const startedCount = (body.plan_starts || []).reduce(
          (sum: number, item: { started?: unknown[] }) => sum + (item.started?.length || 0),
          0,
        );
        setCdcActionInfo(startedCount
          ? `${syncText}, ${status === "RUNNING" ? "запущено CDC строк" : "CDC строк ждут коннектор"}: ${startedCount}`
          : body.cdc_queue_kicked
            ? `${syncText}, очередь CDC продолжена`
            : syncText);
      }
    } catch (e) {
      setCdcActionErr(e instanceof Error ? e.message : String(e));
    } finally {
      setCdcActionBusy("");
    }
  }

  async function startCdcGroup(group: MigrationPlanCdcGroup) {
    setCdcActionBusy("start");
    setCdcActionErr("");
    setCdcActionInfo("");
    try {
      const res = await fetch(`/api/connector-groups/${group.group_id}/start`, { method: "POST" });
      const body = await res.json().catch(() => ({})) as CdcConnectorActionResp;
      if (!res.ok) {
        throw new Error(body.error || `HTTP ${res.status}`);
      }
      onReload();
      loadDebeziumSyncStatus(group.group_id);
      const connectorStatus = String(body.status || "").toUpperCase();
      const connectorText = connectorStatus === "RUNNING"
        ? "CDC-коннектор RUNNING"
        : connectorStatus
          ? `Запуск CDC-коннектора: ${connectorStatus}`
          : "Запуск CDC-коннектора запрошен";
      if (body.plan_start_error) {
        setCdcActionErr(`${connectorText}, но очередь не продолжена: ${body.plan_start_error}`);
      } else {
        const startedCount = (body.plan_starts || []).reduce(
          (sum: number, item: { started?: unknown[] }) => sum + (item.started?.length || 0),
          0,
        );
        const rowText = connectorStatus === "RUNNING"
          ? "запущено CDC строк"
          : "CDC строк переведено в ожидание коннектора";
        setCdcActionInfo(startedCount
          ? `${connectorText}, ${rowText}: ${startedCount}`
          : body.cdc_queue_kicked
            ? `${connectorText}, очередь CDC продолжена`
          : connectorText);
      }
    } catch (e) {
      setCdcActionErr(e instanceof Error ? e.message : String(e));
    } finally {
      setCdcActionBusy("");
    }
  }

  async function removeCdcGroupTable(group: MigrationPlanCdcGroup, table: MigrationPlanCdcTable) {
    const label = tableLabel(table);
    if (!window.confirm(`Убрать ${label} из CDC-коннектора? Debezium table.include.list будет обновлен.`)) return;
    setCdcActionBusy(label);
    setCdcActionErr("");
    setCdcActionInfo("");
    try {
      const res = await fetch(
        `/api/connector-groups/${group.group_id}/tables/${encodeURIComponent(table.source_schema)}/${encodeURIComponent(table.source_table)}`,
        { method: "DELETE" },
      );
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error || `HTTP ${res.status}`);
      }
      const body = await res.json().catch(() => ({}));
      onReload();
      loadDebeziumSyncStatus(group.group_id);
      if (body.sync_error) {
        setCdcActionErr(`Таблица убрана из CDC-коннектора, но Debezium не синхронизирован: ${body.sync_error}`);
      } else {
        setCdcActionInfo(`${label} убрана из CDC-коннектора`);
      }
    } catch (e) {
      setCdcActionErr(e instanceof Error ? e.message : String(e));
    } finally {
      setCdcActionBusy("");
    }
  }

  if (!plan && loading) {
    return <Shell><Muted>Загрузка пачки...</Muted></Shell>;
  }
  if (!plan) {
    return (
      <Shell>
        <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: effectiveCdcGroup ? 10 : 0 }}>
          <div>
            <Title>Пачка таблиц</Title>
            <Muted>
              {effectiveCdcGroup
                ? "Plan еще не создан, но CDC-коннектор этой миграции уже содержит таблицы."
                : "Пока нет plan для этой миграции. Выделите таблицы и добавьте их в обычную пачку или CDC-коннектор."}
            </Muted>
          </div>
        </div>
        {effectiveCdcGroup && (
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            <CdcConnectorCard
              group={effectiveCdcGroup}
              planItems={[]}
              busyAction={cdcActionBusy}
              syncStatus={cdcSyncStatus}
              syncStatusLoading={cdcSyncStatusLoading}
              syncStatusErr={cdcSyncStatusErr}
              onSync={syncCdcGroup}
              onStart={startCdcGroup}
              showExtraTables={false}
            />
            <CdcConnectorDetails
              group={effectiveCdcGroup}
              planItems={[]}
              planSourceSchema=""
              busyKey={cdcActionBusy}
              onRemoveExtra={removeCdcGroupTable}
            />
          </div>
        )}
        {cdcActionErr && (
          <div style={{
            marginTop: 10, padding: "7px 10px", borderRadius: t.radius.sm,
            background: `${t.red.border}22`, border: `1px solid ${t.red.border}`,
            color: t.red.fg, fontSize: 12,
          }}>
            {cdcActionErr}
          </div>
        )}
        {cdcActionInfo && (
          <div style={{
            marginTop: 10, padding: "7px 10px", borderRadius: t.radius.sm,
            background: t.green.bg, border: `1px solid ${t.green.dim}`,
            color: t.green.fg, fontSize: 12,
          }}>
            {cdcActionInfo}
          </div>
        )}
      </Shell>
    );
  }

  const total = plan.items.length;
  const done = plan.items.filter(isDoneItem).length;
  const failed = plan.items.filter(isFailedItem).length;
  const active = plan.items.filter(isActiveWorkItem).length;
  const running = plan.items.filter(isRunningItem).length;
  const pending = plan.items.filter(isQueuedItem).length;
  const actualPending = plan.items.filter(i => i.status === "PENDING").length;
  const progress = total ? done / total * 100 : 0;
  const hasPending = actualPending > 0;
  const nextPendingBatch = batches.find(([, items]) => items.some(i => i.status === "PENDING"));
  const nextPendingItems = nextPendingBatch?.[1].filter(i => i.status === "PENDING") || [];
  const runningItems = plan.items.filter(isRunningItem);
  const runningHasNonCdc = runningItems.some(i => !isCdcItem(i));
  const nextPendingIsCdc = nextPendingItems.length > 0 && nextPendingItems.every(isCdcItem);
  const canStart = ["READY", "RUNNING"].includes(plan.status)
    && hasPending
    && (running === 0 || (nextPendingIsCdc && !runningHasNonCdc));
  const currentBatch = batches.find(([, items]) => items.some(isActiveWorkItem))
    || batches.find(([, items]) => items.some(isRunningItem))
    || batches.find(([, items]) => items.some(i => i.status === "PENDING"))
    || batches[batches.length - 1];

  return (
    <Shell>
      <div style={{
        display: "flex", justifyContent: "space-between",
        alignItems: "flex-start", gap: 12, marginBottom: 12,
      }}>
        <div>
          <Title>Пачка таблиц</Title>
          <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap", marginTop: 5 }}>
            <Badge tone={failed ? "bad" : plan.status === "RUNNING" ? "run" : done === total ? "ok" : "idle"}>
              {plan.status}
            </Badge>
            <span style={{ fontFamily: t.font.mono, color: t.text.muted, fontSize: 12 }}>
              #{plan.plan_id} · {done}/{total} готово · {active} в работе
            </span>
          </div>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          {variant === "overview" && onOpenDetails && (
            <button onClick={onOpenDetails} style={secondaryActionStyle(false)}>Детали</button>
          )}
          <button onClick={onReload} style={secondaryActionStyle(false)}>Обновить</button>
          <button
            onClick={onStart}
            disabled={!canStart || busy}
            style={{
              ...primaryActionStyle(busy),
              opacity: canStart && !busy ? 1 : 0.45,
              cursor: canStart && !busy ? "pointer" : "default",
            }}
          >
            {busy ? "Запуск..." : plan.status === "RUNNING" ? "Продолжить" : "Старт"}
          </button>
        </div>
      </div>

      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 12 }}>
        <ProgressBar value={progress} tone={failed ? "error" : "info"} height={7}/>
        <span style={{ minWidth: 52, textAlign: "right", fontFamily: t.font.mono, fontSize: 12 }}>
          {progress.toFixed(0)}%
        </span>
      </div>

      {error && (
        <div style={{
          marginBottom: 10, padding: "7px 10px", borderRadius: t.radius.sm,
          background: `${t.red.border}22`, border: `1px solid ${t.red.border}`,
          color: t.red.fg, fontSize: 12,
        }}>
          {error}
        </div>
      )}
      {cdcActionErr && (
        <div style={{
          marginBottom: 10, padding: "7px 10px", borderRadius: t.radius.sm,
          background: `${t.red.border}22`, border: `1px solid ${t.red.border}`,
          color: t.red.fg, fontSize: 12,
        }}>
          {cdcActionErr}
        </div>
      )}
      {cdcActionInfo && (
        <div style={{
          marginBottom: 10, padding: "7px 10px", borderRadius: t.radius.sm,
          background: t.green.bg, border: `1px solid ${t.green.dim}`,
          color: t.green.fg, fontSize: 12,
        }}>
          {cdcActionInfo}
        </div>
      )}

      {variant === "overview" && (
        <PlanOverview
          batchCount={batches.length}
          total={total}
          done={done}
          running={active}
          pending={pending}
          failed={failed}
          currentBatch={currentBatch}
          items={plan.items}
          planSourceSchema={plan.src_schema}
          cdcGroup={effectiveCdcGroup}
          cdcActionBusy={cdcActionBusy}
          cdcSyncStatus={cdcSyncStatus}
          cdcSyncStatusLoading={cdcSyncStatusLoading}
          cdcSyncStatusErr={cdcSyncStatusErr}
          onSyncCdcGroup={syncCdcGroup}
          onStartCdcGroup={startCdcGroup}
          canStart={canStart}
        />
      )}

      {variant === "detail" && (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {packGroups(plan.items).map(pack => (
            <div key={pack.key} style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              <div style={{
                fontSize: 12,
                fontWeight: 700,
                color: t.text.primary,
                padding: "4px 2px",
              }}>
                {pack.title} · {pack.items.length} таблиц
              </div>
              {pack.items.length === 0 ? (
                <div style={{ fontSize: 12, color: t.text.muted, padding: "4px 2px" }}>
                  Таблицы ещё не добавлены.
                </div>
              ) : groupBatches(pack.items).map(([batch, items]) => (
                <div key={`${pack.key}-${batch}`} style={{
                  border: `1px solid ${t.border.subtle}`,
                  borderRadius: t.radius.md,
                  overflow: "hidden",
                  background: t.bg.s2,
                }}>
                  <div style={{
                    display: "flex", justifyContent: "space-between", alignItems: "center",
                    padding: "7px 10px", borderBottom: `1px solid ${t.border.subtle}`,
                  }}>
                    <span style={{ fontSize: 12, fontWeight: 700, color: t.text.primary }}>Позиция {batch}</span>
                    <span style={{ fontSize: 11, color: t.text.muted }}>{items.length} таблиц</span>
                  </div>
                  {items.map(item => (
                    <PlanRow
                      key={item.item_id}
                      item={item}
                      cdcGroupStatus={pack.key === "cdc" ? effectiveCdcGroup?.status : undefined}
                      onReload={onReload}
                      sseEvents={sseEvents}
                    />
                  ))}
                </div>
              ))}
              {pack.key === "cdc" && effectiveCdcGroup && (
                <>
                  <CdcConnectorCard
                    group={effectiveCdcGroup}
                    planItems={pack.items}
                    planSourceSchema={plan.src_schema}
                    busyAction={cdcActionBusy}
                    syncStatus={cdcSyncStatus}
                    syncStatusLoading={cdcSyncStatusLoading}
                    syncStatusErr={cdcSyncStatusErr}
                    onSync={syncCdcGroup}
                    onStart={startCdcGroup}
                  />
                  <CdcConnectorDetails
                    group={effectiveCdcGroup}
                    planItems={pack.items}
                    planSourceSchema={plan.src_schema}
                    busyKey={cdcActionBusy}
                    onRemoveExtra={removeCdcGroupTable}
                  />
                </>
              )}
            </div>
          ))}
        </div>
      )}
    </Shell>
  );
}

function groupBatches(items: MigrationPlanItem[]) {
  const map = new Map<number, MigrationPlanItem[]>();
  for (const item of items) {
    const key = item.batch_order || 1;
    if (!map.has(key)) map.set(key, []);
    map.get(key)!.push(item);
  }
  return Array.from(map.entries()).sort((a, b) => a[0] - b[0]);
}

function isCdcItem(item: MigrationPlanItem) {
  return item.mode === "CDC" || String(item.strategy || "").startsWith("CDC");
}

function packGroups(items: MigrationPlanItem[]) {
  return [
    { key: "bulk", title: "Обычная пачка", items: items.filter(i => !isCdcItem(i)) },
    { key: "cdc", title: "CDC-коннектор", items: items.filter(isCdcItem) },
  ];
}

function PlanOverview({
  batchCount,
  total,
  done,
  running,
  pending,
  failed,
  currentBatch,
  items,
  planSourceSchema,
  cdcGroup,
  cdcActionBusy,
  cdcSyncStatus,
  cdcSyncStatusLoading,
  cdcSyncStatusErr,
  onSyncCdcGroup,
  onStartCdcGroup,
  canStart,
}: {
  batchCount: number;
  total: number;
  done: number;
  running: number;
  pending: number;
  failed: number;
  currentBatch?: [number, MigrationPlanItem[]];
  items: MigrationPlanItem[];
  planSourceSchema: string;
  cdcGroup: MigrationPlanCdcGroup | null;
  cdcActionBusy: string;
  cdcSyncStatus: DebeziumSyncStatus | null;
  cdcSyncStatusLoading: boolean;
  cdcSyncStatusErr: string;
  onSyncCdcGroup: (group: MigrationPlanCdcGroup) => void;
  onStartCdcGroup: (group: MigrationPlanCdcGroup) => void;
  canStart: boolean;
}) {
  const [batchNo, batchItems]: [number, MigrationPlanItem[]] = currentBatch || [0, []];
  const batchDone = batchItems.filter(isDoneItem).length;
  const batchRunning = batchItems.filter(isActiveWorkItem).length;
  const batchFailed = batchItems.filter(isFailedItem).length;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
      <div style={{
        display: "grid",
        gridTemplateColumns: "repeat(5, minmax(92px, 1fr))",
        gap: 8,
      }}>
        <Stat label="Позиций" value={batchCount}/>
        <Stat label="Таблиц" value={total}/>
        <Stat label="Готово" value={done}/>
        <Stat label="В работе" value={running}/>
        <Stat label="Ошибки" value={failed}/>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
        {packGroups(items).map(pack => <PackCard key={pack.key} title={pack.title} items={pack.items}/>)}
      </div>

      {cdcGroup && (
        <CdcConnectorCard
          group={cdcGroup}
          planItems={items.filter(isCdcItem)}
          planSourceSchema={planSourceSchema}
          busyAction={cdcActionBusy}
          syncStatus={cdcSyncStatus}
          syncStatusLoading={cdcSyncStatusLoading}
          syncStatusErr={cdcSyncStatusErr}
          onSync={onSyncCdcGroup}
          onStart={onStartCdcGroup}
        />
      )}

      <div style={{
        display: "grid",
        gridTemplateColumns: "minmax(0, 1fr)",
        gap: 10,
      }}>
        <div style={{
          padding: "8px 10px",
          border: `1px solid ${t.border.subtle}`,
          borderRadius: t.radius.md,
          background: t.bg.s2,
          minWidth: 0,
        }}>
          <div style={{ fontSize: 11, color: t.text.muted, marginBottom: 5 }}>Текущая позиция запуска</div>
          {batchNo ? (
            <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
              <span style={{ fontSize: 13, fontWeight: 700, color: t.text.primary }}>Позиция {batchNo}</span>
              <span style={{ fontSize: 12, color: t.text.muted }}>
                {batchDone}/{batchItems.length} готово · {batchRunning} в работе · {batchFailed} ошибок
              </span>
            </div>
          ) : (
            <div style={{ fontSize: 12, color: t.text.muted }}>Нет таблиц в пачке</div>
          )}
        </div>
      </div>

      {pending > 0 && canStart && failed === 0 && (
        <div style={{ fontSize: 12, color: t.text.muted }}>
          Готово к запуску: в очереди {pending} таблиц.
        </div>
      )}
    </div>
  );
}

function CdcConnectorCard({
  group,
  planItems,
  planSourceSchema = "",
  busyAction,
  syncStatus,
  syncStatusLoading,
  syncStatusErr,
  onSync,
  onStart,
  showExtraTables = true,
}: {
  group: MigrationPlanCdcGroup;
  planItems: MigrationPlanItem[];
  planSourceSchema?: string;
  busyAction: string;
  syncStatus: DebeziumSyncStatus | null;
  syncStatusLoading: boolean;
  syncStatusErr: string;
  onSync: (group: MigrationPlanCdcGroup) => void;
  onStart: (group: MigrationPlanCdcGroup) => void;
  showExtraTables?: boolean;
}) {
  const planKeys = new Set(planItems.map(item => planItemTableKey(item, planSourceSchema)));
  const connectorTables = group.tables || [];
  const extraTables = showExtraTables
    ? connectorTables.filter(tbl => !planKeys.has(cdcTableKey(tbl)))
    : [];
  const keyColsCount = connectorTables.filter(tbl => {
    if (tbl.source_pk_exists || tbl.source_uk_exists) return false;
    const raw = tbl.effective_key_columns_json;
    if (Array.isArray(raw)) return raw.length > 0;
    return String(raw || "[]") !== "[]";
  }).length;
  const connectorPreview = connectorTables.slice(0, 6).map(tableLabel);
  const connectorRest = Math.max(0, connectorTables.length - connectorPreview.length);
  const status = String(group.status || "").toUpperCase();
  const pendingDraftCdc = planItems.filter(item => {
    const itemStatus = String(item.status || "").toUpperCase();
    const phase = String(item.phase || "").toUpperCase();
    return itemStatus === "PENDING" || phase === "DRAFT";
  }).length;
  const waitingConnector = planItems.filter(item => isNewPhase(item) && status !== "RUNNING").length;
  const runnableNewCdc = planItems.filter(item => isNewPhase(item) && status === "RUNNING");
  const queuedCdc = runnableNewCdc.filter(item => item.queue_position != null).length;
  const readyCdc = runnableNewCdc.length - queuedCdc;
  const applyingCdc = planItems.filter(item => {
    const phase = String(item.phase || "").toUpperCase();
    return phase === "CDC_APPLY_STARTING" || phase === "CDC_APPLYING" || phase === "CDC_CATCHING_UP";
  }).length;
  const hasRawConfig = Boolean(
    group.table_include_list
    || group.active_topic_prefix
    || group.topic_prefix
    || group.message_key_columns,
  );
  const canStartConnector = !["RUNNING", "TOPICS_CREATING", "CONNECTOR_STARTING", "STOPPING"].includes(status);
  const syncBusy = busyAction === "sync";
  const startBusy = busyAction === "start";
  const syncProblem = Boolean(
    syncStatusErr
    || (syncStatus && (!syncStatus.exists || !syncStatus.in_sync)),
  );

  return (
    <div style={{
      padding: "9px 10px",
      border: `1px solid ${extraTables.length ? t.amber.dim : t.border.subtle}`,
      borderRadius: t.radius.md,
      background: extraTables.length ? t.amber.bg : t.bg.s2,
      minWidth: 0,
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
        <div style={{ display: "flex", gap: 8, alignItems: "center", minWidth: 0, flexWrap: "wrap" }}>
          <div style={{ fontSize: 13, fontWeight: 700, color: t.text.primary }}>CDC-коннектор</div>
          <Badge tone={group.status === "RUNNING" ? "run" : group.status === "FAILED" ? "bad" : "idle"}>
            {group.status}
          </Badge>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center", minWidth: 0 }}>
          <span style={{ fontFamily: t.font.mono, color: t.text.muted, fontSize: 11, overflow: "hidden", textOverflow: "ellipsis" }}>
            {group.active_connector_name || group.connector_name}
          </span>
          {canStartConnector && (
            <button
              onClick={() => onStart(group)}
              disabled={!!busyAction}
              style={{
                ...primaryActionStyle(!!busyAction),
                padding: "3px 8px",
                fontSize: 11,
                opacity: startBusy ? 0.55 : 1,
              }}
            >
              {startBusy ? "Запуск..." : "Запустить"}
            </button>
          )}
          <button
            onClick={() => onSync(group)}
            disabled={!!busyAction}
            style={{
              ...secondaryActionStyle(false),
              padding: "3px 8px",
              fontSize: 11,
              opacity: syncBusy ? 0.55 : 1,
            }}
          >
            {syncBusy ? "Синхронизация..." : "Синхронизировать"}
          </button>
        </div>
      </div>
      <div style={{ marginTop: 7, display: "flex", gap: 12, flexWrap: "wrap", fontSize: 12, color: t.text.muted }}>
        <span>Таблиц в Debezium: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{connectorTables.length}</strong></span>
        <span>Строк CDC в пачке: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{planItems.length}</strong></span>
        <span>Не запущены: <strong style={{ color: pendingDraftCdc ? t.amber.fg : t.text.primary, fontFamily: t.font.mono }}>{pendingDraftCdc}</strong></span>
        <span>Ждут коннектор: <strong style={{ color: waitingConnector ? t.amber.fg : t.text.primary, fontFamily: t.font.mono }}>{waitingConnector}</strong></span>
        <span>Стартуют: <strong style={{ color: readyCdc ? t.blue.fg : t.text.primary, fontFamily: t.font.mono }}>{readyCdc}</strong></span>
        <span>В очереди: <strong style={{ color: queuedCdc ? t.blue.fg : t.text.primary, fontFamily: t.font.mono }}>{queuedCdc}</strong></span>
        <span>Применяются: <strong style={{ color: applyingCdc ? t.green.fg : t.text.primary, fontFamily: t.font.mono }}>{applyingCdc}</strong></span>
        <span>Ручных ключей: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{keyColsCount}</strong></span>
      </div>
      <div style={{ marginTop: 7, fontSize: 12, color: t.text.secondary, lineHeight: 1.45 }}>
        {connectorPreview.length > 0 ? (
          <>
            Debezium читает весь состав CDC-коннектора: <span style={{ fontFamily: t.font.mono, color: t.text.primary }}>{connectorPreview.join(", ")}</span>
            {connectorRest > 0 && <span style={{ color: t.text.muted }}> +{connectorRest} еще</span>}
          </>
        ) : (
          <span style={{ color: t.text.muted }}>В Debezium пока нет таблиц.</span>
        )}
      </div>
      {extraTables.length > 0 && (
        <div style={{ marginTop: 7, fontSize: 12, color: t.amber.fg }}>
          В Debezium уже есть таблицы без активной строки в очереди: {extraTables.map(tableLabel).join(", ")}
        </div>
      )}
      {(syncStatusLoading || syncStatusErr || syncStatus) && (
        <div style={{
          marginTop: 7,
          padding: "6px 8px",
          borderRadius: t.radius.sm,
          border: `1px solid ${
            syncStatusErr
              ? t.red.border
              : syncProblem
                ? t.amber.dim
                : t.green.dim
          }`,
          background: syncStatusErr
            ? `${t.red.border}22`
            : syncProblem
              ? t.amber.bg
              : t.green.bg,
          color: syncStatusErr
            ? t.red.fg
            : syncProblem
              ? t.amber.fg
              : t.green.fg,
          fontSize: 12,
          lineHeight: 1.4,
          overflowWrap: "anywhere",
        }}>
          {syncStatusLoading && <div>Проверяю фактический config Kafka Connect...</div>}
          {syncStatusErr && <div>Kafka Connect config не прочитан: {syncStatusErr}</div>}
          {syncStatus && (
            <>
              <div>
                Kafka Connect config: <strong>{syncStatus.exists ? (syncStatus.in_sync ? "совпадает" : "есть расхождение") : "коннектор не найден"}</strong>
                {" "}({syncStatus.connector_name})
              </div>
              {syncStatus.missing_tables.length > 0 && (
                <div style={{ marginTop: 3 }}>
                  Нет в Kafka Connect: <span style={{ fontFamily: t.font.mono, color: t.text.primary }}>{syncStatus.missing_tables.join(", ")}</span>
                </div>
              )}
              {syncStatus.extra_tables.length > 0 && (
                <div style={{ marginTop: 3 }}>
                  Лишние в Kafka Connect: <span style={{ fontFamily: t.font.mono, color: t.text.primary }}>{syncStatus.extra_tables.join(", ")}</span>
                </div>
              )}
              {!syncStatus.key_columns_match && (
                <div style={{ marginTop: 3 }}>Расходятся CDC key columns.</div>
              )}
              {syncStatus.actual_table_include_list && (
                <div style={{ marginTop: 3, fontFamily: t.font.mono, color: t.text.primary }}>
                  actual table.include.list: {syncStatus.actual_table_include_list}
                </div>
              )}
            </>
          )}
        </div>
      )}
      {group.error_text && (
        <div style={{
          marginTop: 7,
          padding: "6px 8px",
          borderRadius: t.radius.sm,
          border: `1px solid ${t.red.border}`,
          background: `${t.red.border}22`,
          color: t.red.fg,
          fontSize: 12,
          lineHeight: 1.35,
          overflowWrap: "anywhere",
        }}>
          {group.error_text}
        </div>
      )}
      {group.status !== "RUNNING" && planItems.some(item => String(item.phase || "").toUpperCase() === "NEW") && (
        <div style={{ marginTop: 7, fontSize: 12, color: t.text.muted }}>
          CDC-строки ждут запуска коннектора и продолжат работу после статуса RUNNING.
        </div>
      )}
      {pendingDraftCdc > 0 && (
        <div style={{ marginTop: 7, fontSize: 12, color: t.amber.fg, lineHeight: 1.4 }}>
          Есть CDC-строки, которые еще не переведены в NEW. Обычно это значит, что Debezium не синхронизировался при добавлении; нажмите "Синхронизировать" или "Запустить" после проверки ошибки коннектора.
        </div>
      )}
      {hasRawConfig && (
        <details style={{ marginTop: 6 }}>
          <summary style={{ cursor: "pointer", color: t.text.muted, fontSize: 11 }}>
            Диагностика Debezium
          </summary>
          {group.table_include_list && (
            <MonoLine>table.include.list: {group.table_include_list}</MonoLine>
          )}
          {(group.active_topic_prefix || group.topic_prefix) && (
            <MonoLine>topic.prefix: {group.active_topic_prefix || group.topic_prefix}</MonoLine>
          )}
          {group.message_key_columns && (
            <MonoLine>message.key.columns: {group.message_key_columns}</MonoLine>
          )}
        </details>
      )}
    </div>
  );
}

function CdcConnectorDetails({
  group,
  planItems,
  planSourceSchema,
  busyKey,
  onRemoveExtra,
}: {
  group: MigrationPlanCdcGroup;
  planItems: MigrationPlanItem[];
  planSourceSchema: string;
  busyKey: string;
  onRemoveExtra: (group: MigrationPlanCdcGroup, table: MigrationPlanCdcTable) => void;
}) {
  const planItemsByTable = new Map<string, MigrationPlanItem[]>();
  for (const item of planItems) {
    const key = planItemTableKey(item, planSourceSchema);
    if (!planItemsByTable.has(key)) planItemsByTable.set(key, []);
    planItemsByTable.get(key)!.push(item);
  }
  const rows = group.tables || [];
  if (rows.length === 0) return null;
  return (
    <div style={{
      border: `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.md,
      overflow: "hidden",
      background: t.bg.s2,
    }}>
      <div style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        padding: "7px 10px",
        borderBottom: `1px solid ${t.border.subtle}`,
      }}>
        <span style={{ fontSize: 12, fontWeight: 700, color: t.text.primary }}>Фактический состав единственного CDC-коннектора</span>
        <span style={{ fontSize: 11, color: t.text.muted }}>table.include.list: {rows.length} таблиц</span>
      </div>
      {rows.map(tbl => {
        const tablePlanItems = planItemsByTable.get(cdcTableKey(tbl)) || [];
        const inPlan = tablePlanItems.length > 0;
        const hasActivePlanItem = tablePlanItems.some(isActiveCdcPlanItem);
        const canRemove = !hasActivePlanItem;
        return (
          <div key={tbl.id} style={{
            display: "grid",
            gridTemplateColumns: "minmax(170px, 1fr) 100px minmax(150px, 1fr) 92px",
            gap: 10,
            alignItems: "center",
            padding: "7px 10px",
            borderTop: `1px solid ${t.bg.s1}`,
            fontSize: 12,
          }}>
            <div style={{ fontFamily: t.font.mono, color: t.text.primary, overflow: "hidden", textOverflow: "ellipsis" }}>
              {tableLabel(tbl)}
            </div>
            <Badge tone={inPlan ? "ok" : "idle"}>{inPlan ? "в пачке" : "коннектор"}</Badge>
            <div style={{ fontFamily: t.font.mono, color: t.text.muted, overflow: "hidden", textOverflow: "ellipsis" }}>
              {tbl.topic_name || "-"}
            </div>
            <div style={{ textAlign: "right" }}>
              {canRemove && (
                <button
                  onClick={() => onRemoveExtra(group, tbl)}
                  disabled={busyKey === tableLabel(tbl)}
                  style={{
                    ...secondaryActionStyle(false),
                    padding: "3px 8px",
                    fontSize: 11,
                    opacity: busyKey === tableLabel(tbl) ? 0.55 : 1,
                  }}
                >
                  {busyKey === tableLabel(tbl) ? "..." : "Убрать"}
                </button>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function PackCard({ title, items }: { title: string; items: MigrationPlanItem[] }) {
  const done = items.filter(isDoneItem).length;
  const running = items.filter(isActiveWorkItem).length;
  const failed = items.filter(isFailedItem).length;
  const steps = new Set(items.map(i => i.batch_order || 1)).size;
  return (
    <div style={{
      padding: "9px 10px",
      border: `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.md,
      background: t.bg.s2,
      minWidth: 0,
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "center" }}>
        <div style={{ fontSize: 13, fontWeight: 700, color: t.text.primary }}>{title}</div>
        <Badge tone={failed ? "bad" : running ? "run" : done && done === items.length ? "ok" : "idle"}>
          {items.length ? `${done}/${items.length}` : "empty"}
        </Badge>
      </div>
      <div style={{ marginTop: 7, display: "flex", gap: 12, flexWrap: "wrap", fontSize: 12, color: t.text.muted }}>
        <span>Таблиц: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{items.length}</strong></span>
        <span>Позиций: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{steps}</strong></span>
        <span>В работе: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{running}</strong></span>
        <span>Ошибки: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{failed}</strong></span>
      </div>
    </div>
  );
}

function tableLabel(tbl: { source_schema: string; source_table: string }) {
  return `${tbl.source_schema}.${tbl.source_table}`;
}

function cdcTableKey(tbl: { source_schema: string; source_table: string }) {
  return `${tbl.source_schema}.${tbl.source_table}`.toUpperCase();
}

function planItemTableKey(item: MigrationPlanItem, sourceSchema: string) {
  return `${sourceSchema}.${item.table_name}`.toUpperCase();
}

function MonoLine({ children }: { children: React.ReactNode }) {
  return (
    <div style={{
      marginTop: 6,
      fontFamily: t.font.mono,
      fontSize: 11,
      color: t.text.muted,
      overflow: "hidden",
      textOverflow: "ellipsis",
      whiteSpace: "nowrap",
    }}>
      {children}
    </div>
  );
}

function Stat({ label, value }: { label: string; value: number }) {
  return (
    <div style={{
      padding: "8px 10px",
      border: `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.md,
      background: t.bg.s2,
    }}>
      <div style={{ fontSize: 10.5, color: t.text.muted, marginBottom: 3 }}>{label}</div>
      <div style={{ fontSize: 16, fontWeight: 700, fontFamily: t.font.mono, color: t.text.primary }}>
        {value}
      </div>
    </div>
  );
}

function isNewPhase(item: MigrationPlanItem) {
  return String(item.phase || "").toUpperCase() === "NEW";
}

function PlanRow({
  item,
  cdcGroupStatus,
  onReload,
  sseEvents,
}: {
  item: MigrationPlanItem;
  cdcGroupStatus?: string;
  onReload: () => void;
  sseEvents: SSEEvent[];
}) {
  const rowsLoaded = item.rows_loaded || 0;
  const totalRows = item.total_rows || 0;
  const progress = totalRows ? rowsLoaded / totalRows * 100 : undefined;
  const status = itemStatusLabel(item, cdcGroupStatus);
  const progressText = itemProgressText(item, progress, cdcGroupStatus);
  const visual = itemVisualState(item);
  const showTriggerJob = shouldShowTriggerJob(item);
  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: "minmax(160px, 1fr) minmax(145px, auto) minmax(150px, 180px) minmax(120px, auto)",
      gap: 10,
      alignItems: "center",
      padding: "7px 10px",
      borderTop: `1px solid ${t.bg.s1}`,
      fontSize: 12,
    }}>
      <div style={{ minWidth: 0 }}>
        <div style={{ fontFamily: t.font.mono, color: t.text.primary, overflow: "hidden", textOverflow: "ellipsis" }}>
          {item.table_name}
        </div>
        {item.error_text && (
          <div style={{ color: t.red.fg, fontSize: 11, marginTop: 2, overflow: "hidden", textOverflow: "ellipsis" }}>
            {item.error_text}
          </div>
        )}
      </div>
      <Badge tone={visual === "done" ? "ok" : visual === "running" ? "run" : visual === "failed" ? "bad" : "idle"}>
        {status}
      </Badge>
      <div style={{
        fontFamily: t.font.mono,
        color: t.text.muted,
        textAlign: "right",
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
      }}>
        {progressText}
      </div>
      <div style={{ display: "flex", justifyContent: "flex-end", minWidth: 0 }}>
        {showTriggerJob ? (
          <TriggerJobInlineAction migrationId={item.migration_id!} onDone={onReload} sseEvents={sseEvents} />
        ) : (
          <span style={{ color: t.text.disabled, fontSize: 11 }}>-</span>
        )}
      </div>
    </div>
  );
}

function TriggerJobInlineAction({
  migrationId,
  onDone,
  sseEvents,
}: {
  migrationId: string;
  onDone: () => void;
  sseEvents: SSEEvent[];
}) {
  const [jobs, setJobs] = React.useState<TargetTriggerJob[]>([]);
  const [busy, setBusy] = React.useState("");
  const [err, setErr] = React.useState("");
  const latest = jobs[0];

  const loadJobs = React.useCallback(async () => {
    const res = await fetch(`/api/migrations/${migrationId}/trigger-jobs`);
    if (res.ok) {
      setJobs(await res.json());
    }
  }, [migrationId]);

  React.useEffect(() => {
    let alive = true;
    async function tick() {
      const res = await fetch(`/api/migrations/${migrationId}/trigger-jobs`);
      if (alive && res.ok) setJobs(await res.json());
    }
    tick();
    const id = setInterval(tick, 5000);
    return () => { alive = false; clearInterval(id); };
  }, [migrationId]);

  React.useEffect(() => {
    const event = sseEvents[0];
    if (!event || event.type !== "target_trigger_job" || event.migration_id !== migrationId) return;
    loadJobs();
  }, [sseEvents, migrationId, loadJobs]);

  async function createJob() {
    setBusy("create");
    setErr("");
    try {
      const res = await fetch(`/api/migrations/${migrationId}/trigger-jobs`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ requested_by: "ui" }),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error || `HTTP ${res.status}`);
      }
      await loadJobs();
      onDone();
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy("");
    }
  }

  async function runJob(jobId: string) {
    setBusy("run");
    setErr("");
    try {
      const res = await fetch(`/api/migrations/${migrationId}/trigger-jobs/${jobId}/run`, { method: "POST" });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error || `HTTP ${res.status}`);
      }
      await loadJobs();
      onDone();
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy("");
    }
  }

  if (latest?.state === "DONE") {
    return (
      <span style={{ fontSize: 11, color: t.green.fg, fontWeight: 700, whiteSpace: "nowrap" }}>
        triggers on: {latest.enabled_count}
      </span>
    );
  }

  const failed = latest?.state === "FAILED";
  const running = latest?.state === "RUNNING";
  const pending = latest?.state === "PENDING";
  const label = busy
    ? "..."
    : running
      ? "job running"
      : pending
        ? "Запустить триггеры"
        : "Создать job";

  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 3, minWidth: 0 }}>
      <button
        onClick={pending ? () => runJob(latest.job_id) : createJob}
        disabled={!!busy || running}
        style={{
          ...secondaryActionStyle(false),
          padding: "3px 8px",
          fontSize: 11,
          opacity: busy || running ? 0.55 : 1,
          whiteSpace: "nowrap",
        }}
      >
        {label}
      </button>
      {(failed && latest?.error_text) || err ? (
        <span style={{
          color: t.red.fg,
          fontSize: 10.5,
          maxWidth: 180,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}>
          {err || latest?.error_text}
        </span>
      ) : null}
    </div>
  );
}

function shouldShowTriggerJob(item: MigrationPlanItem) {
  if (!item.migration_id) return false;
  const phase = String(item.phase || "").toUpperCase();
  if (isCdcItem(item)) {
    return ["CDC_CAUGHT_UP", "STEADY_STATE"].includes(phase);
  }
  return phase === "COMPLETED";
}

function itemProgressText(item: MigrationPlanItem, progress: number | undefined, cdcGroupStatus?: string) {
  const phase = String(item.phase || "").toUpperCase();
  const groupStatus = String(cdcGroupStatus || "").toUpperCase();
  if (isCdcItem(item) && phase === "NEW") {
    if (groupStatus && groupStatus !== "RUNNING") return `ждет ${groupStatus}`;
    if (item.queue_position != null) return `очередь #${item.queue_position}`;
    return "стартует";
  }
  if (phase === "NEW" && item.queue_position != null) {
    return `очередь #${item.queue_position}`;
  }
  if (isCdcItem(item) && ["CDC_APPLY_STARTING", "CDC_APPLYING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP", "STEADY_STATE"].includes(phase)) {
    const rows = item.cdc_rows_applied ?? null;
    const lag = item.cdc_total_lag ?? null;
    if (rows !== null || lag !== null) {
      const parts = [];
      if (rows !== null) parts.push(`cdc ${rows}`);
      if (lag !== null) parts.push(`lag ${lag}`);
      return parts.join(" · ");
    }
    return "cdc active";
  }
  return progress === undefined ? "rows n/a" : `${progress.toFixed(0)}%`;
}

function itemStatusLabel(item: MigrationPlanItem, cdcGroupStatus?: string) {
  const phase = String(item.phase || "").toUpperCase();
  const status = String(item.status || "").toUpperCase();
  const groupStatus = String(cdcGroupStatus || "").toUpperCase();
  if (isCdcItem(item) && status === "RUNNING" && phase === "NEW") {
    if (groupStatus && groupStatus !== "RUNNING") return `ЖДЕТ ${groupStatus}`;
    if (item.queue_position != null) return "В ОЧЕРЕДИ";
    return "СТАРТУЕТ";
  }
  return item.phase || item.status;
}

function itemVisualState(item: MigrationPlanItem): "done" | "failed" | "queued" | "running" | "idle" {
  const phase = String(item.phase || "").toUpperCase();
  const status = String(item.status || "").toUpperCase();
  if (status === "DONE" || phase === "COMPLETED" || phase === "STEADY_STATE") return "done";
  if (BAD.has(status) || phase === "FAILED" || phase === "CANCELLED") return "failed";
  if (status === "PENDING" || phase === "DRAFT" || phase === "NEW") return "queued";
  if (status === "RUNNING") return "running";
  return "idle";
}

function isDoneItem(item: MigrationPlanItem) {
  return itemVisualState(item) === "done";
}

function isFailedItem(item: MigrationPlanItem) {
  return itemVisualState(item) === "failed";
}

function isQueuedItem(item: MigrationPlanItem) {
  return itemVisualState(item) === "queued";
}

function isActiveWorkItem(item: MigrationPlanItem) {
  const phase = String(item.phase || "").toUpperCase();
  return ACTIVE_WORK_PHASES.has(phase);
}

function isRunningItem(item: MigrationPlanItem) {
  return itemVisualState(item) === "running";
}

function isActiveCdcPlanItem(item: MigrationPlanItem) {
  const phase = String(item.phase || "").toUpperCase();
  const status = String(item.status || "").toUpperCase();
  if (phase === "FAILED" || phase === "CANCELLED" || phase === "COMPLETED") return false;
  if (status === "FAILED" || status === "CANCELLED") return false;
  return true;
}

function Shell({ children }: { children: React.ReactNode }) {
  return (
    <section style={{
      background: t.bg.s1,
      border: `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.lg,
      padding: 14,
      marginBottom: 12,
      boxShadow: t.shadow.s1,
    }}>
      {children}
    </section>
  );
}

function Title({ children }: { children: React.ReactNode }) {
  return <div style={{ fontSize: 14, fontWeight: 700, color: t.text.primary }}>{children}</div>;
}

function Muted({ children }: { children: React.ReactNode }) {
  return <div style={{ fontSize: 12, color: t.text.muted, marginTop: 4 }}>{children}</div>;
}

function Badge({ children, tone }: { children: React.ReactNode; tone: "ok" | "run" | "bad" | "idle" }) {
  const color = tone === "ok" ? t.green : tone === "run" ? t.blue : tone === "bad" ? t.red : null;
  return (
    <span style={{
      display: "inline-flex", justifyContent: "center",
      minWidth: 82,
      padding: "3px 8px",
      borderRadius: t.radius.sm,
      background: color ? color.bg : t.bg.s3,
      border: `1px solid ${color ? color.dim : t.border.subtle}`,
      color: color ? color.fg : t.text.muted,
      fontSize: 11,
      fontWeight: 700,
      fontFamily: t.font.mono,
      whiteSpace: "nowrap",
    }}>
      {children}
    </span>
  );
}
