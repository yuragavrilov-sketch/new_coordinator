import React, { useMemo } from "react";
import { t } from "../theme";
import { ProgressBar } from "../components/ui";
import { primaryActionStyle, secondaryActionStyle } from "./buttonStyles";
import type { MigrationPlanDetail, MigrationPlanItem } from "./api";

interface Props {
  plan: MigrationPlanDetail | null;
  loading: boolean;
  onStart: () => void;
  onReload: () => void;
  onOpenDetails?: () => void;
  busy: boolean;
  error: string;
  variant?: "overview" | "detail";
}

const DONE = new Set(["DONE"]);
const BAD = new Set(["FAILED", "CANCELLED"]);

export function PlanPanel({
  plan,
  loading,
  onStart,
  onReload,
  onOpenDetails,
  busy,
  error,
  variant = "detail",
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

  if (!plan && loading) {
    return <Shell><Muted>Загрузка пачки...</Muted></Shell>;
  }
  if (!plan) {
    return (
      <Shell>
        <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
          <div>
            <Title>Пачка таблиц</Title>
            <Muted>Пока нет plan для этой миграции. Выделите таблицы и добавьте их в обычную пачку или CDC-коннектор.</Muted>
          </div>
        </div>
      </Shell>
    );
  }

  const total = plan.items.length;
  const done = plan.items.filter(isDoneItem).length;
  const failed = plan.items.filter(isFailedItem).length;
  const running = plan.items.filter(isRunningItem).length;
  const pending = plan.items.filter(isQueuedItem).length;
  const actualPending = plan.items.filter(i => i.status === "PENDING").length;
  const progress = total ? done / total * 100 : 0;
  const hasPending = actualPending > 0;
  const nextPendingBatch = batches.find(([, items]) => items.some(i => i.status === "PENDING"));
  const nextPendingItems = nextPendingBatch?.[1].filter(i => i.status === "PENDING") || [];
  const runningItems = plan.items.filter(i => i.status === "RUNNING");
  const runningHasNonCdc = runningItems.some(i => !isCdcItem(i));
  const nextPendingIsCdc = nextPendingItems.length > 0 && nextPendingItems.every(isCdcItem);
  const canStart = ["READY", "RUNNING"].includes(plan.status)
    && hasPending
    && (running === 0 || (nextPendingIsCdc && !runningHasNonCdc));
  const currentBatch = batches.find(([, items]) => items.some(i => i.status === "RUNNING"))
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
              #{plan.plan_id} · {done}/{total} done · {running} running
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

      {variant === "overview" && (
        <PlanOverview
          batchCount={batches.length}
          total={total}
          done={done}
          running={running}
          pending={pending}
          failed={failed}
          currentBatch={currentBatch}
          items={plan.items}
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
                  {items.map(item => <PlanRow key={item.item_id} item={item}/>)}
                </div>
              ))}
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
  canStart: boolean;
}) {
  const [batchNo, batchItems]: [number, MigrationPlanItem[]] = currentBatch || [0, []];
  const batchDone = batchItems.filter(isDoneItem).length;
  const batchRunning = batchItems.filter(isRunningItem).length;
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
        <Stat label="Done" value={done}/>
        <Stat label="Running" value={running}/>
        <Stat label="Failed" value={failed}/>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
        {packGroups(items).map(pack => <PackCard key={pack.key} title={pack.title} items={pack.items}/>)}
      </div>

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
                {batchDone}/{batchItems.length} done · {batchRunning} running · {batchFailed} failed
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

function PackCard({ title, items }: { title: string; items: MigrationPlanItem[] }) {
  const done = items.filter(isDoneItem).length;
  const running = items.filter(isRunningItem).length;
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
        <span>Running: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{running}</strong></span>
        <span>Failed: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{failed}</strong></span>
      </div>
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

function PlanRow({ item }: { item: MigrationPlanItem }) {
  const rowsLoaded = item.rows_loaded || 0;
  const totalRows = item.total_rows || 0;
  const progress = totalRows ? rowsLoaded / totalRows * 100 : undefined;
  const status = item.phase || item.status;
  const visual = itemVisualState(item);
  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: "minmax(160px, 1fr) 145px 110px",
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
      <div style={{ fontFamily: t.font.mono, color: t.text.muted, textAlign: "right" }}>
        {progress === undefined ? "rows n/a" : `${progress.toFixed(0)}%`}
      </div>
    </div>
  );
}

function itemVisualState(item: MigrationPlanItem): "done" | "failed" | "queued" | "running" | "idle" {
  const phase = String(item.phase || "").toUpperCase();
  const status = String(item.status || "").toUpperCase();
  if (status === "DONE" || phase === "COMPLETED") return "done";
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

function isRunningItem(item: MigrationPlanItem) {
  return itemVisualState(item) === "running";
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
