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
            <Muted>Пока нет plan для этой миграции. Выделите таблицы и добавьте их в историческую или CDC-пачку.</Muted>
          </div>
        </div>
      </Shell>
    );
  }

  const total = plan.items.length;
  const done = plan.items.filter(i => DONE.has(i.status)).length;
  const failed = plan.items.filter(i => BAD.has(i.status)).length;
  const running = plan.items.filter(i => i.status === "RUNNING").length;
  const pending = plan.items.filter(i => i.status === "PENDING").length;
  const progress = total ? done / total * 100 : 0;
  const hasPending = pending > 0;
  const canStart = ["READY", "RUNNING"].includes(plan.status) && hasPending && running === 0;
  const currentBatch = batches.find(([, items]) => items.some(i => i.status === "RUNNING"))
    || batches.find(([, items]) => items.some(i => i.status === "PENDING"))
    || batches[batches.length - 1];
  const modeCounts = countModes(plan.items);
  const groupLabel = "Шаг";

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
          modeCounts={modeCounts}
          groupLabel={groupLabel}
        />
      )}

      {variant === "detail" && (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {batches.map(([batch, items]) => (
            <div key={batch} style={{
              border: `1px solid ${t.border.subtle}`,
              borderRadius: t.radius.md,
              overflow: "hidden",
              background: t.bg.s2,
            }}>
              <div style={{
                display: "flex", justifyContent: "space-between", alignItems: "center",
                padding: "7px 10px", borderBottom: `1px solid ${t.border.subtle}`,
              }}>
                <span style={{ fontSize: 12, fontWeight: 700, color: t.text.primary }}>{groupLabel} {batch}</span>
                <span style={{ fontSize: 11, color: t.text.muted }}>{items.length} таблиц</span>
              </div>
              {items.map(item => <PlanRow key={item.item_id} item={item}/>)}
            </div>
          ))}
        </div>
      )}
    </Shell>
  );
}

function countModes(items: MigrationPlanItem[]) {
  const out = new Map<string, number>();
  for (const item of items) {
    const mode = item.mode || item.strategy || "UNKNOWN";
    out.set(mode, (out.get(mode) || 0) + 1);
  }
  return Array.from(out.entries()).sort((a, b) => b[1] - a[1]);
}

function PlanOverview({
  batchCount,
  total,
  done,
  running,
  pending,
  failed,
  currentBatch,
  modeCounts,
  groupLabel,
}: {
  batchCount: number;
  total: number;
  done: number;
  running: number;
  pending: number;
  failed: number;
  currentBatch?: [number, MigrationPlanItem[]];
  modeCounts: Array<[string, number]>;
  groupLabel: string;
}) {
  const [batchNo, batchItems]: [number, MigrationPlanItem[]] = currentBatch || [0, []];
  const batchDone = batchItems.filter(i => DONE.has(i.status)).length;
  const batchRunning = batchItems.filter(i => i.status === "RUNNING").length;
  const batchFailed = batchItems.filter(i => BAD.has(i.status)).length;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
      <div style={{
        display: "grid",
        gridTemplateColumns: "repeat(5, minmax(92px, 1fr))",
        gap: 8,
      }}>
        <Stat label="Шагов" value={batchCount}/>
        <Stat label="Таблиц" value={total}/>
        <Stat label="Done" value={done}/>
        <Stat label="Running" value={running}/>
        <Stat label="Failed" value={failed}/>
      </div>

      <div style={{
        display: "grid",
        gridTemplateColumns: "minmax(0, 1fr) minmax(160px, 260px)",
        gap: 10,
      }}>
        <div style={{
          padding: "8px 10px",
          border: `1px solid ${t.border.subtle}`,
          borderRadius: t.radius.md,
          background: t.bg.s2,
          minWidth: 0,
        }}>
          <div style={{ fontSize: 11, color: t.text.muted, marginBottom: 5 }}>Текущий шаг запуска</div>
          {batchNo ? (
            <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
              <span style={{ fontSize: 13, fontWeight: 700, color: t.text.primary }}>{groupLabel} {batchNo}</span>
              <span style={{ fontSize: 12, color: t.text.muted }}>
                {batchDone}/{batchItems.length} done · {batchRunning} running · {batchFailed} failed
              </span>
            </div>
          ) : (
            <div style={{ fontSize: 12, color: t.text.muted }}>Нет таблиц в пачке</div>
          )}
        </div>

        <div style={{
          padding: "8px 10px",
          border: `1px solid ${t.border.subtle}`,
          borderRadius: t.radius.md,
          background: t.bg.s2,
          minWidth: 0,
        }}>
          <div style={{ fontSize: 11, color: t.text.muted, marginBottom: 5 }}>Режимы</div>
          <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
            {modeCounts.length === 0 ? (
              <span style={{ fontSize: 12, color: t.text.muted }}>n/a</span>
            ) : modeCounts.map(([mode, count]) => (
              <span key={mode} style={{
                padding: "3px 7px",
                borderRadius: t.radius.sm,
                border: `1px solid ${t.border.subtle}`,
                background: t.bg.s1,
                color: t.text.secondary,
                fontSize: 11,
                fontFamily: t.font.mono,
              }}>{mode}: {count}</span>
            ))}
          </div>
        </div>
      </div>

      {pending > 0 && running === 0 && failed === 0 && (
        <div style={{ fontSize: 12, color: t.text.muted }}>
          Готово к запуску: в очереди {pending} таблиц.
        </div>
      )}
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
      <Badge tone={item.status === "DONE" ? "ok" : item.status === "RUNNING" ? "run" : BAD.has(item.status) ? "bad" : "idle"}>
        {status}
      </Badge>
      <div style={{ fontFamily: t.font.mono, color: t.text.muted, textAlign: "right" }}>
        {progress === undefined ? "rows n/a" : `${progress.toFixed(0)}%`}
      </div>
    </div>
  );
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
