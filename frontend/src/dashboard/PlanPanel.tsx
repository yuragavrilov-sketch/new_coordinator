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
  busy: boolean;
  error: string;
}

const DONE = new Set(["DONE"]);
const BAD = new Set(["FAILED", "CANCELLED"]);

export function PlanPanel({ plan, loading, onStart, onReload, busy, error }: Props) {
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
            <Muted>Пока нет plan для этой миграции. Выделите таблицы и добавьте их в историческую пачку.</Muted>
          </div>
        </div>
      </Shell>
    );
  }

  const total = plan.items.length;
  const done = plan.items.filter(i => DONE.has(i.status)).length;
  const failed = plan.items.filter(i => BAD.has(i.status)).length;
  const running = plan.items.filter(i => i.status === "RUNNING").length;
  const progress = total ? done / total * 100 : 0;
  const hasPending = plan.items.some(i => i.status === "PENDING");
  const canStart = ["READY", "RUNNING"].includes(plan.status) && hasPending && running === 0;

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
              <span style={{ fontSize: 12, fontWeight: 700, color: t.text.primary }}>Batch {batch}</span>
              <span style={{ fontSize: 11, color: t.text.muted }}>{items.length} таблиц</span>
            </div>
            {items.map(item => <PlanRow key={item.item_id} item={item}/>)}
          </div>
        ))}
      </div>
    </Shell>
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
