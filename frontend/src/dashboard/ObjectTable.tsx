import React from "react";
import { t } from "../theme";
import { Icon, ObjStatusBadge, ProgressBar } from "../components/ui";
import { fmtCompactNum, fmtMb } from "../utils/format";
import {
  STATUS_MAP, OBJECT_TYPES,
  type SchemaObject, type ObjectStatus, type ObjectType,
} from "./types";

interface Props {
  objects:   SchemaObject[];
  layout?:   "flat" | "grouped";
  onOpen:    (o: SchemaObject) => void;
  onAction:  (o: SchemaObject, action: "pause" | "retry" | "more") => void;
}

const TYPE_COLORS: Record<ObjectType, { bg: string; fg: string }> = {
  TABLE:     { bg: `color-mix(in oklab, ${t.tone.info} 14%, transparent)`,                   fg: t.tone.info },
  INDEX:     { bg: "color-mix(in oklab, oklch(0.6 0.13 200) 14%, transparent)", fg: "oklch(0.5 0.13 200)" },
  MVIEW:     { bg: "color-mix(in oklab, oklch(0.6 0.13 320) 14%, transparent)", fg: "oklch(0.5 0.13 320)" },
  SEQUENCE:  { bg: t.bg.s3, fg: t.text.secondary },
  VIEW:      { bg: "color-mix(in oklab, oklch(0.65 0.11 280) 14%, transparent)", fg: "oklch(0.5 0.13 280)" },
  PACKAGE:   { bg: "color-mix(in oklab, oklch(0.65 0.11 140) 16%, transparent)", fg: "oklch(0.45 0.11 140)" },
  PROCEDURE: { bg: "color-mix(in oklab, oklch(0.65 0.11 140) 12%, transparent)", fg: "oklch(0.45 0.11 140)" },
  FUNCTION:  { bg: "color-mix(in oklab, oklch(0.65 0.11 140) 12%, transparent)", fg: "oklch(0.45 0.11 140)" },
  TRIGGER:   { bg: `color-mix(in oklab, ${t.tone.warn} 12%, transparent)`,                   fg: t.tone.warn },
  TYPE:      { bg: t.bg.s3, fg: t.text.secondary },
  SYNONYM:   { bg: t.bg.s3, fg: t.text.muted     },
  GRANT:     { bg: t.bg.s3, fg: t.text.muted     },
  DBLINK:    { bg: t.bg.s3, fg: t.text.muted     },
  JOB:       { bg: "color-mix(in oklab, oklch(0.65 0.11 240) 14%, transparent)", fg: "oklch(0.5 0.13 240)" },
};

export function ObjectTable({ objects, layout = "flat", onOpen, onAction }: Props) {
  // Group by category when layout=grouped
  const grouped: [string, SchemaObject[]][] = (() => {
    if (layout !== "grouped") return [];
    const map = new Map<string, SchemaObject[]>();
    objects.forEach(o => {
      const g = OBJECT_TYPES[o.type].group;
      if (!map.has(g)) map.set(g, []);
      map.get(g)!.push(o);
    });
    return Array.from(map.entries());
  })();

  return (
    <div style={{
      background:   t.bg.s1,
      border:       `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.lg,
      overflow:     "hidden",
      boxShadow:    t.shadow.s1,
    }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
        <thead>
          <tr>
            <Th>Тип</Th>
            <Th>Имя</Th>
            <Th>Статус</Th>
            <Th>Прогресс</Th>
            <Th right>Rows</Th>
            <Th right>Размер</Th>
            <Th right>Скорость</Th>
            <Th right>Compat</Th>
            <Th>Issues</Th>
            <Th right>ETA</Th>
            <Th>{""}</Th>
          </tr>
        </thead>
        <tbody>
          {layout === "grouped" && grouped.length > 0
            ? grouped.flatMap(([group, rows]) => [
                <tr key={`g-${group}`} style={{ background: t.bg.s2 }}>
                  <td colSpan={11} style={{ padding: "6px 12px" }}>
                    <span style={{
                      fontSize: "10.5px", fontWeight: 700,
                      textTransform: "uppercase", letterSpacing: "0.08em",
                      color: t.text.secondary, marginRight: 8,
                    }}>{group}</span>
                    <span style={{ fontFamily: t.font.mono, fontSize: "10.5px", color: t.text.muted }}>
                      {rows.length}
                    </span>
                  </td>
                </tr>,
                ...rows.map(o => <ObjectRow key={o.id} o={o} onOpen={onOpen} onAction={onAction}/>),
              ])
            : objects.map(o => <ObjectRow key={o.id} o={o} onOpen={onOpen} onAction={onAction}/>)
          }
          {objects.length === 0 && (
            <tr><td colSpan={11} style={{
              textAlign: "center", padding: 40, color: t.text.muted,
            }}>Нет объектов под текущие фильтры</td></tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function Th({ children, right }: { children: React.ReactNode; right?: boolean }) {
  return (
    <th style={{
      textAlign: right ? "right" : "left",
      fontSize: 10, textTransform: "uppercase", letterSpacing: "0.05em",
      color: t.text.muted, fontWeight: 600,
      padding: "8px 12px",
      borderBottom: `1px solid ${t.border.subtle}`,
      background: t.bg.s2,
      position: "sticky", top: 0,
    }}>
      {children}
    </th>
  );
}

function ObjectRow({ o, onOpen, onAction }: {
  o: SchemaObject;
  onOpen:   (o: SchemaObject) => void;
  onAction: (o: SchemaObject, a: "pause" | "retry" | "more") => void;
}) {
  const isData = o.type === "TABLE" || o.type === "INDEX" || o.type === "MVIEW";
  const status: ObjectStatus = o.status;
  const tone =
    status === "error" ? "error" :
    status === "warn"  ? "warn"  :
    status === "done"  ? "ok"    :
                         "info";

  const rowBg =
    status === "error"   ? `color-mix(in oklab, ${t.tone.errorSoft} 50%, transparent)` :
    status === "running" ? `color-mix(in oklab, ${t.tone.infoSoft}  32%, transparent)` :
                           "transparent";

  const typeColor = TYPE_COLORS[o.type];

  return (
    <tr
      onClick={() => onOpen(o)}
      style={{
        cursor: "pointer",
        background: rowBg,
        transition: "background 80ms",
      }}
      onMouseEnter={e => {
        if (status !== "error" && status !== "running") {
          (e.currentTarget as HTMLTableRowElement).style.background = t.bg.s2;
        }
      }}
      onMouseLeave={e => {
        (e.currentTarget as HTMLTableRowElement).style.background = rowBg;
      }}
    >
      <Td>
        <span style={{
          display: "inline-block",
          fontFamily: t.font.mono,
          fontSize: "9.5px", fontWeight: 600,
          letterSpacing: "0.05em", textTransform: "uppercase",
          padding: "2px 6px", borderRadius: 3,
          background: typeColor.bg, color: typeColor.fg,
        }}>
          {OBJECT_TYPES[o.type].label}
        </span>
      </Td>
      <Td>
        <div style={{ fontFamily: t.font.mono, fontSize: "12.5px", fontWeight: 500, letterSpacing: "-0.01em" }}>
          {o.name}
        </div>
        {o.note && (
          <div style={{
            fontSize: "10.5px", color: t.text.muted, marginTop: 2,
            maxWidth: 460, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
          }}>
            {o.note}
          </div>
        )}
      </Td>
      <Td><ObjStatusBadge tone={STATUS_MAP[status].tone} label={STATUS_MAP[status].label}/></Td>
      <Td>
        {status === "queued" || status === "skipped" ? (
          <span style={{ color: t.text.muted, fontFamily: t.font.mono, fontSize: 11 }}>
            {o.eta === "blocked" ? "blocked" : "—"}
          </span>
        ) : (
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <ProgressBar value={o.progress} tone={tone}/>
            <span style={{
              fontFamily: t.font.mono, fontSize: "10.5px",
              minWidth: 32, textAlign: "right", color: t.text.muted,
            }}>
              {o.progress.toFixed(0)}%
            </span>
          </div>
        )}
      </Td>
      <Td right mono>
        {o.rows == null ? <span style={{ color: t.text.muted }}>—</span> : (
          <>
            <span>{fmtCompactNum(o.rowsDone)}</span>
            <span style={{ color: t.text.muted }}>/{fmtCompactNum(o.rows)}</span>
          </>
        )}
      </Td>
      <Td right mono>{o.sizeMb < 0.1 ? <span style={{ color: t.text.muted }}>—</span> : fmtMb(o.sizeMb)}</Td>
      <Td right mono>
        {o.rowsPerSec > 0
          ? <>{fmtCompactNum(o.rowsPerSec)}<span style={{ color: t.text.muted }}>/s</span></>
          : o.mbPerSec > 0
            ? <>{o.mbPerSec}<span style={{ color: t.text.muted }}> MB/s</span></>
            : <span style={{ color: t.text.muted }}>—</span>}
      </Td>
      <Td right mono>
        {!isData && o.compat < 100 ? (
          <span style={{
            fontWeight: 500, padding: "1px 5px", borderRadius: 3,
            background: o.compat < 80 ? t.tone.errorSoft : o.compat < 95 ? t.tone.warnSoft : "transparent",
            color:      o.compat < 80 ? t.tone.error     : o.compat < 95 ? t.tone.warn     : t.tone.ok,
          }}>{o.compat}%</span>
        ) : <span style={{ color: t.text.muted }}>{o.compat}%</span>}
      </Td>
      <Td>
        {o.err > 0 && (
          <span style={{
            display: "inline-flex", alignItems: "center", gap: 3,
            fontSize: "10.5px", padding: "1px 5px", borderRadius: 3,
            fontFamily: t.font.mono, fontWeight: 500,
            background: t.tone.errorSoft, color: t.tone.error,
            marginRight: 4,
          }}>
            <Icon name="error" size={11}/> {o.err}
          </span>
        )}
        {o.warn > 0 && (
          <span style={{
            display: "inline-flex", alignItems: "center", gap: 3,
            fontSize: "10.5px", padding: "1px 5px", borderRadius: 3,
            fontFamily: t.font.mono, fontWeight: 500,
            background: t.tone.warnSoft, color: t.tone.warn,
          }}>
            <Icon name="warn" size={11}/> {o.warn}
          </span>
        )}
        {o.err === 0 && o.warn === 0 && <span style={{ color: t.text.muted }}>—</span>}
      </Td>
      <Td right mono>{o.eta}</Td>
      <Td right>
        <div style={{ display: "flex", gap: 2, justifyContent: "flex-end" }}
             onClick={e => e.stopPropagation()}>
          {status === "running" && <RowBtn icon="pause"  onClick={() => onAction(o, "pause")}/>}
          {status === "error"   && <RowBtn icon="rotate" onClick={() => onAction(o, "retry")}/>}
          <RowBtn icon="more" onClick={() => onAction(o, "more")}/>
        </div>
      </Td>
    </tr>
  );
}

function Td({ children, right, mono }: {
  children?: React.ReactNode;
  right?:    boolean;
  mono?:     boolean;
}) {
  return (
    <td style={{
      padding: "7px 12px",
      borderBottom: `1px solid ${t.border.subtle}`,
      verticalAlign: "middle",
      height: 32,
      textAlign:  right ? "right" : "left",
      whiteSpace: right ? "nowrap" : undefined,
      fontFamily: mono ? t.font.mono : undefined,
    }}>
      {children}
    </td>
  );
}

function RowBtn({ icon, onClick }: { icon: "pause" | "rotate" | "more"; onClick: () => void }) {
  return (
    <button onClick={onClick} style={{
      background: "transparent", border: "none",
      padding: 4, borderRadius: 4,
      color: t.text.muted, cursor: "pointer",
      display: "inline-flex",
    }}>
      <Icon name={icon} size={13}/>
    </button>
  );
}
