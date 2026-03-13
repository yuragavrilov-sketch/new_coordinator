import React, { useState, useEffect, useCallback, useMemo, useRef } from "react";

// ── Types ────────────────────────────────────────────────────────────────────

interface ColInfo {
  name: string;
  data_type: string;
  data_length: number | null;
  data_precision: number | null;
  data_scale: number | null;
  nullable: boolean;
  data_default: string | null;
  column_id: number;
}

interface Constraint {
  name: string;
  type: string;
  type_code: string;
  status: string;
  columns: string[];
}

interface OraIndex {
  name: string;
  unique: boolean;
  index_type: string;
  status: string;
  columns: string[];
}

interface Trigger {
  name: string;
  trigger_type: string;
  event: string;
  status: string;
}

interface TableDDL {
  schema: string;
  table: string;
  columns: ColInfo[];
  constraints: Constraint[];
  indexes: OraIndex[];
  triggers: Trigger[];
}

interface DDLData {
  source: TableDDL;
  target: TableDDL;
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function fmtType(c: ColInfo): string {
  if (c.data_precision != null) {
    return c.data_scale != null && c.data_scale !== 0
      ? `${c.data_type}(${c.data_precision},${c.data_scale})`
      : `${c.data_type}(${c.data_precision})`;
  }
  const hasLen = ["VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR", "RAW"].includes(c.data_type);
  return hasLen && c.data_length != null ? `${c.data_type}(${c.data_length})` : c.data_type;
}

// ── Shared mini-components ────────────────────────────────────────────────────

function Dot({ color }: { color: string }) {
  return (
    <span style={{
      width: 7, height: 7, borderRadius: "50%",
      background: color, display: "inline-block", flexShrink: 0,
    }} />
  );
}

function StatusPill({ status, ok, warn }: { status: string; ok?: string; warn?: string }) {
  const color = status === ok ? "#22c55e" : status === warn ? "#eab308" : "#ef4444";
  return (
    <span style={{
      background: color + "22", border: `1px solid ${color}55`,
      color, borderRadius: 4, padding: "1px 7px", fontSize: 11, fontWeight: 600,
      whiteSpace: "nowrap",
    }}>
      {status}
    </span>
  );
}

function ActionBtn({
  label, onClick, busy, variant = "danger",
}: {
  label: string; onClick: () => void; busy?: boolean; variant?: "danger" | "success";
}) {
  const c = variant === "success" ? "#22c55e" : "#ef4444";
  return (
    <button
      onClick={onClick}
      disabled={busy}
      style={{
        background: c + "22", border: `1px solid ${c}55`,
        borderRadius: 4, color: "#e2e8f0", padding: "3px 10px",
        fontSize: 11, cursor: busy ? "not-allowed" : "pointer",
        opacity: busy ? 0.55 : 1, fontWeight: 500, whiteSpace: "nowrap",
      }}
    >
      {busy ? "..." : label}
    </button>
  );
}

function BulkDangerBtn({ label, onClick }: { label: string; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      style={{
        background: "#7f1d1d33", border: "1px solid #7f1d1d88",
        borderRadius: 4, color: "#fca5a5", padding: "3px 11px",
        fontSize: 11, cursor: "pointer", fontWeight: 500,
      }}
    >
      {label}
    </button>
  );
}

function EmptyRow({ text }: { text: string }) {
  return (
    <div style={{ padding: "20px 14px", textAlign: "center", color: "#475569", fontSize: 12 }}>
      {text}
    </div>
  );
}

// ── Searchable select (combobox) ──────────────────────────────────────────────

function highlightMatch(text: string, query: string) {
  const idx = text.toLowerCase().indexOf(query.toLowerCase());
  if (idx === -1) return <>{text}</>;
  return (
    <>
      {text.slice(0, idx)}
      <mark style={{ background: "#1d4ed844", color: "#93c5fd", padding: 0 }}>
        {text.slice(idx, idx + query.length)}
      </mark>
      {text.slice(idx + query.length)}
    </>
  );
}

function SearchSelect({
  value, onChange, options, placeholder, disabled,
}: {
  value: string;
  onChange: (v: string) => void;
  options: string[];
  placeholder: string;
  disabled?: boolean;
}) {
  const [query,  setQuery]  = useState("");
  const [open,   setOpen]   = useState(false);
  const wrapRef  = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Close on outside click
  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) {
        setOpen(false);
        setQuery("");
      }
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [open]);

  // Auto-focus input when dropdown opens
  useEffect(() => {
    if (open) inputRef.current?.focus();
  }, [open]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    return q ? options.filter(o => o.toLowerCase().includes(q)) : options;
  }, [options, query]);

  const handleOpen = () => {
    if (!disabled) { setOpen(o => !o); setQuery(""); }
  };

  const handleSelect = (opt: string) => {
    onChange(opt);
    setOpen(false);
    setQuery("");
  };

  const handleKey = (e: React.KeyboardEvent) => {
    if (e.key === "Escape") { setOpen(false); setQuery(""); }
    if (e.key === "Enter" && filtered.length === 1) handleSelect(filtered[0]);
  };

  return (
    <div ref={wrapRef} style={{ position: "relative", minWidth: 165 }}>
      {/* Trigger */}
      <div
        onClick={handleOpen}
        style={{
          display: "flex", alignItems: "center", gap: 6,
          background: "#1e293b",
          border: `1px solid ${open ? "#3b82f6" : "#334155"}`,
          borderRadius: 4, padding: "0 8px", height: 30,
          cursor: disabled ? "not-allowed" : "pointer",
          opacity: disabled ? 0.5 : 1,
          userSelect: "none",
        }}
      >
        <span style={{
          fontSize: 12, flex: 1,
          color: value ? "#e2e8f0" : "#475569",
          overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
        }}>
          {value || placeholder}
        </span>
        <span style={{ color: "#475569", fontSize: 9, flexShrink: 0 }}>{open ? "▲" : "▼"}</span>
      </div>

      {/* Dropdown */}
      {open && (
        <div style={{
          position: "absolute", top: "calc(100% + 3px)", left: 0, right: 0,
          background: "#1e293b", border: "1px solid #334155",
          borderRadius: 4, zIndex: 200,
          boxShadow: "0 6px 20px rgba(0,0,0,0.5)",
          display: "flex", flexDirection: "column",
        }}>
          {/* Search input */}
          <div style={{
            padding: "6px 8px", borderBottom: "1px solid #0f1e35",
            display: "flex", alignItems: "center", gap: 6,
          }}>
            <span style={{ color: "#475569", fontSize: 11 }}>🔍</span>
            <input
              ref={inputRef}
              value={query}
              onChange={e => setQuery(e.target.value)}
              onKeyDown={handleKey}
              placeholder="Поиск..."
              style={{
                background: "none", border: "none", color: "#e2e8f0",
                fontSize: 12, width: "100%", outline: "none",
              }}
            />
            {query && (
              <span
                onClick={() => setQuery("")}
                style={{ color: "#475569", cursor: "pointer", fontSize: 11, flexShrink: 0 }}
              >
                ✕
              </span>
            )}
          </div>

          {/* Options list */}
          <div style={{ maxHeight: 200, overflowY: "auto" }}>
            {value && (
              <div
                onMouseDown={() => handleSelect("")}
                style={{
                  padding: "5px 10px", fontSize: 11, cursor: "pointer",
                  color: "#475569", borderBottom: "1px solid #0f1e35",
                }}
                onMouseEnter={e => (e.currentTarget.style.background = "#0f1e35")}
                onMouseLeave={e => (e.currentTarget.style.background = "transparent")}
              >
                — Очистить —
              </div>
            )}
            {filtered.length === 0 ? (
              <div style={{ padding: "8px 10px", color: "#475569", fontSize: 12 }}>
                Нет совпадений
              </div>
            ) : (
              filtered.map(o => (
                <div
                  key={o}
                  onMouseDown={() => handleSelect(o)}
                  style={{
                    padding: "6px 10px", fontSize: 12, cursor: "pointer",
                    background: o === value ? "#1d3a5f" : "transparent",
                    color: o === value ? "#93c5fd" : "#e2e8f0",
                  }}
                  onMouseEnter={e => (e.currentTarget.style.background = o === value ? "#1d3a5f" : "#0f1624")}
                  onMouseLeave={e => (e.currentTarget.style.background = o === value ? "#1d3a5f" : "transparent")}
                >
                  {query ? highlightMatch(o, query) : o}
                </div>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function Section({
  title, count, status, bulkAction, children,
}: {
  title: string;
  count: number;
  status: "ok" | "warn" | "info";
  bulkAction?: React.ReactNode;
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(true);
  const dotC = status === "ok" ? "#22c55e" : status === "warn" ? "#eab308" : "#64748b";
  return (
    <div style={{ border: "1px solid #1e293b", borderRadius: 8, overflow: "hidden" }}>
      <div
        style={{
          background: "#0f1e35", padding: "9px 14px",
          display: "flex", alignItems: "center", gap: 10,
          cursor: "pointer", userSelect: "none",
        }}
        onClick={() => setOpen(o => !o)}
      >
        <span style={{ color: "#475569", fontSize: 11, width: 12 }}>{open ? "▼" : "▶"}</span>
        <span style={{ fontWeight: 600, fontSize: 13, color: "#e2e8f0" }}>{title}</span>
        <span style={{
          fontSize: 11, background: dotC + "22", color: dotC,
          padding: "1px 8px", borderRadius: 10,
        }}>
          {count}
        </span>
        {bulkAction && (
          <div style={{ marginLeft: "auto" }} onClick={e => e.stopPropagation()}>
            {bulkAction}
          </div>
        )}
      </div>
      {open && <div style={{ background: "#0a111f" }}>{children}</div>}
    </div>
  );
}

// ── Sub-tables ────────────────────────────────────────────────────────────────

const TH: React.CSSProperties = {
  padding: "6px 10px", textAlign: "left",
  color: "#64748b", fontWeight: 500, fontSize: 12, whiteSpace: "nowrap",
};
const TD: React.CSSProperties = { padding: "5px 10px", fontSize: 12 };
const TR_BORDER: React.CSSProperties = { borderBottom: "1px solid #0f1624" };

function IndexTable({
  src, tgt, busy, actErr, onAction,
}: {
  src: OraIndex[]; tgt: OraIndex[];
  busy: Record<string, boolean>; actErr: Record<string, string>;
  onAction: (action: string, name: string) => void;
}) {
  const srcNames = useMemo(() => new Set(src.map(i => i.name)), [src]);
  const tgtMap   = useMemo(() => new Map(tgt.map(i => [i.name, i])), [tgt]);

  const rows = useMemo(() => {
    const out: { srcIdx: OraIndex | null; tgtIdx: OraIndex | null }[] = [];
    for (const si of src) {
      out.push({ srcIdx: si, tgtIdx: tgtMap.get(si.name) ?? null });
    }
    for (const ti of tgt) {
      if (!srcNames.has(ti.name)) out.push({ srcIdx: null, tgtIdx: ti });
    }
    return out;
  }, [src, tgt, srcNames, tgtMap]);

  if (rows.length === 0) return <EmptyRow text="Нет индексов" />;

  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ borderBottom: "1px solid #1e293b" }}>
            {["Индекс", "Тип", "Колонки", "Есть на источнике", "Статус на таргете", "Действие"].map(h => (
              <th key={h} style={TH}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map(({ srcIdx, tgtIdx }) => {
            const idx = tgtIdx ?? srcIdx!;
            return (
              <tr key={idx.name} style={TR_BORDER}>
                <td style={TD}>
                  <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                    <code style={{ color: "#e2e8f0", fontSize: 11 }}>{idx.name}</code>
                    {idx.unique && (
                      <span style={{
                        fontSize: 10, color: "#3b82f6", background: "#1e3a5f",
                        padding: "1px 5px", borderRadius: 3,
                      }}>UNIQUE</span>
                    )}
                  </div>
                </td>
                <td style={{ ...TD, color: "#64748b", fontSize: 11 }}>{idx.index_type}</td>
                <td style={{ ...TD, color: "#94a3b8", fontSize: 11, fontFamily: "monospace" }}>
                  {idx.columns.join(", ")}
                </td>
                <td style={{ ...TD, textAlign: "center" }}>
                  {srcIdx ? <Dot color="#22c55e" /> : <span style={{ color: "#475569", fontSize: 11 }}>—</span>}
                </td>
                <td style={TD}>
                  {tgtIdx
                    ? <StatusPill status={tgtIdx.status} ok="VALID" warn="UNUSABLE" />
                    : <span style={{ color: "#475569", fontSize: 11 }}>отсутствует</span>}
                </td>
                <td style={TD}>
                  {tgtIdx && (
                    <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
                      <div>
                        {tgtIdx.status === "VALID"
                          ? <ActionBtn label="Отключить" onClick={() => onAction("disable_index", tgtIdx.name)} busy={busy[tgtIdx.name]} variant="danger" />
                          : <ActionBtn label="Перестроить" onClick={() => onAction("enable_index", tgtIdx.name)} busy={busy[tgtIdx.name]} variant="success" />
                        }
                      </div>
                      {actErr[tgtIdx.name] && (
                        <div style={{ fontSize: 10, color: "#ef4444", maxWidth: 240 }}>{actErr[tgtIdx.name]}</div>
                      )}
                    </div>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

const CTYPE_COLOR: Record<string, string> = {
  "PRIMARY KEY": "#3b82f6",
  "UNIQUE":      "#8b5cf6",
  "FOREIGN KEY": "#f59e0b",
  "CHECK":       "#64748b",
};

function ConstraintTable({
  src, tgt, busy, actErr, onAction,
}: {
  src: Constraint[]; tgt: Constraint[];
  busy: Record<string, boolean>; actErr: Record<string, string>;
  onAction: (action: string, name: string) => void;
}) {
  // Match constraints by name first; fallback to type+columns (handles SYS_Cxxxx)
  const rows = useMemo(() => {
    const out: { srcC: Constraint | null; tgtC: Constraint | null; nameMatch: boolean }[] = [];
    const tgtUsed = new Set<string>();
    const srcUsed = new Set<string>();

    // Phase 1: exact name match
    for (const sc of src) {
      const tc = tgt.find(t => t.name === sc.name);
      if (tc) {
        out.push({ srcC: sc, tgtC: tc, nameMatch: true });
        tgtUsed.add(tc.name);
        srcUsed.add(sc.name);
      }
    }
    // Phase 2: type + columns match (for SYS_-named or renamed constraints)
    for (const sc of src.filter(c => !srcUsed.has(c.name))) {
      const colKey = sc.columns.join(",");
      const tc = tgt.find(t => !tgtUsed.has(t.name) && t.type_code === sc.type_code && t.columns.join(",") === colKey);
      if (tc) {
        out.push({ srcC: sc, tgtC: tc, nameMatch: false });
        tgtUsed.add(tc.name);
        srcUsed.add(sc.name);
      } else {
        out.push({ srcC: sc, tgtC: null, nameMatch: false });
      }
    }
    // Phase 3: target-only constraints
    for (const tc of tgt.filter(c => !tgtUsed.has(c.name))) {
      out.push({ srcC: null, tgtC: tc, nameMatch: false });
    }
    return out;
  }, [src, tgt]);

  if (rows.length === 0) return <EmptyRow text="Нет ограничений" />;

  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ borderBottom: "1px solid #1e293b" }}>
            {["Имя (таргет)", "Имя (источник)", "Тип", "Колонки", "Есть на источнике", "Статус на таргете", "Действие"].map(h => (
              <th key={h} style={TH}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map(({ srcC, tgtC, nameMatch }) => {
            const c = tgtC ?? srcC!;
            const isPK = c.type_code === "P";
            const typeC = CTYPE_COLOR[c.type] ?? "#64748b";
            const namesDiffer = !nameMatch && srcC && tgtC && srcC.name !== tgtC.name;
            return (
              <tr key={c.name} style={TR_BORDER}>
                <td style={TD}>
                  <code style={{ color: "#e2e8f0", fontSize: 11 }}>{tgtC?.name ?? "—"}</code>
                </td>
                <td style={TD}>
                  {srcC
                    ? <code style={{ color: namesDiffer ? "#eab308" : "#64748b", fontSize: 11 }}>{srcC.name}</code>
                    : <span style={{ color: "#475569", fontSize: 11 }}>—</span>}
                </td>
                <td style={TD}>
                  <span style={{
                    background: typeC + "22", color: typeC,
                    borderRadius: 4, padding: "1px 6px", fontSize: 10, fontWeight: 600,
                  }}>
                    {c.type}
                  </span>
                </td>
                <td style={{ ...TD, color: "#94a3b8", fontSize: 11, fontFamily: "monospace" }}>
                  {c.columns.join(", ") || "—"}
                </td>
                <td style={{ ...TD, textAlign: "center" }}>
                  {srcC ? <Dot color="#22c55e" /> : <span style={{ color: "#475569", fontSize: 11 }}>—</span>}
                </td>
                <td style={TD}>
                  {tgtC
                    ? <StatusPill status={tgtC.status} ok="ENABLED" warn="DISABLED" />
                    : <span style={{ color: "#475569", fontSize: 11 }}>отсутствует</span>}
                </td>
                <td style={TD}>
                  {tgtC && (
                    <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
                      {isPK
                        ? <span style={{ fontSize: 11, color: "#475569" }}>PRIMARY KEY — нельзя отключить</span>
                        : (
                          <div>
                            {tgtC.status === "ENABLED"
                              ? <ActionBtn label="Отключить" onClick={() => onAction("disable_constraint", tgtC.name)} busy={busy[tgtC.name]} variant="danger" />
                              : <ActionBtn label="Включить"  onClick={() => onAction("enable_constraint",  tgtC.name)} busy={busy[tgtC.name]} variant="success" />
                            }
                          </div>
                        )
                      }
                      {actErr[tgtC.name] && (
                        <div style={{ fontSize: 10, color: "#ef4444", maxWidth: 240 }}>{actErr[tgtC.name]}</div>
                      )}
                    </div>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function TriggerTable({
  src, tgt, busy, actErr, onAction,
}: {
  src: Trigger[]; tgt: Trigger[];
  busy: Record<string, boolean>; actErr: Record<string, string>;
  onAction: (action: string, name: string) => void;
}) {
  // Merge source and target by name; show both
  const rows = useMemo(() => {
    const tgtMap = new Map(tgt.map(t => [t.name, t]));
    const srcMap = new Map(src.map(t => [t.name, t]));
    const out: { srcT: Trigger | null; tgtT: Trigger | null }[] = [];
    for (const st of src) {
      out.push({ srcT: st, tgtT: tgtMap.get(st.name) ?? null });
    }
    for (const tt of tgt) {
      if (!srcMap.has(tt.name)) out.push({ srcT: null, tgtT: tt });
    }
    return out;
  }, [src, tgt]);

  if (rows.length === 0) return <EmptyRow text="Нет триггеров" />;

  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ borderBottom: "1px solid #1e293b" }}>
            {["Триггер", "Тип", "Событие", "На источнике", "На таргете (статус)", "Действие"].map(h => (
              <th key={h} style={TH}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map(({ srcT, tgtT }) => {
            const t = tgtT ?? srcT!;
            const srcEvent  = srcT ? `${srcT.trigger_type} / ${srcT.event}` : null;
            const tgtEvent  = tgtT ? `${tgtT.trigger_type} / ${tgtT.event}` : null;
            const eventDiff = srcEvent && tgtEvent && srcEvent !== tgtEvent;
            return (
              <tr key={t.name} style={TR_BORDER}>
                <td style={TD}>
                  <code style={{ color: "#e2e8f0", fontSize: 11 }}>{t.name}</code>
                </td>
                <td style={{ ...TD, color: "#64748b", fontSize: 11 }}>{t.trigger_type}</td>
                <td style={{ ...TD, color: eventDiff ? "#eab308" : "#94a3b8", fontSize: 11 }}>
                  {tgtEvent ?? srcEvent}
                  {eventDiff && (
                    <div style={{ fontSize: 10, color: "#eab308" }}>src: {srcEvent}</div>
                  )}
                </td>
                <td style={TD}>
                  {srcT
                    ? <StatusPill status={srcT.status} ok="ENABLED" warn="DISABLED" />
                    : <span style={{ color: "#475569", fontSize: 11 }}>—</span>}
                </td>
                <td style={TD}>
                  {tgtT
                    ? <StatusPill status={tgtT.status} ok="ENABLED" warn="DISABLED" />
                    : <span style={{ color: "#475569", fontSize: 11 }}>отсутствует</span>}
                </td>
                <td style={TD}>
                  {tgtT && (
                    <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
                      <div>
                        {tgtT.status === "ENABLED"
                          ? <ActionBtn label="Отключить" onClick={() => onAction("disable_trigger", tgtT.name)} busy={busy[tgtT.name]} variant="danger" />
                          : <ActionBtn label="Включить"  onClick={() => onAction("enable_trigger",  tgtT.name)} busy={busy[tgtT.name]} variant="success" />
                        }
                      </div>
                      {actErr[tgtT.name] && (
                        <div style={{ fontSize: 10, color: "#ef4444", maxWidth: 240 }}>{actErr[tgtT.name]}</div>
                      )}
                    </div>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export function TargetPrep() {
  const [srcSchemas, setSrcSchemas] = useState<string[]>([]);
  const [tgtSchemas, setTgtSchemas] = useState<string[]>([]);
  const [srcTables,  setSrcTables]  = useState<string[]>([]);
  const [tgtTables,  setTgtTables]  = useState<string[]>([]);

  const [srcSchema, setSrcSchema] = useState("");
  const [srcTable,  setSrcTable]  = useState("");
  const [tgtSchema, setTgtSchema] = useState("");
  const [tgtTable,  setTgtTable]  = useState("");

  const [ddl,     setDdl]     = useState<DDLData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error,   setError]   = useState<string | null>(null);

  const [busy,       setBusy]       = useState<Record<string, boolean>>({});
  const [actErr,     setActErr]     = useState<Record<string, string>>({});
  const [syncBusy,   setSyncBusy]   = useState(false);
  const [syncResult, setSyncResult] = useState<{ added: {column: string; type: string}[]; warnings: {column: string; source_type: string; target_type: string}[] } | null>(null);

  // Load schemas once
  useEffect(() => {
    fetch("/api/db/source/schemas").then(r => r.json())
      .then(d => Array.isArray(d) && setSrcSchemas(d)).catch(() => {});
    fetch("/api/db/target/schemas").then(r => r.json())
      .then(d => Array.isArray(d) && setTgtSchemas(d)).catch(() => {});
  }, []);

  // Load source tables — reset table synchronously so auto-fill can set it after
  useEffect(() => {
    setSrcTable("");
    setSrcTables([]);
    if (!srcSchema) return;
    fetch("/api/db/source/tables?" + new URLSearchParams({ schema: srcSchema }))
      .then(r => r.json())
      .then(d => { if (Array.isArray(d)) setSrcTables(d); })
      .catch(() => {});
  }, [srcSchema]);

  // Load target tables — same pattern
  useEffect(() => {
    setTgtTable("");
    setTgtTables([]);
    if (!tgtSchema) return;
    fetch("/api/db/target/tables?" + new URLSearchParams({ schema: tgtSchema }))
      .then(r => r.json())
      .then(d => { if (Array.isArray(d)) setTgtTables(d); })
      .catch(() => {});
  }, [tgtSchema]);

  const fetchDdl = useCallback(async () => {
    if (!srcSchema || !srcTable || !tgtSchema || !tgtTable) return;
    setLoading(true);
    setError(null);
    try {
      const r = await fetch(
        "/api/target-prep/ddl?" + new URLSearchParams({ src_schema: srcSchema, src_table: srcTable, tgt_schema: tgtSchema, tgt_table: tgtTable })
      );
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || "Ошибка загрузки");
      setDdl(d);
      setBusy({});
      setActErr({});
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [srcSchema, srcTable, tgtSchema, tgtTable]);

  const doSyncColumns = useCallback(async () => {
    if (!ddl) return;
    setSyncBusy(true);
    setSyncResult(null);
    try {
      const r = await fetch("/api/target-prep/sync-columns", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          src_schema: ddl.source.schema,
          src_table:  ddl.source.table,
          tgt_schema: ddl.target.schema,
          tgt_table:  ddl.target.table,
        }),
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || "Ошибка синхронизации");
      setSyncResult(d);
      await fetchDdl();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setSyncBusy(false);
    }
  }, [ddl, fetchDdl]);

  const doAction = useCallback(async (action: string, objectName: string) => {
    if (!ddl) return;
    setBusy(p => ({ ...p, [objectName]: true }));
    setActErr(p => { const n = { ...p }; delete n[objectName]; return n; });
    try {
      const r = await fetch("/api/target-prep/action", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          action,
          tgt_schema:  ddl.target.schema,
          tgt_table:   ddl.target.table,
          object_name: objectName,
        }),
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || "Ошибка");
      await fetchDdl();
    } catch (e: any) {
      setActErr(p => ({ ...p, [objectName]: e.message }));
      setBusy(p => { const n = { ...p }; delete n[objectName]; return n; });
    }
  }, [ddl, fetchDdl]);

  // Column diff
  const colDiff = useMemo(() => {
    if (!ddl) return [];
    const tgtMap = new Map(ddl.target.columns.map(c => [c.name, c]));
    const srcMap = new Map(ddl.source.columns.map(c => [c.name, c]));
    const rows: { src: ColInfo | null; tgt: ColInfo | null; state: "ok" | "type" | "noTgt" | "extra" }[] = [];
    for (const sc of ddl.source.columns) {
      const tc = tgtMap.get(sc.name);
      rows.push({ src: sc, tgt: tc ?? null, state: !tc ? "noTgt" : fmtType(sc) === fmtType(tc) ? "ok" : "type" });
    }
    for (const tc of ddl.target.columns) {
      if (!srcMap.has(tc.name)) rows.push({ src: null, tgt: tc, state: "extra" });
    }
    return rows;
  }, [ddl]);

  const colIssues  = colDiff.filter(r => r.state !== "ok").length;
  const canCompare = !!(srcSchema && srcTable && tgtSchema && tgtTable);

  return (
    <div style={{ animation: "fadeIn 0.2s ease-out" }}>

      {/* ── Selector bar ── */}
      <div style={{
        background: "#0f1e35", border: "1px solid #1e293b",
        borderRadius: 8, padding: "14px 18px", marginBottom: 16,
        display: "flex", gap: 20, alignItems: "flex-end", flexWrap: "wrap",
      }}>
        <div>
          <div style={{ fontSize: 11, color: "#64748b", marginBottom: 5, textTransform: "uppercase", letterSpacing: 0.5 }}>
            Источник
          </div>
          <div style={{ display: "flex", gap: 8 }}>
            <SearchSelect
              value={srcSchema}
              onChange={v => { setSrcSchema(v); if (!tgtSchema) setTgtSchema(v); }}
              options={srcSchemas} placeholder="Схема..."
            />
            <SearchSelect
              value={srcTable}
              onChange={v => { setSrcTable(v); if (!tgtTable) setTgtTable(v); }}
              options={srcTables} placeholder="Таблица..." disabled={!srcSchema}
            />
          </div>
        </div>

        <span style={{ color: "#334155", fontSize: 20, paddingBottom: 4 }}>→</span>

        <div>
          <div style={{ fontSize: 11, color: "#64748b", marginBottom: 5, textTransform: "uppercase", letterSpacing: 0.5 }}>
            Таргет
          </div>
          <div style={{ display: "flex", gap: 8 }}>
            <SearchSelect value={tgtSchema} onChange={setTgtSchema} options={tgtSchemas} placeholder="Схема..." />
            <SearchSelect value={tgtTable}  onChange={setTgtTable}  options={tgtTables}  placeholder="Таблица..." disabled={!tgtSchema} />
          </div>
        </div>

        <button
          onClick={fetchDdl}
          disabled={!canCompare || loading}
          style={{
            background: canCompare && !loading ? "#1d4ed8" : "#1e293b",
            border: "none", borderRadius: 6, color: "#fff",
            padding: "7px 22px", fontSize: 13, fontWeight: 600,
            cursor: canCompare && !loading ? "pointer" : "not-allowed",
            opacity: canCompare && !loading ? 1 : 0.5,
          }}
        >
          {loading ? "Загрузка..." : "Сравнить DDL"}
        </button>
      </div>

      {/* ── Error ── */}
      {error && (
        <div style={{
          background: "#7f1d1d22", border: "1px solid #7f1d1d",
          color: "#fca5a5", padding: "10px 14px",
          borderRadius: 6, marginBottom: 16, fontSize: 13,
        }}>
          {error}
        </div>
      )}

      {/* ── Empty hint ── */}
      {!ddl && !loading && !error && (
        <div style={{ textAlign: "center", color: "#334155", padding: "60px 0", fontSize: 13 }}>
          Выберите таблицы источника и таргета, затем нажмите «Сравнить DDL»
        </div>
      )}

      {/* ── DDL sections ── */}
      {ddl && (
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>

          {/* Columns */}
          <Section
            title="Колонки"
            count={ddl.source.columns.length}
            status={colIssues === 0 ? "ok" : "warn"}
            bulkAction={
              colDiff.some(r => r.state === "noTgt")
                ? <ActionBtn
                    label="Добавить недостающие колонки"
                    onClick={doSyncColumns}
                    busy={syncBusy}
                    variant="success"
                  />
                : undefined
            }
          >
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ borderBottom: "1px solid #1e293b" }}>
                    {["#", "Колонка", "Тип (источник)", "Тип (таргет)", "NULL src", "NULL tgt", "Default (tgt)"].map(h => (
                      <th key={h} style={TH}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {colDiff.map((row, i) => {
                    const bg = row.state === "noTgt"  ? "rgba(239,68,68,0.06)"
                             : row.state === "type"   ? "rgba(234,179,8,0.06)"
                             : row.state === "extra"  ? "rgba(249,115,22,0.06)"
                             : "transparent";
                    const dotC = row.state === "ok" ? "#22c55e"
                               : row.state === "type" ? "#eab308" : "#ef4444";
                    return (
                      <tr key={i} style={{ ...TR_BORDER, background: bg }}>
                        <td style={{ ...TD, color: "#475569" }}>{row.src?.column_id ?? "—"}</td>
                        <td style={TD}>
                          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                            <Dot color={dotC} />
                            <code style={{ color: "#e2e8f0", fontSize: 12 }}>{row.src?.name ?? row.tgt?.name}</code>
                          </div>
                        </td>
                        <td style={{ ...TD, color: "#94a3b8", fontFamily: "monospace", fontSize: 11 }}>
                          {row.src ? fmtType(row.src) : <span style={{ color: "#334155" }}>—</span>}
                        </td>
                        <td style={{
                          ...TD, fontFamily: "monospace", fontSize: 11,
                          color: row.state === "type" ? "#eab308" : row.state === "noTgt" ? "#ef4444" : "#94a3b8",
                        }}>
                          {row.tgt
                            ? fmtType(row.tgt)
                            : <span style={{ color: "#ef4444", fontFamily: "sans-serif" }}>отсутствует</span>}
                        </td>
                        <td style={{ ...TD, color: "#64748b", textAlign: "center" }}>
                          {row.src ? (row.src.nullable ? "Y" : "N") : "—"}
                        </td>
                        <td style={{ ...TD, color: "#64748b", textAlign: "center" }}>
                          {row.tgt ? (row.tgt.nullable ? "Y" : "N") : "—"}
                        </td>
                        <td style={{ ...TD, color: "#64748b", fontFamily: "monospace", fontSize: 11 }}>
                          {row.tgt?.data_default ?? "—"}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            {colIssues > 0 && (
              <div style={{ padding: "7px 14px", borderTop: "1px solid #1e293b", fontSize: 11, color: "#64748b" }}>
                {colDiff.filter(r => r.state === "noTgt").length > 0 && (
                  <span style={{ color: "#ef4444", marginRight: 12 }}>
                    ✕ {colDiff.filter(r => r.state === "noTgt").length} отсутствуют в таргете
                  </span>
                )}
                {colDiff.filter(r => r.state === "type").length > 0 && (
                  <span style={{ color: "#eab308", marginRight: 12 }}>
                    ⚠ {colDiff.filter(r => r.state === "type").length} несовпадение типов
                  </span>
                )}
                {colDiff.filter(r => r.state === "extra").length > 0 && (
                  <span style={{ color: "#f97316" }}>
                    + {colDiff.filter(r => r.state === "extra").length} лишних в таргете
                  </span>
                )}
              </div>
            )}
            {syncResult && (
              <div style={{ padding: "8px 14px", borderTop: "1px solid #1e293b", fontSize: 11 }}>
                {syncResult.added.length > 0 && (
                  <div style={{ color: "#22c55e", marginBottom: 4 }}>
                    ✓ Добавлено: {syncResult.added.map(a => `${a.column} (${a.type})`).join(", ")}
                  </div>
                )}
                {syncResult.warnings.length > 0 && (
                  <div style={{ color: "#eab308" }}>
                    ⚠ Несовпадение типов (не применено): {syncResult.warnings.map(w =>
                      `${w.column}: src=${w.source_type} / tgt=${w.target_type}`
                    ).join("; ")}
                  </div>
                )}
                {syncResult.added.length === 0 && syncResult.warnings.length === 0 && (
                  <span style={{ color: "#475569" }}>Нет изменений</span>
                )}
              </div>
            )}
          </Section>

          {/* Indexes */}
          <Section
            title="Индексы"
            count={ddl.target.indexes.length}
            status="info"
            bulkAction={
              ddl.target.indexes.some(ix => ix.status === "VALID")
                ? <BulkDangerBtn
                    label="Отключить все (UNUSABLE)"
                    onClick={() => {
                      ddl.target.indexes
                        .filter(ix => ix.status === "VALID")
                        .forEach(ix => doAction("disable_index", ix.name));
                    }}
                  />
                : ddl.target.indexes.some(ix => ix.status === "UNUSABLE")
                  ? <BulkDangerBtn
                      label="Перестроить все"
                      onClick={() => {
                        ddl.target.indexes
                          .filter(ix => ix.status === "UNUSABLE")
                          .forEach(ix => doAction("enable_index", ix.name));
                      }}
                    />
                  : undefined
            }
          >
            <IndexTable src={ddl.source.indexes} tgt={ddl.target.indexes} busy={busy} actErr={actErr} onAction={doAction} />
          </Section>

          {/* Constraints */}
          <Section
            title="Ограничения (Constraints)"
            count={ddl.target.constraints.length}
            status="info"
            bulkAction={
              ddl.target.constraints.some(c => c.status === "ENABLED" && c.type_code !== "P")
                ? <BulkDangerBtn
                    label="Отключить FK / UK / CHECK"
                    onClick={() => {
                      ddl.target.constraints
                        .filter(c => c.status === "ENABLED" && c.type_code !== "P")
                        .forEach(c => doAction("disable_constraint", c.name));
                    }}
                  />
                : undefined
            }
          >
            <ConstraintTable src={ddl.source.constraints} tgt={ddl.target.constraints} busy={busy} actErr={actErr} onAction={doAction} />
          </Section>

          {/* Triggers */}
          <Section
            title="Триггеры"
            count={ddl.target.triggers.length}
            status="info"
            bulkAction={
              ddl.target.triggers.some(t => t.status === "ENABLED")
                ? <BulkDangerBtn
                    label="Отключить все"
                    onClick={() => {
                      ddl.target.triggers
                        .filter(t => t.status === "ENABLED")
                        .forEach(t => doAction("disable_trigger", t.name));
                    }}
                  />
                : undefined
            }
          >
            <TriggerTable src={ddl.source.triggers} tgt={ddl.target.triggers} busy={busy} actErr={actErr} onAction={doAction} />
          </Section>

        </div>
      )}
    </div>
  );
}
