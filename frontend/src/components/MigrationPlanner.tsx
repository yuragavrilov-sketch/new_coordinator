import React, { useState, useEffect, useCallback, useMemo, useRef } from "react";

// ── Types ────────────────────────────────────────────────────────────────────

interface DiffSummary {
  ok: boolean;
  total: number;
  cols_missing: number;
  cols_extra: number;
  cols_type: number;
  idx_missing: number;
  idx_disabled: number;
  con_missing: number;
  con_disabled: number;
  trg_missing: number;
}

interface TableCompare {
  table: string;
  exists_on_target: boolean;
  diff: DiffSummary | null;
}

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

interface BatchItem {
  table: string;
  mode: "CDC" | "BULK_ONLY";
  strategy: "STAGE" | "DIRECT";
  chunk_size: number;
  workers: number;
}

interface Batch {
  id: number;
  items: BatchItem[];
}

interface PlanDefaults {
  chunk_size: number;
  workers: number;
  strategy: "STAGE" | "DIRECT";
  mode: "CDC" | "BULK_ONLY";
}

interface FKDep {
  table: string;
  depends_on: string[];
}

interface ConnectorGroup {
  id: string;
  group_name: string;
  connector_name: string;
  status: string;
}

// ── Style tokens ─────────────────────────────────────────────────────────────

const S = {
  card: {
    background: "#0f172a", border: "1px solid #1e293b", borderRadius: 8,
    overflow: "hidden" as const,
  },
  cardHeader: {
    padding: "10px 16px", background: "#0a111f",
    borderBottom: "1px solid #1e293b",
    display: "flex" as const, alignItems: "center" as const, gap: 10,
  },
  cardBody: {
    padding: 16, display: "flex" as const, flexDirection: "column" as const, gap: 12,
  },
  row2: { display: "grid" as const, gridTemplateColumns: "1fr 1fr", gap: 10 },
  row3: { display: "grid" as const, gridTemplateColumns: "1fr 1fr 1fr", gap: 10 },
  field: { display: "flex" as const, flexDirection: "column" as const, gap: 4 },
  label: { fontSize: 11, color: "#64748b", fontWeight: 600 as const, letterSpacing: 0.3 },
  input: {
    background: "#1e293b", border: "1px solid #334155", borderRadius: 5,
    color: "#e2e8f0", padding: "7px 10px", fontSize: 13, width: "100%",
  },
  select: {
    background: "#1e293b", border: "1px solid #334155", borderRadius: 5,
    color: "#e2e8f0", padding: "7px 10px", fontSize: 13, width: "100%", cursor: "pointer" as const,
  },
  btnPrimary: {
    background: "#3b82f6", border: "none", borderRadius: 6,
    color: "#fff", padding: "6px 14px", fontSize: 12, fontWeight: 600 as const,
    cursor: "pointer" as const,
  },
  btnSecondary: {
    background: "#1e293b", border: "1px solid #334155", borderRadius: 6,
    color: "#94a3b8", padding: "6px 14px", fontSize: 12, fontWeight: 600 as const,
    cursor: "pointer" as const,
  },
  btnDanger: {
    background: "#7f1d1d33", border: "1px solid #7f1d1d88", borderRadius: 6,
    color: "#fca5a5", padding: "6px 14px", fontSize: 12, fontWeight: 600 as const,
    cursor: "pointer" as const,
  },
  btnSuccess: {
    background: "#22c55e22", border: "1px solid #22c55e55", borderRadius: 6,
    color: "#86efac", padding: "6px 14px", fontSize: 12, fontWeight: 600 as const,
    cursor: "pointer" as const,
  },
  badge: (bg: string, fg: string): React.CSSProperties => ({
    padding: "2px 8px", borderRadius: 10, fontSize: 10, fontWeight: 600,
    background: bg, color: fg, whiteSpace: "nowrap",
  }),
  th: {
    padding: "6px 10px", textAlign: "left" as const,
    color: "#64748b", fontWeight: 500, fontSize: 12, whiteSpace: "nowrap" as const,
  },
  td: { padding: "5px 10px", fontSize: 12 },
  trBorder: { borderBottom: "1px solid #0f1624" },
};

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

/** Topological sort (Kahn's algorithm) — returns sorted table names */
function topoSort(tables: string[], deps: FKDep[]): string[] {
  const graph = new Map<string, string[]>();
  const inDeg = new Map<string, number>();
  const tableSet = new Set(tables);
  for (const t of tables) { graph.set(t, []); inDeg.set(t, 0); }
  for (const d of deps) {
    if (!tableSet.has(d.table)) continue;
    for (const p of d.depends_on) {
      if (!tableSet.has(p)) continue;
      graph.get(p)!.push(d.table);
      inDeg.set(d.table, (inDeg.get(d.table) || 0) + 1);
    }
  }
  const queue: string[] = [];
  for (const [t, deg] of inDeg) if (deg === 0) queue.push(t);
  const sorted: string[] = [];
  while (queue.length > 0) {
    const cur = queue.shift()!;
    sorted.push(cur);
    for (const next of graph.get(cur) || []) {
      const newDeg = (inDeg.get(next) || 1) - 1;
      inDeg.set(next, newDeg);
      if (newDeg === 0) queue.push(next);
    }
  }
  // Add any remaining (cycle) tables at the end
  for (const t of tables) if (!sorted.includes(t)) sorted.push(t);
  return sorted;
}

// ── SearchSelect ─────────────────────────────────────────────────────────────

function SearchSelect({
  value, onChange, options, placeholder, disabled,
}: {
  value: string;
  onChange: (v: string) => void;
  options: string[];
  placeholder: string;
  disabled?: boolean;
}) {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const wrapRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) {
        setOpen(false); setQuery("");
      }
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [open]);

  useEffect(() => { if (open) inputRef.current?.focus(); }, [open]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    return q ? options.filter(o => o.toLowerCase().includes(q)) : options;
  }, [options, query]);

  const handleOpen = () => { if (!disabled) { setOpen(o => !o); setQuery(""); } };
  const handleSelect = (opt: string) => { onChange(opt); setOpen(false); setQuery(""); };
  const handleKey = (e: React.KeyboardEvent) => {
    if (e.key === "Escape") { setOpen(false); setQuery(""); }
    if (e.key === "Enter" && filtered.length === 1) handleSelect(filtered[0]);
  };

  return (
    <div ref={wrapRef} style={{ position: "relative", minWidth: 165 }}>
      <div
        onClick={handleOpen}
        style={{
          display: "flex", alignItems: "center", gap: 6,
          background: "#1e293b", border: `1px solid ${open ? "#3b82f6" : "#334155"}`,
          borderRadius: 4, padding: "0 8px", height: 30,
          cursor: disabled ? "not-allowed" : "pointer",
          opacity: disabled ? 0.5 : 1, userSelect: "none",
        }}
      >
        <span style={{
          fontSize: 12, flex: 1, color: value ? "#e2e8f0" : "#475569",
          overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
        }}>
          {value || placeholder}
        </span>
        <span style={{ color: "#475569", fontSize: 9, flexShrink: 0 }}>{open ? "\u25B2" : "\u25BC"}</span>
      </div>
      {open && (
        <div style={{
          position: "absolute", top: "calc(100% + 3px)", left: 0, right: 0,
          background: "#1e293b", border: "1px solid #334155",
          borderRadius: 4, zIndex: 200, boxShadow: "0 6px 20px rgba(0,0,0,0.5)",
          display: "flex", flexDirection: "column",
        }}>
          <div style={{ padding: "6px 8px", borderBottom: "1px solid #0f1e35", display: "flex", alignItems: "center", gap: 6 }}>
            <input
              ref={inputRef} value={query} onChange={e => setQuery(e.target.value)}
              onKeyDown={handleKey} placeholder="Поиск..."
              style={{ background: "none", border: "none", color: "#e2e8f0", fontSize: 12, width: "100%", outline: "none" }}
            />
            {query && (
              <span onClick={() => setQuery("")} style={{ color: "#475569", cursor: "pointer", fontSize: 11, flexShrink: 0 }}>✕</span>
            )}
          </div>
          <div style={{ maxHeight: 200, overflowY: "auto" }}>
            {filtered.length === 0
              ? <div style={{ padding: "8px 10px", color: "#475569", fontSize: 12 }}>Нет совпадений</div>
              : filtered.map(o => (
                <div
                  key={o} onMouseDown={() => handleSelect(o)}
                  style={{
                    padding: "6px 10px", fontSize: 12, cursor: "pointer",
                    background: o === value ? "#1d3a5f" : "transparent",
                    color: o === value ? "#93c5fd" : "#e2e8f0",
                  }}
                  onMouseEnter={e => (e.currentTarget.style.background = o === value ? "#1d3a5f" : "#0f1624")}
                  onMouseLeave={e => (e.currentTarget.style.background = o === value ? "#1d3a5f" : "transparent")}
                >
                  {o}
                </div>
              ))
            }
          </div>
        </div>
      )}
    </div>
  );
}

// ── StepIndicator ────────────────────────────────────────────────────────────

const STEP_LABELS = ["Сравнение схемы", "Настройки таблиц", "Порядок загрузки", "Обзор и запуск"];

function StepIndicator({ current }: { current: number }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 0, marginBottom: 20 }}>
      {STEP_LABELS.map((label, i) => {
        const done = i < current;
        const active = i === current;
        const color = done ? "#22c55e" : active ? "#3b82f6" : "#334155";
        const textColor = done ? "#86efac" : active ? "#93c5fd" : "#475569";
        return (
          <React.Fragment key={i}>
            {i > 0 && (
              <div style={{
                flex: 1, height: 2, background: done ? "#22c55e55" : "#1e293b",
                margin: "0 4px",
              }} />
            )}
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <div style={{
                width: 24, height: 24, borderRadius: "50%",
                border: `2px solid ${color}`,
                background: done ? "#052e16" : active ? "#1e3a5f" : "transparent",
                display: "flex", alignItems: "center", justifyContent: "center",
                fontSize: 11, fontWeight: 700, color: textColor,
              }}>
                {done ? "\u2713" : i + 1}
              </div>
              <span style={{ fontSize: 12, fontWeight: active ? 700 : 500, color: textColor, whiteSpace: "nowrap" }}>
                {label}
              </span>
            </div>
          </React.Fragment>
        );
      })}
    </div>
  );
}

// ── DDLDetail ────────────────────────────────────────────────────────────────

function DDLDetail({
  srcSchema, table, tgtSchema,
}: {
  srcSchema: string; table: string; tgtSchema: string;
}) {
  const [ddl, setDdl] = useState<DDLData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState(false);

  const load = useCallback(() => {
    setLoading(true); setError(null);
    const qs = new URLSearchParams({
      src_schema: srcSchema, src_table: table,
      tgt_schema: tgtSchema, tgt_table: table,
    });
    fetch(`/api/target-prep/ddl?${qs}`)
      .then(r => r.ok ? r.json() : r.json().then(d => Promise.reject(d.error || "Ошибка")))
      .then(d => { setDdl(d); setExpanded(true); })
      .catch(e => setError(typeof e === "string" ? e : String(e)))
      .finally(() => setLoading(false));
  }, [srcSchema, table, tgtSchema]);

  if (!expanded && !ddl) {
    return (
      <button
        onClick={load}
        disabled={loading}
        style={{ ...S.btnSecondary, fontSize: 11, padding: "3px 10px" }}
      >
        {loading ? "..." : "DDL"}
      </button>
    );
  }

  if (error) {
    return <span style={{ color: "#fca5a5", fontSize: 11 }}>{error}</span>;
  }

  if (!ddl) return null;

  const colDiff = (() => {
    const tgtMap = new Map(ddl.target.columns.map(c => [c.name, c]));
    const srcMap = new Map(ddl.source.columns.map(c => [c.name, c]));
    const rows: { src: ColInfo | null; tgt: ColInfo | null; state: string }[] = [];
    for (const sc of ddl.source.columns) {
      const tc = tgtMap.get(sc.name);
      rows.push({ src: sc, tgt: tc ?? null, state: !tc ? "missing" : fmtType(sc) === fmtType(tc) ? "ok" : "type" });
    }
    for (const tc of ddl.target.columns) {
      if (!srcMap.has(tc.name)) rows.push({ src: null, tgt: tc, state: "extra" });
    }
    return rows;
  })();

  return (
    <div style={{ marginTop: 6 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
        <span style={{ fontSize: 11, fontWeight: 600, color: "#64748b" }}>DDL: {table}</span>
        <button onClick={() => setExpanded(!expanded)} style={{ ...S.btnSecondary, fontSize: 10, padding: "2px 8px" }}>
          {expanded ? "Свернуть" : "Развернуть"}
        </button>
      </div>
      {expanded && (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {/* Columns */}
          <div style={{ border: "1px solid #1e293b", borderRadius: 6, overflow: "hidden" }}>
            <div style={{ padding: "5px 12px", background: "#0a111f", fontSize: 11, fontWeight: 600, color: "#64748b", borderBottom: "1px solid #1e293b" }}>
              Колонки ({ddl.source.columns.length})
            </div>
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ borderBottom: "1px solid #1e293b" }}>
                    {["Колонка", "Тип (src)", "Тип (tgt)", "Статус"].map(h => (
                      <th key={h} style={S.th}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {colDiff.map((r, i) => {
                    const color = r.state === "ok" ? "#22c55e" : r.state === "type" ? "#eab308" : "#ef4444";
                    return (
                      <tr key={i} style={S.trBorder}>
                        <td style={{ ...S.td, fontFamily: "monospace" }}>{r.src?.name ?? r.tgt?.name}</td>
                        <td style={{ ...S.td, color: "#94a3b8", fontFamily: "monospace", fontSize: 11 }}>
                          {r.src ? fmtType(r.src) : "—"}
                        </td>
                        <td style={{ ...S.td, color: r.state === "ok" ? "#94a3b8" : color, fontFamily: "monospace", fontSize: 11 }}>
                          {r.tgt ? fmtType(r.tgt) : <span style={{ color: "#ef4444" }}>отсутствует</span>}
                        </td>
                        <td style={S.td}>
                          <span style={S.badge(color + "22", color)}>
                            {r.state === "ok" ? "OK" : r.state === "type" ? "Тип" : r.state === "missing" ? "Нет" : "Лишняя"}
                          </span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>

          {/* Indexes */}
          {ddl.source.indexes.length + ddl.target.indexes.length > 0 && (
            <div style={{ border: "1px solid #1e293b", borderRadius: 6, overflow: "hidden" }}>
              <div style={{ padding: "5px 12px", background: "#0a111f", fontSize: 11, fontWeight: 600, color: "#64748b", borderBottom: "1px solid #1e293b" }}>
                Индексы (src: {ddl.source.indexes.length}, tgt: {ddl.target.indexes.length})
              </div>
              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse" }}>
                  <thead>
                    <tr style={{ borderBottom: "1px solid #1e293b" }}>
                      {["Индекс", "Тип", "Колонки", "Статус"].map(h => (
                        <th key={h} style={S.th}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {ddl.source.indexes.map(idx => {
                      const tgt = ddl.target.indexes.find(t => t.name === idx.name);
                      return (
                        <tr key={idx.name} style={S.trBorder}>
                          <td style={{ ...S.td, fontFamily: "monospace", fontSize: 11 }}>{idx.name}</td>
                          <td style={{ ...S.td, color: "#64748b", fontSize: 11 }}>{idx.index_type}</td>
                          <td style={{ ...S.td, color: "#94a3b8", fontFamily: "monospace", fontSize: 11 }}>{idx.columns.join(", ")}</td>
                          <td style={S.td}>
                            {tgt
                              ? <span style={S.badge("#22c55e22", "#22c55e")}>{tgt.status}</span>
                              : <span style={S.badge("#ef444422", "#ef4444")}>отсутствует</span>}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Constraints */}
          {ddl.source.constraints.length + ddl.target.constraints.length > 0 && (
            <div style={{ border: "1px solid #1e293b", borderRadius: 6, overflow: "hidden" }}>
              <div style={{ padding: "5px 12px", background: "#0a111f", fontSize: 11, fontWeight: 600, color: "#64748b", borderBottom: "1px solid #1e293b" }}>
                Ограничения (src: {ddl.source.constraints.length}, tgt: {ddl.target.constraints.length})
              </div>
              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse" }}>
                  <thead>
                    <tr style={{ borderBottom: "1px solid #1e293b" }}>
                      {["Имя", "Тип", "Колонки", "Статус"].map(h => (
                        <th key={h} style={S.th}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {ddl.source.constraints.map(con => {
                      const tgt = ddl.target.constraints.find(t => t.name === con.name);
                      return (
                        <tr key={con.name} style={S.trBorder}>
                          <td style={{ ...S.td, fontFamily: "monospace", fontSize: 11 }}>{con.name}</td>
                          <td style={{ ...S.td, fontSize: 11 }}>
                            <span style={S.badge("#3b82f622", "#3b82f6")}>{con.type}</span>
                          </td>
                          <td style={{ ...S.td, color: "#94a3b8", fontFamily: "monospace", fontSize: 11 }}>{con.columns.join(", ")}</td>
                          <td style={S.td}>
                            {tgt
                              ? <span style={S.badge("#22c55e22", "#22c55e")}>{tgt.status}</span>
                              : <span style={S.badge("#ef444422", "#ef4444")}>отсутствует</span>}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Triggers */}
          {ddl.source.triggers.length + ddl.target.triggers.length > 0 && (
            <div style={{ border: "1px solid #1e293b", borderRadius: 6, overflow: "hidden" }}>
              <div style={{ padding: "5px 12px", background: "#0a111f", fontSize: 11, fontWeight: 600, color: "#64748b", borderBottom: "1px solid #1e293b" }}>
                Триггеры (src: {ddl.source.triggers.length}, tgt: {ddl.target.triggers.length})
              </div>
              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse" }}>
                  <thead>
                    <tr style={{ borderBottom: "1px solid #1e293b" }}>
                      {["Триггер", "Тип", "Событие", "Статус"].map(h => (
                        <th key={h} style={S.th}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {ddl.source.triggers.map(trg => {
                      const tgt = ddl.target.triggers.find(t => t.name === trg.name);
                      return (
                        <tr key={trg.name} style={S.trBorder}>
                          <td style={{ ...S.td, fontFamily: "monospace", fontSize: 11 }}>{trg.name}</td>
                          <td style={{ ...S.td, color: "#64748b", fontSize: 11 }}>{trg.trigger_type}</td>
                          <td style={{ ...S.td, color: "#94a3b8", fontSize: 11 }}>{trg.event}</td>
                          <td style={S.td}>
                            {tgt
                              ? <span style={S.badge("#22c55e22", "#22c55e")}>{tgt.status}</span>
                              : <span style={S.badge("#ef444422", "#ef4444")}>отсутствует</span>}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ── SchemaCompareStep (Step 1) ───────────────────────────────────────────────

function SchemaCompareStep({
  srcSchema, tgtSchema, onSrcSchema, onTgtSchema,
  tables, selected, onToggle, onToggleAll,
  comparing, onCompare,
  onSyncTable, syncBusy,
}: {
  srcSchema: string; tgtSchema: string;
  onSrcSchema: (v: string) => void; onTgtSchema: (v: string) => void;
  tables: TableCompare[];
  selected: Set<string>;
  onToggle: (t: string) => void; onToggleAll: () => void;
  comparing: boolean; onCompare: () => void;
  onSyncTable: (table: string, action: "ensure" | "sync_cols" | "sync_objects") => void;
  syncBusy: Set<string>;
}) {
  const [srcSchemas, setSrcSchemas] = useState<string[]>([]);
  const [tgtSchemas, setTgtSchemas] = useState<string[]>([]);
  const [search, setSearch] = useState("");
  const [filterMode, setFilterMode] = useState<"all" | "ok" | "diff" | "missing">("all");
  const [ddlOpen, setDdlOpen] = useState<string | null>(null);

  useEffect(() => {
    fetch("/api/db/source/schemas").then(r => r.json()).then(d => Array.isArray(d) && setSrcSchemas(d)).catch(() => {});
    fetch("/api/db/target/schemas").then(r => r.json()).then(d => Array.isArray(d) && setTgtSchemas(d)).catch(() => {});
  }, []);

  const filtered = useMemo(() => {
    let list = tables;
    if (search) {
      const q = search.toLowerCase();
      list = list.filter(t => t.table.toLowerCase().includes(q));
    }
    if (filterMode === "ok") list = list.filter(t => t.diff?.ok);
    if (filterMode === "diff") list = list.filter(t => t.exists_on_target && t.diff && !t.diff.ok);
    if (filterMode === "missing") list = list.filter(t => !t.exists_on_target);
    return list;
  }, [tables, search, filterMode]);

  const allSelected = filtered.length > 0 && filtered.every(t => selected.has(t.table));
  const okCount = tables.filter(t => t.diff?.ok).length;
  const diffCount = tables.filter(t => t.exists_on_target && t.diff && !t.diff.ok).length;
  const missingCount = tables.filter(t => !t.exists_on_target).length;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* Schema selectors */}
      <div style={S.row2}>
        <div style={S.field}>
          <label style={S.label}>Схема источника</label>
          <SearchSelect value={srcSchema} onChange={onSrcSchema} options={srcSchemas} placeholder="Выберите схему" />
        </div>
        <div style={S.field}>
          <label style={S.label}>Схема таргета</label>
          <SearchSelect value={tgtSchema} onChange={onTgtSchema} options={tgtSchemas} placeholder="Выберите схему" />
        </div>
      </div>

      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <button
          onClick={onCompare}
          disabled={!srcSchema || !tgtSchema || comparing}
          style={{
            ...S.btnPrimary,
            opacity: (!srcSchema || !tgtSchema || comparing) ? 0.5 : 1,
            cursor: (!srcSchema || !tgtSchema || comparing) ? "not-allowed" : "pointer",
          }}
        >
          {comparing ? "Сравнение..." : "Сравнить схемы"}
        </button>
        {tables.length > 0 && (
          <span style={{ fontSize: 11, color: "#64748b" }}>
            Найдено таблиц: {tables.length}
          </span>
        )}
      </div>

      {/* Results table */}
      {tables.length > 0 && (
        <div style={S.card}>
          {/* Toolbar */}
          <div style={{
            ...S.cardHeader,
            justifyContent: "space-between",
          }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <input
                value={search} onChange={e => setSearch(e.target.value)}
                placeholder="Поиск таблицы..."
                style={{ ...S.input, width: 200, padding: "4px 8px", fontSize: 12 }}
              />
              <div style={{ display: "flex", gap: 4 }}>
                {([
                  ["all", `Все (${tables.length})`],
                  ["ok", `OK (${okCount})`],
                  ["diff", `Различия (${diffCount})`],
                  ["missing", `Нет на таргете (${missingCount})`],
                ] as [string, string][]).map(([mode, label]) => (
                  <button
                    key={mode}
                    onClick={() => setFilterMode(mode as typeof filterMode)}
                    style={{
                      background: filterMode === mode ? "#1e3a5f" : "transparent",
                      border: `1px solid ${filterMode === mode ? "#3b82f6" : "#334155"}`,
                      borderRadius: 4, color: filterMode === mode ? "#93c5fd" : "#475569",
                      padding: "3px 8px", fontSize: 11, cursor: "pointer", fontWeight: 500,
                    }}
                  >
                    {label}
                  </button>
                ))}
              </div>
            </div>
            <span style={{ fontSize: 11, color: "#64748b" }}>
              Выбрано: {selected.size}
            </span>
          </div>

          {/* Table */}
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ borderBottom: "1px solid #1e293b" }}>
                  <th style={{ ...S.th, width: 36 }}>
                    <input
                      type="checkbox"
                      checked={allSelected}
                      onChange={onToggleAll}
                      style={{ accentColor: "#3b82f6" }}
                    />
                  </th>
                  <th style={S.th}>Таблица</th>
                  <th style={S.th}>На таргете</th>
                  <th style={S.th}>Колонки</th>
                  <th style={S.th}>Индексы</th>
                  <th style={S.th}>Ограничения</th>
                  <th style={S.th}>Триггеры</th>
                  <th style={S.th}>Действия</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map(t => {
                  const d = t.diff;
                  const isOk = d?.ok;
                  const rowBg = !t.exists_on_target ? "rgba(239,68,68,0.04)" : isOk ? "transparent" : "rgba(234,179,8,0.04)";
                  return (
                    <React.Fragment key={t.table}>
                      <tr style={{ ...S.trBorder, background: rowBg }}>
                        <td style={{ ...S.td, textAlign: "center" }}>
                          <input
                            type="checkbox"
                            checked={selected.has(t.table)}
                            onChange={() => onToggle(t.table)}
                            style={{ accentColor: "#3b82f6" }}
                          />
                        </td>
                        <td style={S.td}>
                          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                            <code style={{ color: "#e2e8f0", fontSize: 12, fontWeight: 600 }}>{t.table}</code>
                            <button
                              onClick={() => setDdlOpen(ddlOpen === t.table ? null : t.table)}
                              style={{
                                background: "none", border: "none", color: "#475569",
                                fontSize: 9, cursor: "pointer", padding: 0,
                              }}
                            >
                              {ddlOpen === t.table ? "\u25B2" : "\u25BC"}
                            </button>
                          </div>
                        </td>
                        <td style={S.td}>
                          {t.exists_on_target
                            ? <span style={S.badge("#22c55e22", "#22c55e")}>Да</span>
                            : <span style={S.badge("#ef444422", "#ef4444")}>Нет</span>}
                        </td>
                        <td style={S.td}>
                          {d ? (
                            d.cols_missing + d.cols_type === 0
                              ? <span style={{ color: "#22c55e", fontSize: 13 }}>{"\u2713"}</span>
                              : <span style={{ color: "#ef4444", fontSize: 11 }}>
                                  {d.cols_missing > 0 && `\u2717 ${d.cols_missing}`}
                                  {d.cols_type > 0 && ` \u26A0 ${d.cols_type}`}
                                </span>
                          ) : <span style={{ color: "#334155" }}>—</span>}
                        </td>
                        <td style={S.td}>
                          {d ? (
                            d.idx_missing + d.idx_disabled === 0
                              ? <span style={{ color: "#22c55e", fontSize: 13 }}>{"\u2713"}</span>
                              : <span style={{ fontSize: 11 }}>
                                  {d.idx_missing > 0 && <span style={{ color: "#ef4444" }}>{"\u2717 "}{d.idx_missing} </span>}
                                  {d.idx_disabled > 0 && <span style={{ color: "#eab308" }}>{"\u26A0 "}{d.idx_disabled}</span>}
                                </span>
                          ) : <span style={{ color: "#334155" }}>—</span>}
                        </td>
                        <td style={S.td}>
                          {d ? (
                            d.con_missing + d.con_disabled === 0
                              ? <span style={{ color: "#22c55e", fontSize: 13 }}>{"\u2713"}</span>
                              : <span style={{ fontSize: 11 }}>
                                  {d.con_missing > 0 && <span style={{ color: "#ef4444" }}>{"\u2717 "}{d.con_missing} </span>}
                                  {d.con_disabled > 0 && <span style={{ color: "#eab308" }}>{"\u26A0 "}{d.con_disabled}</span>}
                                </span>
                          ) : <span style={{ color: "#334155" }}>—</span>}
                        </td>
                        <td style={S.td}>
                          {d ? (
                            d.trg_missing === 0
                              ? <span style={{ color: "#22c55e", fontSize: 13 }}>{"\u2713"}</span>
                              : <span style={{ color: "#ef4444", fontSize: 11 }}>{"\u2717 "}{d.trg_missing}</span>
                          ) : <span style={{ color: "#334155" }}>—</span>}
                        </td>
                        <td style={S.td}>
                          <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
                            {!t.exists_on_target && (
                              <button
                                onClick={() => onSyncTable(t.table, "ensure")}
                                disabled={syncBusy.has(t.table)}
                                style={{ ...S.btnSuccess, fontSize: 10, padding: "2px 8px", opacity: syncBusy.has(t.table) ? 0.5 : 1 }}
                              >
                                {syncBusy.has(t.table) ? "..." : "Создать"}
                              </button>
                            )}
                            {t.exists_on_target && d && d.cols_missing > 0 && (
                              <button
                                onClick={() => onSyncTable(t.table, "sync_cols")}
                                disabled={syncBusy.has(t.table)}
                                style={{ ...S.btnSuccess, fontSize: 10, padding: "2px 8px", opacity: syncBusy.has(t.table) ? 0.5 : 1 }}
                              >
                                {syncBusy.has(t.table) ? "..." : "Колонки"}
                              </button>
                            )}
                            {t.exists_on_target && d && (d.idx_missing + d.con_missing + d.trg_missing > 0) && (
                              <button
                                onClick={() => onSyncTable(t.table, "sync_objects")}
                                disabled={syncBusy.has(t.table)}
                                style={{ ...S.btnSuccess, fontSize: 10, padding: "2px 8px", opacity: syncBusy.has(t.table) ? 0.5 : 1 }}
                              >
                                {syncBusy.has(t.table) ? "..." : "Объекты"}
                              </button>
                            )}
                          </div>
                        </td>
                      </tr>
                      {ddlOpen === t.table && t.exists_on_target && (
                        <tr>
                          <td colSpan={8} style={{ padding: "0 10px 10px", background: "#0a111f" }}>
                            <DDLDetail srcSchema={srcSchema} table={t.table} tgtSchema={tgtSchema} />
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

// ── TableSelectionStep (Step 2) ──────────────────────────────────────────────

function TableSelectionStep({
  selected, defaults, onDefaults,
  tableSettings, onTableSetting,
  groups, selectedGroup, onSelectGroup,
}: {
  selected: string[];
  defaults: PlanDefaults;
  onDefaults: (d: PlanDefaults) => void;
  tableSettings: Map<string, BatchItem>;
  onTableSetting: (table: string, upd: Partial<BatchItem>) => void;
  groups: ConnectorGroup[];
  selectedGroup: string;
  onSelectGroup: (v: string) => void;
}) {
  const [customTables, setCustomTables] = useState<Set<string>>(new Set());

  const toggleCustom = (table: string) => {
    setCustomTables(prev => {
      const next = new Set(prev);
      if (next.has(table)) {
        next.delete(table);
        // Reset to defaults
        onTableSetting(table, {
          mode: defaults.mode,
          strategy: defaults.strategy,
          chunk_size: defaults.chunk_size,
          workers: defaults.workers,
        });
      } else {
        next.add(table);
      }
      return next;
    });
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* Global defaults */}
      <div style={S.card}>
        <div style={S.cardHeader}>
          <span style={{ fontSize: 13, fontWeight: 600, color: "#e2e8f0" }}>Глобальные настройки</span>
        </div>
        <div style={S.cardBody}>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 10 }}>
            <div style={S.field}>
              <label style={S.label}>Chunk size</label>
              <input
                type="number" value={defaults.chunk_size}
                onChange={e => onDefaults({ ...defaults, chunk_size: parseInt(e.target.value) || 50000 })}
                style={S.input}
              />
            </div>
            <div style={S.field}>
              <label style={S.label}>Workers</label>
              <input
                type="number" value={defaults.workers}
                onChange={e => onDefaults({ ...defaults, workers: parseInt(e.target.value) || 4 })}
                style={S.input}
              />
            </div>
            <div style={S.field}>
              <label style={S.label}>Стратегия</label>
              <select
                value={defaults.strategy}
                onChange={e => onDefaults({ ...defaults, strategy: e.target.value as "STAGE" | "DIRECT" })}
                style={S.select}
              >
                <option value="STAGE">STAGE</option>
                <option value="DIRECT">DIRECT</option>
              </select>
            </div>
            <div style={S.field}>
              <label style={S.label}>Режим</label>
              <select
                value={defaults.mode}
                onChange={e => onDefaults({ ...defaults, mode: e.target.value as "CDC" | "BULK_ONLY" })}
                style={S.select}
              >
                <option value="CDC">CDC</option>
                <option value="BULK_ONLY">BULK_ONLY</option>
              </select>
            </div>
          </div>
        </div>
      </div>

      {/* Connector group */}
      <div style={S.card}>
        <div style={S.cardHeader}>
          <span style={{ fontSize: 13, fontWeight: 600, color: "#e2e8f0" }}>Группа коннекторов</span>
        </div>
        <div style={S.cardBody}>
          <div style={S.field}>
            <label style={S.label}>Выберите группу коннекторов</label>
            <SearchSelect
              value={selectedGroup}
              onChange={onSelectGroup}
              options={groups.map(g => g.group_name)}
              placeholder="Выберите группу..."
            />
          </div>
          {selectedGroup && (
            <div style={{ fontSize: 11, color: "#64748b" }}>
              Группа: <span style={{ color: "#93c5fd" }}>{selectedGroup}</span>
              {(() => {
                const g = groups.find(gg => gg.group_name === selectedGroup);
                return g ? <> | Статус: <span style={{ color: g.status === "RUNNING" ? "#22c55e" : "#eab308" }}>{g.status}</span></> : null;
              })()}
            </div>
          )}
        </div>
      </div>

      {/* Per-table settings */}
      <div style={S.card}>
        <div style={S.cardHeader}>
          <span style={{ fontSize: 13, fontWeight: 600, color: "#e2e8f0" }}>Настройки таблиц ({selected.length})</span>
          <span style={{ fontSize: 11, color: "#475569", marginLeft: "auto" }}>
            Индивидуальных: {customTables.size}
          </span>
        </div>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ borderBottom: "1px solid #1e293b" }}>
                {["Таблица", "Режим", "Стратегия", "Chunk", "Workers", "Индивидуально"].map(h => (
                  <th key={h} style={S.th}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {selected.map(table => {
                const ts = tableSettings.get(table)!;
                const isCustom = customTables.has(table);
                return (
                  <tr key={table} style={{ ...S.trBorder, background: isCustom ? "rgba(59,130,246,0.04)" : "transparent" }}>
                    <td style={S.td}>
                      <code style={{ color: "#e2e8f0", fontSize: 12 }}>{table}</code>
                    </td>
                    <td style={S.td}>
                      {isCustom ? (
                        <select
                          value={ts.mode}
                          onChange={e => onTableSetting(table, { mode: e.target.value as "CDC" | "BULK_ONLY" })}
                          style={{ ...S.select, padding: "3px 6px", fontSize: 11 }}
                        >
                          <option value="CDC">CDC</option>
                          <option value="BULK_ONLY">BULK_ONLY</option>
                        </select>
                      ) : (
                        <span style={{ fontSize: 11, color: "#94a3b8" }}>{ts.mode}</span>
                      )}
                    </td>
                    <td style={S.td}>
                      {isCustom ? (
                        <select
                          value={ts.strategy}
                          onChange={e => onTableSetting(table, { strategy: e.target.value as "STAGE" | "DIRECT" })}
                          style={{ ...S.select, padding: "3px 6px", fontSize: 11 }}
                        >
                          <option value="STAGE">STAGE</option>
                          <option value="DIRECT">DIRECT</option>
                        </select>
                      ) : (
                        <span style={{ fontSize: 11, color: "#94a3b8" }}>{ts.strategy}</span>
                      )}
                    </td>
                    <td style={S.td}>
                      {isCustom ? (
                        <input
                          type="number" value={ts.chunk_size}
                          onChange={e => onTableSetting(table, { chunk_size: parseInt(e.target.value) || 50000 })}
                          style={{ ...S.input, width: 80, padding: "3px 6px", fontSize: 11 }}
                        />
                      ) : (
                        <span style={{ fontSize: 11, color: "#94a3b8" }}>{ts.chunk_size.toLocaleString("ru-RU")}</span>
                      )}
                    </td>
                    <td style={S.td}>
                      {isCustom ? (
                        <input
                          type="number" value={ts.workers}
                          onChange={e => onTableSetting(table, { workers: parseInt(e.target.value) || 4 })}
                          style={{ ...S.input, width: 60, padding: "3px 6px", fontSize: 11 }}
                        />
                      ) : (
                        <span style={{ fontSize: 11, color: "#94a3b8" }}>{ts.workers}</span>
                      )}
                    </td>
                    <td style={{ ...S.td, textAlign: "center" }}>
                      <input
                        type="checkbox"
                        checked={isCustom}
                        onChange={() => toggleCustom(table)}
                        style={{ accentColor: "#3b82f6" }}
                      />
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

// ── OrderingStep (Step 3) ────────────────────────────────────────────────────

function OrderingStep({
  batches, onBatches, deps, depsLoading,
}: {
  batches: Batch[];
  onBatches: (b: Batch[]) => void;
  deps: FKDep[];
  depsLoading: boolean;
}) {
  const moveItem = (fromBatch: number, table: string, toBatch: number) => {
    const next = batches.map(b => ({
      ...b,
      items: b.id === fromBatch
        ? b.items.filter(it => it.table !== table)
        : b.id === toBatch
          ? [...b.items, batches.find(bb => bb.id === fromBatch)!.items.find(it => it.table === table)!]
          : b.items,
    }));
    // Remove empty batches (except batch 1)
    onBatches(next.filter(b => b.items.length > 0 || b.id === 1));
  };

  const addBatch = () => {
    const maxId = Math.max(...batches.map(b => b.id), 0);
    onBatches([...batches, { id: maxId + 1, items: [] }]);
  };

  const moveUp = (batchId: number, table: string) => {
    const batch = batches.find(b => b.id === batchId);
    if (!batch) return;
    const idx = batch.items.findIndex(it => it.table === table);
    if (idx <= 0) return;
    const newItems = [...batch.items];
    [newItems[idx - 1], newItems[idx]] = [newItems[idx], newItems[idx - 1]];
    onBatches(batches.map(b => b.id === batchId ? { ...b, items: newItems } : b));
  };

  const moveDown = (batchId: number, table: string) => {
    const batch = batches.find(b => b.id === batchId);
    if (!batch) return;
    const idx = batch.items.findIndex(it => it.table === table);
    if (idx < 0 || idx >= batch.items.length - 1) return;
    const newItems = [...batch.items];
    [newItems[idx], newItems[idx + 1]] = [newItems[idx + 1], newItems[idx]];
    onBatches(batches.map(b => b.id === batchId ? { ...b, items: newItems } : b));
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* FK dependencies info */}
      <div style={S.card}>
        <div style={S.cardHeader}>
          <span style={{ fontSize: 13, fontWeight: 600, color: "#e2e8f0" }}>FK зависимости</span>
          {depsLoading && <span style={{ fontSize: 11, color: "#64748b" }}>Загрузка...</span>}
        </div>
        <div style={S.cardBody}>
          {deps.length === 0 && !depsLoading && (
            <span style={{ fontSize: 12, color: "#475569" }}>Нет FK зависимостей между выбранными таблицами</span>
          )}
          {deps.length > 0 && (
            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              {deps.map(d => (
                <div key={d.table} style={{ fontSize: 12, color: "#94a3b8" }}>
                  <code style={{ color: "#e2e8f0" }}>{d.table}</code>
                  <span style={{ color: "#475569", margin: "0 6px" }}>{"\u2192"}</span>
                  {d.depends_on.map((dep, i) => (
                    <React.Fragment key={dep}>
                      {i > 0 && <span style={{ color: "#475569" }}>, </span>}
                      <code style={{ color: "#93c5fd" }}>{dep}</code>
                    </React.Fragment>
                  ))}
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Batches */}
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
        <span style={{ fontSize: 13, fontWeight: 600, color: "#e2e8f0" }}>Батчи ({batches.length})</span>
        <button onClick={addBatch} style={{ ...S.btnSecondary, fontSize: 11, padding: "3px 10px" }}>
          + Добавить батч
        </button>
      </div>

      {batches.map(batch => (
        <div key={batch.id} style={S.card}>
          <div style={{
            ...S.cardHeader,
            justifyContent: "space-between",
          }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <span style={{ fontSize: 13, fontWeight: 600, color: "#e2e8f0" }}>
                Батч #{batch.id}
              </span>
              <span style={S.badge("#1e3a5f", "#93c5fd")}>
                {batch.items.length} таблиц
              </span>
            </div>
          </div>
          {batch.items.length === 0 ? (
            <div style={{ padding: "16px", textAlign: "center", color: "#475569", fontSize: 12 }}>
              Пустой батч — перетащите сюда таблицы
            </div>
          ) : (
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ borderBottom: "1px solid #1e293b" }}>
                    {["#", "Таблица", "Режим", "Стратегия", "Порядок", "Переместить"].map(h => (
                      <th key={h} style={S.th}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {batch.items.map((item, idx) => {
                    const hasDep = deps.some(d => d.table === item.table);
                    return (
                      <tr key={item.table} style={S.trBorder}>
                        <td style={{ ...S.td, color: "#475569" }}>{idx + 1}</td>
                        <td style={S.td}>
                          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                            <code style={{ color: "#e2e8f0", fontSize: 12 }}>{item.table}</code>
                            {hasDep && (
                              <span style={S.badge("#eab30822", "#eab308")}>FK</span>
                            )}
                          </div>
                        </td>
                        <td style={S.td}>
                          <span style={{ fontSize: 11, color: "#94a3b8" }}>{item.mode}</span>
                        </td>
                        <td style={S.td}>
                          <span style={{ fontSize: 11, color: "#94a3b8" }}>{item.strategy}</span>
                        </td>
                        <td style={S.td}>
                          <div style={{ display: "flex", gap: 4 }}>
                            <button
                              onClick={() => moveUp(batch.id, item.table)}
                              disabled={idx === 0}
                              style={{
                                ...S.btnSecondary, fontSize: 10, padding: "2px 6px",
                                opacity: idx === 0 ? 0.3 : 1,
                              }}
                            >
                              {"\u25B2"}
                            </button>
                            <button
                              onClick={() => moveDown(batch.id, item.table)}
                              disabled={idx === batch.items.length - 1}
                              style={{
                                ...S.btnSecondary, fontSize: 10, padding: "2px 6px",
                                opacity: idx === batch.items.length - 1 ? 0.3 : 1,
                              }}
                            >
                              {"\u25BC"}
                            </button>
                          </div>
                        </td>
                        <td style={S.td}>
                          <div style={{ display: "flex", gap: 4 }}>
                            {batches.filter(b => b.id !== batch.id).map(b => (
                              <button
                                key={b.id}
                                onClick={() => moveItem(batch.id, item.table, b.id)}
                                style={{ ...S.btnSecondary, fontSize: 10, padding: "2px 8px" }}
                              >
                                {"\u2192"} #{b.id}
                              </button>
                            ))}
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

// ── ReviewStep (Step 4) ──────────────────────────────────────────────────────

function ReviewStep({
  srcSchema, tgtSchema, selectedGroup,
  defaults, batches, executing, onExecute,
  planId, starting, onStart,
}: {
  srcSchema: string; tgtSchema: string; selectedGroup: string;
  defaults: PlanDefaults;
  batches: Batch[];
  executing: boolean; onExecute: () => void;
  planId: string | null;
  starting: boolean; onStart: () => void;
}) {
  const totalTables = batches.reduce((acc, b) => acc + b.items.length, 0);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* Summary cards */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12 }}>
        <div style={{ ...S.card, padding: 16 }}>
          <div style={{ fontSize: 11, color: "#64748b", marginBottom: 4 }}>Схемы</div>
          <div style={{ fontSize: 13, color: "#e2e8f0" }}>
            <span style={{ color: "#93c5fd" }}>{srcSchema}</span>
            <span style={{ color: "#475569", margin: "0 6px" }}>{"\u2192"}</span>
            <span style={{ color: "#86efac" }}>{tgtSchema}</span>
          </div>
        </div>
        <div style={{ ...S.card, padding: 16 }}>
          <div style={{ fontSize: 11, color: "#64748b", marginBottom: 4 }}>Таблиц / Батчей</div>
          <div style={{ fontSize: 13, color: "#e2e8f0" }}>
            <span style={{ fontWeight: 700 }}>{totalTables}</span>
            <span style={{ color: "#475569" }}> / </span>
            <span style={{ fontWeight: 700 }}>{batches.length}</span>
          </div>
        </div>
        <div style={{ ...S.card, padding: 16 }}>
          <div style={{ fontSize: 11, color: "#64748b", marginBottom: 4 }}>Группа коннекторов</div>
          <div style={{ fontSize: 13, color: "#93c5fd" }}>{selectedGroup || "—"}</div>
        </div>
      </div>

      {/* Defaults */}
      <div style={S.card}>
        <div style={S.cardHeader}>
          <span style={{ fontSize: 13, fontWeight: 600, color: "#e2e8f0" }}>Параметры по умолчанию</span>
        </div>
        <div style={{ ...S.cardBody, flexDirection: "row", gap: 20, flexWrap: "wrap" }}>
          {([
            ["Режим", defaults.mode],
            ["Стратегия", defaults.strategy],
            ["Chunk size", defaults.chunk_size.toLocaleString("ru-RU")],
            ["Workers", String(defaults.workers)],
          ] as [string, string][]).map(([label, value]) => (
            <div key={label} style={{ display: "flex", flexDirection: "column", gap: 2 }}>
              <span style={{ fontSize: 10, color: "#64748b" }}>{label}</span>
              <span style={{ fontSize: 12, color: "#e2e8f0", fontWeight: 600 }}>{value}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Batch list */}
      {batches.map(batch => (
        <div key={batch.id} style={S.card}>
          <div style={S.cardHeader}>
            <span style={{ fontSize: 13, fontWeight: 600, color: "#e2e8f0" }}>Батч #{batch.id}</span>
            <span style={S.badge("#1e3a5f", "#93c5fd")}>{batch.items.length} таблиц</span>
          </div>
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ borderBottom: "1px solid #1e293b" }}>
                  {["#", "Таблица", "Режим", "Стратегия", "Chunk", "Workers"].map(h => (
                    <th key={h} style={S.th}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {batch.items.map((item, idx) => (
                  <tr key={item.table} style={S.trBorder}>
                    <td style={{ ...S.td, color: "#475569" }}>{idx + 1}</td>
                    <td style={S.td}>
                      <code style={{ color: "#e2e8f0", fontSize: 12 }}>{item.table}</code>
                    </td>
                    <td style={S.td}><span style={{ fontSize: 11, color: "#94a3b8" }}>{item.mode}</span></td>
                    <td style={S.td}><span style={{ fontSize: 11, color: "#94a3b8" }}>{item.strategy}</span></td>
                    <td style={S.td}><span style={{ fontSize: 11, color: "#94a3b8" }}>{item.chunk_size.toLocaleString("ru-RU")}</span></td>
                    <td style={S.td}><span style={{ fontSize: 11, color: "#94a3b8" }}>{item.workers}</span></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ))}

      {/* Execute / Start */}
      <div style={{
        display: "flex", alignItems: "center", gap: 12,
        padding: "16px 0", borderTop: "1px solid #1e293b",
      }}>
        {!planId ? (
          <button
            onClick={onExecute}
            disabled={executing || totalTables === 0}
            style={{
              ...S.btnPrimary, padding: "8px 24px", fontSize: 13,
              opacity: (executing || totalTables === 0) ? 0.5 : 1,
              cursor: (executing || totalTables === 0) ? "not-allowed" : "pointer",
            }}
          >
            {executing ? "Создание плана..." : "Создать план и миграции (DRAFT)"}
          </button>
        ) : (
          <>
            <span style={S.badge("#052e16", "#86efac")}>
              План создан: {planId}
            </span>
            <button
              onClick={onStart}
              disabled={starting}
              style={{
                ...S.btnSuccess, padding: "8px 24px", fontSize: 13,
                opacity: starting ? 0.5 : 1,
                cursor: starting ? "not-allowed" : "pointer",
              }}
            >
              {starting ? "Запуск..." : "Запустить первый батч"}
            </button>
          </>
        )}
      </div>
    </div>
  );
}

// ── MigrationPlanner (main wizard) ───────────────────────────────────────────

export function MigrationPlanner() {
  const [step, setStep] = useState(0);

  // Step 1 state
  const [srcSchema, setSrcSchema] = useState("");
  const [tgtSchema, setTgtSchema] = useState("");
  const [tables, setTables] = useState<TableCompare[]>([]);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [comparing, setComparing] = useState(false);
  const [compareError, setCompareError] = useState<string | null>(null);
  const [syncBusy, setSyncBusy] = useState<Set<string>>(new Set());

  // Step 2 state
  const [defaults, setDefaults] = useState<PlanDefaults>({
    chunk_size: 50000, workers: 4, strategy: "STAGE", mode: "CDC",
  });
  const [tableSettings, setTableSettings] = useState<Map<string, BatchItem>>(new Map());
  const [groups, setGroups] = useState<ConnectorGroup[]>([]);
  const [selectedGroup, setSelectedGroup] = useState("");

  // Step 3 state
  const [batches, setBatches] = useState<Batch[]>([]);
  const [deps, setDeps] = useState<FKDep[]>([]);
  const [depsLoading, setDepsLoading] = useState(false);

  // Step 4 state
  const [executing, setExecuting] = useState(false);
  const [executeError, setExecuteError] = useState<string | null>(null);
  const [planId, setPlanId] = useState<string | null>(null);
  const [starting, setStarting] = useState(false);
  const [startError, setStartError] = useState<string | null>(null);

  // Load connector groups
  useEffect(() => {
    fetch("/api/connector-groups")
      .then(r => r.ok ? r.json() : Promise.reject())
      .then((data: ConnectorGroup[]) => setGroups(data))
      .catch(() => {});
  }, []);

  // Compare schemas
  const doCompare = useCallback(() => {
    if (!srcSchema || !tgtSchema) return;
    setComparing(true); setCompareError(null);
    const qs = new URLSearchParams({ src_schema: srcSchema, tgt_schema: tgtSchema });
    fetch(`/api/planner/compare-schema?${qs}`)
      .then(r => r.ok ? r.json() : r.json().then(d => Promise.reject(d.error || "Ошибка")))
      .then((data: TableCompare[]) => {
        setTables(data);
        // Auto-select tables that exist on target
        const sel = new Set(data.filter(t => t.exists_on_target).map(t => t.table));
        setSelected(sel);
      })
      .catch(e => setCompareError(typeof e === "string" ? e : String(e)))
      .finally(() => setComparing(false));
  }, [srcSchema, tgtSchema]);

  // Toggle selection
  const toggleTable = (table: string) => {
    setSelected(prev => {
      const next = new Set(prev);
      if (next.has(table)) next.delete(table); else next.add(table);
      return next;
    });
  };

  const toggleAll = () => {
    const allTables = tables.map(t => t.table);
    if (allTables.every(t => selected.has(t))) {
      setSelected(new Set());
    } else {
      setSelected(new Set(allTables));
    }
  };

  // Sync table actions
  const doSyncTable = useCallback((table: string, action: "ensure" | "sync_cols" | "sync_objects") => {
    setSyncBusy(prev => new Set(prev).add(table));
    const body: Record<string, string> = {
      src_schema: srcSchema, src_table: table,
      tgt_schema: tgtSchema, tgt_table: table,
    };
    let url = "";
    if (action === "ensure") url = "/api/target-prep/ensure-table";
    else if (action === "sync_cols") url = "/api/target-prep/sync-columns";
    else url = "/api/target-prep/sync-objects";

    if (action === "sync_objects") {
      (body as Record<string, unknown>).types = ["constraints", "indexes", "triggers"];
    }

    fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })
      .then(r => r.ok ? r.json() : r.json().then(d => Promise.reject(d.error || "Ошибка")))
      .then(() => {
        // Re-compare this single table
        return fetch("/api/target-prep/compare-summary", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ src_schema: srcSchema, src_table: table, tgt_schema: tgtSchema, tgt_table: table }),
        }).then(r => r.ok ? r.json() : null);
      })
      .then(newDiff => {
        if (newDiff) {
          setTables(prev => prev.map(t =>
            t.table === table
              ? { ...t, exists_on_target: true, diff: newDiff }
              : t
          ));
        }
      })
      .catch(() => {})
      .finally(() => {
        setSyncBusy(prev => { const next = new Set(prev); next.delete(table); return next; });
      });
  }, [srcSchema, tgtSchema]);

  // Build table settings when moving to step 2
  const initTableSettings = useCallback(() => {
    const map = new Map<string, BatchItem>();
    for (const table of selected) {
      const existing = tableSettings.get(table);
      map.set(table, existing ?? {
        table,
        mode: defaults.mode,
        strategy: defaults.strategy,
        chunk_size: defaults.chunk_size,
        workers: defaults.workers,
      });
    }
    setTableSettings(map);
  }, [selected, defaults, tableSettings]);

  const updateTableSetting = (table: string, upd: Partial<BatchItem>) => {
    setTableSettings(prev => {
      const next = new Map(prev);
      const cur = next.get(table);
      if (cur) next.set(table, { ...cur, ...upd });
      return next;
    });
  };

  // Load FK deps and build initial batches when moving to step 3
  const initOrdering = useCallback(() => {
    const selectedArr = Array.from(selected);
    if (selectedArr.length === 0) return;

    setDepsLoading(true);
    const qs = new URLSearchParams({ schema: srcSchema, tables: selectedArr.join(",") });
    fetch(`/api/planner/fk-dependencies?${qs}`)
      .then(r => r.ok ? r.json() : Promise.reject())
      .then((data: FKDep[]) => {
        setDeps(data);
        // Topo sort and put all into batch 1
        const sorted = topoSort(selectedArr, data);
        const items: BatchItem[] = sorted.map(table => {
          const ts = tableSettings.get(table);
          return ts ?? { table, mode: defaults.mode, strategy: defaults.strategy, chunk_size: defaults.chunk_size, workers: defaults.workers };
        });
        setBatches([{ id: 1, items }]);
      })
      .catch(() => {
        setDeps([]);
        const items: BatchItem[] = selectedArr.map(table => {
          const ts = tableSettings.get(table);
          return ts ?? { table, mode: defaults.mode, strategy: defaults.strategy, chunk_size: defaults.chunk_size, workers: defaults.workers };
        });
        setBatches([{ id: 1, items }]);
      })
      .finally(() => setDepsLoading(false));
  }, [selected, srcSchema, tableSettings, defaults]);

  // Execute plan
  const doExecute = useCallback(() => {
    setExecuting(true); setExecuteError(null);
    const group = groups.find(g => g.group_name === selectedGroup);
    const payload = {
      src_schema: srcSchema,
      tgt_schema: tgtSchema,
      group_id: group?.id ?? null,
      defaults: {
        chunk_size: defaults.chunk_size,
        max_parallel_workers: defaults.workers,
        migration_strategy: defaults.strategy,
        migration_mode: defaults.mode,
      },
      batches: batches.map(b => ({
        batch_order: b.id,
        tables: b.items.map(it => ({
          source_table: it.table,
          target_table: it.table,
          migration_mode: it.mode,
          migration_strategy: it.strategy,
          chunk_size: it.chunk_size,
          max_parallel_workers: it.workers,
        })),
      })),
    };
    fetch("/api/planner/execute", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    })
      .then(r => r.ok ? r.json() : r.json().then(d => Promise.reject(d.error || "Ошибка")))
      .then((data: { plan_id: string }) => setPlanId(data.plan_id))
      .catch(e => setExecuteError(typeof e === "string" ? e : String(e)))
      .finally(() => setExecuting(false));
  }, [srcSchema, tgtSchema, selectedGroup, groups, defaults, batches]);

  // Start first batch
  const doStart = useCallback(() => {
    if (!planId) return;
    setStarting(true); setStartError(null);
    fetch(`/api/planner/plans/${planId}/start`, { method: "POST" })
      .then(r => r.ok ? r.json() : r.json().then(d => Promise.reject(d.error || "Ошибка")))
      .then(() => { /* Success — could navigate away */ })
      .catch(e => setStartError(typeof e === "string" ? e : String(e)))
      .finally(() => setStarting(false));
  }, [planId]);

  // Navigation
  const canNext = (): boolean => {
    if (step === 0) return selected.size > 0;
    if (step === 1) return true;
    if (step === 2) return batches.length > 0 && batches.some(b => b.items.length > 0);
    return false;
  };

  const goNext = () => {
    if (step === 0) { initTableSettings(); setStep(1); }
    else if (step === 1) { initOrdering(); setStep(2); }
    else if (step === 2) { setStep(3); }
  };

  const goBack = () => { if (step > 0) setStep(step - 1); };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 0 }}>
      <StepIndicator current={step} />

      {/* Error banners */}
      {compareError && (
        <div style={{
          background: "#7f1d1d22", border: "1px solid #7f1d1d", borderRadius: 6,
          color: "#fca5a5", padding: "8px 14px", fontSize: 12, marginBottom: 12,
        }}>
          {compareError}
        </div>
      )}
      {executeError && (
        <div style={{
          background: "#7f1d1d22", border: "1px solid #7f1d1d", borderRadius: 6,
          color: "#fca5a5", padding: "8px 14px", fontSize: 12, marginBottom: 12,
        }}>
          {executeError}
        </div>
      )}
      {startError && (
        <div style={{
          background: "#7f1d1d22", border: "1px solid #7f1d1d", borderRadius: 6,
          color: "#fca5a5", padding: "8px 14px", fontSize: 12, marginBottom: 12,
        }}>
          {startError}
        </div>
      )}

      {/* Step content */}
      {step === 0 && (
        <SchemaCompareStep
          srcSchema={srcSchema} tgtSchema={tgtSchema}
          onSrcSchema={setSrcSchema} onTgtSchema={setTgtSchema}
          tables={tables} selected={selected}
          onToggle={toggleTable} onToggleAll={toggleAll}
          comparing={comparing} onCompare={doCompare}
          onSyncTable={doSyncTable} syncBusy={syncBusy}
        />
      )}

      {step === 1 && (
        <TableSelectionStep
          selected={Array.from(selected)}
          defaults={defaults} onDefaults={setDefaults}
          tableSettings={tableSettings} onTableSetting={updateTableSetting}
          groups={groups} selectedGroup={selectedGroup}
          onSelectGroup={setSelectedGroup}
        />
      )}

      {step === 2 && (
        <OrderingStep
          batches={batches} onBatches={setBatches}
          deps={deps} depsLoading={depsLoading}
        />
      )}

      {step === 3 && (
        <ReviewStep
          srcSchema={srcSchema} tgtSchema={tgtSchema}
          selectedGroup={selectedGroup}
          defaults={defaults} batches={batches}
          executing={executing} onExecute={doExecute}
          planId={planId} starting={starting} onStart={doStart}
        />
      )}

      {/* Navigation */}
      <div style={{
        display: "flex", justifyContent: "space-between", alignItems: "center",
        marginTop: 20, paddingTop: 16, borderTop: "1px solid #1e293b",
      }}>
        <button
          onClick={goBack}
          disabled={step === 0}
          style={{
            ...S.btnSecondary,
            opacity: step === 0 ? 0.3 : 1,
            cursor: step === 0 ? "not-allowed" : "pointer",
          }}
        >
          Назад
        </button>
        {step < 3 && (
          <button
            onClick={goNext}
            disabled={!canNext()}
            style={{
              ...S.btnPrimary,
              opacity: canNext() ? 1 : 0.5,
              cursor: canNext() ? "pointer" : "not-allowed",
            }}
          >
            Далее
          </button>
        )}
      </div>
    </div>
  );
}
