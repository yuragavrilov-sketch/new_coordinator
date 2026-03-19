import React, { useState, useEffect, useCallback, useRef } from "react";
import ReactDOM from "react-dom";

// ── Types ────────────────────────────────────────────────────────────────────

type Decision = "migrate" | "skip" | "archive";
type Status   = "done" | "pending";

interface TableEntry {
  schema: string;
  table: string;
  decision: Decision;
  status: Status;
}

interface ChecklistData {
  name: string;
  tables: TableEntry[];
}

const LS_KEY = "mig_checklists";

const DECISION_LABELS: Record<Decision, string> = {
  migrate: "Переносить",
  skip:    "Нет",
  archive: "Архивные данные",
};

const DECISION_COLORS: Record<Decision, { bg: string; text: string }> = {
  migrate: { bg: "#052e16", text: "#86efac" },
  skip:    { bg: "#1e293b", text: "#64748b" },
  archive: { bg: "#3b2000", text: "#fcd34d" },
};

const STATUS_LABELS: Record<Status, string> = {
  done:    "Перенесено",
  pending: "Не перенесено",
};

// ── Persistence ──────────────────────────────────────────────────────────────

function loadAll(): ChecklistData[] {
  try { return JSON.parse(localStorage.getItem(LS_KEY) || "[]"); }
  catch { return []; }
}

function saveAll(data: ChecklistData[]) {
  localStorage.setItem(LS_KEY, JSON.stringify(data));
}

// ── SearchableSelect ─────────────────────────────────────────────────────────

function SearchableSelect({ items, value, onChange, placeholder, loading }: {
  items: string[];
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  loading?: boolean;
}) {
  const [open, setOpen]     = useState(false);
  const [filter, setFilter] = useState("");
  const [pos, setPos]       = useState({ top: 0, left: 0, width: 0 });
  const triggerRef = useRef<HTMLDivElement>(null);
  const dropRef    = useRef<HTMLDivElement>(null);
  const inputRef   = useRef<HTMLInputElement>(null);

  const filtered = filter
    ? items.filter(i => i.toLowerCase().includes(filter.toLowerCase()))
    : items;

  useEffect(() => {
    function onClickOutside(e: MouseEvent) {
      const t = e.target as Node;
      if (triggerRef.current?.contains(t) || dropRef.current?.contains(t)) return;
      setOpen(false);
    }
    document.addEventListener("mousedown", onClickOutside);
    return () => document.removeEventListener("mousedown", onClickOutside);
  }, []);

  useEffect(() => {
    if (open && triggerRef.current) {
      const r = triggerRef.current.getBoundingClientRect();
      setPos({ top: r.bottom + 2, left: r.left, width: Math.max(r.width, 200) });
      setTimeout(() => inputRef.current?.focus(), 0);
    }
  }, [open]);

  function pick(v: string) { onChange(v); setFilter(""); setOpen(false); }

  return (
    <>
      <div
        ref={triggerRef}
        onClick={() => setOpen(!open)}
        style={{
          background: "#0f172a", border: "1px solid #334155", borderRadius: 4,
          color: "#e2e8f0", padding: "4px 8px", fontSize: 12,
          cursor: "pointer", display: "flex", alignItems: "center", gap: 4,
          minHeight: 28, width: "100%",
        }}
      >
        <span style={{ flex: 1, color: value ? "#e2e8f0" : "#475569", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
          {loading ? "Загрузка…" : value || placeholder || "Выбрать…"}
        </span>
        <span style={{ color: "#475569", fontSize: 9 }}>{open ? "\u25B2" : "\u25BC"}</span>
      </div>
      {open && ReactDOM.createPortal(
        <div ref={dropRef} style={{
          position: "fixed", top: pos.top, left: pos.left, width: pos.width,
          zIndex: 9999, background: "#0f172a", border: "1px solid #334155",
          borderRadius: 5, maxHeight: 260, display: "flex", flexDirection: "column",
          boxShadow: "0 8px 24px rgba(0,0,0,.6)",
        }}>
          <input
            ref={inputRef}
            value={filter}
            onChange={e => setFilter(e.target.value)}
            placeholder="Поиск…"
            onKeyDown={e => {
              if (e.key === "Escape") setOpen(false);
              if (e.key === "Enter" && filtered.length === 1) pick(filtered[0]);
            }}
            style={{
              background: "#0f172a", color: "#e2e8f0", border: "none",
              borderBottom: "1px solid #1e293b", padding: "7px 10px", fontSize: 12,
              outline: "none",
            }}
          />
          <div style={{ overflowY: "auto", maxHeight: 220 }}>
            {filtered.length === 0 && (
              <div style={{ padding: "8px 10px", color: "#334155", fontSize: 12 }}>
                {loading ? "Загрузка…" : filter ? "Ничего не найдено" : "Нет данных"}
              </div>
            )}
            {filtered.map(t => (
              <div key={t} onClick={() => pick(t)} style={{
                padding: "5px 10px", fontSize: 12, cursor: "pointer",
                color: t === value ? "#93c5fd" : "#e2e8f0",
                background: t === value ? "#1e3a5f" : "transparent",
              }}
                onMouseEnter={e => (e.currentTarget.style.background = "#1e293b")}
                onMouseLeave={e => (e.currentTarget.style.background = t === value ? "#1e3a5f" : "transparent")}
              >
                {t}
              </div>
            ))}
          </div>
          <div style={{
            padding: "3px 10px", fontSize: 10, color: "#475569",
            borderTop: "1px solid #1e293b", textAlign: "right",
          }}>
            {filtered.length} / {items.length}
          </div>
        </div>,
        document.body,
      )}
    </>
  );
}

// ── Component ────────────────────────────────────────────────────────────────

export function Checklist() {
  const [lists, setLists]           = useState<ChecklistData[]>(loadAll);
  const [activeIdx, setActiveIdx]   = useState(0);
  const [newListName, setNewListName] = useState("");

  // Source DB data
  const [schemas, setSchemas]       = useState<string[]>([]);
  const [schemasLoading, setSchemasLoading] = useState(false);
  const [srcTables, setSrcTables]   = useState<string[]>([]);
  const [tablesLoading, setTablesLoading] = useState(false);
  const [newSchema, setNewSchema]   = useState("");
  const [newTable, setNewTable]     = useState("");
  const [loadAllBusy, setLoadAllBusy] = useState(false);
  const [loadAllError, setLoadAllError] = useState("");

  useEffect(() => { saveAll(lists); }, [lists]);

  const active = lists[activeIdx] as ChecklistData | undefined;

  // Load schemas on mount
  useEffect(() => {
    setSchemasLoading(true);
    fetch("/api/db/source/schemas")
      .then(r => r.ok ? r.json() : Promise.reject())
      .then(setSchemas)
      .catch(() => {})
      .finally(() => setSchemasLoading(false));
  }, []);

  // Load tables when schema changes
  useEffect(() => {
    setSrcTables([]);
    setNewTable("");
    if (!newSchema) return;
    setTablesLoading(true);
    fetch(`/api/db/source/tables?schema=${encodeURIComponent(newSchema)}`)
      .then(r => r.ok ? r.json() : Promise.reject())
      .then(setSrcTables)
      .catch(() => {})
      .finally(() => setTablesLoading(false));
  }, [newSchema]);

  const addList = useCallback(() => {
    const name = newListName.trim();
    if (!name) return;
    setLists(prev => [...prev, { name, tables: [] }]);
    setActiveIdx(lists.length);
    setNewListName("");
  }, [newListName, lists.length]);

  const deleteList = useCallback(() => {
    if (!active || !confirm(`Удалить «${active.name}»?`)) return;
    setLists(prev => prev.filter((_, i) => i !== activeIdx));
    setActiveIdx(0);
  }, [active, activeIdx]);

  const addRow = useCallback(() => {
    if (!newSchema || !newTable) return;
    setLists(prev => prev.map((l, i) =>
      i === activeIdx
        ? { ...l, tables: [...l.tables, { schema: newSchema, table: newTable, decision: "migrate", status: "pending" }] }
        : l
    ));
    setNewTable("");
  }, [newSchema, newTable, activeIdx]);

  const addRows = useCallback((rows: { schema: string; table: string }[]) => {
    setLists(prev => prev.map((l, i) => {
      if (i !== activeIdx) return l;
      const existing = new Set(l.tables.map(t => `${t.schema}.${t.table}`));
      const newEntries = rows
        .filter(r => !existing.has(`${r.schema}.${r.table}`))
        .map(r => ({ schema: r.schema, table: r.table, decision: "migrate" as Decision, status: "pending" as Status }));
      return { ...l, tables: [...l.tables, ...newEntries] };
    }));
  }, [activeIdx]);

  const loadAllFromSource = useCallback(async () => {
    if (!newSchema) { setLoadAllError("Сначала выберите схему"); return; }
    setLoadAllBusy(true);
    setLoadAllError("");
    try {
      const res = await fetch(`/api/db/source/tables?schema=${encodeURIComponent(newSchema)}`);
      if (!res.ok) throw new Error("Не удалось загрузить таблицы");
      const tables: string[] = await res.json();
      addRows(tables.map(t => ({ schema: newSchema, table: t })));
    } catch (e: unknown) {
      setLoadAllError(e instanceof Error ? e.message : "Ошибка загрузки");
    } finally {
      setLoadAllBusy(false);
    }
  }, [addRows, newSchema]);

  const updateRow = useCallback((rowIdx: number, patch: Partial<TableEntry>) => {
    setLists(prev => prev.map((l, i) =>
      i === activeIdx
        ? { ...l, tables: l.tables.map((r, j) => j === rowIdx ? { ...r, ...patch } : r) }
        : l
    ));
  }, [activeIdx]);

  const deleteRow = useCallback((rowIdx: number) => {
    setLists(prev => prev.map((l, i) =>
      i === activeIdx
        ? { ...l, tables: l.tables.filter((_, j) => j !== rowIdx) }
        : l
    ));
  }, [activeIdx]);

  const tables = active?.tables || [];
  const toMigrate = tables.filter(t => t.decision !== "skip");
  const doneCount = toMigrate.filter(t => t.status === "done").length;

  return (
    <div style={{ maxWidth: 1100, margin: "0 auto" }}>
      {/* List selector */}
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 16, flexWrap: "wrap" }}>
        <select
          value={activeIdx}
          onChange={e => setActiveIdx(Number(e.target.value))}
          style={selectStyle}
        >
          {lists.length === 0 && <option value={0}>-- создайте список --</option>}
          {lists.map((l, i) => <option key={i} value={i}>{l.name}</option>)}
        </select>
        <input
          placeholder="Новый список..."
          value={newListName}
          onChange={e => setNewListName(e.target.value)}
          onKeyDown={e => e.key === "Enter" && addList()}
          style={inputStyle}
        />
        <button onClick={addList} style={btnStyle("#1d4ed8")}>Создать</button>
        {active && <button onClick={deleteList} style={btnStyle("#7f1d1d")}>Удалить</button>}
      </div>

      {!active && (
        <div style={{ textAlign: "center", padding: 48, color: "#475569", fontSize: 14 }}>
          Создайте список таблиц для миграции.
        </div>
      )}

      {active && (
        <>
          {/* Stats + load all */}
          <div style={{ display: "flex", gap: 16, marginBottom: 12, fontSize: 12, color: "#64748b", alignItems: "center", flexWrap: "wrap" }}>
            <span>Всего: <b style={{ color: "#e2e8f0" }}>{tables.length}</b></span>
            {toMigrate.length > 0 && (
              <>
                <span>К переносу: <b style={{ color: "#86efac" }}>{toMigrate.length}</b></span>
                <span>Перенесено: <b style={{ color: doneCount === toMigrate.length && doneCount > 0 ? "#22c55e" : "#93c5fd" }}>{doneCount}/{toMigrate.length}</b></span>
              </>
            )}
            <div style={{ marginLeft: "auto" }}>
              <button
                onClick={loadAllFromSource}
                disabled={loadAllBusy}
                style={{
                  ...btnStyle("#1e293b"),
                  opacity: loadAllBusy ? 0.5 : 1,
                }}
              >
                {loadAllBusy ? "Загрузка…" : "Загрузить все таблицы схемы"}
              </button>
            </div>
            {loadAllError && <span style={{ color: "#fca5a5", fontSize: 11 }}>{loadAllError}</span>}
          </div>

          {/* Table */}
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
            <thead>
              <tr style={{ borderBottom: "2px solid #1e293b" }}>
                <th style={thStyle}>Схема</th>
                <th style={thStyle}>Таблица</th>
                <th style={{ ...thStyle, width: 160 }}>Решение</th>
                <th style={{ ...thStyle, width: 160 }}>Статус</th>
                <th style={{ ...thStyle, width: 40 }}></th>
              </tr>
            </thead>
            <tbody>
              {tables.map((row, idx) => {
                const dc = DECISION_COLORS[row.decision];
                return (
                  <tr key={idx} style={{ borderBottom: "1px solid #1e293b" }}>
                    <td style={tdStyle}>{row.schema}</td>
                    <td style={{ ...tdStyle, fontWeight: 600 }}>{row.table}</td>
                    <td style={tdStyle}>
                      <select
                        value={row.decision}
                        onChange={e => updateRow(idx, { decision: e.target.value as Decision })}
                        style={{
                          ...cellSelectStyle,
                          background: dc.bg,
                          color: dc.text,
                          borderColor: dc.text + "44",
                        }}
                      >
                        {(Object.keys(DECISION_LABELS) as Decision[]).map(d => (
                          <option key={d} value={d}>{DECISION_LABELS[d]}</option>
                        ))}
                      </select>
                    </td>
                    <td style={tdStyle}>
                      {row.decision !== "skip" ? (
                        <select
                          value={row.status}
                          onChange={e => updateRow(idx, { status: e.target.value as Status })}
                          style={{
                            ...cellSelectStyle,
                            background: row.status === "done" ? "#052e16" : "#1e293b",
                            color: row.status === "done" ? "#86efac" : "#94a3b8",
                            borderColor: row.status === "done" ? "#16a34a44" : "#33415544",
                          }}
                        >
                          {(Object.keys(STATUS_LABELS) as Status[]).map(s => (
                            <option key={s} value={s}>{STATUS_LABELS[s]}</option>
                          ))}
                        </select>
                      ) : (
                        <span style={{ color: "#334155", fontSize: 12 }}>—</span>
                      )}
                    </td>
                    <td style={{ ...tdStyle, textAlign: "center" }}>
                      <button
                        onClick={() => deleteRow(idx)}
                        style={{ background: "none", border: "none", color: "#475569", cursor: "pointer", fontSize: 14 }}
                        title="Удалить"
                      >
                        ×
                      </button>
                    </td>
                  </tr>
                );
              })}

              {/* Add row */}
              <tr>
                <td style={tdStyle}>
                  <SearchableSelect
                    items={schemas}
                    value={newSchema}
                    onChange={setNewSchema}
                    placeholder="Схема…"
                    loading={schemasLoading}
                  />
                </td>
                <td style={tdStyle}>
                  <SearchableSelect
                    items={srcTables}
                    value={newTable}
                    onChange={setNewTable}
                    placeholder="Таблица…"
                    loading={tablesLoading}
                  />
                </td>
                <td style={tdStyle} colSpan={2}>
                  <button onClick={addRow} disabled={!newSchema || !newTable} style={{
                    ...btnStyle("#1e293b"),
                    opacity: (!newSchema || !newTable) ? 0.4 : 1,
                  }}>+ Добавить</button>
                </td>
                <td />
              </tr>
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}

// ── Styles ───────────────────────────────────────────────────────────────────

const thStyle: React.CSSProperties = {
  padding: "8px 10px", fontSize: 11, fontWeight: 600, color: "#475569",
  textTransform: "uppercase", letterSpacing: 0.5, textAlign: "left",
};

const tdStyle: React.CSSProperties = { padding: "8px 10px" };

const selectStyle: React.CSSProperties = {
  background: "#1e293b", border: "1px solid #334155", borderRadius: 6,
  color: "#e2e8f0", padding: "5px 10px", fontSize: 12, minWidth: 180,
};

const inputStyle: React.CSSProperties = {
  background: "#0f172a", border: "1px solid #334155", borderRadius: 6,
  color: "#e2e8f0", padding: "5px 10px", fontSize: 12, width: 200,
};

const cellSelectStyle: React.CSSProperties = {
  border: "1px solid", borderRadius: 4, padding: "3px 8px",
  fontSize: 12, cursor: "pointer", width: "100%",
};

function btnStyle(bg: string): React.CSSProperties {
  return {
    background: bg, border: "1px solid #334155", borderRadius: 6,
    color: "#e2e8f0", padding: "5px 12px", fontSize: 12, cursor: "pointer", fontWeight: 500,
  };
}
