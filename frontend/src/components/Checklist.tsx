import React, { useState, useEffect, useCallback, useRef } from "react";
import ReactDOM from "react-dom";

// ── Types ────────────────────────────────────────────────────────────────────

type Decision = "migrate" | "skip" | "archive";
type Status   = "done" | "pending";

interface TableItem {
  item_id: number;
  schema: string;
  table: string;
  decision: Decision;
  status: Status;
}

interface ChecklistList {
  list_id: number;
  name: string;
  tables: TableItem[];
}

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

// ── API helpers ──────────────────────────────────────────────────────────────

async function api<T>(url: string, opts?: RequestInit): Promise<T> {
  const res = await fetch(url, { headers: { "Content-Type": "application/json" }, ...opts });
  if (res.status === 204) return undefined as T;
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error || `HTTP ${res.status}`);
  }
  return res.json();
}

// ── Component ────────────────────────────────────────────────────────────────

export function Checklist() {
  const [lists, setLists]           = useState<ChecklistList[]>([]);
  const [activeId, setActiveId]     = useState<number | null>(null);
  const [newListName, setNewListName] = useState("");
  const [loading, setLoading]       = useState(true);

  // Source DB data
  const [schemas, setSchemas]       = useState<string[]>([]);
  const [schemasLoading, setSchemasLoading] = useState(false);
  const [srcTables, setSrcTables]   = useState<string[]>([]);
  const [tablesLoading, setTablesLoading] = useState(false);
  const [newSchema, setNewSchema]   = useState("");
  const [newTable, setNewTable]     = useState("");
  const [loadAllBusy, setLoadAllBusy] = useState(false);
  const [loadAllError, setLoadAllError] = useState("");

  const active = lists.find(l => l.list_id === activeId) ?? null;

  // Load lists from DB
  const fetchLists = useCallback(async () => {
    try {
      const data = await api<ChecklistList[]>("/api/checklists");
      setLists(data);
      if (data.length > 0 && !data.find(l => l.list_id === activeId)) {
        setActiveId(data[0].list_id);
      }
    } catch { /* ignore */ }
    finally { setLoading(false); }
  }, [activeId]);

  useEffect(() => { fetchLists(); }, []);

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

  // ── List CRUD ──────────────────────────────────────────────────────────────

  const addList = useCallback(async () => {
    const name = newListName.trim();
    if (!name) return;
    try {
      const res = await api<ChecklistList>("/api/checklists", {
        method: "POST", body: JSON.stringify({ name }),
      });
      setLists(prev => [...prev, { ...res, tables: res.tables || [] }]);
      setActiveId(res.list_id);
      setNewListName("");
    } catch { /* ignore */ }
  }, [newListName]);

  const deleteList = useCallback(async () => {
    if (!active || !confirm(`Удалить «${active.name}»?`)) return;
    await api(`/api/checklists/${active.list_id}`, { method: "DELETE" });
    setLists(prev => prev.filter(l => l.list_id !== active.list_id));
    setActiveId(lists.find(l => l.list_id !== active.list_id)?.list_id ?? null);
  }, [active, lists]);

  // ── Item CRUD ──────────────────────────────────────────────────────────────

  const addRow = useCallback(async () => {
    if (!active || !newSchema || !newTable) return;
    try {
      const res = await api<{ added: TableItem[] }>(`/api/checklists/${active.list_id}/items`, {
        method: "POST",
        body: JSON.stringify({ items: [{ schema: newSchema, table: newTable }] }),
      });
      if (res.added.length > 0) {
        setLists(prev => prev.map(l =>
          l.list_id === active.list_id
            ? { ...l, tables: [...l.tables, ...res.added] }
            : l
        ));
      }
      setNewTable("");
    } catch { /* ignore */ }
  }, [active, newSchema, newTable]);

  const loadAllFromSchema = useCallback(async () => {
    if (!active || !newSchema) { setLoadAllError("Сначала выберите схему"); return; }
    setLoadAllBusy(true);
    setLoadAllError("");
    try {
      const res = await fetch(`/api/db/source/tables?schema=${encodeURIComponent(newSchema)}`);
      if (!res.ok) throw new Error("Не удалось загрузить таблицы");
      const tables: string[] = await res.json();
      const addRes = await api<{ added: TableItem[] }>(`/api/checklists/${active.list_id}/items`, {
        method: "POST",
        body: JSON.stringify({ items: tables.map(t => ({ schema: newSchema, table: t })) }),
      });
      if (addRes.added.length > 0) {
        setLists(prev => prev.map(l =>
          l.list_id === active.list_id
            ? { ...l, tables: [...l.tables, ...addRes.added] }
            : l
        ));
      }
    } catch (e: unknown) {
      setLoadAllError(e instanceof Error ? e.message : "Ошибка загрузки");
    } finally {
      setLoadAllBusy(false);
    }
  }, [active, newSchema]);

  const updateItem = useCallback(async (item: TableItem, patch: Partial<Pick<TableItem, "decision" | "status">>) => {
    if (!active) return;
    await api(`/api/checklists/${active.list_id}/items/${item.item_id}`, {
      method: "PATCH", body: JSON.stringify(patch),
    });
    setLists(prev => prev.map(l =>
      l.list_id === active.list_id
        ? { ...l, tables: l.tables.map(t => t.item_id === item.item_id ? { ...t, ...patch } : t) }
        : l
    ));
  }, [active]);

  const deleteItem = useCallback(async (item: TableItem) => {
    if (!active) return;
    await api(`/api/checklists/${active.list_id}/items/${item.item_id}`, { method: "DELETE" });
    setLists(prev => prev.map(l =>
      l.list_id === active.list_id
        ? { ...l, tables: l.tables.filter(t => t.item_id !== item.item_id) }
        : l
    ));
  }, [active]);

  const tables = active?.tables || [];
  const toMigrate = tables.filter(t => t.decision !== "skip");
  const doneCount = toMigrate.filter(t => t.status === "done").length;

  if (loading) return <div style={{ color: "#475569", padding: 24, textAlign: "center" }}>Загрузка…</div>;

  return (
    <div style={{ maxWidth: 1100, margin: "0 auto" }}>
      {/* List selector */}
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 16, flexWrap: "wrap" }}>
        <select
          value={activeId ?? ""}
          onChange={e => setActiveId(Number(e.target.value) || null)}
          style={selectStyle}
        >
          {lists.length === 0 && <option value="">-- создайте список --</option>}
          {lists.map(l => <option key={l.list_id} value={l.list_id}>{l.name}</option>)}
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
                onClick={loadAllFromSchema}
                disabled={loadAllBusy}
                style={{ ...btnStyle("#1e293b"), opacity: loadAllBusy ? 0.5 : 1 }}
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
              {tables.map(row => {
                const dc = DECISION_COLORS[row.decision];
                return (
                  <tr key={row.item_id} style={{ borderBottom: "1px solid #1e293b" }}>
                    <td style={tdStyle}>{row.schema}</td>
                    <td style={{ ...tdStyle, fontWeight: 600 }}>{row.table}</td>
                    <td style={tdStyle}>
                      <select
                        value={row.decision}
                        onChange={e => updateItem(row, { decision: e.target.value as Decision })}
                        style={{
                          ...cellSelectStyle,
                          background: dc.bg, color: dc.text,
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
                          onChange={e => updateItem(row, { status: e.target.value as Status })}
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
                        onClick={() => deleteItem(row)}
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
