import React, { useState, useEffect, useCallback } from "react";

// ── Types ────────────────────────────────────────────────────────────────────

type Decision = "migrate" | "skip" | "archive";
type Status   = "done" | "pending";

interface TableEntry {
  schema: string;
  table: string;
  decision: Decision;
  status: Status;
  comment: string;
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

// ── Component ────────────────────────────────────────────────────────────────

export function Checklist() {
  const [lists, setLists]     = useState<ChecklistData[]>(loadAll);
  const [activeIdx, setActiveIdx] = useState(0);
  const [newListName, setNewListName] = useState("");
  const [newSchema, setNewSchema] = useState("");
  const [newTable, setNewTable]   = useState("");

  useEffect(() => { saveAll(lists); }, [lists]);

  const active = lists[activeIdx] as ChecklistData | undefined;

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
    const s = newSchema.trim();
    const t = newTable.trim();
    if (!t) return;
    setLists(prev => prev.map((l, i) =>
      i === activeIdx
        ? { ...l, tables: [...l.tables, { schema: s, table: t, decision: "migrate", status: "pending", comment: "" }] }
        : l
    ));
    setNewTable("");
  }, [newSchema, newTable, activeIdx]);

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
  const toMigrate = tables.filter(t => t.decision === "migrate");
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
          {/* Stats */}
          {toMigrate.length > 0 && (
            <div style={{
              display: "flex", gap: 16, marginBottom: 12, fontSize: 12, color: "#64748b",
            }}>
              <span>Всего: <b style={{ color: "#e2e8f0" }}>{tables.length}</b></span>
              <span>К переносу: <b style={{ color: "#86efac" }}>{toMigrate.length}</b></span>
              <span>Перенесено: <b style={{ color: doneCount === toMigrate.length && doneCount > 0 ? "#22c55e" : "#93c5fd" }}>{doneCount}/{toMigrate.length}</b></span>
            </div>
          )}

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
                      {row.decision === "migrate" ? (
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
                  <input
                    placeholder="SCHEMA"
                    value={newSchema}
                    onChange={e => setNewSchema(e.target.value.toUpperCase())}
                    onKeyDown={e => e.key === "Enter" && addRow()}
                    style={{ ...cellInputStyle, width: "100%" }}
                  />
                </td>
                <td style={tdStyle}>
                  <input
                    placeholder="TABLE_NAME"
                    value={newTable}
                    onChange={e => setNewTable(e.target.value.toUpperCase())}
                    onKeyDown={e => e.key === "Enter" && addRow()}
                    style={{ ...cellInputStyle, width: "100%" }}
                  />
                </td>
                <td style={tdStyle} colSpan={2}>
                  <button onClick={addRow} style={btnStyle("#1e293b")}>+ Добавить</button>
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

const cellInputStyle: React.CSSProperties = {
  background: "#0f172a", border: "1px solid #334155", borderRadius: 4,
  color: "#e2e8f0", padding: "4px 8px", fontSize: 12,
};

function btnStyle(bg: string): React.CSSProperties {
  return {
    background: bg, border: "1px solid #334155", borderRadius: 6,
    color: "#e2e8f0", padding: "5px 12px", fontSize: 12, cursor: "pointer", fontWeight: 500,
  };
}
