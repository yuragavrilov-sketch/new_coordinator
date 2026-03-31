import React, { useState, useCallback, useEffect } from "react";
import { S } from "./styles";
import { SearchSelect } from "../ui/SearchSelect";

// ── Types ─────────────────────────────────────────────────────────────────────

interface Column { name: string; type: string; nullable: boolean }
interface UkConstraint { name: string; columns: string[] }
interface TableInfo {
  columns: Column[];
  pk_columns: string[];
  uk_constraints: UkConstraint[];
}

interface TableKeyEntry {
  tableInfo: TableInfo | null;
  loadingInfo: boolean;
  infoError: string;
  effective_key_type: string;
  effective_key_columns: string[];
  selected_uk_index: number;
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

// ── topoSort helper ───────────────────────────────────────────────────────────

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

// ── Key config components ────────────────────────────────────────────────────

function Chip({ label, color, bg }: { label: string; color: string; bg: string }) {
  return (
    <span style={{
      background: bg, color, border: `1px solid ${color}44`,
      borderRadius: 4, fontSize: 10, fontWeight: 700, padding: "2px 8px",
      display: "inline-block",
    }}>{label}</span>
  );
}

function KeyTypeBtn({ label, active, disabled, onClick }: {
  label: string; active: boolean; disabled?: boolean; onClick: () => void;
}) {
  return (
    <button onClick={onClick} disabled={disabled} style={{
      padding: "4px 10px", fontSize: 10, fontWeight: 700, borderRadius: 5,
      border: `1px solid ${active ? "#3b82f6" : "#334155"}`,
      background: active ? "#1e3a5f" : "#1e293b",
      color: disabled ? "#334155" : active ? "#93c5fd" : "#64748b",
      cursor: disabled ? "not-allowed" : "pointer",
    }}>{label}</button>
  );
}

function TableKeyConfig({ entry, onChange }: {
  entry: TableKeyEntry;
  onChange: (upd: Partial<TableKeyEntry>) => void;
}) {
  const info = entry.tableInfo;

  function setKeyType(kt: string) {
    let cols: string[] = [];
    if (kt === "PRIMARY_KEY") cols = info?.pk_columns ?? [];
    if (kt === "UNIQUE_KEY") cols = info?.uk_constraints[entry.selected_uk_index]?.columns ?? [];
    onChange({ effective_key_type: kt, effective_key_columns: cols });
  }

  function toggleKeyCol(col: string, checked: boolean) {
    const cols = checked
      ? [...entry.effective_key_columns, col]
      : entry.effective_key_columns.filter(c => c !== col);
    onChange({ effective_key_columns: cols });
  }

  if (entry.loadingInfo) {
    return <div style={{ fontSize: 11, color: "#475569", padding: "4px 0" }}>Загрузка информации...</div>;
  }
  if (entry.infoError) {
    return <div style={{ fontSize: 10, color: "#fca5a5" }}>{entry.infoError}</div>;
  }
  if (!info) return null;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      {/* chips */}
      <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
        <Chip label={`${info.columns.length} кол.`} color="#94a3b8" bg="#1e293b" />
        {info.pk_columns.length > 0
          ? <Chip label={`PK: ${info.pk_columns.join(", ")}`} color="#86efac" bg="#052e16" />
          : <Chip label="Нет PK" color="#fca5a5" bg="#450a0a" />}
        {info.uk_constraints.length > 0 && (
          <Chip label={`UK: ${info.uk_constraints.length}`} color="#c4b5fd" bg="#2e1065" />
        )}
      </div>

      {/* key type selector */}
      <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
        <KeyTypeBtn label="PRIMARY KEY"
          active={entry.effective_key_type === "PRIMARY_KEY"}
          disabled={info.pk_columns.length === 0}
          onClick={() => setKeyType("PRIMARY_KEY")} />
        <KeyTypeBtn label="UNIQUE KEY"
          active={entry.effective_key_type === "UNIQUE_KEY"}
          disabled={info.uk_constraints.length === 0}
          onClick={() => setKeyType("UNIQUE_KEY")} />
        <KeyTypeBtn label="USER DEFINED"
          active={entry.effective_key_type === "USER_DEFINED"}
          onClick={() => setKeyType("USER_DEFINED")} />
        <KeyTypeBtn label="NONE"
          active={entry.effective_key_type === "NONE"}
          onClick={() => setKeyType("NONE")} />
      </div>

      {/* PK display */}
      {entry.effective_key_type === "PRIMARY_KEY" && (
        <div style={{ fontSize: 11, color: "#86efac" }}>
          Ключ: <strong>{info.pk_columns.join(", ")}</strong>
        </div>
      )}

      {/* UK selector */}
      {entry.effective_key_type === "UNIQUE_KEY" && (
        <>
          {info.uk_constraints.length > 1 && (
            <select style={{ ...S.select, fontSize: 11 }} value={entry.selected_uk_index}
              onChange={e => {
                const idx = parseInt(e.target.value);
                onChange({
                  selected_uk_index: idx,
                  effective_key_columns: info.uk_constraints[idx]?.columns ?? [],
                });
              }}>
              {info.uk_constraints.map((uk, i) => (
                <option key={uk.name} value={i}>{uk.name} ({uk.columns.join(", ")})</option>
              ))}
            </select>
          )}
          <div style={{ fontSize: 11, color: "#c4b5fd" }}>
            Ключ: <strong>{info.uk_constraints[entry.selected_uk_index]?.columns.join(", ")}</strong>
          </div>
        </>
      )}

      {/* User-defined column picker */}
      {entry.effective_key_type === "USER_DEFINED" && (
        <div>
          <div style={{
            maxHeight: 140, overflowY: "auto",
            border: "1px solid #334155", borderRadius: 5, background: "#1e293b",
          }}>
            {info.columns.map(col => (
              <label key={col.name} style={{
                display: "flex", alignItems: "center", gap: 8,
                padding: "4px 10px", cursor: "pointer",
                borderBottom: "1px solid #0f172a",
              }}>
                <input type="checkbox"
                  checked={entry.effective_key_columns.includes(col.name)}
                  onChange={e => toggleKeyCol(col.name, e.target.checked)}
                />
                <span style={{ fontSize: 11, color: "#e2e8f0", fontFamily: "monospace" }}>{col.name}</span>
                <span style={{ fontSize: 9, color: "#475569" }}>{col.type}</span>
                {!col.nullable && (
                  <span style={{ fontSize: 9, color: "#ef4444", marginLeft: "auto" }}>NOT NULL</span>
                )}
              </label>
            ))}
          </div>
          {entry.effective_key_columns.length > 0 && (
            <div style={{ fontSize: 10, color: "#475569" }}>Выбрано: {entry.effective_key_columns.join(", ")}</div>
          )}
          {entry.effective_key_columns.length === 0 && (
            <div style={{ fontSize: 10, color: "#fca5a5" }}>Выберите хотя бы один столбец</div>
          )}
        </div>
      )}
    </div>
  );
}

// ── StepIndicator ─────────────────────────────────────────────────────────────

const STEP_LABELS = ["Настройки таблиц", "Порядок загрузки", "Обзор и запуск"];

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

// ── TableSelectionStep (Step 0) ───────────────────────────────────────────────

function TableSelectionStep({
  selected, defaults, onDefaults,
  tableSettings, onTableSetting,
  groups, selectedGroup, onSelectGroup,
  tableKeyEntries, onTableKeyEntry,
}: {
  selected: string[];
  defaults: PlanDefaults;
  onDefaults: (d: PlanDefaults) => void;
  tableSettings: Map<string, BatchItem>;
  onTableSetting: (table: string, upd: Partial<BatchItem>) => void;
  groups: ConnectorGroup[];
  selectedGroup: string;
  onSelectGroup: (v: string) => void;
  tableKeyEntries: Map<string, TableKeyEntry>;
  onTableKeyEntry: (table: string, upd: Partial<TableKeyEntry>) => void;
}) {
  const [customTables, setCustomTables] = useState<Set<string>>(new Set());
  const [expandedKey, setExpandedKey] = useState<string | null>(null);

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
              showClear={false}
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
                {["Таблица", "Ключ", "Режим", "Стратегия", "Chunk", "Workers", "Индивидуально"].map(h => (
                  <th key={h} style={S.th}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {selected.map(table => {
                const ts = tableSettings.get(table)!;
                const isCustom = customTables.has(table);
                const keyEntry = tableKeyEntries.get(table);
                const isExpanded = expandedKey === table;
                const keyLabel = keyEntry?.effective_key_type
                  ? keyEntry.effective_key_type.replace("_", " ")
                  : keyEntry?.loadingInfo ? "..." : "—";
                const keyColor = keyEntry?.effective_key_type === "PRIMARY_KEY" ? "#86efac"
                  : keyEntry?.effective_key_type === "UNIQUE_KEY" ? "#c4b5fd"
                  : keyEntry?.effective_key_type === "USER_DEFINED" ? "#fbbf24"
                  : keyEntry?.effective_key_type === "NONE" ? "#fca5a5"
                  : "#475569";
                return (
                  <React.Fragment key={table}>
                    <tr style={{ ...S.trBorder, background: isCustom ? "rgba(59,130,246,0.04)" : "transparent" }}>
                      <td style={S.td}>
                        <code style={{ color: "#e2e8f0", fontSize: 12 }}>{table}</code>
                      </td>
                      <td style={S.td}>
                        <button
                          onClick={() => setExpandedKey(isExpanded ? null : table)}
                          style={{
                            background: "none", border: `1px solid ${keyColor}55`,
                            borderRadius: 4, padding: "2px 8px", fontSize: 10,
                            fontWeight: 700, color: keyColor, cursor: "pointer",
                            whiteSpace: "nowrap",
                          }}
                          title="Настроить ключ"
                        >
                          {keyLabel} {isExpanded ? "\u25B2" : "\u25BC"}
                        </button>
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
                    {isExpanded && keyEntry && (
                      <tr>
                        <td colSpan={7} style={{
                          padding: "10px 16px", background: "#0a111f",
                          borderBottom: "1px solid #1e293b",
                        }}>
                          <TableKeyConfig
                            entry={keyEntry}
                            onChange={upd => onTableKeyEntry(table, upd)}
                          />
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
    </div>
  );
}

// ── OrderingStep (Step 1) ─────────────────────────────────────────────────────

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

// ── ReviewStep (Step 2) ───────────────────────────────────────────────────────

function ReviewStep({
  srcSchema, tgtSchema, selectedGroup,
  defaults, batches, executing, onExecute,
  planId, starting, onStart,
  tableKeyEntries,
}: {
  srcSchema: string; tgtSchema: string; selectedGroup: string;
  defaults: PlanDefaults;
  batches: Batch[];
  executing: boolean; onExecute: () => void;
  planId: string | null;
  starting: boolean; onStart: () => void;
  tableKeyEntries: Map<string, TableKeyEntry>;
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
                  {["#", "Таблица", "Ключ", "Режим", "Стратегия", "Chunk", "Workers"].map(h => (
                    <th key={h} style={S.th}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {batch.items.map((item, idx) => {
                  const ke = tableKeyEntries.get(item.table);
                  const keyLabel = ke?.effective_key_type
                    ? ke.effective_key_type.replace("_", " ")
                    : "—";
                  const keyCols = ke?.effective_key_columns?.length
                    ? ke.effective_key_columns.join(", ")
                    : "";
                  return (
                    <tr key={item.table} style={S.trBorder}>
                      <td style={{ ...S.td, color: "#475569" }}>{idx + 1}</td>
                      <td style={S.td}>
                        <code style={{ color: "#e2e8f0", fontSize: 12 }}>{item.table}</code>
                      </td>
                      <td style={S.td}>
                        <div style={{ fontSize: 10 }}>
                          <span style={{ color: "#93c5fd", fontWeight: 600 }}>{keyLabel}</span>
                          {keyCols && (
                            <div style={{ color: "#475569", fontSize: 9, marginTop: 1 }}>{keyCols}</div>
                          )}
                        </div>
                      </td>
                      <td style={S.td}><span style={{ fontSize: 11, color: "#94a3b8" }}>{item.mode}</span></td>
                      <td style={S.td}><span style={{ fontSize: 11, color: "#94a3b8" }}>{item.strategy}</span></td>
                      <td style={S.td}><span style={{ fontSize: 11, color: "#94a3b8" }}>{item.chunk_size.toLocaleString("ru-RU")}</span></td>
                      <td style={S.td}><span style={{ fontSize: 11, color: "#94a3b8" }}>{item.workers}</span></td>
                    </tr>
                  );
                })}
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

// ── Props ─────────────────────────────────────────────────────────────────────

interface Props {
  selectedTables: string[];
  srcSchema: string;
  tgtSchema: string;
  onClose: () => void;
}

// ── PlannerWizard (main export) ───────────────────────────────────────────────

export function PlannerWizard({ selectedTables, srcSchema, tgtSchema, onClose }: Props) {
  const [step, setStep] = useState(0);

  // Step 0 state
  const [defaults, setDefaults] = useState<PlanDefaults>({
    chunk_size: 50000, workers: 4, strategy: "STAGE", mode: "CDC",
  });
  const [tableSettings, setTableSettings] = useState<Map<string, BatchItem>>(() => {
    const map = new Map<string, BatchItem>();
    for (const table of selectedTables) {
      map.set(table, {
        table,
        mode: "CDC",
        strategy: "STAGE",
        chunk_size: 50000,
        workers: 4,
      });
    }
    return map;
  });
  const [groups, setGroups] = useState<ConnectorGroup[]>([]);
  const [selectedGroup, setSelectedGroup] = useState("");

  // Key config state
  const [tableKeyEntries, setTableKeyEntries] = useState<Map<string, TableKeyEntry>>(() => {
    const map = new Map<string, TableKeyEntry>();
    for (const table of selectedTables) {
      map.set(table, {
        tableInfo: null,
        loadingInfo: true,
        infoError: "",
        effective_key_type: "",
        effective_key_columns: [],
        selected_uk_index: 0,
      });
    }
    return map;
  });

  // Step 1 state
  const [batches, setBatches] = useState<Batch[]>([]);
  const [deps, setDeps] = useState<FKDep[]>([]);
  const [depsLoading, setDepsLoading] = useState(false);

  // Step 2 state
  const [executing, setExecuting] = useState(false);
  const [executeError, setExecuteError] = useState<string | null>(null);
  const [planId, setPlanId] = useState<string | null>(null);
  const [starting, setStarting] = useState(false);
  const [startError, setStartError] = useState<string | null>(null);

  // Load connector groups on mount
  useEffect(() => {
    fetch("/api/connector-groups")
      .then(r => r.ok ? r.json() : Promise.reject())
      .then((data: ConnectorGroup[]) => setGroups(data))
      .catch(() => {});
  }, []);

  // Load table info (columns, PK, UK) for all selected tables on mount
  useEffect(() => {
    for (const table of selectedTables) {
      const p = `schema=${encodeURIComponent(srcSchema)}&table=${encodeURIComponent(table)}`;
      fetch(`/api/db/source/table-info?${p}`)
        .then(r => r.json())
        .then((d: TableInfo & { error?: string }) => {
          setTableKeyEntries(prev => {
            const next = new Map(prev);
            const cur = next.get(table);
            if (!cur) return next;
            if (d.error) {
              next.set(table, { ...cur, loadingInfo: false, infoError: d.error });
            } else {
              let keyType = "USER_DEFINED";
              let keyCols: string[] = [];
              if (d.pk_columns.length > 0) {
                keyType = "PRIMARY_KEY"; keyCols = d.pk_columns;
              } else if (d.uk_constraints.length > 0) {
                keyType = "UNIQUE_KEY"; keyCols = d.uk_constraints[0].columns;
              }
              next.set(table, {
                ...cur,
                tableInfo: d,
                loadingInfo: false,
                effective_key_type: keyType,
                effective_key_columns: keyCols,
              });
            }
            return next;
          });
        })
        .catch(e => {
          setTableKeyEntries(prev => {
            const next = new Map(prev);
            const cur = next.get(table);
            if (cur) next.set(table, { ...cur, loadingInfo: false, infoError: String(e) });
            return next;
          });
        });
    }
  }, [selectedTables, srcSchema]);

  const updateTableKeyEntry = (table: string, upd: Partial<TableKeyEntry>) => {
    setTableKeyEntries(prev => {
      const next = new Map(prev);
      const cur = next.get(table);
      if (cur) next.set(table, { ...cur, ...upd });
      return next;
    });
  };

  const updateTableSetting = (table: string, upd: Partial<BatchItem>) => {
    setTableSettings(prev => {
      const next = new Map(prev);
      const cur = next.get(table);
      if (cur) next.set(table, { ...cur, ...upd });
      return next;
    });
  };

  // Load FK deps and build initial batches when moving to step 1
  const initOrdering = useCallback(() => {
    if (selectedTables.length === 0) return;

    setDepsLoading(true);
    const qs = new URLSearchParams({ schema: srcSchema, tables: selectedTables.join(",") });
    fetch(`/api/planner/fk-dependencies?${qs}`)
      .then(r => r.ok ? r.json() : Promise.reject())
      .then((data: FKDep[]) => {
        setDeps(data);
        const sorted = topoSort(selectedTables, data);
        const items: BatchItem[] = sorted.map(table => {
          const ts = tableSettings.get(table);
          return ts ?? { table, mode: defaults.mode, strategy: defaults.strategy, chunk_size: defaults.chunk_size, workers: defaults.workers };
        });
        setBatches([{ id: 1, items }]);
      })
      .catch(() => {
        setDeps([]);
        const items: BatchItem[] = selectedTables.map(table => {
          const ts = tableSettings.get(table);
          return ts ?? { table, mode: defaults.mode, strategy: defaults.strategy, chunk_size: defaults.chunk_size, workers: defaults.workers };
        });
        setBatches([{ id: 1, items }]);
      })
      .finally(() => setDepsLoading(false));
  }, [selectedTables, srcSchema, tableSettings, defaults]);

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
        tables: b.items.map(it => {
          const keyEntry = tableKeyEntries.get(it.table);
          return {
            source_table: it.table,
            target_table: it.table,
            migration_mode: it.mode,
            migration_strategy: it.strategy,
            chunk_size: it.chunk_size,
            max_parallel_workers: it.workers,
            effective_key_type: keyEntry?.effective_key_type ?? "",
            effective_key_columns: keyEntry?.effective_key_columns ?? [],
          };
        }),
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
  }, [srcSchema, tgtSchema, selectedGroup, groups, defaults, batches, tableKeyEntries]);

  // Start first batch
  const doStart = useCallback(() => {
    if (!planId) return;
    setStarting(true); setStartError(null);
    fetch(`/api/planner/plans/${planId}/start`, { method: "POST" })
      .then(r => r.ok ? r.json() : r.json().then(d => Promise.reject(d.error || "Ошибка")))
      .then(() => { onClose(); })
      .catch(e => setStartError(typeof e === "string" ? e : String(e)))
      .finally(() => setStarting(false));
  }, [planId, onClose]);

  // Navigation
  const canNext = (): boolean => {
    if (step === 0) return true;
    if (step === 1) return batches.length > 0 && batches.some(b => b.items.length > 0);
    return false;
  };

  const goNext = () => {
    if (step === 0) { initOrdering(); setStep(1); }
    else if (step === 1) { setStep(2); }
  };

  const goBack = () => { if (step > 0) setStep(step - 1); };

  return (
    <div style={{
      background: "#0f172a",
      border: "1px solid #3b82f6",
      borderRadius: 10,
      overflow: "hidden",
      display: "flex",
      flexDirection: "column",
    }}>
      {/* Header */}
      <div style={{
        padding: "12px 16px",
        background: "#0a111f",
        borderBottom: "1px solid #1e293b",
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
      }}>
        <span style={{ fontSize: 14, fontWeight: 700, color: "#e2e8f0" }}>
          Настройка миграции
          <span style={{ fontSize: 12, fontWeight: 400, color: "#64748b", marginLeft: 8 }}>
            {selectedTables.length} таблиц
          </span>
        </span>
        <button
          onClick={onClose}
          style={{
            background: "none", border: "none", color: "#475569",
            fontSize: 16, cursor: "pointer", padding: "2px 6px", lineHeight: 1,
          }}
          title="Закрыть"
        >
          ✕
        </button>
      </div>

      {/* Body */}
      <div style={{ padding: 20, display: "flex", flexDirection: "column", gap: 0 }}>
        <StepIndicator current={step} />

        {/* Error banners */}
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
          <TableSelectionStep
            selected={selectedTables}
            defaults={defaults} onDefaults={setDefaults}
            tableSettings={tableSettings} onTableSetting={updateTableSetting}
            groups={groups} selectedGroup={selectedGroup}
            onSelectGroup={setSelectedGroup}
            tableKeyEntries={tableKeyEntries}
            onTableKeyEntry={updateTableKeyEntry}
          />
        )}

        {step === 1 && (
          <OrderingStep
            batches={batches} onBatches={setBatches}
            deps={deps} depsLoading={depsLoading}
          />
        )}

        {step === 2 && (
          <ReviewStep
            srcSchema={srcSchema} tgtSchema={tgtSchema}
            selectedGroup={selectedGroup}
            defaults={defaults} batches={batches}
            executing={executing} onExecute={doExecute}
            planId={planId} starting={starting} onStart={doStart}
            tableKeyEntries={tableKeyEntries}
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
          {step < 2 && (
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
    </div>
  );
}
