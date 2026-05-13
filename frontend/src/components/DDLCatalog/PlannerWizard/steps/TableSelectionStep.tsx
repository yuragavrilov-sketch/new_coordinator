import React, { useState } from "react";
import { S } from "../../styles";
import { SearchSelect } from "../../../TargetPrep/SearchSelect";
import { TableKeyConfig } from "../TableKeyConfig";
import type { BatchItem, ConnectorGroup, PlanDefaults, TableKeyEntry } from "../types";

interface Props {
  selected:        string[];
  defaults:        PlanDefaults;
  onDefaults:      (d: PlanDefaults) => void;
  tableSettings:   Map<string, BatchItem>;
  onTableSetting:  (table: string, upd: Partial<BatchItem>) => void;
  groups:          ConnectorGroup[];
  selectedGroup:   string;
  onSelectGroup:   (v: string) => void;
  tableKeyEntries: Map<string, TableKeyEntry>;
  onTableKeyEntry: (table: string, upd: Partial<TableKeyEntry>) => void;
}

export function TableSelectionStep({
  selected, defaults, onDefaults,
  tableSettings, onTableSetting,
  groups, selectedGroup, onSelectGroup,
  tableKeyEntries, onTableKeyEntry,
}: Props) {
  const [customTables, setCustomTables] = useState<Set<string>>(new Set());
  const [expandedKey,  setExpandedKey]  = useState<string | null>(null);

  const toggleCustom = (table: string) => {
    setCustomTables(prev => {
      const next = new Set(prev);
      if (next.has(table)) {
        next.delete(table);
        onTableSetting(table, {
          mode:       defaults.mode,
          strategy:   defaults.strategy,
          chunk_size: defaults.chunk_size,
          workers:    defaults.workers,
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
          <span style={{ fontSize: 13, fontWeight: 600, color: "#e2e8f0" }}>
            Настройки таблиц ({selected.length})
          </span>
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
                const ts        = tableSettings.get(table)!;
                const isCustom  = customTables.has(table);
                const keyEntry  = tableKeyEntries.get(table);
                const isExpanded = expandedKey === table;
                const keyLabel  = keyEntry?.effective_key_type
                  ? keyEntry.effective_key_type.replace("_", " ")
                  : keyEntry?.loadingInfo ? "..." : "—";
                const keyColor = keyEntry?.effective_key_type === "PRIMARY_KEY"  ? "#86efac"
                  : keyEntry?.effective_key_type === "UNIQUE_KEY"   ? "#c4b5fd"
                  : keyEntry?.effective_key_type === "USER_DEFINED" ? "#fbbf24"
                  : keyEntry?.effective_key_type === "NONE"         ? "#fca5a5"
                  :                                                   "#475569";
                return (
                  <React.Fragment key={table}>
                    <tr style={{
                      ...S.trBorder,
                      background: isCustom ? "rgba(59,130,246,0.04)" : "transparent",
                    }}>
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
                          {keyLabel} {isExpanded ? "▲" : "▼"}
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
