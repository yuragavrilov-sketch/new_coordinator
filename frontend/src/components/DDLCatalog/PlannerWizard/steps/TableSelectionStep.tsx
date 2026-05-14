import React, { useState } from "react";
import { S } from "../../styles";
import { SearchSelect } from "../../../TargetPrep/SearchSelect";
import { StrategyPicker } from "../../../StrategyPicker";
import { TableKeyConfig } from "../TableKeyConfig";
import type { BatchItem, ConnectorGroup, PlanDefaults, TableKeyEntry } from "../types";
import { t } from "../../../../theme";

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
          strategy:        defaults.strategy,
          truncate_target: defaults.truncate_target,
          chunk_size:      defaults.chunk_size,
          workers:         defaults.workers,
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
          <span style={{ fontSize: 13, fontWeight: 600, color: t.text.primary }}>Глобальные настройки</span>
        </div>
        <div style={S.cardBody}>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 2fr", gap: 10 }}>
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
            <StrategyPicker
              value={defaults.strategy}
              onChange={(s) => onDefaults({ ...defaults, strategy: s })}
              truncateTarget={defaults.truncate_target}
              onTruncateChange={(b) => onDefaults({ ...defaults, truncate_target: b })}
            />
          </div>
        </div>
      </div>

      {/* Connector group */}
      <div style={S.card}>
        <div style={S.cardHeader}>
          <span style={{ fontSize: 13, fontWeight: 600, color: t.text.primary }}>Группа коннекторов</span>
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
            <div style={{ fontSize: 11, color: t.text.muted }}>
              Группа: <span style={{ color: t.blue.fg }}>{selectedGroup}</span>
              {(() => {
                const g = groups.find(gg => gg.group_name === selectedGroup);
                return g ? <> | Статус: <span style={{ color: g.status === "RUNNING" ? t.green.base : t.amber.base }}>{g.status}</span></> : null;
              })()}
            </div>
          )}
        </div>
      </div>

      {/* Per-table settings */}
      <div style={S.card}>
        <div style={S.cardHeader}>
          <span style={{ fontSize: 13, fontWeight: 600, color: t.text.primary }}>
            Настройки таблиц ({selected.length})
          </span>
          <span style={{ fontSize: 11, color: t.text.disabled, marginLeft: "auto" }}>
            Индивидуальных: {customTables.size}
          </span>
        </div>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ borderBottom: `1px solid ${t.border.subtle}` }}>
                {["Таблица", "Ключ", "Стратегия", "Chunk", "Workers", "Индивидуально"].map(h => (
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
                const keyColor = keyEntry?.effective_key_type === "PRIMARY_KEY"  ? t.green.fg
                  : keyEntry?.effective_key_type === "UNIQUE_KEY"   ? t.purple.fg
                  : keyEntry?.effective_key_type === "USER_DEFINED" ? t.amber.base
                  : keyEntry?.effective_key_type === "NONE"         ? t.red.fg
                  :                                                   t.text.disabled;
                return (
                  <React.Fragment key={table}>
                    <tr style={{
                      ...S.trBorder,
                      background: isCustom ? "rgba(59,130,246,0.04)" : "transparent",
                    }}>
                      <td style={S.td}>
                        <code style={{ color: t.text.primary, fontSize: 12 }}>{table}</code>
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
                          <StrategyPicker
                            value={ts.strategy}
                            onChange={(s) => onTableSetting(table, { strategy: s })}
                            truncateTarget={ts.truncate_target}
                            onTruncateChange={(b) => onTableSetting(table, { truncate_target: b })}
                          />
                        ) : (
                          <span style={{ fontSize: 11, color: t.text.secondary }}>{ts.strategy}</span>
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
                          <span style={{ fontSize: 11, color: t.text.secondary }}>{ts.chunk_size.toLocaleString("ru-RU")}</span>
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
                          <span style={{ fontSize: 11, color: t.text.secondary }}>{ts.workers}</span>
                        )}
                      </td>
                      <td style={{ ...S.td, textAlign: "center" }}>
                        <input
                          type="checkbox"
                          checked={isCustom}
                          onChange={() => toggleCustom(table)}
                          style={{ accentColor: t.blue.base }}
                        />
                      </td>
                    </tr>
                    {isExpanded && keyEntry && (
                      <tr>
                        <td colSpan={6} style={{
                          padding: "10px 16px", background: t.bg.s1,
                          borderBottom: `1px solid ${t.border.subtle}`,
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
