import { S } from "../../styles";
import type { Batch, PlanDefaults, TableKeyEntry } from "../types";

interface Props {
  srcSchema:       string;
  tgtSchema:       string;
  selectedGroup:   string;
  defaults:        PlanDefaults;
  batches:         Batch[];
  executing:       boolean;
  onExecute:       () => void;
  planId:          string | null;
  starting:        boolean;
  onStart:         () => void;
  tableKeyEntries: Map<string, TableKeyEntry>;
}

export function ReviewStep({
  srcSchema, tgtSchema, selectedGroup,
  defaults, batches, executing, onExecute,
  planId, starting, onStart,
  tableKeyEntries,
}: Props) {
  const totalTables = batches.reduce((acc, b) => acc + b.items.length, 0);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* Summary cards */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12 }}>
        <div style={{ ...S.card, padding: 16 }}>
          <div style={{ fontSize: 11, color: "#64748b", marginBottom: 4 }}>Схемы</div>
          <div style={{ fontSize: 13, color: "#e2e8f0" }}>
            <span style={{ color: "#93c5fd" }}>{srcSchema}</span>
            <span style={{ color: "#475569", margin: "0 6px" }}>→</span>
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
            ["Режим",       defaults.mode],
            ["Стратегия",   defaults.strategy],
            ["Chunk size",  defaults.chunk_size.toLocaleString("ru-RU")],
            ["Workers",     String(defaults.workers)],
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
