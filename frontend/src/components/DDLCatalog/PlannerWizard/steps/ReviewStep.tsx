import { S } from "../../styles";
import type { Batch, PlanDefaults, TableKeyEntry } from "../types";
import { t } from "../../../../theme";

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
          <div style={{ fontSize: 11, color: t.text.muted, marginBottom: 4 }}>Схемы</div>
          <div style={{ fontSize: 13, color: t.text.primary }}>
            <span style={{ color: t.blue.fg }}>{srcSchema}</span>
            <span style={{ color: t.text.disabled, margin: "0 6px" }}>→</span>
            <span style={{ color: t.green.fg }}>{tgtSchema}</span>
          </div>
        </div>
        <div style={{ ...S.card, padding: 16 }}>
          <div style={{ fontSize: 11, color: t.text.muted, marginBottom: 4 }}>Таблиц / Батчей</div>
          <div style={{ fontSize: 13, color: t.text.primary }}>
            <span style={{ fontWeight: 700 }}>{totalTables}</span>
            <span style={{ color: t.text.disabled }}> / </span>
            <span style={{ fontWeight: 700 }}>{batches.length}</span>
          </div>
        </div>
        <div style={{ ...S.card, padding: 16 }}>
          <div style={{ fontSize: 11, color: t.text.muted, marginBottom: 4 }}>Группа коннекторов</div>
          <div style={{ fontSize: 13, color: t.blue.fg }}>{selectedGroup || "—"}</div>
        </div>
      </div>

      {/* Defaults */}
      <div style={S.card}>
        <div style={S.cardHeader}>
          <span style={{ fontSize: 13, fontWeight: 600, color: t.text.primary }}>Параметры по умолчанию</span>
        </div>
        <div style={{ ...S.cardBody, flexDirection: "row", gap: 20, flexWrap: "wrap" }}>
          {([
            ["Режим",       defaults.mode],
            ["Стратегия",   defaults.strategy],
            ["Chunk size",  defaults.chunk_size.toLocaleString("ru-RU")],
            ["Workers",     String(defaults.workers)],
          ] as [string, string][]).map(([label, value]) => (
            <div key={label} style={{ display: "flex", flexDirection: "column", gap: 2 }}>
              <span style={{ fontSize: 10, color: t.text.muted }}>{label}</span>
              <span style={{ fontSize: 12, color: t.text.primary, fontWeight: 600 }}>{value}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Batch list */}
      {batches.map(batch => (
        <div key={batch.id} style={S.card}>
          <div style={S.cardHeader}>
            <span style={{ fontSize: 13, fontWeight: 600, color: t.text.primary }}>Батч #{batch.id}</span>
            <span style={S.badge(t.bg.s3, t.blue.fg)}>{batch.items.length} таблиц</span>
          </div>
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ borderBottom: `1px solid ${t.border.subtle}` }}>
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
                      <td style={{ ...S.td, color: t.text.disabled }}>{idx + 1}</td>
                      <td style={S.td}>
                        <code style={{ color: t.text.primary, fontSize: 12 }}>{item.table}</code>
                      </td>
                      <td style={S.td}>
                        <div style={{ fontSize: 10 }}>
                          <span style={{ color: t.blue.fg, fontWeight: 600 }}>{keyLabel}</span>
                          {keyCols && (
                            <div style={{ color: t.text.disabled, fontSize: 9, marginTop: 1 }}>{keyCols}</div>
                          )}
                        </div>
                      </td>
                      <td style={S.td}><span style={{ fontSize: 11, color: t.text.secondary }}>{item.mode}</span></td>
                      <td style={S.td}><span style={{ fontSize: 11, color: t.text.secondary }}>{item.strategy}</span></td>
                      <td style={S.td}><span style={{ fontSize: 11, color: t.text.secondary }}>{item.chunk_size.toLocaleString("ru-RU")}</span></td>
                      <td style={S.td}><span style={{ fontSize: 11, color: t.text.secondary }}>{item.workers}</span></td>
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
        padding: "16px 0", borderTop: `1px solid ${t.border.subtle}`,
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
            <span style={S.badge(t.green.bg, t.green.fg)}>
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
