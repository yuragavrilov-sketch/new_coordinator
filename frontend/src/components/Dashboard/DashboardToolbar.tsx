import { SearchSelect } from "../ui/SearchSelect";

interface Counts {
  total: number;
  withMigration: number;
  noMigration: number;
  errors: number;
  completed: number;
  active: number;
}

interface Props {
  schemas: string[];
  selectedSchema: string;
  onSchemaChange: (schema: string) => void;
  onRefresh: () => void;
  refreshing: boolean;
  counts: Counts;
  selectedCount: number;
  onBulkCreate: () => void;
  onBulkGroup: () => void;
}

export function DashboardToolbar({
  schemas, selectedSchema, onSchemaChange,
  onRefresh, refreshing, counts,
  selectedCount, onBulkCreate, onBulkGroup,
}: Props) {
  const pct = counts.total > 0 ? Math.round((counts.completed / counts.total) * 100) : 0;

  return (
    <div style={{ marginBottom: 16 }}>
      {/* Schema + refresh row */}
      <div style={{
        display: "flex", alignItems: "center", gap: 12,
        padding: "10px 16px", background: "#1e293b",
        borderRadius: "8px 8px 0 0", border: "1px solid #334155", borderBottom: "none",
      }}>
        <div style={{ width: 260 }}>
          <SearchSelect value={selectedSchema} onChange={onSchemaChange}
            options={schemas} placeholder="Схема..." />
        </div>
        <button onClick={onRefresh} disabled={refreshing} style={{
          padding: "6px 14px", borderRadius: 6,
          border: "1px solid #334155",
          background: refreshing ? "#334155" : "#3b82f6",
          color: "#e2e8f0", cursor: refreshing ? "not-allowed" : "pointer",
          fontSize: 13, fontWeight: 500,
          display: "flex", alignItems: "center", gap: 6,
        }}>
          {refreshing && <Spinner />}
          Обновить из Oracle
        </button>
        <div style={{ marginLeft: "auto", fontSize: 12, color: "#94a3b8" }}>
          {counts.total > 0 && `${pct}% завершено`}
        </div>
      </div>

      {/* Status cards */}
      <div style={{
        display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 0,
        borderRadius: "0 0 8px 8px", border: "1px solid #334155", overflow: "hidden",
      }}>
        <StatusCard label="Всего" value={counts.total} color="#94a3b8" bg="#1e293b" />
        <StatusCard label="Без миграции" value={counts.noMigration} color="#94a3b8" bg="#1e293b" />
        <StatusCard label="Активные" value={counts.active} color="#60a5fa" bg="#172554"
          accent={counts.active > 0} />
        <StatusCard label="Завершено" value={counts.completed} color="#4ade80" bg="#052e16"
          accent={counts.completed > 0} />
        <StatusCard label="Ошибки" value={counts.errors} color="#f87171" bg="#450a0a"
          accent={counts.errors > 0} />
      </div>

      {/* Progress bar */}
      {counts.total > 0 && (
        <div style={{
          height: 4, background: "#334155", marginTop: 2, borderRadius: 2, overflow: "hidden",
        }}>
          <div style={{
            width: `${pct}%`, height: "100%",
            background: counts.errors > 0 ? "linear-gradient(90deg, #4ade80, #f87171)" : "#4ade80",
            borderRadius: 2, transition: "width 0.5s",
          }} />
        </div>
      )}

      {/* Bulk actions */}
      {selectedCount > 0 && (
        <div style={{
          display: "flex", alignItems: "center", gap: 10,
          padding: "8px 16px", marginTop: 8,
          background: "#1e293b", borderRadius: 8, border: "1px solid #334155",
        }}>
          <span style={{ fontSize: 13, color: "#94a3b8" }}>
            Выбрано: <strong style={{ color: "#e2e8f0" }}>{selectedCount}</strong>
          </span>
          <button onClick={onBulkCreate} style={bulkBtn("#3b82f6")}>
            Создать миграции
          </button>
          <button onClick={onBulkGroup} style={bulkBtn("#1e40af")}>
            Создать группу + миграции
          </button>
        </div>
      )}

      <style>{`@keyframes spin { to { transform: rotate(360deg) } }`}</style>
    </div>
  );
}

function StatusCard({ label, value, color, bg, accent }: {
  label: string; value: number; color: string; bg: string; accent?: boolean;
}) {
  return (
    <div style={{
      padding: "10px 14px", background: accent ? bg : "#0f172a",
      borderRight: "1px solid #334155", textAlign: "center",
    }}>
      <div style={{ fontSize: 22, fontWeight: 700, color: accent ? color : "#475569" }}>
        {value}
      </div>
      <div style={{ fontSize: 11, color: "#64748b", marginTop: 2 }}>{label}</div>
    </div>
  );
}

function Spinner() {
  return (
    <span style={{
      display: "inline-block", width: 14, height: 14,
      border: "2px solid #e2e8f0", borderTopColor: "transparent",
      borderRadius: "50%", animation: "spin 0.8s linear infinite",
    }} />
  );
}

const bulkBtn = (bg: string): React.CSSProperties => ({
  padding: "6px 14px", borderRadius: 6,
  border: "1px solid #334155", background: bg,
  color: "#e2e8f0", cursor: "pointer", fontSize: 13, fontWeight: 500,
});
