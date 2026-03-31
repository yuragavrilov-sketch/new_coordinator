import { useNavigate } from "react-router-dom";
import { PhaseBadge } from "../PhaseBadge";
import { fmtTs, fmtNum } from "../../utils/format";

interface Props {
  tableName: string;
  schema: string;
  migration?: {
    migration_id: string;
    migration_name: string;
    phase: string;
    chunks_done: number;
    total_chunks: number | null;
    rows_loaded: number;
    group_id: string | null;
    state_changed_at: string;
    error_text: string | null;
  };
  onCreateMigration: (tableName: string) => void;
}

export function TableDetail({ tableName, migration, onCreateMigration }: Props) {
  const navigate = useNavigate();
  return (
    <div
      style={{
        background: "#0f172a",
        borderLeft: "3px solid #3b82f6",
        padding: 16,
        marginLeft: 40,
        marginBottom: 4,
      }}
    >
      {!migration ? (
        <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <span style={{ color: "#64748b", fontSize: 13 }}>Миграция не создана</span>
          <button
            onClick={() => onCreateMigration(tableName)}
            style={{
              background: "#3b82f6",
              color: "#fff",
              border: "none",
              borderRadius: 6,
              padding: "6px 16px",
              fontSize: 13,
              fontWeight: 600,
              cursor: "pointer",
            }}
          >
            Создать миграцию
          </button>
        </div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          {/* Header: phase + migration name */}
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <PhaseBadge phase={migration.phase} size="md" />
            <span style={{ color: "#e2e8f0", fontSize: 14, fontWeight: 600 }}>
              {migration.migration_name}
            </span>
          </div>

          {/* Stats row */}
          <div
            style={{
              display: "flex",
              gap: 24,
              fontSize: 13,
              color: "#94a3b8",
              flexWrap: "wrap",
            }}
          >
            <span>
              <strong style={{ color: "#e2e8f0" }}>Чанков:</strong>{" "}
              {fmtNum(migration.chunks_done)}/{migration.total_chunks != null ? fmtNum(migration.total_chunks) : "—"}
            </span>
            <span>
              <strong style={{ color: "#e2e8f0" }}>Строк:</strong> {fmtNum(migration.rows_loaded)}
            </span>
            <span>
              <strong style={{ color: "#e2e8f0" }}>Обновлено:</strong>{" "}
              {fmtTs(migration.state_changed_at, "short")}
            </span>
          </div>

          {/* Error block */}
          {migration.error_text && (
            <div
              style={{
                background: "#450a0a",
                border: "1px solid #7f1d1d",
                borderRadius: 6,
                padding: "8px 12px",
                color: "#fca5a5",
                fontSize: 12,
                fontFamily: "monospace",
                whiteSpace: "pre-wrap",
                wordBreak: "break-word",
              }}
            >
              {migration.error_text}
            </div>
          )}

          {/* Link */}
          <div>
            <span
              onClick={() => navigate("/migrations")}
              style={{
                color: "#3b82f6",
                fontSize: 13,
                cursor: "pointer",
                fontWeight: 600,
              }}
            >
              Подробнее →
            </span>
          </div>
        </div>
      )}
    </div>
  );
}
