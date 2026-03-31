import { useState, useEffect } from "react";
import { PhaseBadge } from "../PhaseBadge";
import { fmtTs, fmtNum } from "../../utils/format";

interface MigrationInfo {
  migration_id: string;
  migration_name: string;
  phase: string;
  chunks_done: number;
  total_chunks: number | null;
  rows_loaded: number;
  group_id: string | null;
  state_changed_at: string;
  error_text: string | null;
}

interface Props {
  tableName: string;
  schema: string;
  migration?: MigrationInfo;
  onCreateMigration: (tableName: string) => void;
}

interface ChunkStats {
  total: number; pending: number; claimed: number;
  running: number; done: number; failed: number;
  rows_loaded: number;
}

interface HistoryEntry {
  from_phase: string | null;
  to_phase: string;
  message: string | null;
  created_at: string;
}

export function TableDetail({ tableName, migration, onCreateMigration }: Props) {
  const [showDetails, setShowDetails] = useState(false);
  const [chunkStats, setChunkStats] = useState<ChunkStats | null>(null);
  const [history, setHistory] = useState<HistoryEntry[]>([]);

  useEffect(() => {
    if (!migration || !showDetails) return;
    const mid = migration.migration_id;

    // Load chunk stats
    fetch(`/api/migrations/${mid}/chunks?chunk_type=BULK&page=1&page_size=1`)
      .then(r => r.ok ? r.json() : null)
      .then(data => { if (data?.stats) setChunkStats(data.stats); })
      .catch(() => {});

    // Load recent history
    fetch(`/api/migrations/${mid}`)
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (data?.history) setHistory(data.history.slice(0, 10));
      })
      .catch(() => {});
  }, [migration, showDetails]);

  // Polling for chunk stats
  useEffect(() => {
    if (!migration || !showDetails) return;
    const mid = migration.migration_id;
    const id = setInterval(() => {
      fetch(`/api/migrations/${mid}/chunks?chunk_type=BULK&page=1&page_size=1`)
        .then(r => r.ok ? r.json() : null)
        .then(data => { if (data?.stats) setChunkStats(data.stats); })
        .catch(() => {});
    }, 5000);
    return () => clearInterval(id);
  }, [migration, showDetails]);

  return (
    <div style={{
      background: "#0f172a",
      borderLeft: "3px solid #3b82f6",
      padding: 16,
      marginLeft: 40,
      marginBottom: 4,
    }}>
      {!migration ? (
        <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <span style={{ color: "#64748b", fontSize: 13 }}>Миграция не создана</span>
          <button
            onClick={() => onCreateMigration(tableName)}
            style={{
              background: "#3b82f6", color: "#fff", border: "none",
              borderRadius: 6, padding: "6px 16px", fontSize: 13,
              fontWeight: 600, cursor: "pointer",
            }}
          >
            Создать миграцию
          </button>
        </div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          {/* Header */}
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <PhaseBadge phase={migration.phase} size="md" />
            <span style={{ color: "#e2e8f0", fontSize: 14, fontWeight: 600 }}>
              {migration.migration_name}
            </span>
            <span style={{ marginLeft: "auto", fontSize: 12, color: "#64748b" }}>
              {fmtTs(migration.state_changed_at, "short")}
            </span>
          </div>

          {/* Stats */}
          <div style={{ display: "flex", gap: 24, fontSize: 13, color: "#94a3b8", flexWrap: "wrap" }}>
            <span>
              <strong style={{ color: "#e2e8f0" }}>Чанков:</strong>{" "}
              {fmtNum(migration.chunks_done)}/{migration.total_chunks != null ? fmtNum(migration.total_chunks) : "—"}
            </span>
            <span>
              <strong style={{ color: "#e2e8f0" }}>Строк:</strong> {fmtNum(migration.rows_loaded)}
            </span>
          </div>

          {/* Progress bar */}
          {migration.total_chunks != null && migration.total_chunks > 0 && (
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <div style={{ flex: 1, height: 8, background: "#334155", borderRadius: 4, overflow: "hidden" }}>
                <div style={{
                  width: `${Math.round((migration.chunks_done / migration.total_chunks) * 100)}%`,
                  height: "100%", background: "#3b82f6", borderRadius: 4, transition: "width 0.3s",
                }} />
              </div>
              <span style={{ fontSize: 12, color: "#94a3b8", whiteSpace: "nowrap" }}>
                {Math.round((migration.chunks_done / migration.total_chunks) * 100)}%
              </span>
            </div>
          )}

          {/* Error */}
          {migration.error_text && (
            <div style={{
              background: "#450a0a", border: "1px solid #7f1d1d",
              borderRadius: 6, padding: "8px 12px", color: "#fca5a5",
              fontSize: 12, fontFamily: "monospace", whiteSpace: "pre-wrap", wordBreak: "break-word",
            }}>
              {migration.error_text}
            </div>
          )}

          {/* Expand/Collapse */}
          <div>
            <span
              onClick={() => setShowDetails(!showDetails)}
              style={{ color: "#3b82f6", fontSize: 13, cursor: "pointer", fontWeight: 600 }}
            >
              {showDetails ? "Свернуть ▲" : "Подробнее ▼"}
            </span>
          </div>

          {/* Expanded details */}
          {showDetails && (
            <div style={{ display: "flex", flexDirection: "column", gap: 12, borderTop: "1px solid #1e293b", paddingTop: 12 }}>
              {/* Chunk stats */}
              {chunkStats && chunkStats.total > 0 && (
                <div>
                  <div style={{ fontSize: 11, color: "#64748b", fontWeight: 700, textTransform: "uppercase", marginBottom: 6 }}>
                    Чанки (BULK)
                  </div>
                  <div style={{ display: "flex", gap: 12, fontSize: 12, color: "#94a3b8", flexWrap: "wrap" }}>
                    <StatChip label="Всего" value={chunkStats.total} />
                    <StatChip label="Ожидает" value={chunkStats.pending} color="#fcd34d" />
                    <StatChip label="В работе" value={chunkStats.claimed + chunkStats.running} color="#60a5fa" />
                    <StatChip label="Готово" value={chunkStats.done} color="#4ade80" />
                    {chunkStats.failed > 0 && <StatChip label="Ошибка" value={chunkStats.failed} color="#f87171" />}
                    <span>
                      Строк: <strong style={{ color: "#e2e8f0" }}>{fmtNum(chunkStats.rows_loaded)}</strong>
                    </span>
                  </div>
                </div>
              )}

              {/* History */}
              {history.length > 0 && (
                <div>
                  <div style={{ fontSize: 11, color: "#64748b", fontWeight: 700, textTransform: "uppercase", marginBottom: 6 }}>
                    История переходов
                  </div>
                  <div style={{ maxHeight: 150, overflowY: "auto" }}>
                    {history.map((h, i) => (
                      <div key={i} style={{
                        display: "flex", gap: 8, alignItems: "center",
                        fontSize: 12, color: "#94a3b8", padding: "3px 0",
                        borderBottom: i < history.length - 1 ? "1px solid #1e293b" : "none",
                      }}>
                        <span style={{ color: "#64748b", fontSize: 11, minWidth: 110 }}>
                          {fmtTs(h.created_at, "short")}
                        </span>
                        {h.from_phase && (
                          <>
                            <PhaseBadge phase={h.from_phase} size="sm" />
                            <span style={{ color: "#475569" }}>→</span>
                          </>
                        )}
                        <PhaseBadge phase={h.to_phase} size="sm" />
                        {h.message && (
                          <span style={{ color: "#64748b", fontSize: 11, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                            {h.message}
                          </span>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function StatChip({ label, value, color }: { label: string; value: number; color?: string }) {
  return (
    <span>
      {label}: <strong style={{ color: color || "#e2e8f0" }}>{fmtNum(value)}</strong>
    </span>
  );
}
