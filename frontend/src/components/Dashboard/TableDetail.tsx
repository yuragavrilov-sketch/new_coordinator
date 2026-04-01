import { useState, useEffect, useCallback } from "react";
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
  onMigrationChanged?: () => void;
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

type ChunkTab = "bulk" | "baseline" | "compare";

const CHUNK_TABS: { key: ChunkTab; label: string; apiType: string }[] = [
  { key: "bulk", label: "Bulk", apiType: "BULK" },
  { key: "baseline", label: "Baseline", apiType: "BASELINE" },
  { key: "compare", label: "Compare", apiType: "COMPARE" },
];

export function TableDetail({ tableName, migration, onCreateMigration, onMigrationChanged }: Props) {
  const [showDetails, setShowDetails] = useState(false);
  const [actionBusy, setActionBusy] = useState(false);
  const [chunkTab, setChunkTab] = useState<ChunkTab>("bulk");
  const [chunkStats, setChunkStats] = useState<Record<ChunkTab, ChunkStats | null>>({
    bulk: null, baseline: null, compare: null,
  });
  const [history, setHistory] = useState<HistoryEntry[]>([]);

  const loadChunkStats = useCallback((mid: string) => {
    for (const tab of CHUNK_TABS) {
      if (tab.key === "compare") {
        // Compare chunks are in data_compare_chunks, loaded via migration detail
        fetch(`/api/migrations/${mid}`)
          .then(r => r.ok ? r.json() : null)
          .then(data => {
            if (data?.data_compare_task_id) {
              fetch(`/api/data-compare/${data.data_compare_task_id}`)
                .then(r => r.ok ? r.json() : null)
                .then(task => {
                  if (task) {
                    setChunkStats(prev => ({
                      ...prev,
                      compare: {
                        total: task.chunks_total || 0,
                        pending: 0, claimed: 0, running: 0,
                        done: task.chunks_done || 0,
                        failed: (task.chunks_total || 0) - (task.chunks_done || 0),
                        rows_loaded: 0,
                      },
                    }));
                  }
                })
                .catch(() => {});
            }
          })
          .catch(() => {});
      } else {
        fetch(`/api/migrations/${mid}/chunks?chunk_type=${tab.apiType}&page=1&page_size=1`)
          .then(r => r.ok ? r.json() : null)
          .then(data => {
            if (data?.stats) {
              setChunkStats(prev => ({ ...prev, [tab.key]: data.stats }));
            }
          })
          .catch(() => {});
      }
    }
  }, []);

  useEffect(() => {
    if (!migration || !showDetails) return;
    const mid = migration.migration_id;
    loadChunkStats(mid);

    // Load history
    fetch(`/api/migrations/${mid}`)
      .then(r => r.ok ? r.json() : null)
      .then(data => { if (data?.history) setHistory(data.history.slice(0, 15)); })
      .catch(() => {});
  }, [migration, showDetails, loadChunkStats]);

  const handleCancel = async () => {
    if (!migration || !confirm("Остановить миграцию?")) return;
    setActionBusy(true);
    try {
      await fetch(`/api/migrations/${migration.migration_id}/action`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "cancel" }),
      });
      onMigrationChanged?.();
    } catch (e) { console.error(e); }
    finally { setActionBusy(false); }
  };

  const handleDelete = async () => {
    if (!migration || !confirm("Удалить миграцию? Это действие необратимо.")) return;
    setActionBusy(true);
    try {
      const r = await fetch(`/api/migrations/${migration.migration_id}`, { method: "DELETE" });
      if (r.ok) onMigrationChanged?.();
      else {
        const d = await r.json().catch(() => ({}));
        alert(d.error || "Ошибка удаления");
      }
    } catch (e) { console.error(e); }
    finally { setActionBusy(false); }
  };

  // Polling
  useEffect(() => {
    if (!migration || !showDetails) return;
    const id = setInterval(() => loadChunkStats(migration.migration_id), 5000);
    return () => clearInterval(id);
  }, [migration, showDetails, loadChunkStats]);

  return (
    <div style={{
      background: "#0f172a", borderLeft: "3px solid #3b82f6",
      padding: 16, marginLeft: 40, marginBottom: 4,
    }}>
      {!migration ? (
        <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <span style={{ color: "#64748b", fontSize: 13 }}>Миграция не создана</span>
          <button onClick={() => onCreateMigration(tableName)} style={btnCreate}>
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
            <span style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 8 }}>
              <span style={{ fontSize: 12, color: "#64748b" }}>
                {fmtTs(migration.state_changed_at, "short")}
              </span>
              {migration.phase !== "COMPLETED" && migration.phase !== "CANCELLED" && migration.phase !== "CANCELLING" && (
                <button onClick={handleCancel} disabled={actionBusy} style={btnDanger}>
                  Остановить
                </button>
              )}
              <button onClick={handleDelete} disabled={actionBusy} style={btnDangerOutline}>
                Удалить
              </button>
            </span>
          </div>

          {/* Stats */}
          <div style={{ display: "flex", gap: 24, fontSize: 13, color: "#94a3b8", flexWrap: "wrap" }}>
            <span><strong style={{ color: "#e2e8f0" }}>Чанков:</strong> {fmtNum(migration.chunks_done)}/{migration.total_chunks != null ? fmtNum(migration.total_chunks) : "—"}</span>
            <span><strong style={{ color: "#e2e8f0" }}>Строк:</strong> {fmtNum(migration.rows_loaded)}</span>
          </div>

          {/* Progress bar */}
          {migration.total_chunks != null && migration.total_chunks > 0 && (
            <ProgressBar done={migration.chunks_done} total={migration.total_chunks} />
          )}

          {/* Error */}
          {migration.error_text && (
            <div style={errBlock}>{migration.error_text}</div>
          )}

          {/* Expand */}
          <div>
            <span onClick={() => setShowDetails(!showDetails)}
              style={{ color: "#3b82f6", fontSize: 13, cursor: "pointer", fontWeight: 600 }}>
              {showDetails ? "Свернуть ▲" : "Подробнее ▼"}
            </span>
          </div>

          {/* Expanded */}
          {showDetails && (
            <div style={{ display: "flex", flexDirection: "column", gap: 12, borderTop: "1px solid #1e293b", paddingTop: 12 }}>

              {/* Chunk tabs */}
              <div>
                <div style={{ display: "flex", gap: 0, borderBottom: "1px solid #1e293b", marginBottom: 8 }}>
                  {CHUNK_TABS.map(tab => {
                    const stats = chunkStats[tab.key];
                    const hasData = stats && stats.total > 0;
                    return (
                      <button key={tab.key} onClick={() => setChunkTab(tab.key)} style={{
                        background: "none", border: "none",
                        borderBottom: `2px solid ${chunkTab === tab.key ? "#3b82f6" : "transparent"}`,
                        color: chunkTab === tab.key ? "#93c5fd" : hasData ? "#94a3b8" : "#475569",
                        padding: "6px 14px", fontSize: 12, fontWeight: 600,
                        cursor: "pointer", marginBottom: -1,
                      }}>
                        {tab.label}
                        {hasData && <span style={{ marginLeft: 4, fontSize: 10, color: "#64748b" }}>({stats.total})</span>}
                      </button>
                    );
                  })}
                </div>

                {/* Active tab content */}
                <ChunkStatsPanel stats={chunkStats[chunkTab]} label={CHUNK_TABS.find(t => t.key === chunkTab)!.label} />
              </div>

              {/* History */}
              {history.length > 0 && (
                <div>
                  <div style={sectionTitle}>История переходов</div>
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
                          <><PhaseBadge phase={h.from_phase} size="sm" /><span style={{ color: "#475569" }}>→</span></>
                        )}
                        <PhaseBadge phase={h.to_phase} size="sm" />
                        {h.message && (
                          <span style={{ color: "#64748b", fontSize: 11, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 }}>
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

// ── Sub-components ──────────────────────────────────────────────────────────

function ChunkStatsPanel({ stats, label }: { stats: ChunkStats | null; label: string }) {
  if (!stats || stats.total === 0) {
    return <div style={{ fontSize: 12, color: "#475569", padding: "8px 0" }}>Нет чанков типа {label}</div>;
  }
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      <div style={{ display: "flex", gap: 12, fontSize: 12, color: "#94a3b8", flexWrap: "wrap" }}>
        <Chip label="Всего" value={stats.total} />
        {stats.pending > 0 && <Chip label="Ожидает" value={stats.pending} color="#fcd34d" />}
        {(stats.claimed + stats.running) > 0 && <Chip label="В работе" value={stats.claimed + stats.running} color="#60a5fa" />}
        <Chip label="Готово" value={stats.done} color="#4ade80" />
        {stats.failed > 0 && <Chip label="Ошибка" value={stats.failed} color="#f87171" />}
        {stats.rows_loaded > 0 && (
          <span>Строк: <strong style={{ color: "#e2e8f0" }}>{fmtNum(stats.rows_loaded)}</strong></span>
        )}
      </div>
      <ProgressBar done={stats.done} total={stats.total} />
    </div>
  );
}

function ProgressBar({ done, total }: { done: number; total: number }) {
  const pct = total > 0 ? Math.round((done / total) * 100) : 0;
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
      <div style={{ flex: 1, height: 6, background: "#334155", borderRadius: 3, overflow: "hidden" }}>
        <div style={{ width: `${pct}%`, height: "100%", background: "#3b82f6", borderRadius: 3, transition: "width 0.3s" }} />
      </div>
      <span style={{ fontSize: 11, color: "#94a3b8", whiteSpace: "nowrap" }}>{pct}%</span>
    </div>
  );
}

function Chip({ label, value, color }: { label: string; value: number; color?: string }) {
  return <span>{label}: <strong style={{ color: color || "#e2e8f0" }}>{fmtNum(value)}</strong></span>;
}

// ── Styles ──────────────────────────────────────────────────────────────────

const btnDanger: React.CSSProperties = {
  background: "#991b1b", color: "#fca5a5", border: "1px solid #7f1d1d",
  borderRadius: 4, padding: "3px 10px", fontSize: 11, fontWeight: 600, cursor: "pointer",
};

const btnDangerOutline: React.CSSProperties = {
  background: "transparent", color: "#f87171", border: "1px solid #7f1d1d",
  borderRadius: 4, padding: "3px 10px", fontSize: 11, fontWeight: 600, cursor: "pointer",
};

const btnCreate: React.CSSProperties = {
  background: "#3b82f6", color: "#fff", border: "none",
  borderRadius: 6, padding: "6px 16px", fontSize: 13, fontWeight: 600, cursor: "pointer",
};

const errBlock: React.CSSProperties = {
  background: "#450a0a", border: "1px solid #7f1d1d", borderRadius: 6,
  padding: "8px 12px", color: "#fca5a5", fontSize: 12,
  fontFamily: "monospace", whiteSpace: "pre-wrap", wordBreak: "break-word",
};

const sectionTitle: React.CSSProperties = {
  fontSize: 11, color: "#64748b", fontWeight: 700,
  textTransform: "uppercase", marginBottom: 6,
};
