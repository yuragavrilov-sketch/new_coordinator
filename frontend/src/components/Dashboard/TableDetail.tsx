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

interface CompareResult {
  task_id: string;
  status: string;
  source_count: number | null;
  target_count: number | null;
  counts_match: boolean | null;
  hash_match: boolean | null;
  error_text: string | null;
}

type ChunkTab = "bulk" | "baseline" | "compare";

const CHUNK_TABS: { key: ChunkTab; label: string; apiType: string }[] = [
  { key: "bulk", label: "Bulk", apiType: "BULK" },
  { key: "baseline", label: "Baseline", apiType: "BASELINE" },
  { key: "compare", label: "Compare", apiType: "COMPARE" },
];

export function TableDetail({ tableName, schema, migration, onCreateMigration, onMigrationChanged }: Props) {
  const [showDetails, setShowDetails] = useState(false);
  const [actionBusy, setActionBusy] = useState(false);
  const [compareResult, setCompareResult] = useState<CompareResult | null>(null);
  const [comparing, setComparing] = useState(false);
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

  const handleCompare = async () => {
    setComparing(true);
    setCompareResult(null);
    try {
      const r = await fetch("/api/data-compare/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          source_schema: schema,
          source_table: tableName,
          target_schema: schema,
          target_table: tableName,
          compare_mode: "full",
        }),
      });
      const data = await r.json();
      if (!r.ok) { setCompareResult({ task_id: "", status: "FAILED", source_count: null, target_count: null, counts_match: null, hash_match: null, error_text: data.error }); return; }
      // Poll until done
      const taskId = data.task_id;
      const poll = async () => {
        const resp = await fetch("/api/data-compare/tasks");
        const tasks = await resp.json();
        const task = tasks.find((t: any) => t.task_id === taskId);
        if (!task) return;
        if (task.status === "DONE" || task.status === "FAILED") {
          setCompareResult(task);
          setComparing(false);
        } else {
          setTimeout(poll, 2000);
        }
      };
      poll();
    } catch (e: any) {
      setCompareResult({ task_id: "", status: "FAILED", source_count: null, target_count: null, counts_match: null, hash_match: null, error_text: e.message });
      setComparing(false);
    }
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
        <div style={{ display: "flex", alignItems: "center", gap: 16, flexWrap: "wrap" }}>
          <span style={{ color: "#64748b", fontSize: 13 }}>Миграция не создана</span>
          <button onClick={() => onCreateMigration(tableName)} style={btnCreate}>
            Создать миграцию
          </button>
          <button onClick={handleCompare} disabled={comparing} style={btnCompare}>
            {comparing ? "Сравниваем..." : "Сравнить данные"}
          </button>
          {compareResult && <CompareResultPanel result={compareResult} />}
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
              <button onClick={handleCompare} disabled={comparing} style={btnCompare}>
                {comparing ? "Сравниваем..." : "Сравнить данные"}
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

          {/* Compare result */}
          {compareResult && (
            <CompareResultPanel result={compareResult} />
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

const btnCompare: React.CSSProperties = {
  background: "#1e293b", color: "#93c5fd", border: "1px solid #334155",
  borderRadius: 4, padding: "3px 10px", fontSize: 11, fontWeight: 600, cursor: "pointer",
};

const sectionTitle: React.CSSProperties = {
  fontSize: 11, color: "#64748b", fontWeight: 700,
  textTransform: "uppercase", marginBottom: 6,
};

// ── Compare result inline ───────────────────────────────────────────────────

function CompareResultPanel({ result: r }: { result: CompareResult }) {
  const [diffTab, setDiffTab] = useState<"columns" | "rows" | null>(null);

  if (r.status === "FAILED") {
    return (
      <div style={{ background: "#450a0a", border: "1px solid #7f1d1d", borderRadius: 6, padding: "6px 10px", fontSize: 11, color: "#fca5a5", width: "100%" }}>
        Ошибка: {r.error_text}
      </div>
    );
  }

  const ok = r.counts_match && r.hash_match;
  const hasDiff = !r.counts_match || !r.hash_match;

  return (
    <div style={{
      background: ok ? "#052e16" : "#1a0f00",
      border: `1px solid ${ok ? "#166534" : "#78350f"}`,
      borderRadius: 6, padding: "8px 12px", fontSize: 11, width: "100%", marginTop: 4,
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
        <span style={{ color: ok ? "#4ade80" : "#fbbf24", fontWeight: 700 }}>
          {ok ? "Данные совпадают" : "Найдены расхождения"}
        </span>
        <span style={{ color: "#94a3b8" }}>
          Source: {fmtNum(r.source_count)} | Target: {fmtNum(r.target_count)}
        </span>
        <span style={{ color: r.counts_match ? "#4ade80" : "#ef4444", fontWeight: 600 }}>
          Count: {r.counts_match ? "OK" : "DIFF"}
        </span>
        <span style={{ color: r.hash_match ? "#4ade80" : "#ef4444", fontWeight: 600 }}>
          Hash: {r.hash_match ? "OK" : "DIFF"}
        </span>
        {hasDiff && (
          <div style={{ display: "flex", gap: 0, marginLeft: "auto" }}>
            {(["columns", "rows"] as const).map((tab) => (
              <button
                key={tab}
                onClick={() => setDiffTab(diffTab === tab ? null : tab)}
                style={{
                  background: "none", border: "1px solid #334155",
                  borderRadius: tab === "columns" ? "3px 0 0 3px" : "0 3px 3px 0",
                  borderLeft: tab === "rows" ? "none" : undefined,
                  padding: "2px 8px", fontSize: 10, cursor: "pointer",
                  color: diffTab === tab ? "#93c5fd" : "#64748b",
                  fontWeight: diffTab === tab ? 600 : 400,
                }}
              >
                {tab === "columns" ? "По колонкам" : "По строкам"}
              </button>
            ))}
          </div>
        )}
      </div>
      {diffTab === "columns" && r.task_id && <InlineColDiff taskId={r.task_id} />}
      {diffTab === "rows" && r.task_id && <InlineRowDiff taskId={r.task_id} />}
    </div>
  );
}

function InlineColDiff({ taskId }: { taskId: string }) {
  const [cols, setCols] = useState<{ column: string; data_type: string; match: boolean; source_hash: string | null; target_hash: string | null }[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [showAll, setShowAll] = useState(false);

  useEffect(() => {
    fetch(`/api/data-compare/column-diff/${taskId}`)
      .then((r) => r.ok ? r.json() : Promise.reject())
      .then((d) => setCols(d.columns))
      .catch(() => setCols([]))
      .finally(() => setLoading(false));
  }, [taskId]);

  if (loading) return <div style={{ padding: "6px 0", fontSize: 10, color: "#94a3b8" }}>Загрузка...</div>;
  if (!cols || cols.length === 0) return null;

  const mismatched = cols.filter((c) => !c.match);
  const displayed = showAll ? cols : mismatched;

  return (
    <div style={{ marginTop: 8 }}>
      <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 4 }}>
        <span style={{ fontSize: 10, color: "#94a3b8" }}>Различия в {mismatched.length} из {cols.length} колонок</span>
        <label style={{ fontSize: 10, color: "#64748b", cursor: "pointer", display: "flex", alignItems: "center", gap: 3 }}>
          <input type="checkbox" checked={showAll} onChange={(e) => setShowAll(e.target.checked)} style={{ accentColor: "#3b82f6" }} />
          Все
        </label>
      </div>
      <table style={{ borderCollapse: "collapse", fontSize: 10, width: "100%" }}>
        <tbody>
          {displayed.map((c) => (
            <tr key={c.column} style={{ borderBottom: "1px solid #1e293b" }}>
              <td style={{ padding: "2px 6px", color: c.match ? "#94a3b8" : "#fca5a5", fontWeight: c.match ? 400 : 600 }}>{c.column}</td>
              <td style={{ padding: "2px 6px", color: "#64748b" }}>{c.data_type}</td>
              <td style={{ padding: "2px 6px", color: c.match ? "#4ade80" : "#ef4444", fontWeight: 600 }}>{c.match ? "OK" : "DIFF"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function InlineRowDiff({ taskId }: { taskId: string }) {
  const [data, setData] = useState<{ columns: string[]; source_only: Record<string, unknown>[]; target_only: Record<string, unknown>[]; limit: number } | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`/api/data-compare/row-diff/${taskId}?limit=10`)
      .then((r) => r.ok ? r.json() : Promise.reject())
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, [taskId]);

  if (loading) return <div style={{ padding: "6px 0", fontSize: 10, color: "#94a3b8" }}>Ищем строки...</div>;
  if (!data) return null;

  if (data.source_only.length === 0 && data.target_only.length === 0) {
    return <div style={{ padding: "6px 0", fontSize: 10, color: "#4ade80" }}>MINUS не нашёл расхождений</div>;
  }

  const renderRows = (rows: Record<string, unknown>[], label: string, color: string) => {
    if (rows.length === 0) return null;
    return (
      <div style={{ marginTop: 6 }}>
        <div style={{ fontSize: 10, fontWeight: 600, color, marginBottom: 3 }}>{label} ({rows.length}{rows.length >= data.limit ? "+" : ""})</div>
        <div style={{ overflowX: "auto", maxHeight: 150 }}>
          <table style={{ borderCollapse: "collapse", fontSize: 9, whiteSpace: "nowrap" }}>
            <thead>
              <tr>
                {data.columns.map((c) => (
                  <th key={c} style={{ padding: "2px 4px", textAlign: "left", color: "#64748b", fontWeight: 500, borderBottom: "1px solid #1e293b" }}>{c}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, i) => (
                <tr key={i}>
                  {data.columns.map((c) => (
                    <td key={c} style={{ padding: "1px 4px", color: "#94a3b8", maxWidth: 120, overflow: "hidden", textOverflow: "ellipsis" }} title={String(row[c] ?? "NULL")}>
                      {row[c] != null ? String(row[c]) : <span style={{ color: "#475569" }}>NULL</span>}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  return (
    <div style={{ marginTop: 8 }}>
      {renderRows(data.source_only, "Только в Source", "#f59e0b")}
      {renderRows(data.target_only, "Только в Target", "#3b82f6")}
    </div>
  );
}
