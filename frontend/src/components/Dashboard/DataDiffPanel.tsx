import { useState, useEffect, useCallback } from "react";
import { fmtNum } from "../../utils/format";

interface CompareTask {
  task_id: string;
  source_schema: string;
  source_table: string;
  target_schema: string;
  target_table: string;
  compare_mode: string;
  status: string;
  source_count: number | null;
  target_count: number | null;
  counts_match: boolean | null;
  hash_match: boolean | null;
  chunks_total: number;
  chunks_done: number;
  error_text: string | null;
  started_at: string | null;
  completed_at: string | null;
}

interface ColDiff {
  column: string;
  data_type: string;
  source_hash: string | null;
  target_hash: string | null;
  match: boolean;
}

interface RowDiffData {
  columns: string[];
  source_only: Record<string, unknown>[];
  target_only: Record<string, unknown>[];
  limit: number;
}

interface Props {
  schema: string;
  tables: string[];
}

export function DataDiffPanel({ schema, tables }: Props) {
  const [tasks, setTasks] = useState<CompareTask[]>([]);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedTask, setExpandedTask] = useState<string | null>(null);
  const [diffTab, setDiffTab] = useState<"columns" | "rows">("columns");

  const loadTasks = useCallback(() => {
    fetch("/api/data-compare/tasks")
      .then((r) => (r.ok ? r.json() : []))
      .then((all: CompareTask[]) => {
        // Filter to current schema tables
        const schemaUpper = schema.toUpperCase();
        const relevant = all.filter(
          (t) => t.source_schema === schemaUpper && tables.map(n => n.toUpperCase()).includes(t.source_table),
        );
        setTasks(relevant);
      })
      .catch(() => {});
  }, [schema, tables]);

  useEffect(() => { loadTasks(); }, [loadTasks]);

  // Poll while running
  useEffect(() => {
    const hasRunning = tasks.some((t) => t.status === "PENDING" || t.status === "RUNNING" || t.status === "CHUNKING");
    if (!hasRunning) return;
    const id = setInterval(loadTasks, 3000);
    return () => clearInterval(id);
  }, [tasks, loadTasks]);

  const handleCompareAll = async () => {
    setRunning(true);
    setError(null);
    try {
      for (const tbl of tables) {
        await fetch("/api/data-compare/run", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            source_schema: schema,
            source_table: tbl,
            target_schema: schema,
            target_table: tbl,
            compare_mode: "full",
          }),
        });
      }
      loadTasks();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setRunning(false);
    }
  };

  // Summary
  const done = tasks.filter((t) => t.status === "DONE");
  const matched = done.filter((t) => t.counts_match && t.hash_match);
  const mismatched = done.filter((t) => !t.counts_match || !t.hash_match);
  const failed = tasks.filter((t) => t.status === "FAILED");
  const inProgress = tasks.filter((t) => ["PENDING", "RUNNING", "CHUNKING"].includes(t.status));

  return (
    <div style={panelStyle}>
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: tasks.length > 0 ? 10 : 0 }}>
        <span style={{ fontSize: 13, fontWeight: 700, color: "#e2e8f0" }}>
          Сравнение данных
        </span>
        {tables.length > 0 && (
          <button
            onClick={handleCompareAll}
            disabled={running}
            style={{
              ...btnS("#1d4ed8"),
              color: "#e2e8f0",
              fontWeight: 600,
            }}
          >
            {running ? "Запуск..." : `Сравнить все (${tables.length})`}
          </button>
        )}
        <button onClick={loadTasks} style={{ ...btnS("#1e293b"), marginLeft: "auto" }}>
          Обновить
        </button>
      </div>

      {error && (
        <div style={{ color: "#fca5a5", fontSize: 12, marginBottom: 8 }}>{error}</div>
      )}

      {/* Summary */}
      {tasks.length > 0 && (
        <div style={{ display: "flex", gap: 16, fontSize: 11, color: "#64748b", marginBottom: 10 }}>
          {inProgress.length > 0 && <span>В процессе: <b style={{ color: "#93c5fd" }}>{inProgress.length}</b></span>}
          {matched.length > 0 && <span>Совпадают: <b style={{ color: "#4ade80" }}>{matched.length}</b></span>}
          {mismatched.length > 0 && <span>Расхождения: <b style={{ color: "#f59e0b" }}>{mismatched.length}</b></span>}
          {failed.length > 0 && <span>Ошибки: <b style={{ color: "#fca5a5" }}>{failed.length}</b></span>}
        </div>
      )}

      {/* Results */}
      {tasks.length > 0 && (
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
            <thead>
              <tr style={{ borderBottom: "1px solid #334155" }}>
                <th style={thS}>Таблица</th>
                <th style={thS}>Статус</th>
                <th style={{ ...thS, textAlign: "right" }}>Source</th>
                <th style={{ ...thS, textAlign: "right" }}>Target</th>
                <th style={{ ...thS, textAlign: "center" }}>Count</th>
                <th style={{ ...thS, textAlign: "center" }}>Hash</th>
                <th style={{ ...thS, textAlign: "center" }}>Время</th>
              </tr>
            </thead>
            <tbody>
              {tasks.map((t) => {
                const canExpand = t.status === "DONE" && (!t.counts_match || !t.hash_match);
                const isExpanded = expandedTask === t.task_id;
                const elapsed = t.started_at && t.completed_at
                  ? `${((new Date(t.completed_at).getTime() - new Date(t.started_at).getTime()) / 1000).toFixed(1)}с`
                  : t.status === "RUNNING" ? "..." : "\u2014";

                return (
                  <TaskRows
                    key={t.task_id}
                    task={t}
                    elapsed={elapsed}
                    canExpand={canExpand}
                    isExpanded={isExpanded}
                    onToggle={() => { setExpandedTask(isExpanded ? null : t.task_id); setDiffTab("columns"); }}
                    diffTab={diffTab}
                    onDiffTabChange={setDiffTab}
                  />
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {tables.length === 0 && (
        <div style={{ color: "#475569", fontSize: 12, textAlign: "center", padding: 16 }}>
          Нет завершённых миграций для сравнения
        </div>
      )}
    </div>
  );
}

// ── Task row + expandable diff ──────────────────────────────────────────────

function TaskRows({
  task: t, elapsed, canExpand, isExpanded, onToggle, diffTab, onDiffTabChange,
}: {
  task: CompareTask;
  elapsed: string;
  canExpand: boolean;
  isExpanded: boolean;
  onToggle: () => void;
  diffTab: "columns" | "rows";
  onDiffTabChange: (t: "columns" | "rows") => void;
}) {
  const statusBadge = (status: string) => {
    const map: Record<string, { bg: string; fg: string }> = {
      PENDING: { bg: "#1e293b", fg: "#94a3b8" },
      CHUNKING: { bg: "#2e1065", fg: "#c4b5fd" },
      RUNNING: { bg: "#1e3a5f", fg: "#93c5fd" },
      DONE: { bg: "#052e16", fg: "#86efac" },
      FAILED: { bg: "#450a0a", fg: "#fca5a5" },
    };
    const c = map[status] || map.PENDING;
    return (
      <span style={{
        background: c.bg, color: c.fg, padding: "1px 6px", borderRadius: 3,
        fontSize: 10, fontWeight: 600,
        ...(status === "RUNNING" || status === "CHUNKING" ? { animation: "pulse 1.5s infinite" } : {}),
      }}>
        {status}
        {(status === "RUNNING" || status === "CHUNKING") && t.chunks_total > 0 &&
          ` ${t.chunks_done}/${t.chunks_total}`}
      </span>
    );
  };

  const matchDot = (match: boolean | null) => {
    if (match === null) return <span style={{ color: "#475569" }}>\u2014</span>;
    return (
      <span style={{ color: match ? "#4ade80" : "#ef4444", fontWeight: 600 }}>
        {match ? "OK" : "DIFF"}
      </span>
    );
  };

  return (
    <>
      <tr
        style={{
          borderBottom: isExpanded ? "none" : "1px solid #1e293b",
          cursor: canExpand ? "pointer" : "default",
        }}
        onClick={canExpand ? onToggle : undefined}
      >
        <td style={{ ...tdS, color: "#e2e8f0", fontFamily: "monospace" }}>{t.source_table}</td>
        <td style={tdS}>{statusBadge(t.status)}</td>
        <td style={{ ...tdS, textAlign: "right", fontFamily: "monospace" }}>{fmtNum(t.source_count)}</td>
        <td style={{ ...tdS, textAlign: "right", fontFamily: "monospace" }}>{fmtNum(t.target_count)}</td>
        <td style={{ ...tdS, textAlign: "center" }}>{matchDot(t.counts_match)}</td>
        <td style={{ ...tdS, textAlign: "center" }}>
          {canExpand ? (
            <span style={{ color: "#ef4444", fontWeight: 600, textDecoration: "underline", textDecorationStyle: "dotted", cursor: "pointer" }}>
              DIFF {isExpanded ? "\u25B2" : "\u25BC"}
            </span>
          ) : matchDot(t.hash_match)}
        </td>
        <td style={{ ...tdS, textAlign: "center", color: "#64748b" }}>{elapsed}</td>
      </tr>
      {isExpanded && (
        <tr style={{ borderBottom: "1px solid #1e293b" }}>
          <td colSpan={7} style={{ background: "#0a0f1a", padding: 0 }}>
            <div style={{ display: "flex", gap: 0, borderBottom: "1px solid #1e293b" }}>
              {(["columns", "rows"] as const).map((tab) => (
                <button
                  key={tab}
                  onClick={(e) => { e.stopPropagation(); onDiffTabChange(tab); }}
                  style={{
                    background: "none", border: "none", cursor: "pointer",
                    padding: "5px 12px", fontSize: 10, fontWeight: 600,
                    color: diffTab === tab ? "#93c5fd" : "#64748b",
                    borderBottom: `2px solid ${diffTab === tab ? "#3b82f6" : "transparent"}`,
                  }}
                >
                  {tab === "columns" ? "По колонкам" : "По строкам (MINUS)"}
                </button>
              ))}
            </div>
            {diffTab === "columns"
              ? <InlineColumnDiff taskId={t.task_id} />
              : <InlineRowDiff taskId={t.task_id} />}
          </td>
        </tr>
      )}
    </>
  );
}

// ── Inline column diff ──────────────────────────────────────────────────────

function InlineColumnDiff({ taskId }: { taskId: string }) {
  const [cols, setCols] = useState<ColDiff[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [showAll, setShowAll] = useState(false);

  useEffect(() => {
    fetch(`/api/data-compare/column-diff/${taskId}`)
      .then((r) => r.ok ? r.json() : Promise.reject())
      .then((d) => setCols(d.columns))
      .catch(() => setCols([]))
      .finally(() => setLoading(false));
  }, [taskId]);

  if (loading) return <div style={{ padding: 8, fontSize: 10, color: "#94a3b8" }}>Загрузка...</div>;
  if (!cols || cols.length === 0) return <div style={{ padding: 8, fontSize: 10, color: "#475569" }}>Нет данных</div>;

  const mismatched = cols.filter((c) => !c.match);
  const displayed = showAll ? cols : mismatched;

  return (
    <div style={{ padding: 8 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
        <span style={{ fontSize: 10, color: "#94a3b8" }}>
          Различия в {mismatched.length} из {cols.length} колонок
        </span>
        <label style={{ fontSize: 10, color: "#64748b", cursor: "pointer", display: "flex", alignItems: "center", gap: 3 }}>
          <input type="checkbox" checked={showAll} onChange={(e) => setShowAll(e.target.checked)} style={{ accentColor: "#3b82f6" }} />
          Все
        </label>
      </div>
      <table style={{ borderCollapse: "collapse", fontSize: 10, width: "100%" }}>
        <thead>
          <tr>
            {["Колонка", "Тип", "Source hash", "Target hash", ""].map((h) => (
              <th key={h} style={{ padding: "2px 6px", textAlign: "left", color: "#64748b", fontWeight: 500, borderBottom: "1px solid #1e293b" }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {displayed.map((c) => (
            <tr key={c.column} style={{ background: c.match ? "transparent" : "#1c0a0a" }}>
              <td style={{ padding: "2px 6px", color: c.match ? "#94a3b8" : "#fca5a5", fontWeight: c.match ? 400 : 600 }}>{c.column}</td>
              <td style={{ padding: "2px 6px", color: "#64748b" }}>{c.data_type}</td>
              <td style={{ padding: "2px 6px", color: "#94a3b8", fontFamily: "monospace" }}>{c.source_hash ?? "NULL"}</td>
              <td style={{ padding: "2px 6px", color: "#94a3b8", fontFamily: "monospace" }}>{c.target_hash ?? "NULL"}</td>
              <td style={{ padding: "2px 6px", color: c.match ? "#4ade80" : "#ef4444", fontWeight: 600 }}>{c.match ? "OK" : "DIFF"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Inline row diff ─────────────────────────────────────────────────────────

function InlineRowDiff({ taskId }: { taskId: string }) {
  const [data, setData] = useState<RowDiffData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch(`/api/data-compare/row-diff/${taskId}?limit=20`)
      .then((r) => r.ok ? r.json() : r.json().then((d) => Promise.reject(d.error)))
      .then(setData)
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
  }, [taskId]);

  if (loading) return <div style={{ padding: 8, fontSize: 10, color: "#94a3b8" }}>Ищем строки (MINUS)...</div>;
  if (error) return <div style={{ padding: 8, fontSize: 10, color: "#fca5a5" }}>{error}</div>;
  if (!data) return null;

  const { columns, source_only, target_only } = data;

  if (source_only.length === 0 && target_only.length === 0) {
    return <div style={{ padding: 8, fontSize: 10, color: "#4ade80" }}>MINUS не нашёл расхождений</div>;
  }

  const renderBlock = (rows: Record<string, unknown>[], label: string, color: string) => {
    if (rows.length === 0) return null;
    return (
      <div style={{ marginBottom: 8 }}>
        <div style={{ fontSize: 10, fontWeight: 600, color, marginBottom: 3 }}>
          {label} ({rows.length}{rows.length >= data.limit ? "+" : ""})
        </div>
        <div style={{ overflowX: "auto", maxHeight: 200 }}>
          <table style={{ borderCollapse: "collapse", fontSize: 9, whiteSpace: "nowrap" }}>
            <thead>
              <tr>
                {columns.map((c) => (
                  <th key={c} style={{ padding: "2px 5px", textAlign: "left", color: "#64748b", fontWeight: 500, borderBottom: "1px solid #1e293b", position: "sticky", top: 0, background: "#0a0f1a" }}>{c}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, i) => (
                <tr key={i} style={{ borderBottom: "1px solid #0f172a" }}>
                  {columns.map((c) => (
                    <td key={c} style={{ padding: "1px 5px", color: "#94a3b8", maxWidth: 150, overflow: "hidden", textOverflow: "ellipsis" }} title={String(row[c] ?? "NULL")}>
                      {row[c] !== null && row[c] !== undefined ? String(row[c]) : <span style={{ color: "#475569" }}>NULL</span>}
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
    <div style={{ padding: 8 }}>
      <div style={{ fontSize: 10, color: "#94a3b8", marginBottom: 6 }}>
        Source only: {source_only.length}, Target only: {target_only.length}
      </div>
      {renderBlock(source_only, "Только в Source", "#f59e0b")}
      {renderBlock(target_only, "Только в Target", "#3b82f6")}
    </div>
  );
}

// ── Styles ──────────────────────────────────────────────────────────────────

const panelStyle: React.CSSProperties = {
  background: "#0f172a", border: "1px solid #334155",
  borderRadius: 8, padding: "12px 16px", marginBottom: 16,
};

const thS: React.CSSProperties = {
  padding: "4px 8px", textAlign: "left", color: "#475569",
  fontWeight: 600, fontSize: 10, textTransform: "uppercase",
};

const tdS: React.CSSProperties = { padding: "4px 8px", color: "#94a3b8" };

function btnS(bg: string): React.CSSProperties {
  return {
    background: bg, border: "1px solid #334155", borderRadius: 4,
    color: "#94a3b8", padding: "3px 10px", fontSize: 11, cursor: "pointer",
  };
}
