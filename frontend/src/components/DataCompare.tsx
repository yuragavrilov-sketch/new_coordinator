import React, { useState, useEffect, useCallback } from "react";
import { fmtNum } from "../utils/format";
import { SearchSelect } from "./ui/SearchSelect";

// ── Types ────────────────────────────────────────────────────────────────────

interface CompareTask {
  task_id: string;
  source_schema: string;
  source_table: string;
  target_schema: string;
  target_table: string;
  compare_mode: "full" | "last_n";
  last_n: number | null;
  order_column: string | null;
  status: "PENDING" | "RUNNING" | "DONE" | "FAILED" | "CHUNKING";
  source_count: number | null;
  target_count: number | null;
  source_hash: string | null;
  target_hash: string | null;
  counts_match: boolean | null;
  hash_match: boolean | null;
  chunks_total: number;
  chunks_done: number;
  error_text: string | null;
  started_at: string | null;
  completed_at: string | null;
  created_at: string;
}

interface ColInfo {
  name: string;
  type: string;
  nullable: boolean;
}

// ── Main component ───────────────────────────────────────────────────────────

export function DataCompare() {
  // Selectors state
  const [srcSchema, setSrcSchema] = useState("");
  const [srcTable, setSrcTable] = useState("");
  const [tgtSchema, setTgtSchema] = useState("");
  const [tgtTable, setTgtTable] = useState("");
  const [mirrorTarget, setMirrorTarget] = useState(true);

  // Mode
  const [mode, setMode] = useState<"full" | "last_n">("full");
  const [lastN, setLastN] = useState(1000);
  const [orderColumn, setOrderColumn] = useState("");

  // Options lists
  const [srcSchemas, setSrcSchemas] = useState<string[]>([]);
  const [srcTables, setSrcTables] = useState<string[]>([]);
  const [tgtSchemas, setTgtSchemas] = useState<string[]>([]);
  const [tgtTables, setTgtTables] = useState<string[]>([]);
  const [columns, setColumns] = useState<ColInfo[]>([]);

  // Running state
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Task history
  const [tasks, setTasks] = useState<CompareTask[]>([]);
  const [loadingTasks, setLoadingTasks] = useState(false);

  // ----------- Load schemas on mount -----------
  useEffect(() => {
    fetch("/api/db/source/schemas").then(r => r.ok ? r.json() : []).then(setSrcSchemas).catch(() => {});
    fetch("/api/db/target/schemas").then(r => r.ok ? r.json() : []).then(setTgtSchemas).catch(() => {});
    loadTasks();
  }, []);

  // ----------- Load tables when schema changes -----------
  useEffect(() => {
    if (!srcSchema) { setSrcTables([]); return; }
    fetch(`/api/db/source/tables?schema=${srcSchema}`).then(r => r.ok ? r.json() : []).then(setSrcTables).catch(() => {});
  }, [srcSchema]);

  useEffect(() => {
    const schema = mirrorTarget ? srcSchema : tgtSchema;
    if (!schema) { setTgtTables([]); return; }
    fetch(`/api/db/target/tables?schema=${schema}`).then(r => r.ok ? r.json() : []).then(setTgtTables).catch(() => {});
  }, [tgtSchema, mirrorTarget, srcSchema]);

  // ----------- Mirror target from source -----------
  useEffect(() => {
    if (mirrorTarget) {
      setTgtSchema(srcSchema);
      setTgtTable(srcTable);
    }
  }, [srcSchema, srcTable, mirrorTarget]);

  // ----------- Load columns for order-by selector -----------
  useEffect(() => {
    if (!srcSchema || !srcTable) { setColumns([]); return; }
    fetch(`/api/db/source/table-info?schema=${srcSchema}&table=${srcTable}`)
      .then(r => r.ok ? r.json() : null)
      .then(d => { if (d?.columns) setColumns(d.columns); })
      .catch(() => {});
  }, [srcSchema, srcTable]);

  // ----------- Load tasks -----------
  const loadTasks = useCallback(() => {
    setLoadingTasks(true);
    fetch("/api/data-compare/tasks")
      .then(r => r.ok ? r.json() : [])
      .then(setTasks)
      .catch(() => {})
      .finally(() => setLoadingTasks(false));
  }, []);

  // ----------- Poll while tasks are running -----------
  useEffect(() => {
    const hasRunning = tasks.some(t => t.status === "PENDING" || t.status === "RUNNING" || t.status === "CHUNKING");
    if (!hasRunning) return;
    const id = setInterval(loadTasks, 3000);
    return () => clearInterval(id);
  }, [tasks, loadTasks]);

  // ----------- Submit comparison -----------
  const handleRun = async () => {
    setError(null);
    setSubmitting(true);
    try {
      const body: Record<string, unknown> = {
        source_schema: srcSchema,
        source_table: srcTable,
        target_schema: mirrorTarget ? srcSchema : tgtSchema,
        target_table: mirrorTarget ? srcTable : tgtTable,
        compare_mode: mode,
      };
      if (mode === "last_n") {
        body.last_n = lastN;
        body.order_column = orderColumn;
      }
      const r = await fetch("/api/data-compare/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await r.json();
      if (!r.ok) { setError(data.error || "Ошибка"); return; }
      loadTasks();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setSubmitting(false);
    }
  };

  // ----------- Delete task -----------
  const handleDelete = async (id: string) => {
    await fetch(`/api/data-compare/tasks/${id}`, { method: "DELETE" });
    loadTasks();
  };

  const canRun = srcSchema && srcTable && (mirrorTarget || (tgtSchema && tgtTable))
    && (mode === "full" || (lastN > 0 && orderColumn));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* ── Form ── */}
      <div style={{
        background: "#0f172a", border: "1px solid #1e293b", borderRadius: 8,
        padding: 16, display: "flex", flexDirection: "column", gap: 14,
      }}>
        {/* Source row */}
        <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
          <span style={{ fontSize: 12, color: "#64748b", width: 55 }}>Source</span>
          <SearchSelect value={srcSchema} onChange={v => { setSrcSchema(v); setSrcTable(""); }} options={srcSchemas} placeholder="Схема" />
          <SearchSelect value={srcTable} onChange={setSrcTable} options={srcTables} placeholder="Таблица" disabled={!srcSchema} />
        </div>

        {/* Mirror toggle */}
        <label style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 12, color: "#94a3b8", cursor: "pointer", userSelect: "none" }}>
          <input
            type="checkbox" checked={mirrorTarget}
            onChange={e => setMirrorTarget(e.target.checked)}
            style={{ accentColor: "#3b82f6" }}
          />
          Target = Source (та же схема/таблица)
        </label>

        {/* Target row (only when not mirroring) */}
        {!mirrorTarget && (
          <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
            <span style={{ fontSize: 12, color: "#64748b", width: 55 }}>Target</span>
            <SearchSelect value={tgtSchema} onChange={v => { setTgtSchema(v); setTgtTable(""); }} options={tgtSchemas} placeholder="Схема" />
            <SearchSelect value={tgtTable} onChange={setTgtTable} options={tgtTables} placeholder="Таблица" disabled={!tgtSchema} />
          </div>
        )}

        {/* Mode selector */}
        <div style={{ display: "flex", alignItems: "center", gap: 14, flexWrap: "wrap" }}>
          <span style={{ fontSize: 12, color: "#64748b", width: 55 }}>Режим</span>
          <div style={{ display: "flex", gap: 0, borderRadius: 4, overflow: "hidden", border: "1px solid #334155" }}>
            <button
              onClick={() => setMode("full")}
              style={{
                padding: "5px 14px", fontSize: 12, border: "none", cursor: "pointer",
                background: mode === "full" ? "#1d4ed8" : "#1e293b",
                color: mode === "full" ? "#e2e8f0" : "#64748b",
                fontWeight: mode === "full" ? 600 : 400,
              }}
            >
              Вся таблица
            </button>
            <button
              onClick={() => setMode("last_n")}
              style={{
                padding: "5px 14px", fontSize: 12, border: "none", cursor: "pointer",
                borderLeft: "1px solid #334155",
                background: mode === "last_n" ? "#1d4ed8" : "#1e293b",
                color: mode === "last_n" ? "#e2e8f0" : "#64748b",
                fontWeight: mode === "last_n" ? 600 : 400,
              }}
            >
              Последние N записей
            </button>
          </div>

          {mode === "last_n" && (
            <>
              <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                <span style={{ fontSize: 12, color: "#64748b" }}>N =</span>
                <input
                  type="number" min={1} value={lastN}
                  onChange={e => setLastN(Math.max(1, Number(e.target.value)))}
                  style={{
                    width: 80, background: "#1e293b", border: "1px solid #334155",
                    borderRadius: 4, color: "#e2e8f0", padding: "4px 8px", fontSize: 12,
                  }}
                />
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                <span style={{ fontSize: 12, color: "#64748b" }}>ORDER BY</span>
                <SearchSelect
                  value={orderColumn}
                  onChange={setOrderColumn}
                  options={columns.map(c => c.name)}
                  placeholder="Колонка"
                  disabled={columns.length === 0}
                />
                <span style={{ fontSize: 11, color: "#475569" }}>DESC</span>
              </div>
            </>
          )}
        </div>

        {/* Run button */}
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <button
            onClick={handleRun}
            disabled={!canRun || submitting}
            style={{
              background: canRun && !submitting ? "#1d4ed8" : "#1e293b",
              border: `1px solid ${canRun && !submitting ? "#3b82f6" : "#334155"}`,
              borderRadius: 6, color: "#e2e8f0", padding: "6px 20px",
              fontSize: 13, fontWeight: 600, cursor: canRun && !submitting ? "pointer" : "not-allowed",
              opacity: canRun && !submitting ? 1 : 0.5,
            }}
          >
            {submitting ? "Запуск..." : "Сравнить"}
          </button>
          {error && <span style={{ fontSize: 12, color: "#ef4444" }}>{error}</span>}
        </div>
      </div>

      {/* ── Results table ── */}
      <div style={{
        background: "#0f172a", border: "1px solid #1e293b", borderRadius: 8, overflow: "hidden",
      }}>
        <div style={{
          padding: "10px 14px", borderBottom: "1px solid #1e293b",
          display: "flex", alignItems: "center", gap: 10,
        }}>
          <span style={{ fontWeight: 600, fontSize: 13, color: "#e2e8f0" }}>
            Результаты сравнений
          </span>
          <span style={{ fontSize: 11, color: "#475569" }}>({tasks.length})</span>
          <button
            onClick={loadTasks}
            disabled={loadingTasks}
            style={{
              marginLeft: "auto", background: "none", border: "1px solid #334155",
              borderRadius: 4, color: "#64748b", padding: "3px 10px",
              fontSize: 11, cursor: "pointer",
            }}
          >
            {loadingTasks ? "..." : "Обновить"}
          </button>
        </div>

        {tasks.length === 0 ? (
          <div style={{ padding: "24px 14px", textAlign: "center", color: "#475569", fontSize: 12 }}>
            Нет результатов. Выберите таблицу и нажмите «Сравнить».
          </div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ borderBottom: "1px solid #1e293b" }}>
                  {["Таблица", "Режим", "Статус", "Source", "Target", "Count", "Hash", "Время", ""].map(h => (
                    <th key={h} style={{
                      padding: "7px 10px", textAlign: "left", color: "#64748b",
                      fontWeight: 500, fontSize: 11, whiteSpace: "nowrap",
                    }}>
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {tasks.map(t => (
                  <TaskRow key={t.task_id} task={t} onDelete={handleDelete} />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

// ── Column diff detail ───────────────────────────────────────────────────────

interface ColDiff {
  column: string;
  data_type: string;
  source_hash: string | null;
  target_hash: string | null;
  match: boolean;
}

function ColumnDiffPanel({ taskId }: { taskId: string }) {
  const [cols, setCols] = useState<ColDiff[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showAll, setShowAll] = useState(false);

  useEffect(() => {
    fetch(`/api/data-compare/column-diff/${taskId}`)
      .then(r => r.ok ? r.json() : r.json().then(d => Promise.reject(d.error || "error")))
      .then(d => setCols(d.columns))
      .catch(e => setError(String(e)))
      .finally(() => setLoading(false));
  }, [taskId]);

  if (loading) return <div style={{ padding: "8px 10px", fontSize: 11, color: "#94a3b8" }}>Загрузка поколоночного сравнения...</div>;
  if (error) return <div style={{ padding: "8px 10px", fontSize: 11, color: "#fca5a5" }}>{error}</div>;
  if (!cols || cols.length === 0) return <div style={{ padding: "8px 10px", fontSize: 11, color: "#475569" }}>Нет колонок для сравнения</div>;

  const mismatched = cols.filter(c => !c.match);
  const displayed = showAll ? cols : mismatched;

  return (
    <div style={{ padding: "8px 10px" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
        <span style={{ fontSize: 11, color: "#94a3b8" }}>
          Различия в {mismatched.length} из {cols.length} колонок
        </span>
        <label style={{ fontSize: 11, color: "#64748b", cursor: "pointer", display: "flex", alignItems: "center", gap: 4 }}>
          <input type="checkbox" checked={showAll} onChange={e => setShowAll(e.target.checked)} style={{ accentColor: "#3b82f6" }} />
          Показать все
        </label>
      </div>
      <table style={{ borderCollapse: "collapse", fontSize: 11, width: "100%" }}>
        <thead>
          <tr>
            {["Колонка", "Тип", "Source hash", "Target hash", ""].map(h => (
              <th key={h} style={{ padding: "4px 8px", textAlign: "left", color: "#64748b", fontWeight: 500, borderBottom: "1px solid #1e293b" }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {displayed.map(c => (
            <tr key={c.column} style={{ background: c.match ? "transparent" : "#1c0a0a" }}>
              <td style={{ padding: "3px 8px", color: c.match ? "#94a3b8" : "#fca5a5", fontWeight: c.match ? 400 : 600 }}>{c.column}</td>
              <td style={{ padding: "3px 8px", color: "#64748b" }}>{c.data_type}</td>
              <td style={{ padding: "3px 8px", color: "#94a3b8", fontVariantNumeric: "tabular-nums" }}>{c.source_hash ?? "NULL"}</td>
              <td style={{ padding: "3px 8px", color: "#94a3b8", fontVariantNumeric: "tabular-nums" }}>{c.target_hash ?? "NULL"}</td>
              <td style={{ padding: "3px 8px" }}>
                {c.match
                  ? <span style={{ color: "#22c55e" }}>OK</span>
                  : <span style={{ color: "#ef4444", fontWeight: 600 }}>DIFF</span>}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Task result row ──────────────────────────────────────────────────────────

function TaskRow({ task: t, onDelete }: { task: CompareTask; onDelete: (id: string) => void }) {
  const [expanded, setExpanded] = useState(false);
  const canExpand = t.status === "DONE" && t.hash_match === false;

  const statusColors: Record<string, { bg: string; text: string }> = {
    PENDING:  { bg: "#1e293b", text: "#94a3b8" },
    CHUNKING: { bg: "#2e1065", text: "#c4b5fd" },
    RUNNING:  { bg: "#1e3a5f", text: "#93c5fd" },
    DONE:     { bg: "#052e16", text: "#86efac" },
    FAILED:   { bg: "#450a0a", text: "#fca5a5" },
  };
  const sc = statusColors[t.status] || statusColors.PENDING;

  const matchBadge = (match: boolean | null, label: string) => {
    if (match === null) return <span style={{ color: "#475569", fontSize: 11 }}>—</span>;
    const ok = match;
    return (
      <span style={{
        display: "inline-flex", alignItems: "center", gap: 4,
        fontSize: 11, fontWeight: 600,
        color: ok ? "#22c55e" : "#ef4444",
      }}>
        <span style={{
          width: 7, height: 7, borderRadius: "50%",
          background: ok ? "#22c55e" : "#ef4444",
          display: "inline-block",
        }} />
        {ok ? "OK" : label}
      </span>
    );
  };

  const modeLabel = t.compare_mode === "full"
    ? "Вся таблица"
    : `Послед. ${t.last_n?.toLocaleString("ru-RU")}`;

  const elapsed = t.started_at && t.completed_at
    ? `${((new Date(t.completed_at).getTime() - new Date(t.started_at).getTime()) / 1000).toFixed(1)}с`
    : (t.status === "RUNNING" || t.status === "CHUNKING") ? "..." : "—";

  return (
    <>
    <tr style={{ borderBottom: expanded ? "none" : "1px solid #0f1624" }}>
      {/* Table */}
      <td style={{ padding: "6px 10px" }}>
        <div style={{ fontSize: 12, color: "#e2e8f0" }}>
          {t.source_schema}.{t.source_table}
        </div>
        {(t.source_schema !== t.target_schema || t.source_table !== t.target_table) && (
          <div style={{ fontSize: 11, color: "#64748b" }}>
            → {t.target_schema}.{t.target_table}
          </div>
        )}
      </td>

      {/* Mode */}
      <td style={{ padding: "6px 10px" }}>
        <span style={{ fontSize: 11, color: "#94a3b8" }}>{modeLabel}</span>
        {t.compare_mode === "last_n" && t.order_column && (
          <div style={{ fontSize: 10, color: "#475569" }}>by {t.order_column}</div>
        )}
      </td>

      {/* Status */}
      <td style={{ padding: "6px 10px" }}>
        <span style={{
          background: sc.bg, color: sc.text,
          padding: "2px 8px", borderRadius: 4, fontSize: 11, fontWeight: 600,
          ...((t.status === "RUNNING" || t.status === "CHUNKING") ? { animation: "pulse 1.5s infinite" } : {}),
        }}>
          {t.status}
        </span>
        {t.status === "FAILED" && t.error_text && (
          <div style={{ fontSize: 10, color: "#fca5a5", maxWidth: 200, marginTop: 3 }} title={t.error_text}>
            {t.error_text.length > 60 ? t.error_text.slice(0, 60) + "..." : t.error_text}
          </div>
        )}
        {(t.status === "RUNNING" || t.status === "CHUNKING") && t.chunks_total > 0 && (
          <div style={{ marginTop: 4 }}>
            <div style={{
              height: 4, borderRadius: 2, background: "#1e293b", overflow: "hidden", width: 100,
            }}>
              <div style={{
                height: "100%", borderRadius: 2,
                background: "#3b82f6",
                width: `${Math.round((t.chunks_done / t.chunks_total) * 100)}%`,
                transition: "width 0.3s",
              }} />
            </div>
            <div style={{ fontSize: 10, color: "#64748b", marginTop: 2 }}>
              {t.chunks_done} / {t.chunks_total}
            </div>
          </div>
        )}
      </td>

      {/* Source count */}
      <td style={{ padding: "6px 10px", fontSize: 12, color: "#e2e8f0", fontVariantNumeric: "tabular-nums" }}>
        {fmtNum(t.source_count)}
      </td>

      {/* Target count */}
      <td style={{ padding: "6px 10px", fontSize: 12, color: "#e2e8f0", fontVariantNumeric: "tabular-nums" }}>
        {fmtNum(t.target_count)}
      </td>

      {/* Count match */}
      <td style={{ padding: "6px 10px" }}>
        {matchBadge(t.counts_match, t.source_count !== null && t.target_count !== null
          ? `${(Math.abs((t.source_count || 0) - (t.target_count || 0))).toLocaleString("ru-RU")}`
          : "Mismatch")}
      </td>

      {/* Hash match */}
      <td style={{ padding: "6px 10px" }}>
        {canExpand ? (
          <span
            onClick={() => setExpanded(v => !v)}
            style={{
              display: "inline-flex", alignItems: "center", gap: 4,
              fontSize: 11, fontWeight: 600, color: "#ef4444",
              cursor: "pointer", textDecoration: "underline", textDecorationStyle: "dotted",
            }}
            title="Показать разницу по колонкам"
          >
            <span style={{
              width: 7, height: 7, borderRadius: "50%",
              background: "#ef4444", display: "inline-block",
            }} />
            Diff {expanded ? "▲" : "▼"}
          </span>
        ) : (
          matchBadge(t.hash_match, "Diff")
        )}
      </td>

      {/* Time */}
      <td style={{ padding: "6px 10px", fontSize: 11, color: "#64748b", whiteSpace: "nowrap" }}>
        {elapsed}
      </td>

      {/* Delete */}
      <td style={{ padding: "6px 10px" }}>
        <button
          onClick={() => onDelete(t.task_id)}
          title="Удалить"
          style={{
            background: "none", border: "none", color: "#475569",
            cursor: "pointer", fontSize: 13, padding: "2px 6px",
          }}
          onMouseEnter={e => (e.currentTarget.style.color = "#ef4444")}
          onMouseLeave={e => (e.currentTarget.style.color = "#475569")}
        >
          &#10005;
        </button>
      </td>
    </tr>
    {expanded && (
      <tr style={{ borderBottom: "1px solid #0f1624" }}>
        <td colSpan={9} style={{ background: "#0a0f1a", padding: 0 }}>
          <ColumnDiffPanel taskId={t.task_id} />
        </td>
      </tr>
    )}
    </>
  );
}
