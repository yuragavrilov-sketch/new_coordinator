import { useState, useEffect, useCallback } from "react";
import { SearchSelect } from "./ui/SearchSelect";

interface SeqRow {
  sequence_name: string;
  source_value: number | null;
  target_value: number | null;
  delta: number | null;
  increment_by: number;
  cache_size: number;
  source_only: boolean;
  target_only: boolean;
}

interface AdvanceResult {
  sequence_name: string;
  status: "ok" | "skip" | "error";
  message?: string;
  error?: string;
  source_value?: number;
  old_target_value?: number;
  new_value?: number;
}

type SortKey = "name" | "source" | "target" | "delta";
type SortDir = "asc" | "desc";
type Filter = "all" | "diff" | "behind" | "ahead" | "synced" | "missing";

export function Sequences() {
  const [schemas, setSchemas] = useState<string[]>([]);
  const [schema, setSchema] = useState("");
  const [rows, setRows] = useState<SeqRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [compared, setCompared] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [delta, setDelta] = useState(1000);
  const [advancing, setAdvancing] = useState(false);
  const [advanceResults, setAdvanceResults] = useState<AdvanceResult[] | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("delta");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [filter, setFilter] = useState<Filter>("diff");

  useEffect(() => {
    fetch("/api/db/source/schemas")
      .then((r) => (r.ok ? r.json() : []))
      .then(setSchemas)
      .catch(() => {});
  }, []);

  // Reset when schema changes
  useEffect(() => {
    setRows([]);
    setCompared(false);
    setAdvanceResults(null);
    setSelected(new Set());
  }, [schema]);

  const handleCompare = useCallback(() => {
    if (!schema) return;
    setLoading(true);
    setError(null);
    setAdvanceResults(null);
    fetch(`/api/sequences/compare?schema=${schema}`)
      .then((r) => {
        if (!r.ok) return r.json().then((d) => Promise.reject(d.error || r.statusText));
        return r.json();
      })
      .then((data: SeqRow[]) => {
        setRows(data);
        setCompared(true);
        setSelected(new Set());
      })
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
  }, [schema]);

  const handleAdvance = () => {
    const seqs = selected.size > 0 ? [...selected] : [];
    setAdvancing(true);
    setAdvanceResults(null);
    fetch("/api/sequences/advance", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ schema, delta, sequences: seqs }),
    })
      .then((r) => r.json())
      .then((data: AdvanceResult[]) => {
        setAdvanceResults(data);
        handleCompare(); // refresh after advance
      })
      .catch((e) => setError(String(e)))
      .finally(() => setAdvancing(false));
  };

  // Counts
  const behindCount = rows.filter((r) => r.delta !== null && r.delta > 0).length;
  const aheadCount = rows.filter((r) => r.delta !== null && r.delta < 0).length;
  const missingCount = rows.filter((r) => r.source_only || r.target_only).length;
  const syncedCount = rows.filter((r) => !r.source_only && !r.target_only && r.delta === 0).length;
  const diffCount = behindCount + aheadCount + missingCount;

  // Filter & sort
  const filtered = rows
    .filter((r) => {
      if (search && !r.sequence_name.toLowerCase().includes(search.toLowerCase())) return false;
      if (filter === "diff") return (r.delta !== null && r.delta !== 0) || r.source_only || r.target_only;
      if (filter === "behind") return r.delta !== null && r.delta > 0;
      if (filter === "ahead") return r.delta !== null && r.delta < 0;
      if (filter === "synced") return !r.source_only && !r.target_only && r.delta === 0;
      if (filter === "missing") return r.source_only || r.target_only;
      return true;
    })
    .sort((a, b) => {
      const dir = sortDir === "asc" ? 1 : -1;
      switch (sortKey) {
        case "name": return a.sequence_name.localeCompare(b.sequence_name) * dir;
        case "source": return ((a.source_value ?? 0) - (b.source_value ?? 0)) * dir;
        case "target": return ((a.target_value ?? 0) - (b.target_value ?? 0)) * dir;
        case "delta": return (Math.abs(a.delta ?? 0) - Math.abs(b.delta ?? 0)) * dir;
        default: return 0;
      }
    });

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    else { setSortKey(key); setSortDir(key === "delta" ? "desc" : "asc"); }
  };

  const toggleSelect = (name: string) =>
    setSelected((prev) => { const n = new Set(prev); n.has(name) ? n.delete(name) : n.add(name); return n; });

  const toggleAll = () => {
    if (selected.size === filtered.length) setSelected(new Set());
    else setSelected(new Set(filtered.map((r) => r.sequence_name)));
  };

  const sortArrow = (key: SortKey) =>
    sortKey === key ? (sortDir === "asc" ? " \u25B2" : " \u25BC") : "";

  return (
    <div>
      {/* Toolbar */}
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 16, flexWrap: "wrap" }}>
        <span style={{ fontSize: 12, color: "#94a3b8" }}>Schema:</span>
        <SearchSelect value={schema} onChange={setSchema} options={schemas} placeholder="Select schema..." />

        <button
          onClick={handleCompare}
          disabled={!schema || loading}
          style={{
            ...btnStyle("#1d4ed8"),
            color: "#e2e8f0",
            fontWeight: 600,
            padding: "6px 20px",
          }}
        >
          {loading ? "Сравниваем..." : "Сравнить"}
        </button>

        {compared && rows.length > 0 && (
          <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 8 }}>
            <span style={{ fontSize: 12, color: "#94a3b8" }}>Delta:</span>
            <input
              type="number"
              value={delta}
              onChange={(e) => setDelta(Number(e.target.value))}
              style={{
                background: "#1e293b", border: "1px solid #334155", borderRadius: 4,
                color: "#e2e8f0", padding: "4px 8px", width: 100, fontSize: 12,
              }}
            />
            <button
              onClick={handleAdvance}
              disabled={advancing || rows.length === 0}
              style={{ ...btnStyle("#166534"), color: "#4ade80", fontWeight: 600 }}
            >
              {advancing ? "..." : selected.size > 0 ? `Подвинуть (${selected.size})` : "Подвинуть все"}
            </button>
          </div>
        )}
      </div>

      {error && (
        <div style={{ background: "#7f1d1d", color: "#fca5a5", padding: "8px 12px", borderRadius: 6, marginBottom: 12, fontSize: 12 }}>
          {error}
        </div>
      )}

      {/* Diff summary panel */}
      {compared && rows.length > 0 && (
        <div style={{
          background: diffCount > 0 ? "#1a0f00" : "#0a1a0f",
          border: `1px solid ${diffCount > 0 ? "#78350f" : "#166534"}`,
          borderRadius: 6, padding: 14, marginBottom: 16,
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 16, marginBottom: diffCount > 0 ? 10 : 0 }}>
            <span style={{
              fontSize: 14, fontWeight: 700,
              color: diffCount > 0 ? "#fbbf24" : "#4ade80",
            }}>
              {diffCount > 0
                ? `Найдено ${diffCount} расхождений`
                : "Все сиквенсы синхронизированы"}
            </span>
            <span style={{ fontSize: 11, color: "#64748b" }}>
              Всего: {rows.length} | Совпадают: {syncedCount}
            </span>
          </div>

          {diffCount > 0 && (
            <div style={{ display: "flex", gap: 20, fontSize: 12 }}>
              {behindCount > 0 && (
                <DiffGroup
                  label="Target отстает"
                  color="#f59e0b"
                  items={rows.filter((r) => r.delta !== null && r.delta > 0)}
                />
              )}
              {aheadCount > 0 && (
                <DiffGroup
                  label="Target впереди"
                  color="#3b82f6"
                  items={rows.filter((r) => r.delta !== null && r.delta < 0)}
                />
              )}
              {missingCount > 0 && (
                <DiffGroup
                  label="Отсутствуют"
                  color="#ef4444"
                  items={rows.filter((r) => r.source_only || r.target_only)}
                  showMissing
                />
              )}
            </div>
          )}
        </div>
      )}

      {/* Advance results */}
      {advanceResults && (
        <div style={{
          background: "#0f2a1a", border: "1px solid #166534", borderRadius: 6,
          padding: 12, marginBottom: 12, fontSize: 12,
        }}>
          <div style={{ color: "#4ade80", fontWeight: 600, marginBottom: 6 }}>
            Результат:
          </div>
          {advanceResults.map((r) => (
            <div key={r.sequence_name} style={{ color: resultColor(r.status), marginBottom: 2 }}>
              <span style={{ fontFamily: "monospace" }}>{r.sequence_name}</span>:{" "}
              {r.status === "ok"
                ? `${fmtNum(r.old_target_value ?? null)} \u2192 ${fmtNum(r.new_value ?? null)}`
                : r.status === "skip" ? r.message : r.error}
            </div>
          ))}
        </div>
      )}

      {/* Filter bar & table */}
      {compared && rows.length > 0 && (
        <>
          <div style={{ display: "flex", gap: 6, marginBottom: 10, alignItems: "center", flexWrap: "wrap" }}>
            {([
              { key: "all" as Filter, label: `Все (${rows.length})` },
              { key: "diff" as Filter, label: `Расхождения (${diffCount})`, highlight: diffCount > 0 },
              { key: "behind" as Filter, label: `Отстает (${behindCount})` },
              { key: "ahead" as Filter, label: `Впереди (${aheadCount})` },
              { key: "synced" as Filter, label: `Синхр. (${syncedCount})` },
              { key: "missing" as Filter, label: `Нет (${missingCount})` },
            ]).map((f) => (
              <button
                key={f.key}
                onClick={() => setFilter(f.key)}
                style={{
                  ...btnStyle(filter === f.key ? "#334155" : "#1e293b"),
                  borderColor: filter === f.key ? "#3b82f6" : "#334155",
                  color: filter === f.key
                    ? (f.highlight ? "#fbbf24" : "#93c5fd")
                    : "#94a3b8",
                  fontSize: 11,
                  padding: "4px 10px",
                }}
              >
                {f.label}
              </button>
            ))}
            <input
              placeholder="Поиск..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              style={{
                marginLeft: "auto", background: "#1e293b", border: "1px solid #334155",
                borderRadius: 4, color: "#e2e8f0", padding: "4px 8px", fontSize: 12, width: 200,
              }}
            />
          </div>

          {filtered.length > 0 ? (
            <div style={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 6, overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, tableLayout: "fixed" }}>
                <colgroup>
                  <col style={{ width: 36 }} />
                  <col />
                  <col style={{ width: 140 }} />
                  <col style={{ width: 140 }} />
                  <col style={{ width: 130 }} />
                  <col style={{ width: 60 }} />
                  <col style={{ width: 60 }} />
                  <col style={{ width: 100 }} />
                </colgroup>
                <thead>
                  <tr style={{ borderBottom: "1px solid #334155" }}>
                    <th style={thStyle}>
                      <input type="checkbox" checked={selected.size === filtered.length && filtered.length > 0} onChange={toggleAll} />
                    </th>
                    <Th label="Sequence" sortKey="name" current={sortKey} dir={sortDir} onClick={toggleSort} arrow={sortArrow} />
                    <Th label="Source" sortKey="source" current={sortKey} dir={sortDir} onClick={toggleSort} arrow={sortArrow} align="right" />
                    <Th label="Target" sortKey="target" current={sortKey} dir={sortDir} onClick={toggleSort} arrow={sortArrow} align="right" />
                    <Th label="Delta" sortKey="delta" current={sortKey} dir={sortDir} onClick={toggleSort} arrow={sortArrow} align="right" />
                    <th style={{ ...thStyle, textAlign: "center" }}>Inc</th>
                    <th style={{ ...thStyle, textAlign: "center" }}>Cache</th>
                    <th style={{ ...thStyle, textAlign: "center" }}>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {filtered.map((r) => {
                    const isDiff = (r.delta !== null && r.delta !== 0) || r.source_only || r.target_only;
                    return (
                      <tr
                        key={r.sequence_name}
                        style={{
                          borderBottom: "1px solid #0f172a",
                          background: selected.has(r.sequence_name)
                            ? "#1e3a5f33"
                            : isDiff ? "#2a1a0a11" : "transparent",
                        }}
                      >
                        <td style={tdStyle}>
                          <input type="checkbox" checked={selected.has(r.sequence_name)} onChange={() => toggleSelect(r.sequence_name)} />
                        </td>
                        <td
                          style={{ ...tdStyle, fontFamily: "monospace", color: "#e2e8f0", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                          title={r.sequence_name}
                        >
                          {r.sequence_name}
                        </td>
                        <td style={{ ...tdStyle, textAlign: "right", fontFamily: "monospace" }}>
                          {fmtNum(r.source_value)}
                        </td>
                        <td style={{ ...tdStyle, textAlign: "right", fontFamily: "monospace" }}>
                          {fmtNum(r.target_value)}
                        </td>
                        <td style={{
                          ...tdStyle, textAlign: "right", fontFamily: "monospace",
                          color: deltaColor(r.delta),
                          fontWeight: r.delta !== null && r.delta !== 0 ? 600 : 400,
                        }}>
                          {r.delta !== null
                            ? (r.delta > 0 ? "+" : "") + r.delta.toLocaleString("ru-RU")
                            : "\u2014"}
                        </td>
                        <td style={{ ...tdStyle, textAlign: "center", color: "#64748b" }}>{r.increment_by}</td>
                        <td style={{ ...tdStyle, textAlign: "center", color: "#64748b" }}>{r.cache_size}</td>
                        <td style={{ ...tdStyle, textAlign: "center" }}>
                          <SeqStatus row={r} />
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <div style={{ color: "#475569", textAlign: "center", padding: 30, fontSize: 13 }}>
              Нет записей по фильтру
            </div>
          )}
        </>
      )}

      {!loading && !compared && schema && (
        <div style={{ color: "#475569", textAlign: "center", padding: 60, fontSize: 13 }}>
          Нажмите "Сравнить" для загрузки сиквенсов
        </div>
      )}
      {!schema && (
        <div style={{ color: "#475569", textAlign: "center", padding: 60, fontSize: 13 }}>
          Выберите схему
        </div>
      )}
    </div>
  );
}

// ── Sub-components ──────────────────────────────────────────────────────────

function DiffGroup({
  label, color, items, showMissing,
}: {
  label: string;
  color: string;
  items: SeqRow[];
  showMissing?: boolean;
}) {
  const [expanded, setExpanded] = useState(items.length <= 5);
  const display = expanded ? items : items.slice(0, 3);

  return (
    <div style={{ flex: 1, minWidth: 200 }}>
      <div style={{ color, fontWeight: 600, marginBottom: 4 }}>
        {label} ({items.length})
      </div>
      {display.map((r) => (
        <div
          key={r.sequence_name}
          style={{ color: "#94a3b8", fontSize: 11, marginBottom: 1, fontFamily: "monospace" }}
        >
          {r.sequence_name}
          {!showMissing && r.delta !== null && (
            <span style={{ color, marginLeft: 6 }}>
              {r.delta > 0 ? "+" : ""}{r.delta.toLocaleString("ru-RU")}
            </span>
          )}
          {showMissing && r.source_only && (
            <span style={{ color: "#fca5a5", marginLeft: 6 }}>only source</span>
          )}
          {showMissing && r.target_only && (
            <span style={{ color: "#93c5fd", marginLeft: 6 }}>only target</span>
          )}
        </div>
      ))}
      {!expanded && items.length > 3 && (
        <button
          onClick={() => setExpanded(true)}
          style={{ background: "none", border: "none", color: "#64748b", fontSize: 11, cursor: "pointer", padding: 0 }}
        >
          ... ещё {items.length - 3}
        </button>
      )}
    </div>
  );
}

function SeqStatus({ row: r }: { row: SeqRow }) {
  if (r.source_only) return <span style={badge("#7f1d1d", "#fca5a5")}>Source only</span>;
  if (r.target_only) return <span style={badge("#1e3a5f", "#93c5fd")}>Target only</span>;
  if (r.delta === 0) return <span style={badge("#052e16", "#4ade80")}>OK</span>;
  if (r.delta !== null && r.delta > 0) return <span style={badge("#422006", "#f59e0b")}>Behind</span>;
  if (r.delta !== null && r.delta < 0) return <span style={badge("#1e3a5f", "#3b82f6")}>Ahead</span>;
  return null;
}

function Th({
  label, sortKey, current, dir, onClick, arrow, align,
}: {
  label: string; sortKey: SortKey; current: SortKey; dir: SortDir;
  onClick: (k: SortKey) => void; arrow: (k: SortKey) => string;
  align?: "left" | "right" | "center";
}) {
  return (
    <th style={{ ...thStyle, cursor: "pointer", userSelect: "none", textAlign: align || "left" }} onClick={() => onClick(sortKey)}>
      {label}{arrow(sortKey)}
    </th>
  );
}

// ── Styles ──────────────────────────────────────────────────────────────────

const thStyle: React.CSSProperties = {
  padding: "8px 10px", textAlign: "left", color: "#64748b",
  fontWeight: 600, fontSize: 11, textTransform: "uppercase", letterSpacing: 0.5,
};

const tdStyle: React.CSSProperties = { padding: "6px 10px", color: "#94a3b8" };

function btnStyle(bg: string): React.CSSProperties {
  return {
    background: bg, border: "1px solid #334155", borderRadius: 4,
    color: "#94a3b8", padding: "5px 12px", fontSize: 12, cursor: "pointer", fontWeight: 500,
  };
}

function badge(bg: string, fg: string): React.CSSProperties {
  return { background: bg, color: fg, padding: "2px 8px", borderRadius: 4, fontSize: 10, fontWeight: 600 };
}

function deltaColor(d: number | null): string {
  if (d === null || d === 0) return "#64748b";
  return d > 0 ? "#f59e0b" : "#3b82f6";
}

function resultColor(status: string): string {
  if (status === "ok") return "#4ade80";
  if (status === "skip") return "#94a3b8";
  return "#fca5a5";
}

function fmtNum(n: number | null): string {
  return n !== null ? n.toLocaleString("ru-RU") : "\u2014";
}
