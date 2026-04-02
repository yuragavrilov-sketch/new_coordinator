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

export function Sequences() {
  const [schemas, setSchemas] = useState<string[]>([]);
  const [schema, setSchema] = useState("");
  const [rows, setRows] = useState<SeqRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [delta, setDelta] = useState(1000);
  const [advancing, setAdvancing] = useState(false);
  const [advanceResults, setAdvanceResults] = useState<AdvanceResult[] | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("name");
  const [sortDir, setSortDir] = useState<SortDir>("asc");
  const [filter, setFilter] = useState<"all" | "behind" | "ahead" | "missing">("all");

  // Load schemas
  useEffect(() => {
    fetch("/api/db/source/schemas")
      .then((r) => (r.ok ? r.json() : []))
      .then(setSchemas)
      .catch(() => {});
  }, []);

  // Compare sequences
  const loadSequences = useCallback(() => {
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
        setSelected(new Set());
      })
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
  }, [schema]);

  useEffect(() => {
    loadSequences();
  }, [loadSequences]);

  // Advance sequences
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
        loadSequences();
      })
      .catch((e) => setError(String(e)))
      .finally(() => setAdvancing(false));
  };

  // Filter & sort
  const filtered = rows
    .filter((r) => {
      if (search && !r.sequence_name.toLowerCase().includes(search.toLowerCase())) return false;
      if (filter === "behind") return r.delta !== null && r.delta > 0;
      if (filter === "ahead") return r.delta !== null && r.delta < 0;
      if (filter === "missing") return r.source_only || r.target_only;
      return true;
    })
    .sort((a, b) => {
      const dir = sortDir === "asc" ? 1 : -1;
      switch (sortKey) {
        case "name":
          return a.sequence_name.localeCompare(b.sequence_name) * dir;
        case "source":
          return ((a.source_value ?? 0) - (b.source_value ?? 0)) * dir;
        case "target":
          return ((a.target_value ?? 0) - (b.target_value ?? 0)) * dir;
        case "delta":
          return ((a.delta ?? 0) - (b.delta ?? 0)) * dir;
        default:
          return 0;
      }
    });

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    else {
      setSortKey(key);
      setSortDir("asc");
    }
  };

  const toggleSelect = (name: string) =>
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });

  const toggleAll = () => {
    if (selected.size === filtered.length) setSelected(new Set());
    else setSelected(new Set(filtered.map((r) => r.sequence_name)));
  };

  const behindCount = rows.filter((r) => r.delta !== null && r.delta > 0).length;
  const aheadCount = rows.filter((r) => r.delta !== null && r.delta < 0).length;
  const missingCount = rows.filter((r) => r.source_only || r.target_only).length;

  const sortArrow = (key: SortKey) =>
    sortKey === key ? (sortDir === "asc" ? " \u25B2" : " \u25BC") : "";

  const fmtNum = (n: number | null) =>
    n !== null ? n.toLocaleString("ru-RU") : "\u2014";

  return (
    <div>
      {/* Toolbar */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 12,
          marginBottom: 16,
          flexWrap: "wrap",
        }}
      >
        <span style={{ fontSize: 12, color: "#94a3b8" }}>Schema:</span>
        <SearchSelect
          value={schema}
          onChange={setSchema}
          options={schemas}
          placeholder="Select schema..."
        />
        <button onClick={loadSequences} disabled={!schema || loading} style={btnStyle("#1e293b")}>
          {loading ? "Loading..." : "Refresh"}
        </button>

        <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{ fontSize: 12, color: "#94a3b8" }}>Delta:</span>
          <input
            type="number"
            value={delta}
            onChange={(e) => setDelta(Number(e.target.value))}
            style={{
              background: "#1e293b",
              border: "1px solid #334155",
              borderRadius: 4,
              color: "#e2e8f0",
              padding: "4px 8px",
              width: 100,
              fontSize: 12,
            }}
          />
          <button
            onClick={handleAdvance}
            disabled={!schema || advancing || rows.length === 0}
            style={btnStyle("#1d4ed8")}
          >
            {advancing
              ? "Advancing..."
              : selected.size > 0
                ? `Advance ${selected.size} seq`
                : "Advance all"}
          </button>
        </div>
      </div>

      {/* Stats cards */}
      {rows.length > 0 && (
        <div style={{ display: "flex", gap: 12, marginBottom: 16 }}>
          <StatCard label="Total" value={rows.length} color="#94a3b8" />
          <StatCard
            label="Target behind"
            value={behindCount}
            color={behindCount > 0 ? "#f59e0b" : "#94a3b8"}
          />
          <StatCard
            label="Target ahead"
            value={aheadCount}
            color={aheadCount > 0 ? "#3b82f6" : "#94a3b8"}
          />
          <StatCard
            label="Missing"
            value={missingCount}
            color={missingCount > 0 ? "#ef4444" : "#94a3b8"}
          />
        </div>
      )}

      {error && (
        <div
          style={{
            background: "#7f1d1d",
            color: "#fca5a5",
            padding: "8px 12px",
            borderRadius: 6,
            marginBottom: 12,
            fontSize: 12,
          }}
        >
          {error}
        </div>
      )}

      {/* Advance results */}
      {advanceResults && (
        <div
          style={{
            background: "#0f2a1a",
            border: "1px solid #166534",
            borderRadius: 6,
            padding: 12,
            marginBottom: 12,
            fontSize: 12,
          }}
        >
          <div style={{ color: "#4ade80", fontWeight: 600, marginBottom: 6 }}>
            Advance results:
          </div>
          {advanceResults.map((r) => (
            <div key={r.sequence_name} style={{ color: resultColor(r.status), marginBottom: 2 }}>
              {r.sequence_name}:{" "}
              {r.status === "ok"
                ? `${fmtNum(r.old_target_value ?? null)} -> ${fmtNum(r.new_value ?? null)}`
                : r.status === "skip"
                  ? r.message
                  : r.error}
            </div>
          ))}
        </div>
      )}

      {/* Filter & search */}
      {rows.length > 0 && (
        <div style={{ display: "flex", gap: 8, marginBottom: 10, alignItems: "center" }}>
          {(["all", "behind", "ahead", "missing"] as const).map((f) => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              style={{
                ...btnStyle(filter === f ? "#334155" : "#1e293b"),
                borderColor: filter === f ? "#3b82f6" : "#334155",
                color: filter === f ? "#93c5fd" : "#94a3b8",
              }}
            >
              {f === "all"
                ? "All"
                : f === "behind"
                  ? `Behind (${behindCount})`
                  : f === "ahead"
                    ? `Ahead (${aheadCount})`
                    : `Missing (${missingCount})`}
            </button>
          ))}
          <input
            placeholder="Search..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{
              marginLeft: "auto",
              background: "#1e293b",
              border: "1px solid #334155",
              borderRadius: 4,
              color: "#e2e8f0",
              padding: "4px 8px",
              fontSize: 12,
              width: 200,
            }}
          />
        </div>
      )}

      {/* Table */}
      {filtered.length > 0 && (
        <div
          style={{
            background: "#1e293b",
            border: "1px solid #334155",
            borderRadius: 6,
            overflowX: "auto",
          }}
        >
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              fontSize: 12,
              tableLayout: "fixed",
            }}
          >
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
                  <input
                    type="checkbox"
                    checked={selected.size === filtered.length && filtered.length > 0}
                    onChange={toggleAll}
                  />
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
              {filtered.map((r) => (
                <tr
                  key={r.sequence_name}
                  style={{
                    borderBottom: "1px solid #0f172a",
                    background: selected.has(r.sequence_name) ? "#1e3a5f22" : "transparent",
                  }}
                >
                  <td style={tdStyle}>
                    <input
                      type="checkbox"
                      checked={selected.has(r.sequence_name)}
                      onChange={() => toggleSelect(r.sequence_name)}
                    />
                  </td>
                  <td
                    style={{
                      ...tdStyle,
                      fontFamily: "monospace",
                      color: "#e2e8f0",
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
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
                  <td
                    style={{
                      ...tdStyle,
                      textAlign: "right",
                      fontFamily: "monospace",
                      color: deltaColor(r.delta),
                      fontWeight: r.delta !== null && r.delta !== 0 ? 600 : 400,
                    }}
                  >
                    {r.delta !== null
                      ? (r.delta > 0 ? "+" : "") + r.delta.toLocaleString("ru-RU")
                      : "\u2014"}
                  </td>
                  <td style={{ ...tdStyle, textAlign: "center", color: "#64748b" }}>
                    {r.increment_by}
                  </td>
                  <td style={{ ...tdStyle, textAlign: "center", color: "#64748b" }}>
                    {r.cache_size}
                  </td>
                  <td style={{ ...tdStyle, textAlign: "center" }}>
                    {r.source_only && (
                      <span style={badge("#7f1d1d", "#fca5a5")}>Source only</span>
                    )}
                    {r.target_only && (
                      <span style={badge("#1e3a5f", "#93c5fd")}>Target only</span>
                    )}
                    {!r.source_only && !r.target_only && r.delta === 0 && (
                      <span style={badge("#052e16", "#4ade80")}>Synced</span>
                    )}
                    {!r.source_only &&
                      !r.target_only &&
                      r.delta !== null &&
                      r.delta > 0 && (
                        <span style={badge("#422006", "#f59e0b")}>Behind</span>
                      )}
                    {!r.source_only &&
                      !r.target_only &&
                      r.delta !== null &&
                      r.delta < 0 && (
                        <span style={badge("#1e3a5f", "#3b82f6")}>Ahead</span>
                      )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {!loading && rows.length === 0 && schema && (
        <div style={{ color: "#475569", textAlign: "center", padding: 40, fontSize: 13 }}>
          No sequences found for schema {schema}
        </div>
      )}
    </div>
  );
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function StatCard({ label, value, color }: { label: string; value: number; color: string }) {
  return (
    <div
      style={{
        background: "#0f172a",
        border: "1px solid #334155",
        borderRadius: 6,
        padding: "8px 16px",
        minWidth: 100,
        textAlign: "center",
      }}
    >
      <div style={{ fontSize: 18, fontWeight: 700, color }}>{value}</div>
      <div style={{ fontSize: 10, color: "#64748b", marginTop: 2 }}>{label}</div>
    </div>
  );
}

function Th({
  label,
  sortKey,
  current,
  dir,
  onClick,
  arrow,
  align,
}: {
  label: string;
  sortKey: SortKey;
  current: SortKey;
  dir: SortDir;
  onClick: (k: SortKey) => void;
  arrow: (k: SortKey) => string;
  align?: "left" | "right" | "center";
}) {
  return (
    <th
      style={{ ...thStyle, cursor: "pointer", userSelect: "none", textAlign: align || "left" }}
      onClick={() => onClick(sortKey)}
    >
      {label}
      {arrow(sortKey)}
    </th>
  );
}

const thStyle: React.CSSProperties = {
  padding: "8px 10px",
  textAlign: "left",
  color: "#64748b",
  fontWeight: 600,
  fontSize: 11,
  textTransform: "uppercase",
  letterSpacing: 0.5,
};

const tdStyle: React.CSSProperties = {
  padding: "6px 10px",
  color: "#94a3b8",
};

function btnStyle(bg: string): React.CSSProperties {
  return {
    background: bg,
    border: "1px solid #334155",
    borderRadius: 4,
    color: "#94a3b8",
    padding: "5px 12px",
    fontSize: 12,
    cursor: "pointer",
    fontWeight: 500,
  };
}

function badge(bg: string, fg: string): React.CSSProperties {
  return {
    background: bg,
    color: fg,
    padding: "2px 8px",
    borderRadius: 4,
    fontSize: 10,
    fontWeight: 600,
  };
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
