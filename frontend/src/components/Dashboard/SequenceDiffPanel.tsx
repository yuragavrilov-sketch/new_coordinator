import { useState, useEffect, useCallback } from "react";

interface SeqRow {
  sequence_name: string;
  source_value: number | null;
  target_value: number | null;
  delta: number | null;
  increment_by: number;
  source_only: boolean;
  target_only: boolean;
}

interface Props {
  schema: string;
}

export function SequenceDiffPanel({ schema }: Props) {
  const [rows, setRows] = useState<SeqRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showAll, setShowAll] = useState(false);

  const load = useCallback(() => {
    if (!schema) return;
    setLoading(true);
    setError(null);
    fetch(`/api/sequences/compare?schema=${schema}`)
      .then((r) => (r.ok ? r.json() : r.json().then((d) => Promise.reject(d.error))))
      .then(setRows)
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
  }, [schema]);

  useEffect(() => { load(); }, [load]);

  const diffs = rows.filter(
    (r) => (r.delta !== null && r.delta !== 0) || r.source_only || r.target_only,
  );
  const behind = diffs.filter((r) => r.delta !== null && r.delta > 0);
  const ahead = diffs.filter((r) => r.delta !== null && r.delta < 0);
  const missing = diffs.filter((r) => r.source_only || r.target_only);

  const display = showAll ? diffs : diffs.slice(0, 10);

  const fmtNum = (n: number | null) =>
    n !== null ? n.toLocaleString("ru-RU") : "\u2014";

  if (loading) {
    return (
      <div style={{ ...panelStyle, textAlign: "center", color: "#64748b" }}>
        Сравниваем сиквенсы...
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ ...panelStyle, background: "#1c1117", borderColor: "#7f1d1d" }}>
        <span style={{ color: "#fca5a5", fontSize: 12 }}>Ошибка: {error}</span>
        <button onClick={load} style={retryBtn}>Повторить</button>
      </div>
    );
  }

  if (rows.length === 0) return null;

  return (
    <div style={panelStyle}>
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: diffs.length > 0 ? 10 : 0 }}>
        <span style={{ fontSize: 13, fontWeight: 700, color: diffs.length > 0 ? "#fbbf24" : "#4ade80" }}>
          {diffs.length > 0
            ? `Сиквенсы: ${diffs.length} расхождений`
            : `Сиквенсы: все ${rows.length} синхронизированы`}
        </span>
        {diffs.length > 0 && (
          <div style={{ display: "flex", gap: 8, fontSize: 11, color: "#64748b" }}>
            {behind.length > 0 && <span>отстает: <b style={{ color: "#f59e0b" }}>{behind.length}</b></span>}
            {ahead.length > 0 && <span>впереди: <b style={{ color: "#3b82f6" }}>{ahead.length}</b></span>}
            {missing.length > 0 && <span>нет: <b style={{ color: "#ef4444" }}>{missing.length}</b></span>}
          </div>
        )}
        <button onClick={load} style={{ ...retryBtn, marginLeft: "auto" }}>Обновить</button>
      </div>

      {/* Diff table */}
      {diffs.length > 0 && (
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
            <thead>
              <tr style={{ borderBottom: "1px solid #334155" }}>
                <th style={thS}>Sequence</th>
                <th style={{ ...thS, textAlign: "right" }}>Source</th>
                <th style={{ ...thS, textAlign: "right" }}>Target</th>
                <th style={{ ...thS, textAlign: "right" }}>Delta</th>
                <th style={{ ...thS, textAlign: "center" }}>Status</th>
              </tr>
            </thead>
            <tbody>
              {display.map((r) => (
                <tr key={r.sequence_name} style={{ borderBottom: "1px solid #1e293b" }}>
                  <td style={{ ...tdS, fontFamily: "monospace", color: "#e2e8f0" }}>{r.sequence_name}</td>
                  <td style={{ ...tdS, textAlign: "right", fontFamily: "monospace" }}>{fmtNum(r.source_value)}</td>
                  <td style={{ ...tdS, textAlign: "right", fontFamily: "monospace" }}>{fmtNum(r.target_value)}</td>
                  <td style={{
                    ...tdS, textAlign: "right", fontFamily: "monospace",
                    color: r.delta !== null && r.delta > 0 ? "#f59e0b" : r.delta !== null && r.delta < 0 ? "#3b82f6" : "#64748b",
                    fontWeight: 600,
                  }}>
                    {r.delta !== null ? (r.delta > 0 ? "+" : "") + r.delta.toLocaleString("ru-RU") : "\u2014"}
                  </td>
                  <td style={{ ...tdS, textAlign: "center" }}>
                    {r.source_only && <span style={badgeS("#7f1d1d", "#fca5a5")}>Source only</span>}
                    {r.target_only && <span style={badgeS("#1e3a5f", "#93c5fd")}>Target only</span>}
                    {r.delta !== null && r.delta > 0 && <span style={badgeS("#422006", "#f59e0b")}>Behind</span>}
                    {r.delta !== null && r.delta < 0 && <span style={badgeS("#1e3a5f", "#3b82f6")}>Ahead</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {!showAll && diffs.length > 10 && (
            <button
              onClick={() => setShowAll(true)}
              style={{ background: "none", border: "none", color: "#64748b", fontSize: 11, cursor: "pointer", padding: "6px 0" }}
            >
              Показать все ({diffs.length})...
            </button>
          )}
        </div>
      )}
    </div>
  );
}

const panelStyle: React.CSSProperties = {
  background: "#0f172a",
  border: "1px solid #334155",
  borderRadius: 8,
  padding: "12px 16px",
  marginBottom: 16,
};

const thS: React.CSSProperties = {
  padding: "4px 8px", textAlign: "left", color: "#475569",
  fontWeight: 600, fontSize: 10, textTransform: "uppercase",
};

const tdS: React.CSSProperties = { padding: "4px 8px", color: "#94a3b8" };

function badgeS(bg: string, fg: string): React.CSSProperties {
  return { background: bg, color: fg, padding: "1px 6px", borderRadius: 3, fontSize: 9, fontWeight: 600 };
}

const retryBtn: React.CSSProperties = {
  background: "#1e293b", border: "1px solid #334155", borderRadius: 4,
  color: "#94a3b8", padding: "3px 10px", fontSize: 11, cursor: "pointer",
};
