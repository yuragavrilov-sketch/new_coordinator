import React, { useEffect, useState } from "react";
import { useSSE } from "./hooks/useSSE";
import { EventTable } from "./components/EventTable";
import { Stats } from "./components/Stats";
import { StatusBadge } from "./components/StatusBadge";

const BACKEND = "http://127.0.0.1:5000";
const SSE_URL = `${BACKEND}/api/events`;

type BackendStatus = "checking" | "ok" | "unreachable";

function useBackendHealth(url: string): BackendStatus {
  const [s, setS] = useState<BackendStatus>("checking");
  useEffect(() => {
    let cancelled = false;
    const check = () =>
      fetch(`${url}/api/health`, { signal: AbortSignal.timeout(3000) })
        .then((r) => { if (!cancelled) setS(r.ok ? "ok" : "unreachable"); })
        .catch(() => { if (!cancelled) setS("unreachable"); });
    check();
    const id = setInterval(check, 5000);
    return () => { cancelled = true; clearInterval(id); };
  }, [url]);
  return s;
}

export default function App() {
  const { events, status, clear } = useSSE({ url: SSE_URL });
  const backendStatus = useBackendHealth(BACKEND);
  const [filter, setFilter] = useState("");
  const [paused, setPaused] = useState(false);
  const [frozen, setFrozen] = useState(events);
  const displayed = paused ? frozen : events;

  function togglePause() {
    if (!paused) setFrozen(events);
    setPaused((p) => !p);
  }

  return (
    <div
      style={{
        minHeight: "100vh",
        background: "#0f172a",
        color: "#e2e8f0",
        fontFamily: "'Inter', 'Segoe UI', sans-serif",
        padding: 24,
      }}
    >
      <style>{`
        @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.4} }
        @keyframes fadeIn { from{opacity:0;transform:translateY(-4px)} to{opacity:1;transform:none} }
        * { box-sizing: border-box; }
        input { outline: none; }
      `}</style>

      {/* Backend unreachable banner */}
      {backendStatus === "unreachable" && (
        <div style={{
          background: "#7f1d1d", color: "#fca5a5", padding: "10px 16px",
          borderRadius: 6, marginBottom: 16, fontSize: 13,
        }}>
          Flask backend unreachable at <code>{BACKEND}</code> — запусти: <code>python backend/app.py</code>
        </div>
      )}

      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", gap: 16, marginBottom: 24 }}>
        <h1 style={{ margin: 0, fontSize: 20, fontWeight: 700, letterSpacing: -0.5 }}>
          CDC UI
        </h1>
        <StatusBadge status={status} />
        <div style={{ marginLeft: "auto", display: "flex", gap: 8 }}>
          <button onClick={togglePause} style={btnStyle(paused ? "#3b82f6" : "#334155")}>
            {paused ? "▶ Resume" : "⏸ Pause"}
          </button>
          <button onClick={clear} style={btnStyle("#334155")}>
            Clear
          </button>
        </div>
      </div>

      {/* Stats */}
      <Stats events={displayed} />

      {/* Filter */}
      <div style={{ margin: "16px 0" }}>
        <input
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="Filter by table, schema, or operation…"
          style={{
            background: "#1e293b",
            border: "1px solid #334155",
            borderRadius: 6,
            color: "#e2e8f0",
            padding: "7px 12px",
            fontSize: 13,
            width: "100%",
            maxWidth: 400,
          }}
        />
      </div>

      {/* Event table */}
      <div
        style={{
          background: "#0f172a",
          border: "1px solid #1e293b",
          borderRadius: 8,
          overflow: "hidden",
        }}
      >
        <EventTable events={displayed} filter={filter} />
      </div>

      <div style={{ marginTop: 12, color: "#334155", fontSize: 11 }}>
        Showing {displayed.length} events · SSE → {SSE_URL}
      </div>
    </div>
  );
}

function btnStyle(bg: string): React.CSSProperties {
  return {
    background: bg,
    border: "none",
    borderRadius: 6,
    color: "#e2e8f0",
    padding: "6px 14px",
    fontSize: 13,
    cursor: "pointer",
    fontWeight: 600,
  };
}
