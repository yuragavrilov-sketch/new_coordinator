import React, { useEffect, useState } from "react";
import { useSSE } from "./hooks/useSSE";
import { ServiceStatusBar } from "./components/ServiceStatusBar";
import { SettingsModal } from "./components/SettingsModal";
import { MigrationList } from "./components/MigrationList";
import { TargetPrep } from "./components/TargetPrep";
import { StatusBadge } from "./components/StatusBadge";

const SSE_URL = "/api/events";

type BackendStatus = "checking" | "ok" | "unreachable";
type Tab = "migrations" | "target-prep";

function useBackendHealth(): BackendStatus {
  const [s, setS] = useState<BackendStatus>("checking");
  useEffect(() => {
    let cancelled = false;
    const check = () =>
      fetch("/api/health", { signal: AbortSignal.timeout(3000) })
        .then(r => { if (!cancelled) setS(r.ok ? "ok" : "unreachable"); })
        .catch(() => { if (!cancelled) setS("unreachable"); });
    check();
    const id = setInterval(check, 5000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);
  return s;
}

const ACTIVE_PHASES_SET = new Set([
  "NEW", "PREPARING", "SCN_FIXED", "CONNECTOR_STARTING", "CDC_BUFFERING",
  "CHUNKING", "BULK_LOADING", "BULK_LOADED",
  "STAGE_VALIDATING", "STAGE_VALIDATED",
  "BASELINE_PUBLISHING", "BASELINE_PUBLISHED",
  "CDC_APPLY_STARTING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP",
  "STEADY_STATE",
]);

function SystemStats() {
  const [stats, setStats] = useState<{ active: number; total: number; rows: number } | null>(null);

  useEffect(() => {
    const load = () => {
      fetch("/api/migrations")
        .then(r => r.ok ? r.json() : Promise.reject())
        .then((data: { phase: string; rows_loaded: number }[]) => {
          const active = data.filter(m => ACTIVE_PHASES_SET.has(m.phase)).length;
          const rows   = data.reduce((acc, m) => acc + (m.rows_loaded || 0), 0);
          setStats({ active, total: data.length, rows });
        })
        .catch(() => {});
    };
    load();
    const id = setInterval(load, 10_000);
    return () => clearInterval(id);
  }, []);

  if (!stats) return null;

  return (
    <div style={{
      display: "flex", gap: 16, fontSize: 11, color: "#475569",
      alignItems: "center",
    }}>
      <span>
        Активных:{" "}
        <strong style={{ color: stats.active > 0 ? "#22c55e" : "#475569" }}>
          {stats.active}
        </strong>
      </span>
      <span>
        Всего:{" "}
        <strong style={{ color: "#64748b" }}>{stats.total}</strong>
      </span>
      {stats.rows > 0 && (
        <span>
          Строк:{" "}
          <strong style={{ color: "#64748b" }}>
            {stats.rows.toLocaleString("ru-RU")}
          </strong>
        </span>
      )}
    </div>
  );
}

export default function App() {
  const { events, status, serviceStatuses, reconnect } = useSSE({ url: SSE_URL });
  const backendStatus = useBackendHealth();
  const [activeTab, setActiveTab] = useState<Tab>("migrations");
  const [showSettings, setShowSettings] = useState(false);

  return (
    <div style={{
      minHeight: "100vh",
      background: "#0f172a",
      color: "#e2e8f0",
      fontFamily: "'Inter', 'Segoe UI', sans-serif",
      padding: 24,
    }}>
      <style>{`
        @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.4} }
        @keyframes fadeIn { from{opacity:0;transform:translateY(-4px)} to{opacity:1;transform:none} }
        * { box-sizing: border-box; }
        input { outline: none; }
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-track { background: #0a111f; }
        ::-webkit-scrollbar-thumb { background: #1e293b; border-radius: 3px; }
        ::-webkit-scrollbar-thumb:hover { background: #334155; }
      `}</style>

      {showSettings && <SettingsModal onClose={() => setShowSettings(false)} />}

      {/* Backend unreachable banner */}
      {backendStatus === "unreachable" && (
        <div style={{
          background: "#7f1d1d", color: "#fca5a5", padding: "10px 16px",
          borderRadius: 6, marginBottom: 16, fontSize: 13,
        }}>
          Flask backend недоступен — запусти: <code>python backend/app.py</code>
        </div>
      )}

      {/* Service status bar */}
      <ServiceStatusBar statuses={serviceStatuses} />

      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 0 }}>
        <h1 style={{ margin: 0, fontSize: 18, fontWeight: 700, letterSpacing: -0.5, color: "#e2e8f0" }}>
          DB Migration
        </h1>
        <StatusBadge status={status} onReconnect={reconnect} />
        <SystemStats />
        <div style={{ marginLeft: "auto", display: "flex", gap: 8 }}>
          <button onClick={() => setShowSettings(true)} style={btnStyle("#1e293b")} title="Connection settings">
            ⚙ Настройки
          </button>
        </div>
      </div>

      {/* Tab bar */}
      <div style={{
        display: "flex",
        gap: 0,
        marginTop: 16,
        borderBottom: "1px solid #1e293b",
      }}>
        <TabButton
          label="Миграции"
          active={activeTab === "migrations"}
          onClick={() => setActiveTab("migrations")}
        />
        <TabButton
          label="Подготовка таргета"
          active={activeTab === "target-prep"}
          onClick={() => setActiveTab("target-prep")}
        />
      </div>

      {/* Tab content */}
      <div style={{ marginTop: 16 }}>
        {activeTab === "migrations"   && <MigrationList sseEvents={events} />}
        {activeTab === "target-prep"  && <TargetPrep />}
      </div>
    </div>
  );
}

function TabButton({ label, active, onClick }: { label: string; active: boolean; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      style={{
        background: "none",
        border: "none",
        borderBottom: `2px solid ${active ? "#3b82f6" : "transparent"}`,
        color: active ? "#93c5fd" : "#475569",
        padding: "8px 16px",
        fontSize: 13,
        fontWeight: active ? 700 : 500,
        cursor: "pointer",
        marginBottom: -1,
        transition: "color 0.15s, border-color 0.15s",
      }}
    >
      {label}
    </button>
  );
}

function btnStyle(bg: string): React.CSSProperties {
  return {
    background: bg,
    border: "1px solid #334155",
    borderRadius: 6,
    color: "#94a3b8",
    padding: "5px 12px",
    fontSize: 12,
    cursor: "pointer",
    fontWeight: 500,
  };
}
