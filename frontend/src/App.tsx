import React, { useEffect, useState } from "react";
import { useSSE } from "./hooks/useSSE";
import { SettingsModal } from "./components/SettingsModal";
import { MigrationList } from "./components/MigrationList";
import { ConnectorGroupsPanel } from "./components/ConnectorGroupsPanel";
import { Sidebar, type NavKey } from "./shell/Sidebar";
import { RightRail } from "./shell/RightRail";
import { RulesTabs } from "./shell/RulesTabs";
import { Dashboard } from "./dashboard/Dashboard";
import { schemaInfo, initialMetrics, initialEvents } from "./dashboard/mockData";
import { t } from "./theme";

const SSE_URL = "/api/events";

type BackendStatus = "checking" | "ok" | "unreachable";

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

export default function App() {
  const { events: sseEvents } = useSSE({ url: SSE_URL });
  const backendStatus = useBackendHealth();
  const [nav, setNav] = useState<NavKey>("dashboard");
  const [showSettings, setShowSettings] = useState(false);

  // Settings is special — clicking sidebar opens modal, doesn't switch view
  const onNavChange = (key: NavKey) => {
    if (key === "settings") { setShowSettings(true); return; }
    setNav(key);
  };

  return (
    <div style={{
      minHeight: "100vh",
      background: t.bg.app,
      color: t.text.primary,
      fontFamily: t.font.sans,
      display: "grid",
      gridTemplateColumns: "224px minmax(0, 1fr) 312px",
    }}>
      <Sidebar
        active={nav}
        onChange={onNavChange}
        schemaName={schemaInfo.name}
        migrationId={schemaInfo.id}
        userName="Антон Волков"
        userInitials="АВ"
        userRole="DBA · admin"
      />

      <main style={{ padding: "18px 22px", minWidth: 0 }}>
        {backendStatus === "unreachable" && (
          <div style={{
            background: t.tone.errorSoft,
            color: t.tone.error,
            padding: "10px 16px",
            borderRadius: t.radius.sm,
            marginBottom: 16, fontSize: 13,
            border: `1px solid color-mix(in oklab, ${t.tone.error} 26%, transparent)`,
          }}>
            Flask backend недоступен — запусти: <code>python backend/app.py</code>
          </div>
        )}

        {nav === "dashboard" && <Dashboard/>}
        {nav === "history"   && <MigrationList sseEvents={sseEvents}/>}
        {nav === "clusters"  && <ConnectorGroupsPanel/>}
        {nav === "rules"     && <RulesTabs/>}
      </main>

      <RightRail
        schemaName={schemaInfo.name}
        metrics={initialMetrics}
        events={initialEvents}
      />

      {showSettings && <SettingsModal onClose={() => setShowSettings(false)}/>}
    </div>
  );
}
