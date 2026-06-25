import React, { useEffect, useState } from "react";
import { useSSE } from "./hooks/useSSE";
import { useApi } from "./hooks/useApi";
import { SettingsModal } from "./components/SettingsModal";
import { MigrationList } from "./components/MigrationList";
import { ConnectorGroupsPanel } from "./components/ConnectorGroupsPanel";
import { Sidebar, type NavKey } from "./shell/Sidebar";
import { RightRail } from "./shell/RightRail";
import { RulesTabs } from "./shell/RulesTabs";
import { Dashboard } from "./dashboard/Dashboard";
import { PlanDetailsPage } from "./dashboard/PlanDetailsPage";
import { initialMetrics } from "./dashboard/mockData";
import type { SchemaMigrationListItem } from "./dashboard/api";
import type { MigrationEvent, LiveMetrics } from "./dashboard/types";
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
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [planOverride, setPlanOverride] = useState<{ schemaId: string; planId: number } | null>(null);

  // Schema migration list — auto-poll every 10s
  const listApi = useApi<SchemaMigrationListItem[]>("/api/schema-migrations", {
    intervalMs: 10_000,
  });
  const list = listApi.data || [];

  // Auto-select first schema migration when list arrives and nothing selected
  useEffect(() => {
    if (selectedId === null && list.length > 0) {
      setSelectedId(list[0].id);
    }
  }, [list, selectedId]);

  const selectedSchema: SchemaMigrationListItem | null =
    list.find(s => s.id === selectedId) || null;
  const selectedPlanId =
    planOverride?.schemaId === selectedId ? planOverride.planId : selectedSchema?.planId ?? null;

  // Events for right rail (filtered to selected schema)
  const eventsApi = useApi<MigrationEvent[]>(
    selectedId ? `/api/schema-migrations/${selectedId}/events?limit=50` : null,
    { intervalMs: 5000 },
  );
  const metricsApi = useApi<LiveMetrics>(
    selectedId ? `/api/schema-migrations/${selectedId}/metrics` : null,
    { intervalMs: 5000 },
  );
  const rightRailEvents  = eventsApi.data  || [];
  const rightRailMetrics = metricsApi.data || initialMetrics;

  useEffect(() => {
    const event = sseEvents[0];
    if (!event) return;

    if (event.type === "schema_migration.plan_items_added") {
      listApi.reload();
      if (event.id === selectedId) eventsApi.reload();
      return;
    }

    if (event.type === "connector_group_status") {
      listApi.reload();
      return;
    }

    if (event.type === "migration_phase") {
      listApi.reload();
      if (selectedId) eventsApi.reload();
    }
  }, [sseEvents, selectedId, listApi.reload, eventsApi.reload]);

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
        schemaName={selectedSchema?.name || "—"}
        migrationId={selectedSchema ? selectedSchema.id.slice(0, 8) : "—"}
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

        {nav === "dashboard" && (
          <Dashboard
            selectedId={selectedId}
            schema={selectedSchema}
            planId={selectedPlanId}
            onCreated={id => { setSelectedId(id); listApi.reload(); }}
            onPlanChanged={planId => {
              if (selectedId) setPlanOverride({ schemaId: selectedId, planId });
              listApi.reload();
            }}
            onOpenPlan={() => setNav("plan")}
            showEmptyState={backendStatus === "ok" && !listApi.loading && list.length === 0}
            sseEvents={sseEvents}
          />
        )}
        {nav === "plan" && (
          <PlanDetailsPage
            schema={selectedSchema}
            planId={selectedPlanId}
            onBack={() => setNav("dashboard")}
            sseEvents={sseEvents}
          />
        )}
        {nav === "history"   && <MigrationList sseEvents={sseEvents}/>}
        {nav === "clusters"  && <ConnectorGroupsPanel/>}
        {nav === "rules"     && <RulesTabs/>}
      </main>

      <RightRail
        schemaName={selectedSchema?.name || "—"}
        metrics={rightRailMetrics}
        events={rightRailEvents}
      />

      {showSettings && <SettingsModal onClose={() => setShowSettings(false)}/>}
    </div>
  );
}
