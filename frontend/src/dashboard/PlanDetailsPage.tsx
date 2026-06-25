import React, { useCallback, useEffect, useState } from "react";
import { useApi } from "../hooks/useApi";
import type { SSEEvent } from "../hooks/useSSE";
import { t } from "../theme";
import { secondaryActionStyle } from "./buttonStyles";
import { PlanPanel } from "./PlanPanel";
import { startMigrationPlan, type MigrationPlanDetail, type SchemaMigrationListItem } from "./api";

interface Props {
  schema: SchemaMigrationListItem | null;
  planId: number | null;
  onBack: () => void;
  sseEvents: SSEEvent[];
}

export function PlanDetailsPage({ schema, planId, onBack, sseEvents }: Props) {
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");
  const planApi = useApi<MigrationPlanDetail>(
    planId ? `/api/planner/plans/${planId}` : null,
    { intervalMs: 5000 },
  );

  const handleStartPlan = useCallback(async () => {
    if (!planId) return;
    setBusy(true);
    setErr("");
    try {
      await startMigrationPlan(planId);
      planApi.reload();
    } catch (e) {
      setErr(String(e instanceof Error ? e.message : e));
    } finally {
      setBusy(false);
    }
  }, [planId, planApi]);

  useEffect(() => {
    const event = sseEvents[0];
    if (!event || !planId) return;
    if (
      event.type === "migration_phase"
      || event.type === "connector_group_status"
      || event.type === "target_trigger_job"
      || (event.type === "schema_migration.plan_items_added" && event.plan_id === planId)
    ) {
      planApi.reload();
    }
  }, [sseEvents, planId, planApi.reload]);

  return (
    <div>
      <div style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "flex-start",
        gap: 12,
        marginBottom: 14,
      }}>
        <div>
          <div style={{ fontSize: 20, fontWeight: 700, color: t.text.primary }}>Пачка таблиц</div>
          <div style={{
            marginTop: 4,
            color: t.text.muted,
            fontSize: 12,
            fontFamily: t.font.mono,
          }}>
            {schema ? `${schema.src_schema || "-"} -> ${schema.tgt_schema || "-"} · ${schema.id.slice(0, 8)}` : "Миграция не выбрана"}
          </div>
        </div>
        <button onClick={onBack} style={secondaryActionStyle(false)}>К таблицам</button>
      </div>

      <PlanPanel
        plan={planId ? (planApi.data || null) : null}
        loading={!!planId && planApi.loading}
        onStart={handleStartPlan}
        onReload={() => planApi.reload()}
        busy={busy}
        error={err || planApi.error || ""}
        variant="detail"
        sseEvents={sseEvents}
      />
    </div>
  );
}
