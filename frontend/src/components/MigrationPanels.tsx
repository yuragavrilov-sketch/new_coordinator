/**
 * Phase-aware monitoring panels shown inside MigrationDetail.
 *
 * BulkProgressPanel   — chunk stats (CHUNKING / BULK_LOADING / BULK_LOADED)
 * ConnectorPanel      — Debezium connector status (CONNECTOR_STARTING … CDC_BUFFERING)
 * KafkaLagPanel       — consumer group lag (CDC_APPLY_STARTING … STEADY_STATE)
 * ValidationPanel     — stage validation result (STAGE_VALIDATING / STAGE_VALIDATED+)
 */

import { useEffect } from "react";
import type { SSEEvent } from "../hooks/useSSE";
import { Panel, Row, Button, Badge } from "./ui";
import { t } from "../theme";
import { useApi } from "../hooks/useApi";
import { fmtTs } from "../utils/format";

// ── BulkProgressPanel ─────────────────────────────────────────────────────────

interface ChunkStatsResp {
  stats: {
    total: number; pending: number; claimed: number;
    running: number; done: number; failed: number;
    rows_loaded: number;
  };
}

export function BulkProgressPanel({
  migrationId, sseEvents, chunkType = "BULK",
}: { migrationId: string; sseEvents: SSEEvent[]; chunkType?: "BULK" | "BASELINE" }) {
  const { data, loading, reload } = useApi<ChunkStatsResp>(
    `/api/migrations/${migrationId}/chunks?chunk_type=${chunkType}&page=1&page_size=1`,
    { intervalMs: 5_000, deps: [migrationId, chunkType] },
  );

  const isBaseline = chunkType === "BASELINE";
  const accent     = isBaseline ? t.purple.base : t.amber.dim;
  const panelTitle = isBaseline ? "Baseline Load Progress" : "Bulk Load Progress";

  useEffect(() => {
    const last = sseEvents[0];
    if (!last) return;
    if (
      ("migration_id" in last && last.migration_id === migrationId) &&
      (last.type === "chunk_progress" || last.type === "migration_phase" ||
       last.type === "baseline_progress")
    ) {
      reload();
    }
  }, [sseEvents, migrationId, reload]);

  async function retryFailed() {
    await fetch(
      `/api/migrations/${migrationId}/retry-chunks?chunk_type=${chunkType}`,
      { method: "POST" },
    );
    reload();
  }

  if (loading || !data?.stats || data.stats.total === 0) return null;
  const stats  = data.stats;
  const pct    = stats.total > 0 ? Math.round((stats.done / stats.total) * 100) : 0;
  const active = stats.claimed + stats.running;

  return (
    <Panel accent={accent} title={panelTitle} style={{ marginBottom: t.space[4] }}>
      {/* Progress bar */}
      <div style={{ marginBottom: t.space[3] }}>
        <div style={{
          background: t.bg.s2, borderRadius: t.radius.sm, height: 8, overflow: "hidden",
        }}>
          <div style={{
            width:      `${pct}%`,
            height:     "100%",
            background: stats.failed > 0 ? t.red.base : t.green.base,
            transition: "width 0.4s",
          }} />
        </div>
        <div style={{
          display: "flex", justifyContent: "space-between",
          fontSize: t.size.sm, color: t.text.muted, marginTop: 4,
        }}>
          <span>{stats.done} / {stats.total} чанков · {stats.rows_loaded.toLocaleString()} строк</span>
          <span style={{ color: pct === 100 ? t.green.base : t.amber.fg, fontWeight: 700 }}>
            {pct}%
          </span>
        </div>
      </div>

      <div style={{ display: "flex", gap: t.space[3], flexWrap: "wrap", alignItems: "flex-end" }}>
        {[
          { label: "Ожидают", value: stats.pending, color: t.text.muted   },
          { label: "Активны", value: active,        color: t.amber.fg     },
          { label: "Готово",  value: stats.done,    color: t.green.base   },
          { label: "Ошибки",  value: stats.failed,  color: t.red.base     },
        ].map(s => (
          <div key={s.label} style={{ textAlign: "center" }}>
            <div style={{ fontSize: 16, fontWeight: 700, color: s.color }}>{s.value}</div>
            <div style={{ fontSize: t.size.xs, color: t.text.disabled }}>{s.label}</div>
          </div>
        ))}
        {stats.failed > 0 && (
          <Button
            variant="danger"
            size="sm"
            onClick={retryFailed}
            style={{ marginLeft: "auto" }}
          >
            ↺ Повторить ошибки ({stats.failed})
          </Button>
        )}
      </div>
    </Panel>
  );
}

// ── ConnectorPanel ────────────────────────────────────────────────────────────

interface ConnectorData {
  connector_name: string;
  status:         string;
}

export function ConnectorPanel({
  migrationId, sseEvents,
}: { migrationId: string; sseEvents: SSEEvent[] }) {
  const { data, loading, reload } = useApi<ConnectorData>(
    `/api/migrations/${migrationId}/connector`,
    { intervalMs: 10_000, deps: [migrationId] },
  );

  useEffect(() => {
    const last = sseEvents[0];
    if ((last && "migration_id" in last && last.migration_id === migrationId) &&
        (last.type === "connector_status" || last.type === "migration_phase")) {
      reload();
    }
  }, [sseEvents, migrationId, reload]);

  if (loading || !data) return null;

  const statusColor = data.status === "RUNNING"   ? t.green.base
                    : data.status === "FAILED"    ? t.red.base
                    : data.status === "NOT_FOUND" ? t.text.muted
                                                  : t.amber.fg;

  return (
    <Panel accent={t.purple.base} title="Debezium Connector" style={{ marginBottom: t.space[4] }}>
      <Row label="Коннектор" mono value={data.connector_name || "—"} />
      <Row label="Статус" value={
        <span style={{ fontWeight: 700, color: statusColor }}>{data.status}</span>
      } />
    </Panel>
  );
}

// ── KafkaLagPanel ─────────────────────────────────────────────────────────────

interface LagData {
  total_lag:        number | null;
  lag_by_partition: Record<string, number> | null;
  worker_id:        string | null;
  worker_heartbeat: string | null;
  updated_at:       string | null;
  rows_applied:     number | null;
}

export function KafkaLagPanel({
  migrationId, sseEvents,
}: { migrationId: string; sseEvents: SSEEvent[] }) {
  const { data, loading, reload } = useApi<LagData>(
    `/api/migrations/${migrationId}/lag`,
    { intervalMs: 5_000, deps: [migrationId] },
  );

  useEffect(() => {
    const last = sseEvents[0];
    if ((last && "migration_id" in last && last.migration_id === migrationId) &&
        (last.type === "kafka_lag" || last.type === "migration_phase")) {
      reload();
    }
  }, [sseEvents, migrationId, reload]);

  if (loading || !data) return null;

  const lag = data.total_lag ?? null;
  const lagColor =
    lag === 0                           ? t.green.base
    : lag !== null && lag < 10_000      ? t.amber.fg
                                        : t.red.base;
  const parts = data.lag_by_partition ?? {};

  return (
    <Panel accent={t.amber.dim} title="Kafka Lag" style={{ marginBottom: t.space[4] }}>
      <div style={{ display: "flex", alignItems: "baseline", gap: t.space[2], marginBottom: t.space[2] }}>
        <span style={{
          fontSize:           28,
          fontWeight:         800,
          color:              lagColor,
          fontVariantNumeric: "tabular-nums",
        }}>
          {lag !== null ? lag.toLocaleString() : "—"}
        </span>
        <span style={{ fontSize: t.size.sm, color: t.text.muted }}>messages behind</span>
        {lag === 0 && <Badge tone="success" size="sm">CAUGHT UP</Badge>}
      </div>

      {Object.keys(parts).length > 0 && (
        <div style={{ marginBottom: t.space[2] }}>
          {Object.entries(parts).map(([k, v]) => (
            <div key={k} style={{
              display: "flex", justifyContent: "space-between",
              fontSize: t.size.xs, color: t.text.muted, marginBottom: 2,
            }}>
              <span>{k}</span>
              <span style={{ color: v === 0 ? t.green.base : t.amber.fg }}>
                {v.toLocaleString()}
              </span>
            </div>
          ))}
        </div>
      )}

      {data.rows_applied !== null && data.rows_applied > 0 && (
        <Row label="Применено строк" value={
          <span style={{ color: t.green.fg, fontVariantNumeric: "tabular-nums" }}>
            {data.rows_applied.toLocaleString()}
          </span>
        } />
      )}
      <Row label="Worker"    value={data.worker_id ?? "—"} />
      <Row label="Heartbeat" value={fmtTs(data.worker_heartbeat, { timeOnly: true, withSeconds: true })} />
      <Row label="Обновлено" value={fmtTs(data.updated_at,       { timeOnly: true, withSeconds: true })} />
    </Panel>
  );
}

// ── ValidationPanel ───────────────────────────────────────────────────────────

interface ValidationResp {
  result: {
    ok: boolean;
    message: string;
    details?: Record<string, unknown>;
  };
}

export function ValidationPanel({ migrationId }: { migrationId: string }) {
  const { data, loading } = useApi<ValidationResp>(
    `/api/migrations/${migrationId}/validation`,
    { deps: [migrationId] },
  );

  if (loading || !data?.result) return null;
  const result = data.result;
  const accent = result.ok ? t.green.dim : t.red.dim;

  return (
    <Panel accent={accent} title="Результат валидации" style={{ marginBottom: t.space[4] }}>
      <div style={{ display: "flex", alignItems: "center", gap: t.space[2], marginBottom: t.space[2] }}>
        <span style={{ fontSize: 20, color: result.ok ? t.green.base : t.red.base }}>
          {result.ok ? "✓" : "✗"}
        </span>
        <span style={{ fontSize: t.size.base, color: result.ok ? t.green.fg : t.red.fg }}>
          {result.message}
        </span>
      </div>
      {result.details && Object.entries(result.details).map(([k, v]) => (
        <Row key={k} label={k} value={String(v)} />
      ))}
    </Panel>
  );
}
