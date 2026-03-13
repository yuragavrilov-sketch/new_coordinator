/**
 * Phase-aware monitoring panels shown inside MigrationDetail.
 *
 * BulkProgressPanel   — chunk stats (CHUNKING / BULK_LOADING / BULK_LOADED)
 * ConnectorPanel      — Debezium connector status (CONNECTOR_STARTING … CDC_BUFFERING)
 * KafkaLagPanel       — consumer group lag (CDC_APPLY_STARTING … STEADY_STATE)
 * ValidationPanel     — stage validation result (STAGE_VALIDATING / STAGE_VALIDATED+)
 */

import React, { useEffect, useState } from "react";
import type { SSEEvent } from "../hooks/useSSE";

// ── Shared style helpers ──────────────────────────────────────────────────────

function PanelWrap({ accent, title, children }: {
  accent: string; title: string; children: React.ReactNode;
}) {
  return (
    <div style={{
      border:        `1px solid ${accent}40`,
      borderRadius:  7,
      overflow:      "hidden",
      marginBottom:  14,
    }}>
      <div style={{
        padding:    "6px 12px",
        background: "#0a111f",
        borderBottom: `1px solid ${accent}30`,
        fontSize:   11,
        fontWeight: 700,
        color:      accent,
        textTransform: "uppercase",
        letterSpacing: 0.8,
      }}>
        {title}
      </div>
      <div style={{ padding: "10px 12px", background: "#060e1a" }}>
        {children}
      </div>
    </div>
  );
}

function Row({ label, value, mono }: { label: string; value: React.ReactNode; mono?: boolean }) {
  return (
    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center",
                  fontSize: 12, marginBottom: 5 }}>
      <span style={{ color: "#64748b" }}>{label}</span>
      <span style={{ color: "#e2e8f0", fontFamily: mono ? "monospace" : undefined }}>{value}</span>
    </div>
  );
}

// ── BulkProgressPanel ─────────────────────────────────────────────────────────

interface ChunkStats {
  total: number; pending: number; claimed: number;
  running: number; done: number; failed: number;
}

interface ChunkRow {
  chunk_id: string; chunk_seq: number;
  rowid_start: string; rowid_end: string;
  status: string; rows_loaded: number;
  worker_id: string | null; error_text: string | null;
}

export function BulkProgressPanel({
  migrationId, sseEvents,
}: { migrationId: string; sseEvents: SSEEvent[] }) {
  const [stats,   setStats]   = useState<ChunkStats | null>(null);
  const [chunks,  setChunks]  = useState<ChunkRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [showAll, setShowAll] = useState(false);

  function load() {
    fetch(`/api/migrations/${migrationId}/chunks`)
      .then(r => r.ok ? r.json() : Promise.reject())
      .then(d => { setStats(d.stats); setChunks(d.chunks || []); setLoading(false); })
      .catch(() => setLoading(false));
  }

  useEffect(() => { load(); }, [migrationId]);

  // Refresh when a chunk_progress or migration_phase event arrives for this migration
  useEffect(() => {
    const last = sseEvents[0];
    if (!last) return;
    if (
      last.migration_id === migrationId &&
      (last.type === "chunk_progress" || last.type === "migration_phase")
    ) {
      load();
    }
  }, [sseEvents]); // eslint-disable-line

  // Polling every 5s while loading phase is active
  useEffect(() => {
    const id = setInterval(load, 5_000);
    return () => clearInterval(id);
  }, [migrationId]); // eslint-disable-line

  if (loading) return null;
  if (!stats || stats.total === 0) return null;

  const pct = stats.total > 0 ? Math.round((stats.done / stats.total) * 100) : 0;
  const active = stats.claimed + stats.running;
  const visible = showAll ? chunks : chunks.slice(0, 20);

  return (
    <PanelWrap accent="#d97706" title="Bulk Load Progress">
      {/* Progress bar */}
      <div style={{ marginBottom: 10 }}>
        <div style={{
          background: "#1e293b", borderRadius: 4, height: 8, overflow: "hidden",
        }}>
          <div style={{
            width: `${pct}%`, height: "100%",
            background: stats.failed > 0 ? "#ef4444" : "#22c55e",
            transition: "width 0.4s",
          }} />
        </div>
        <div style={{ display: "flex", justifyContent: "space-between",
                      fontSize: 11, color: "#64748b", marginTop: 4 }}>
          <span>{stats.done} / {stats.total} чанков</span>
          <span style={{ color: pct === 100 ? "#22c55e" : "#fcd34d", fontWeight: 700 }}>
            {pct}%
          </span>
        </div>
      </div>

      {/* Stats row */}
      <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 8 }}>
        {[
          { label: "Ожидают",   value: stats.pending, color: "#64748b" },
          { label: "Активны",   value: active,        color: "#fcd34d" },
          { label: "Готово",    value: stats.done,    color: "#22c55e" },
          { label: "Ошибки",    value: stats.failed,  color: "#ef4444" },
        ].map(s => (
          <div key={s.label} style={{ textAlign: "center" }}>
            <div style={{ fontSize: 16, fontWeight: 700, color: s.color }}>{s.value}</div>
            <div style={{ fontSize: 10, color: "#475569" }}>{s.label}</div>
          </div>
        ))}
      </div>

      {/* Chunk table (compact) */}
      {chunks.length > 0 && (
        <div>
          <div style={{
            display: "grid",
            gridTemplateColumns: "40px 1fr 70px 70px",
            gap: "2px 8px",
            fontSize: 10,
            color: "#475569",
            marginBottom: 3,
            paddingBottom: 3,
            borderBottom: "1px solid #1e293b",
          }}>
            <span>#</span><span>Статус / Worker</span>
            <span style={{ textAlign: "right" }}>Строки</span>
            <span style={{ textAlign: "right" }}>Ошибка</span>
          </div>
          {visible.map(c => (
            <div key={c.chunk_id} style={{
              display: "grid",
              gridTemplateColumns: "40px 1fr 70px 70px",
              gap: "1px 8px",
              fontSize: 10,
              padding: "2px 0",
              borderBottom: "1px solid #0f172a",
              color: c.status === "FAILED" ? "#fca5a5"
                   : c.status === "DONE"   ? "#86efac"
                   : c.status === "RUNNING" ? "#fcd34d"
                   : "#64748b",
            }}>
              <span>{c.chunk_seq}</span>
              <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {c.status}
                {c.worker_id && <span style={{ color: "#334155", marginLeft: 4 }}>
                  {c.worker_id.split(":")[0]}
                </span>}
              </span>
              <span style={{ textAlign: "right" }}>
                {c.rows_loaded > 0 ? c.rows_loaded.toLocaleString() : "—"}
              </span>
              <span style={{ textAlign: "right", color: "#fca5a5" }} title={c.error_text ?? ""}>
                {c.error_text ? "!" : "—"}
              </span>
            </div>
          ))}
          {chunks.length > 20 && (
            <button onClick={() => setShowAll(v => !v)} style={{
              background: "none", border: "none", color: "#475569",
              fontSize: 10, cursor: "pointer", marginTop: 4, padding: 0,
            }}>
              {showAll ? "▲ Свернуть" : `▼ Показать все ${chunks.length} чанков`}
            </button>
          )}
        </div>
      )}
    </PanelWrap>
  );
}

// ── ConnectorPanel ────────────────────────────────────────────────────────────

export function ConnectorPanel({
  migrationId, sseEvents,
}: { migrationId: string; sseEvents: SSEEvent[] }) {
  const [data,    setData]    = useState<{ connector_name: string; status: string } | null>(null);
  const [loading, setLoading] = useState(true);

  function load() {
    fetch(`/api/migrations/${migrationId}/connector`)
      .then(r => r.ok ? r.json() : Promise.reject())
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }

  useEffect(() => { load(); }, [migrationId]);

  useEffect(() => {
    const last = sseEvents[0];
    if (last?.migration_id === migrationId &&
        (last.type === "connector_status" || last.type === "migration_phase")) {
      load();
    }
  }, [sseEvents]); // eslint-disable-line

  useEffect(() => {
    const id = setInterval(load, 10_000);
    return () => clearInterval(id);
  }, [migrationId]); // eslint-disable-line

  if (loading || !data) return null;

  const statusColor = data.status === "RUNNING"  ? "#22c55e"
                    : data.status === "FAILED"    ? "#ef4444"
                    : data.status === "NOT_FOUND" ? "#64748b"
                    : "#fcd34d";

  return (
    <PanelWrap accent="#7c3aed" title="Debezium Connector">
      <Row label="Коннектор" value={
        <span style={{ fontFamily: "monospace", fontSize: 11 }}>
          {data.connector_name || "—"}
        </span>
      } />
      <Row label="Статус" value={
        <span style={{ fontWeight: 700, color: statusColor }}>{data.status}</span>
      } />
    </PanelWrap>
  );
}

// ── KafkaLagPanel ─────────────────────────────────────────────────────────────

interface LagData {
  total_lag: number | null;
  lag_by_partition: Record<string, number> | null;
  worker_id: string | null;
  worker_heartbeat: string | null;
  updated_at: string | null;
}

function fmtTs(iso: string | null | undefined): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString("ru-RU", {
      hour: "2-digit", minute: "2-digit", second: "2-digit",
    });
  } catch { return iso ?? "—"; }
}

export function KafkaLagPanel({
  migrationId, sseEvents,
}: { migrationId: string; sseEvents: SSEEvent[] }) {
  const [data,    setData]    = useState<LagData | null>(null);
  const [loading, setLoading] = useState(true);

  function load() {
    fetch(`/api/migrations/${migrationId}/lag`)
      .then(r => r.ok ? r.json() : Promise.reject())
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }

  useEffect(() => { load(); }, [migrationId]);

  useEffect(() => {
    const last = sseEvents[0];
    if (last?.migration_id === migrationId &&
        (last.type === "kafka_lag" || last.type === "migration_phase")) {
      load();
    }
  }, [sseEvents]); // eslint-disable-line

  useEffect(() => {
    const id = setInterval(load, 5_000);
    return () => clearInterval(id);
  }, [migrationId]); // eslint-disable-line

  if (loading || !data) return null;

  const lag      = data.total_lag ?? null;
  const lagColor = lag === 0        ? "#22c55e"
                 : lag !== null && lag < 10000 ? "#fcd34d"
                 : "#ef4444";
  const parts    = data.lag_by_partition ?? {};

  return (
    <PanelWrap accent="#ea580c" title="Kafka Lag">
      <div style={{ display: "flex", alignItems: "baseline", gap: 8, marginBottom: 8 }}>
        <span style={{
          fontSize: 28, fontWeight: 800,
          color: lagColor,
          fontVariantNumeric: "tabular-nums",
        }}>
          {lag !== null ? lag.toLocaleString() : "—"}
        </span>
        <span style={{ fontSize: 11, color: "#64748b" }}>messages behind</span>
        {lag === 0 && (
          <span style={{
            background: "#052e16", color: "#86efac",
            border: "1px solid #16a34a",
            borderRadius: 4, fontSize: 10, fontWeight: 700, padding: "1px 6px",
          }}>CAUGHT UP</span>
        )}
      </div>

      {Object.keys(parts).length > 0 && (
        <div style={{ marginBottom: 8 }}>
          {Object.entries(parts).map(([k, v]) => (
            <div key={k} style={{
              display: "flex", justifyContent: "space-between",
              fontSize: 10, color: "#64748b", marginBottom: 2,
            }}>
              <span>{k}</span>
              <span style={{ color: v === 0 ? "#22c55e" : "#fcd34d" }}>
                {v.toLocaleString()}
              </span>
            </div>
          ))}
        </div>
      )}

      <Row label="Worker"    value={data.worker_id ?? "—"} />
      <Row label="Heartbeat" value={fmtTs(data.worker_heartbeat)} />
      <Row label="Обновлено" value={fmtTs(data.updated_at)} />
    </PanelWrap>
  );
}

// ── ValidationPanel ───────────────────────────────────────────────────────────

interface ValidationResult {
  ok: boolean;
  message: string;
  details?: Record<string, unknown>;
}

export function ValidationPanel({
  migrationId,
}: { migrationId: string }) {
  const [result,  setResult]  = useState<ValidationResult | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`/api/migrations/${migrationId}/validation`)
      .then(r => r.ok ? r.json() : Promise.reject())
      .then(d => { setResult(d.result); setLoading(false); })
      .catch(() => setLoading(false));
  }, [migrationId]);

  if (loading || !result) return null;

  const accent = result.ok ? "#16a34a" : "#dc2626";

  return (
    <PanelWrap accent={accent} title="Результат валидации">
      <div style={{
        display: "flex", alignItems: "center", gap: 8, marginBottom: 8,
      }}>
        <span style={{
          fontSize: 20, color: result.ok ? "#22c55e" : "#ef4444",
        }}>
          {result.ok ? "✓" : "✗"}
        </span>
        <span style={{ fontSize: 12, color: result.ok ? "#86efac" : "#fca5a5" }}>
          {result.message}
        </span>
      </div>
      {result.details && Object.entries(result.details).map(([k, v]) => (
        <Row key={k} label={k} value={String(v)} />
      ))}
    </PanelWrap>
  );
}
