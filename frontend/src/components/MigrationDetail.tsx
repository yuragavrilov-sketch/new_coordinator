import React, { useEffect, useState } from "react";
import type { MigrationDetail, StateHistoryEntry } from "../types/migration";
import type { SSEEvent } from "../hooks/useSSE";
import { PhaseBadge } from "./PhaseBadge";
import {
  BulkProgressPanel,
  ConnectorPanel,
  KafkaLagPanel,
  ValidationPanel,
} from "./MigrationPanels";

interface Props {
  migrationId: string;
  onClose: () => void;
  sseEvents?: SSEEvent[];
}

// ── Formatters ────────────────────────────────────────────────────────────────

function fmtTs(iso: string | null | undefined): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString("ru-RU", {
      day: "2-digit", month: "2-digit", year: "numeric",
      hour: "2-digit", minute: "2-digit", second: "2-digit",
    });
  } catch { return iso ?? "—"; }
}

function fmtDuration(ms: number): string {
  if (ms < 0) ms = 0;
  if (ms < 1000) return `${ms} мс`;
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s} сек`;
  const m = Math.floor(s / 60);
  const rs = s % 60;
  if (m < 60) return `${m} мин ${rs > 0 ? ` ${rs} сек` : ""}`.trim();
  const h = Math.floor(m / 60);
  const rm = m % 60;
  return `${h} ч ${rm > 0 ? `${rm} мин` : ""}`.trim();
}

function fmtNum(n: number | null | undefined): string {
  if (n == null) return "—";
  return n.toLocaleString("ru-RU");
}

// ── Shared small widgets ──────────────────────────────────────────────────────

function InfoRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div style={{ display: "contents" }}>
      <span style={{ color: "#64748b", fontSize: 12 }}>{label}</span>
      <span style={{ color: "#e2e8f0", fontSize: 12, wordBreak: "break-all" }}>
        {value ?? <span style={{ color: "#334155" }}>—</span>}
      </span>
    </div>
  );
}

function StatusDot({ status }: { status: string }) {
  const color = status === "SUCCESS" ? "#22c55e" : status === "FAILED" ? "#ef4444" : "#f59e0b";
  return (
    <span style={{
      width: 7, height: 7, borderRadius: "50%", background: color,
      display: "inline-block", marginRight: 6, flexShrink: 0,
    }} />
  );
}

function SectionHeader({ children }: { children: React.ReactNode }) {
  return (
    <div style={{
      color: "#475569", fontSize: 11, fontWeight: 700,
      textTransform: "uppercase", letterSpacing: 0.8, marginBottom: 8,
    }}>
      {children}
    </div>
  );
}

// ── Phase sets ────────────────────────────────────────────────────────────────

const BULK_PHASES      = new Set(["CHUNKING", "BULK_LOADING", "BULK_LOADED", "BASELINE_LOADING"]);
const CONNECTOR_PHASES = new Set([
  "SCN_FIXED", "CONNECTOR_STARTING", "CDC_BUFFERING",
  "CHUNKING", "BULK_LOADING", "BULK_LOADED",
  "STAGE_VALIDATING", "STAGE_VALIDATED",
  "BASELINE_PUBLISHING", "BASELINE_PUBLISHED",
  "CDC_APPLY_STARTING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP", "STEADY_STATE",
]);
const LAG_PHASES       = new Set([
  "CDC_APPLY_STARTING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP", "STEADY_STATE",
]);
const VALIDATION_PHASES = new Set([
  "STAGE_VALIDATED", "BASELINE_PUBLISHING", "BASELINE_PUBLISHED",
  "CDC_APPLY_STARTING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP", "STEADY_STATE",
]);

const TERMINAL_PHASES = new Set(["COMPLETED", "CANCELLED", "FAILED"]);

// ── Statistics tab ────────────────────────────────────────────────────────────

interface PhasePeriod {
  phase: string;
  startedAt: Date;
  endedAt: Date | null;
  durationMs: number;
}

function useNow(intervalMs = 5000) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(id);
  }, [intervalMs]);
  return now;
}

function StatTile({
  label, value, sub, color,
}: { label: string; value: React.ReactNode; sub?: string; color?: string }) {
  return (
    <div style={{
      background: "#0a111f", border: "1px solid #1e293b",
      borderRadius: 7, padding: "10px 14px", minWidth: 0,
    }}>
      <div style={{ fontSize: 11, color: "#475569", marginBottom: 4 }}>{label}</div>
      <div style={{
        fontSize: 20, fontWeight: 800, color: color ?? "#e2e8f0",
        fontVariantNumeric: "tabular-nums", lineHeight: 1,
      }}>
        {value}
      </div>
      {sub && <div style={{ fontSize: 10, color: "#334155", marginTop: 3 }}>{sub}</div>}
    </div>
  );
}

function StatisticsTab({ detail }: { detail: MigrationDetail }) {
  const now = useNow(5000);
  const isTerminal = TERMINAL_PHASES.has(detail.phase);

  // Build phase periods from history (history is newest-first → reverse)
  const historyAsc = [...detail.history].reverse();
  const periods: PhasePeriod[] = historyAsc.map((h, i) => {
    const next = historyAsc[i + 1];
    const startedAt = new Date(h.created_at);
    const endedAt   = next ? new Date(next.created_at) : null;
    const refNow    = isTerminal && endedAt == null
      ? new Date(detail.state_changed_at).getTime()
      : now;
    const durationMs = endedAt
      ? endedAt.getTime() - startedAt.getTime()
      : refNow - startedAt.getTime();
    return { phase: h.to_phase, startedAt, endedAt, durationMs };
  });

  const totalMs = periods.reduce((sum, p) => sum + p.durationMs, 0);
  const maxMs   = Math.max(1, ...periods.map(p => p.durationMs));

  const bulkPeriod  = periods.find(p => p.phase === "BULK_LOADING");
  const bulkSec     = bulkPeriod ? bulkPeriod.durationMs / 1000 : null;
  const rowsLoaded  = detail.rows_loaded ?? 0;
  const rowsPerSec  = bulkSec && bulkSec > 0 && rowsLoaded > 0
    ? Math.round(rowsLoaded / bulkSec) : null;

  const chunkPct = detail.total_chunks && detail.total_chunks > 0
    ? Math.round(((detail.chunks_done ?? 0) / detail.total_chunks) * 100) : null;

  // Phase color map (reuse a small subset for the timeline bars)
  const phaseBarColor = (phase: string): string => {
    if (phase === "BULK_LOADING")         return "#d97706";
    if (phase === "CDC_CATCHING_UP")      return "#ea580c";
    if (phase === "STEADY_STATE")         return "#16a34a";
    if (phase === "BASELINE_PUBLISHING")  return "#7c3aed";
    if (phase === "FAILED")               return "#ef4444";
    if (phase === "CANCELLED")            return "#64748b";
    if (phase === "COMPLETED")            return "#22c55e";
    return "#1d4ed8";
  };

  return (
    <div>
      {/* ── Total duration banner ── */}
      <div style={{
        background: "#0a111f", border: "1px solid #1e293b",
        borderRadius: 7, padding: "10px 14px", marginBottom: 14,
        display: "flex", alignItems: "center", justifyContent: "space-between",
        flexWrap: "wrap", gap: 8,
      }}>
        <div>
          <div style={{ fontSize: 10, color: "#475569", textTransform: "uppercase", letterSpacing: 0.8 }}>
            Общая продолжительность
          </div>
          <div style={{
            fontSize: 24, fontWeight: 800, color: "#e2e8f0",
            fontVariantNumeric: "tabular-nums", lineHeight: 1.2, marginTop: 2,
          }}>
            {fmtDuration(totalMs)}
          </div>
        </div>
        <div style={{ textAlign: "right", fontSize: 11, color: "#475569", lineHeight: 1.8 }}>
          <div>Начало: {fmtTs(detail.created_at)}</div>
          <div>
            {isTerminal
              ? `Завершено: ${fmtTs(detail.state_changed_at)}`
              : `Текущая фаза: ${fmtDuration(periods[periods.length - 1]?.durationMs ?? 0)}`}
          </div>
        </div>
      </div>

      {/* ── Key metrics grid ── */}
      <div style={{
        display: "grid",
        gridTemplateColumns: "repeat(auto-fill, minmax(130px, 1fr))",
        gap: 8, marginBottom: 14,
      }}>
        <StatTile
          label="Строк загружено"
          value={fmtNum(rowsLoaded)}
          sub={rowsPerSec != null ? `~${fmtNum(rowsPerSec)} р/сек` : undefined}
          color="#86efac"
        />
        {detail.total_chunks != null && (
          <StatTile
            label="Чанки"
            value={`${detail.chunks_done ?? 0} / ${detail.total_chunks}`}
            sub={chunkPct != null ? `${chunkPct}%` : undefined}
            color={detail.chunks_failed ? "#fca5a5" : "#86efac"}
          />
        )}
        {detail.chunks_failed != null && detail.chunks_failed > 0 && (
          <StatTile
            label="Ошибки чанков"
            value={detail.chunks_failed}
            color="#ef4444"
          />
        )}
        {bulkPeriod && (
          <StatTile
            label="Время bulk load"
            value={fmtDuration(bulkPeriod.durationMs)}
            color="#fcd34d"
          />
        )}
        {detail.start_scn && (
          <StatTile
            label="Start SCN"
            value={<span style={{ fontSize: 13 }}>{detail.start_scn}</span>}
            sub={detail.scn_fixed_at ? fmtTs(detail.scn_fixed_at) : undefined}
          />
        )}
        {detail.kafka_lag != null && (
          <StatTile
            label="Kafka lag"
            value={fmtNum(detail.kafka_lag)}
            color={detail.kafka_lag === 0 ? "#22c55e" : detail.kafka_lag < 10000 ? "#fcd34d" : "#ef4444"}
          />
        )}
      </div>

      {/* ── Phase timeline ── */}
      <SectionHeader>Продолжительность фаз</SectionHeader>
      <div style={{
        background: "#060e1a", border: "1px solid #1e293b",
        borderRadius: 7, overflow: "hidden", marginBottom: 14,
      }}>
        {/* header */}
        <div style={{
          display: "grid",
          gridTemplateColumns: "160px 1fr 110px 90px",
          gap: "2px 10px",
          padding: "5px 12px",
          borderBottom: "1px solid #1e293b",
          fontSize: 10, color: "#475569", fontWeight: 700,
          textTransform: "uppercase", letterSpacing: 0.6,
        }}>
          <span>Фаза</span>
          <span>Относительно</span>
          <span style={{ textAlign: "right" }}>Начало</span>
          <span style={{ textAlign: "right" }}>Длительность</span>
        </div>

        {periods.map((p, i) => {
          const barPct = Math.max(1, Math.round((p.durationMs / maxMs) * 100));
          const isCurrent = !p.endedAt;
          const color = phaseBarColor(p.phase);
          return (
            <div key={i} style={{
              display: "grid",
              gridTemplateColumns: "160px 1fr 110px 90px",
              gap: "2px 10px",
              padding: "5px 12px",
              borderBottom: i < periods.length - 1 ? "1px solid #0f172a" : "none",
              alignItems: "center",
              background: isCurrent ? "#0d1e35" : "transparent",
            }}>
              {/* phase name */}
              <span style={{
                fontSize: 11, color: isCurrent ? "#e2e8f0" : "#94a3b8",
                fontWeight: isCurrent ? 700 : 400,
                overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
              }}>
                {isCurrent && (
                  <span style={{
                    display: "inline-block", width: 6, height: 6, borderRadius: "50%",
                    background: "#22c55e", marginRight: 5,
                    boxShadow: "0 0 4px #22c55e",
                  }} />
                )}
                {p.phase}
              </span>

              {/* bar */}
              <div style={{ height: 6, background: "#1e293b", borderRadius: 3, overflow: "hidden" }}>
                <div style={{
                  width: `${barPct}%`, height: "100%",
                  background: color,
                  opacity: isCurrent ? 1 : 0.6,
                  borderRadius: 3,
                  transition: "width 1s",
                }} />
              </div>

              {/* start time */}
              <span style={{
                fontSize: 10, color: "#475569", textAlign: "right", whiteSpace: "nowrap",
              }}>
                {p.startedAt.toLocaleTimeString("ru-RU")}
              </span>

              {/* duration */}
              <span style={{
                fontSize: 11, color: isCurrent ? "#fcd34d" : "#64748b",
                textAlign: "right", whiteSpace: "nowrap",
                fontVariantNumeric: "tabular-nums",
              }}>
                {fmtDuration(p.durationMs)}
                {isCurrent && <span style={{ color: "#22c55e", marginLeft: 3 }}>▶</span>}
              </span>
            </div>
          );
        })}

        {periods.length === 0 && (
          <div style={{ padding: "12px 16px", color: "#334155", fontSize: 12 }}>
            Нет данных о переходах
          </div>
        )}
      </div>
    </div>
  );
}

// ── History tab ───────────────────────────────────────────────────────────────

function HistoryTab({ history }: { history: StateHistoryEntry[] }) {
  return (
    <div>
      <SectionHeader>История состояний ({history.length})</SectionHeader>
      {history.length === 0 ? (
        <div style={{ color: "#334155", fontSize: 12 }}>Нет записей</div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 1 }}>
          {history.map((h, i) => (
            <HistoryRow key={h.id} entry={h} isFirst={i === 0} />
          ))}
        </div>
      )}
    </div>
  );
}

// ── Overview tab ──────────────────────────────────────────────────────────────

function OverviewTab({
  detail, migrationId, sseEvents, phase,
}: {
  detail: MigrationDetail;
  migrationId: string;
  sseEvents: SSEEvent[];
  phase: string;
}) {
  return (
    <>
      {/* Queue position indicator */}
      {phase === "NEW" && detail.queue_position != null && (
        <div style={{
          background: "#3b2000", border: "1px solid #d97706",
          borderRadius: 6, padding: "8px 12px", marginBottom: 14,
          display: "flex", alignItems: "center", gap: 10,
        }}>
          <span style={{ fontSize: 22, fontWeight: 800, color: "#fcd34d" }}>
            #{detail.queue_position}
          </span>
          <div>
            <div style={{ fontSize: 12, color: "#fcd34d", fontWeight: 700 }}>
              В очереди на загрузку
            </div>
            <div style={{ fontSize: 11, color: "#d4a050" }}>
              Ожидание завершения загрузки другой миграции. SCN ещё не зафиксирован.
            </div>
          </div>
        </div>
      )}

      {/* Error block */}
      {detail.error_code && (
        <div style={{
          background: "#450a0a", border: "1px solid #7f1d1d",
          borderRadius: 6, padding: "8px 12px", marginBottom: 14, fontSize: 12,
        }}>
          <span style={{ color: "#fca5a5", fontWeight: 700 }}>{detail.error_code}</span>
          {detail.failed_phase && (
            <span style={{ color: "#94a3b8", marginLeft: 8 }}>
              в фазе {detail.failed_phase}
            </span>
          )}
          {detail.error_text && (
            <div style={{ color: "#fca5a5", marginTop: 4, opacity: 0.85 }}>
              {detail.error_text}
            </div>
          )}
        </div>
      )}

      {/* Phase-specific panels */}
      {CONNECTOR_PHASES.has(phase) && (
        <ConnectorPanel migrationId={migrationId} sseEvents={sseEvents} />
      )}
      {BULK_PHASES.has(phase) && (
        <BulkProgressPanel
          migrationId={migrationId}
          sseEvents={sseEvents}
          chunkType={phase === "BASELINE_LOADING" ? "BASELINE" : "BULK"}
        />
      )}
      {VALIDATION_PHASES.has(phase) && (
        <ValidationPanel migrationId={migrationId} />
      )}
      {LAG_PHASES.has(phase) && (
        <KafkaLagPanel migrationId={migrationId} sseEvents={sseEvents} />
      )}

      {/* Info grids */}
      <InfoGrid title="Основное">
        <InfoRow label="ID" value={
          <span style={{ fontFamily: "monospace", fontSize: 11 }}>
            {detail.migration_id}
          </span>
        } />
        <InfoRow label="Создана"       value={fmtTs(detail.created_at)} />
        <InfoRow label="Автор"         value={detail.created_by} />
        <InfoRow label="Описание"      value={detail.description} />
        <InfoRow label="Фаза изменена" value={fmtTs(detail.state_changed_at)} />
        <InfoRow label="Обновлена"     value={fmtTs(detail.updated_at)} />
        <InfoRow label="Повторов"      value={
          detail.retry_count > 0
            ? <span style={{ color: "#f59e0b" }}>{detail.retry_count}</span>
            : "0"
        } />
      </InfoGrid>

      <InfoGrid title="Источник → Цель">
        <InfoRow label="Source connection" value={detail.source_connection_id} />
        <InfoRow label="Source table"      value={`${detail.source_schema}.${detail.source_table}`} />
        <InfoRow label="Target connection" value={detail.target_connection_id} />
        <InfoRow label="Target table"      value={`${detail.target_schema}.${detail.target_table}`} />
        <InfoRow label="Stage table"       value={detail.stage_table_name} />
      </InfoGrid>

      <InfoGrid title="Коннектор / Kafka">
        <InfoRow label="Connector"      value={detail.connector_name} />
        <InfoRow label="Topic prefix"   value={detail.topic_prefix} />
        <InfoRow label="Consumer group" value={detail.consumer_group} />
      </InfoGrid>

      <InfoGrid title="Параметры загрузки">
        <InfoRow label="Chunk size"              value={detail.chunk_size?.toLocaleString()} />
        <InfoRow label="Воркеры bulk"            value={detail.max_parallel_workers} />
        <InfoRow label="Воркеры baseline"        value={detail.baseline_parallel_degree} />
        <InfoRow label="Total rows"         value={detail.total_rows != null ? fmtNum(detail.total_rows) : "—"} />
        <InfoRow label="Total chunks"       value={detail.total_chunks ?? "—"} />
        <InfoRow label="Chunks done"        value={detail.chunks_done} />
        <InfoRow label="Rows loaded"        value={fmtNum(detail.rows_loaded)} />
        <InfoRow label="Start SCN"          value={detail.start_scn} />
        <InfoRow label="SCN fixed at"       value={fmtTs(detail.scn_fixed_at)} />
        <InfoRow label="Hash/sample validate" value={
          detail.validate_hash_sample ? "включено" : "выключено"
        } />
      </InfoGrid>

      <InfoGrid title="Ключ">
        <InfoRow label="Key type"    value={detail.effective_key_type} />
        <InfoRow label="Key source"  value={detail.effective_key_source} />
        <InfoRow label="Key columns" value={
          <span style={{ fontFamily: "monospace", fontSize: 11 }}>
            {detail.effective_key_columns_json}
          </span>
        } />
        <InfoRow label="PK exists"   value={detail.source_pk_exists ? "да" : "нет"} />
        <InfoRow label="UK exists"   value={detail.source_uk_exists ? "да" : "нет"} />
      </InfoGrid>
    </>
  );
}

function InfoGrid({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div style={{ fontSize: 12, marginBottom: 16 }}>
      <SectionHeader>{title}</SectionHeader>
      <div style={{
        display: "grid",
        gridTemplateColumns: "minmax(160px, auto) 1fr",
        gap: "5px 16px",
      }}>
        {children}
      </div>
    </div>
  );
}

// ── Chunks tab ────────────────────────────────────────────────────────────────

interface ChunkRow {
  chunk_id: string;
  chunk_seq: number;
  rowid_start: string;
  rowid_end: string;
  status: string;
  rows_loaded: number;
  worker_id: string | null;
  error_text: string | null;
  retry_count: number;
}

interface ChunkStats {
  total: number; pending: number; claimed: number;
  running: number; done: number; failed: number;
  rows_loaded: number;
}

function ChunksSection({
  migrationId, chunkType, sseEvents,
}: { migrationId: string; chunkType: "BULK" | "BASELINE"; sseEvents: SSEEvent[] }) {
  const [stats,         setStats]         = useState<ChunkStats | null>(null);
  const [chunks,        setChunks]        = useState<ChunkRow[]>([]);
  const [loading,       setLoading]       = useState(true);
  const [retrying,      setRetrying]      = useState(false);
  const [retryError,    setRetryError]    = useState<string | null>(null);
  const [expandedChunk, setExpandedChunk] = useState<string | null>(null);
  const [page,          setPage]          = useState(1);
  const [totalChunks,   setTotalChunks]   = useState(0);
  const [statusFilter,  setStatusFilter]  = useState("");
  const PAGE_SIZE = 100;

  const isBaseline = chunkType === "BASELINE";
  const accent     = isBaseline ? "#9333ea" : "#d97706";

  function load(p?: number) {
    const pg = p ?? page;
    const qs = new URLSearchParams({
      chunk_type: chunkType, page: String(pg), page_size: String(PAGE_SIZE),
    });
    if (statusFilter) qs.set("status", statusFilter);
    fetch(`/api/migrations/${migrationId}/chunks?${qs}`)
      .then(r => r.ok ? r.json() : Promise.reject())
      .then(d => {
        setStats(d.stats); setChunks(d.chunks ?? []);
        setTotalChunks(d.total ?? 0); setLoading(false);
      })
      .catch(() => setLoading(false));
  }

  async function retryFailed() {
    setRetrying(true);
    setRetryError(null);
    try {
      const r = await fetch(
        `/api/migrations/${migrationId}/retry-chunks?chunk_type=${chunkType}`,
        { method: "POST" },
      );
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        setRetryError(d.error ?? "Ошибка сервера");
      } else {
        load();
      }
    } catch {
      setRetryError("Сетевая ошибка");
    } finally {
      setRetrying(false);
    }
  }

  useEffect(() => { load(); }, [migrationId, statusFilter]); // eslint-disable-line

  useEffect(() => {
    const last = sseEvents[0];
    if (!last || last.migration_id !== migrationId) return;
    if (
      last.type === "chunk_progress" ||
      last.type === "baseline_progress" ||
      last.type === "migration_phase"
    ) load();
  }, [sseEvents]); // eslint-disable-line

  const totalPages = Math.max(1, Math.ceil(totalChunks / PAGE_SIZE));

  function goPage(p: number) {
    const clamped = Math.max(1, Math.min(totalPages, p));
    setPage(clamped);
    load(clamped);
  }

  const statusOptions = [
    { value: "",        label: "Все"     },
    { value: "PENDING", label: "PENDING" },
    { value: "RUNNING", label: "RUNNING" },
    { value: "DONE",    label: "DONE"    },
    { value: "FAILED",  label: "FAILED"  },
  ];

  const hdr = (
    <div style={{
      padding: "6px 12px", background: "#0a111f",
      borderBottom: `1px solid ${accent}30`,
      display: "flex", alignItems: "center", justifyContent: "space-between",
    }}>
      <span style={{
        fontSize: 11, fontWeight: 700, color: accent,
        textTransform: "uppercase", letterSpacing: 0.8,
      }}>
        {chunkType}
      </span>
      {stats && (
        <span style={{ fontSize: 11, color: "#475569" }}>
          {stats.total} чанков &middot; {stats.rows_loaded.toLocaleString()} строк
        </span>
      )}
    </div>
  );

  if (loading) return (
    <div style={{ border: `1px solid ${accent}30`, borderRadius: 7, overflow: "hidden", marginBottom: 14 }}>
      {hdr}
      <div style={{ padding: "10px 12px", background: "#060e1a", color: "#334155", fontSize: 12 }}>
        Загрузка…
      </div>
    </div>
  );

  if (!stats || stats.total === 0) return (
    <div style={{ border: `1px solid ${accent}20`, borderRadius: 7, overflow: "hidden", marginBottom: 14 }}>
      {hdr}
      <div style={{ padding: "10px 12px", background: "#060e1a", color: "#334155", fontSize: 12 }}>
        Нет чанков
      </div>
    </div>
  );

  const pct    = Math.round((stats.done / stats.total) * 100);
  const active = stats.claimed + stats.running;

  return (
    <div style={{ border: `1px solid ${accent}40`, borderRadius: 7, overflow: "hidden", marginBottom: 14 }}>
      {hdr}
      <div style={{ padding: "10px 12px", background: "#060e1a" }}>

        {/* Progress bar */}
        <div style={{ marginBottom: 10 }}>
          <div style={{ background: "#1e293b", borderRadius: 4, height: 6, overflow: "hidden" }}>
            <div style={{
              width: `${pct}%`, height: "100%",
              background: stats.failed > 0 ? "#ef4444" : "#22c55e",
              transition: "width 0.4s",
            }} />
          </div>
          <div style={{ display: "flex", justifyContent: "space-between",
                        fontSize: 11, color: "#64748b", marginTop: 3 }}>
            <span>{stats.done} / {stats.total}</span>
            <span style={{ color: pct === 100 ? "#22c55e" : "#fcd34d", fontWeight: 700 }}>{pct}%</span>
          </div>
        </div>

        {/* Stats row + retry button */}
        <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 10, alignItems: "flex-end" }}>
          {([
            { label: "Ожидают", value: stats.pending, color: "#64748b" },
            { label: "Активны", value: active,         color: "#fcd34d" },
            { label: "Готово",  value: stats.done,     color: "#22c55e" },
            { label: "Ошибки",  value: stats.failed,   color: "#ef4444" },
          ] as const).map(s => (
            <div key={s.label} style={{ textAlign: "center" }}>
              <div style={{ fontSize: 16, fontWeight: 700, color: s.color }}>{s.value}</div>
              <div style={{ fontSize: 10, color: "#475569" }}>{s.label}</div>
            </div>
          ))}
          {stats.failed > 0 && (
            <button
              onClick={retryFailed}
              disabled={retrying}
              style={{
                marginLeft: "auto",
                background: "#450a0a", border: "1px solid #7f1d1d", borderRadius: 5,
                color: "#fca5a5", fontSize: 10, padding: "4px 10px",
                cursor: retrying ? "not-allowed" : "pointer", fontWeight: 700,
              }}
            >
              {retrying ? "…" : `↺ Повторить (${stats.failed})`}
            </button>
          )}
        </div>

        {retryError && (
          <div style={{ color: "#fca5a5", fontSize: 11, marginBottom: 8 }}>{retryError}</div>
        )}

        {/* Status filter + pagination */}
        <div style={{
          display: "flex", alignItems: "center", justifyContent: "space-between",
          marginBottom: 8, gap: 8,
        }}>
          <div style={{ display: "flex", gap: 4 }}>
            {statusOptions.map(o => (
              <button key={o.value} onClick={() => { setStatusFilter(o.value); setPage(1); }} style={{
                padding: "2px 8px", fontSize: 10, fontWeight: statusFilter === o.value ? 700 : 400,
                borderRadius: 4, border: "1px solid",
                borderColor: statusFilter === o.value ? accent : "#1e293b",
                background: statusFilter === o.value ? accent + "22" : "transparent",
                color: statusFilter === o.value ? accent : "#475569",
                cursor: "pointer",
              }}>
                {o.label}
              </button>
            ))}
          </div>
          {totalPages > 1 && (
            <div style={{ display: "flex", alignItems: "center", gap: 4, fontSize: 10, color: "#64748b" }}>
              <button onClick={() => goPage(1)} disabled={page <= 1}
                style={pgBtn(page <= 1)}>{"<<"}</button>
              <button onClick={() => goPage(page - 1)} disabled={page <= 1}
                style={pgBtn(page <= 1)}>{"<"}</button>
              <span>{page}/{totalPages}</span>
              <button onClick={() => goPage(page + 1)} disabled={page >= totalPages}
                style={pgBtn(page >= totalPages)}>{">"}</button>
              <button onClick={() => goPage(totalPages)} disabled={page >= totalPages}
                style={pgBtn(page >= totalPages)}>{">>"}</button>
            </div>
          )}
        </div>

        {/* Chunk table */}
        <div style={{ maxHeight: 320, overflowY: "auto" }}>
          <div style={{
            display: "grid", gridTemplateColumns: "36px 72px 1fr 64px 52px",
            gap: "2px 8px", fontSize: 10, color: "#475569",
            paddingBottom: 3, marginBottom: 3, borderBottom: "1px solid #1e293b",
          }}>
            <span>#</span><span>Статус</span><span>Worker</span>
            <span style={{ textAlign: "right" }}>Строки</span>
            <span style={{ textAlign: "right" }}>Попыт.</span>
          </div>
          {chunks.map(c => (
            <React.Fragment key={c.chunk_id}>
              <div
                onClick={() => c.status === "FAILED" && c.error_text &&
                  setExpandedChunk(expandedChunk === c.chunk_id ? null : c.chunk_id)}
                style={{
                  display: "grid", gridTemplateColumns: "36px 72px 1fr 64px 52px",
                  gap: "1px 8px", fontSize: 10, padding: "2px 0",
                  borderBottom: "1px solid #0f172a",
                  color: c.status === "FAILED"   ? "#fca5a5"
                       : c.status === "DONE"     ? "#86efac"
                       : c.status === "RUNNING"  ? "#fcd34d"
                       : "#64748b",
                  cursor: c.status === "FAILED" && c.error_text ? "pointer" : "default",
                  userSelect: "none",
                }}
              >
                <span>{c.chunk_seq}</span>
                <span>{c.status}</span>
                <span style={{ overflow: "hidden", textOverflow: "ellipsis",
                               whiteSpace: "nowrap", color: "#334155" }}>
                  {c.worker_id?.split(":")[0] ?? "—"}
                </span>
                <span style={{ textAlign: "right" }}>
                  {c.rows_loaded > 0 ? c.rows_loaded.toLocaleString() : "—"}
                </span>
                <span style={{ textAlign: "right" }}>
                  {c.retry_count > 0 ? c.retry_count : "—"}
                </span>
              </div>
              {expandedChunk === c.chunk_id && c.error_text && (
                <pre style={{
                  margin: "2px 0 4px",
                  fontFamily: "monospace", fontSize: 10, color: "#fca5a5",
                  whiteSpace: "pre-wrap", wordBreak: "break-word",
                  background: "#0f0404", border: "1px solid #3b0f0f",
                  borderRadius: 4, padding: "6px 10px",
                  maxHeight: 150, overflowY: "auto",
                }}>
                  {c.error_text}
                </pre>
              )}
            </React.Fragment>
          ))}
          {chunks.length === 0 && (
            <div style={{ padding: "10px 0", color: "#334155", fontSize: 11, textAlign: "center" }}>
              Нет чанков с данным статусом
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function pgBtn(disabled: boolean): React.CSSProperties {
  return {
    background: "none", border: "1px solid #1e293b", borderRadius: 3,
    color: disabled ? "#1e293b" : "#64748b", cursor: disabled ? "default" : "pointer",
    padding: "1px 5px", fontSize: 10, fontWeight: 700, lineHeight: 1.2,
  };
}

function ChunksTab({ migrationId, sseEvents }: { migrationId: string; sseEvents: SSEEvent[] }) {
  return (
    <div>
      <ChunksSection migrationId={migrationId} chunkType="BULK"     sseEvents={sseEvents} />
      <ChunksSection migrationId={migrationId} chunkType="BASELINE" sseEvents={sseEvents} />
    </div>
  );
}

// ── Errors tab ────────────────────────────────────────────────────────────────

function ErrorsTab({ detail }: { detail: MigrationDetail }) {
  const hasMigError = !!(detail.error_code || detail.error_text);

  if (!hasMigError) {
    return (
      <div style={{
        display: "flex", flexDirection: "column", alignItems: "center",
        justifyContent: "center", padding: "40px 0", gap: 8,
      }}>
        <div style={{ fontSize: 24 }}>✓</div>
        <div style={{ color: "#22c55e", fontSize: 13, fontWeight: 600 }}>Ошибок нет</div>
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
      <div>
        <SectionHeader>Ошибка миграции</SectionHeader>
        <div style={{
          background: "#1a0808", border: "1px solid #7f1d1d",
          borderRadius: 7, padding: 14,
        }}>
          {detail.error_code && (
            <div style={{ marginBottom: 8 }}>
              <div style={{ fontSize: 10, color: "#64748b", textTransform: "uppercase", letterSpacing: 0.6, marginBottom: 2 }}>Код</div>
              <span style={{
                fontFamily: "monospace", fontSize: 13, color: "#fca5a5",
                fontWeight: 700, background: "#2d0a0a",
                border: "1px solid #7f1d1d", borderRadius: 4, padding: "2px 8px",
              }}>
                {detail.error_code}
              </span>
            </div>
          )}
          {detail.failed_phase && (
            <div style={{ marginBottom: 8 }}>
              <div style={{ fontSize: 10, color: "#64748b", textTransform: "uppercase", letterSpacing: 0.6, marginBottom: 2 }}>Фаза</div>
              <span style={{ fontSize: 13, color: "#f87171" }}>{detail.failed_phase}</span>
            </div>
          )}
          {detail.error_text && (
            <div>
              <div style={{ fontSize: 10, color: "#64748b", textTransform: "uppercase", letterSpacing: 0.6, marginBottom: 4 }}>Подробности</div>
              <pre style={{
                margin: 0, fontFamily: "monospace", fontSize: 12, color: "#fca5a5",
                whiteSpace: "pre-wrap", wordBreak: "break-word",
                background: "#0f0404", borderRadius: 5, padding: "10px 12px",
                maxHeight: 320, overflowY: "auto",
                border: "1px solid #3b0f0f",
              }}>
                {detail.error_text}
              </pre>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ── EnableIndexesButton ───────────────────────────────────────────────────────

function EnableIndexesButton({ migrationId, onDone }: { migrationId: string; onDone: () => void }) {
  const [busy,  setBusy]  = useState(false);
  const [errMsg, setErrMsg] = useState<string | null>(null);

  async function handleClick() {
    setBusy(true);
    setErrMsg(null);
    try {
      const r = await fetch(`/api/migrations/${migrationId}/enable-indexes`, { method: "POST" });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        setErrMsg(d.error ?? `Ошибка ${r.status}`);
      } else {
        onDone();
      }
    } catch (e) {
      setErrMsg(String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-start", gap: 4 }}>
      <button
        onClick={handleClick}
        disabled={busy}
        style={{
          background: busy ? "#1e3a5f" : "#1d4ed8",
          color: busy ? "#64748b" : "#e2e8f0",
          border: "1px solid #2563eb",
          borderRadius: 5,
          padding: "5px 14px",
          fontSize: 12,
          fontWeight: 600,
          cursor: busy ? "not-allowed" : "pointer",
        }}
      >
        {busy ? "Запуск..." : "Включить индексы"}
      </button>
      {errMsg && (
        <span style={{ fontSize: 11, color: "#fca5a5" }}>{errMsg}</span>
      )}
    </div>
  );
}

// ── EnableTriggersButton ─────────────────────────────────────────────────────

function EnableTriggersButton({ migrationId, onDone }: { migrationId: string; onDone: () => void }) {
  const [busy, setBusy]   = useState(false);
  const [errMsg, setErrMsg] = useState<string | null>(null);
  const [done, setDone]   = useState(false);

  async function handleClick() {
    setBusy(true);
    setErrMsg(null);
    try {
      const r = await fetch(`/api/migrations/${migrationId}/enable-triggers`, { method: "POST" });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        setErrMsg(d.error ?? `Ошибка ${r.status}`);
      } else {
        setDone(true);
        onDone();
      }
    } catch (e) {
      setErrMsg(String(e));
    } finally {
      setBusy(false);
    }
  }

  if (done) {
    return <span style={{ fontSize: 11, color: "#22c55e", fontWeight: 600 }}>Триггеры включены</span>;
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-start", gap: 4 }}>
      <button
        onClick={handleClick}
        disabled={busy}
        style={{
          background: busy ? "#1e3a2f" : "#15803d",
          color: busy ? "#64748b" : "#e2e8f0",
          border: "1px solid #166534",
          borderRadius: 5,
          padding: "5px 14px",
          fontSize: 12,
          fontWeight: 600,
          cursor: busy ? "not-allowed" : "pointer",
        }}
      >
        {busy ? "Включение..." : "Включить триггеры"}
      </button>
      {errMsg && (
        <span style={{ fontSize: 11, color: "#fca5a5" }}>{errMsg}</span>
      )}
    </div>
  );
}

// ── RestartBaselineButton ─────────────────────────────────────────────────────

function RestartBaselineButton({ migrationId, onDone }: { migrationId: string; onDone: () => void }) {
  const [busy, setBusy]     = useState(false);
  const [errMsg, setErrMsg] = useState<string | null>(null);

  async function handleClick() {
    if (!confirm("Перезапустить baseline? Целевая таблица будет очищена (TRUNCATE) и загрузка начнётся заново.")) return;
    setBusy(true);
    setErrMsg(null);
    try {
      const r = await fetch(`/api/migrations/${migrationId}/restart-baseline`, { method: "POST" });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        setErrMsg(d.error ?? `Ошибка ${r.status}`);
      } else {
        onDone();
      }
    } catch (e) {
      setErrMsg(String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-start", gap: 4 }}>
      <button
        onClick={handleClick}
        disabled={busy}
        style={{
          background: busy ? "#3b2000" : "#92400e",
          color: busy ? "#64748b" : "#fef3c7",
          border: "1px solid #d97706",
          borderRadius: 5,
          padding: "5px 14px",
          fontSize: 12,
          fontWeight: 600,
          cursor: busy ? "not-allowed" : "pointer",
        }}
      >
        {busy ? "Перезапуск..." : "Перезапустить baseline"}
      </button>
      {errMsg && (
        <span style={{ fontSize: 11, color: "#fca5a5" }}>{errMsg}</span>
      )}
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

type Tab = "overview" | "stats" | "chunks" | "errors" | "history";

export function MigrationDetailPanel({ migrationId, onClose, sseEvents = [] }: Props) {
  const [detail,    setDetail]    = useState<MigrationDetail | null>(null);
  const [loading,   setLoading]   = useState(true);
  const [error,     setError]     = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<Tab>("overview");

  function loadDetail() {
    fetch(`/api/migrations/${migrationId}`)
      .then(r => r.ok ? r.json() : Promise.reject(`HTTP ${r.status}`))
      .then(data => { setDetail(data); setLoading(false); })
      .catch(e => { setError(String(e)); setLoading(false); });
  }

  useEffect(() => {
    setLoading(true);
    setError(null);
    loadDetail();
  }, [migrationId]);

  useEffect(() => {
    const last = sseEvents[0];
    if (!last || last.migration_id !== migrationId) return;
    if (last.type === "migration_phase" || last.type === "baseline_progress") {
      loadDetail();
    }
  }, [sseEvents]); // eslint-disable-line

  const phase = detail?.phase ?? "";

  const errorCount    = detail?.error_code ? 1 : 0;
  const failedChunks  = detail?.chunks_failed ?? 0;

  const tabs: { id: Tab; label: React.ReactNode }[] = [
    { id: "overview", label: "Обзор" },
    { id: "stats",    label: "Статистика" },
    {
      id: "chunks",
      label: (
        <span style={{ display: "flex", alignItems: "center", gap: 5 }}>
          Чанки
          {failedChunks > 0 && (
            <span style={{
              fontSize: 10, fontWeight: 700,
              background: "#7f1d1d", color: "#fca5a5",
              borderRadius: 10, padding: "0 5px", lineHeight: "16px",
            }}>
              {failedChunks}
            </span>
          )}
        </span>
      ),
    },
    {
      id: "errors",
      label: (
        <span style={{ display: "flex", alignItems: "center", gap: 5 }}>
          Ошибки
          {errorCount > 0 && (
            <span style={{
              fontSize: 10, fontWeight: 700,
              background: "#7f1d1d", color: "#fca5a5",
              borderRadius: 10, padding: "0 5px", lineHeight: "16px",
            }}>
              {errorCount}
            </span>
          )}
        </span>
      ),
    },
    { id: "history",  label: `История${detail ? ` (${detail.history.length})` : ""}` },
  ];

  return (
    <div style={{
      background: "#0f172a",
      border: "1px solid #1e293b",
      borderRadius: 8,
      overflow: "hidden",
      display: "flex",
      flexDirection: "column",
      height: "100%",
    }}>
      {/* Header */}
      <div style={{
        padding: "12px 16px 0",
        borderBottom: "1px solid #1e293b",
        background: "#0a111f",
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 10 }}>
          {detail && <PhaseBadge phase={detail.phase} />}
          <span style={{ fontWeight: 700, fontSize: 14, color: "#e2e8f0", flex: 1 }}>
            {detail?.migration_name ?? "Загрузка..."}
          </span>
          {(phase === "INDEXES_ENABLING" ||
            (phase === "FAILED" && detail?.error_code === "INDEXES_ENABLE_ERROR")) && (
            <EnableIndexesButton migrationId={migrationId} onDone={loadDetail} />
          )}
          {(phase === "BASELINE_LOADING" ||
            (phase === "FAILED" && (detail?.error_code === "BASELINE_PUBLISH_ERROR" || detail?.error_code === "BASELINE_LOAD_FAILED"))) && (
            <RestartBaselineButton migrationId={migrationId} onDone={loadDetail} />
          )}
          {(phase === "CDC_CATCHING_UP" || phase === "CDC_CAUGHT_UP" || phase === "STEADY_STATE") && (
            <EnableTriggersButton migrationId={migrationId} onDone={loadDetail} />
          )}
          <button onClick={onClose} style={{
            background: "none", border: "none", color: "#475569",
            cursor: "pointer", fontSize: 18, lineHeight: 1, padding: "0 4px",
          }}>✕</button>
        </div>

        {/* Tab bar */}
        <div style={{ display: "flex", gap: 0 }}>
          {tabs.map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              style={{
                background: "none",
                border: "none",
                borderBottom: `2px solid ${activeTab === tab.id ? "#3b82f6" : "transparent"}`,
                color: activeTab === tab.id ? "#93c5fd" : "#475569",
                fontSize: 12,
                fontWeight: activeTab === tab.id ? 700 : 400,
                cursor: "pointer",
                padding: "4px 14px 8px",
                transition: "color 0.15s, border-color 0.15s",
              }}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </div>

      {/* Body */}
      {loading && (
        <div style={{ padding: 24, color: "#475569", fontSize: 13 }}>Загрузка...</div>
      )}
      {error && (
        <div style={{ padding: 24, color: "#fca5a5", fontSize: 13 }}>Ошибка: {error}</div>
      )}

      {detail && !loading && (
        <div style={{ flex: 1, overflow: "auto", padding: 16 }}>
          {activeTab === "overview" && (
            <OverviewTab
              detail={detail}
              migrationId={migrationId}
              sseEvents={sseEvents}
              phase={phase}
            />
          )}
          {activeTab === "stats" && (
            <StatisticsTab detail={detail} />
          )}
          {activeTab === "chunks" && (
            <ChunksTab migrationId={migrationId} sseEvents={sseEvents} />
          )}
          {activeTab === "errors" && (
            <ErrorsTab detail={detail} />
          )}
          {activeTab === "history" && (
            <HistoryTab history={detail.history} />
          )}
        </div>
      )}
    </div>
  );
}

// ── HistoryRow ────────────────────────────────────────────────────────────────

function HistoryRow({ entry, isFirst }: { entry: StateHistoryEntry; isFirst: boolean }) {
  const [expanded, setExpanded] = useState(isFirst);

  return (
    <div style={{
      background: isFirst ? "#0d1e35" : "#0a111f",
      border: `1px solid ${isFirst ? "#1d3558" : "#1e293b"}`,
      borderRadius: 5,
      overflow: "hidden",
    }}>
      <div
        onClick={() => entry.message && setExpanded(e => !e)}
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          padding: "7px 10px",
          cursor: entry.message ? "pointer" : "default",
          fontSize: 12,
        }}
      >
        <StatusDot status={entry.transition_status} />
        <span style={{ color: "#64748b", fontSize: 11 }}>
          {entry.from_phase
            ? <><PhaseBadge phase={entry.from_phase} size="sm" />
                <span style={{ color: "#334155", margin: "0 4px" }}>→</span></>
            : null}
          <PhaseBadge phase={entry.to_phase} size="sm" />
        </span>
        {entry.transition_reason && (
          <span style={{ color: "#64748b", fontSize: 11, marginLeft: 4 }}>
            [{entry.transition_reason}]
          </span>
        )}
        <span style={{ marginLeft: "auto", color: "#334155", fontSize: 11, whiteSpace: "nowrap" }}>
          {entry.actor_type}{entry.actor_id ? ` · ${entry.actor_id}` : ""}
        </span>
        <span style={{ color: "#334155", fontSize: 11, whiteSpace: "nowrap" }}>
          {fmtTs(entry.created_at)}
        </span>
        {entry.message && (
          <span style={{ color: "#334155", fontSize: 11 }}>{expanded ? "▲" : "▼"}</span>
        )}
      </div>
      {expanded && entry.message && (
        <div style={{
          padding: "4px 10px 8px 26px",
          color: "#94a3b8",
          fontSize: 11,
          borderTop: "1px solid #1e293b",
          whiteSpace: "pre-wrap",
          position: "relative",
        }}>
          <button
            onClick={() => navigator.clipboard?.writeText(entry.message!)}
            title="Копировать"
            style={{
              position: "absolute", top: 4, right: 8,
              background: "none", border: "none", color: "#334155",
              cursor: "pointer", fontSize: 11, padding: 2,
            }}
          >
            ⎘
          </button>
          {entry.message}
        </div>
      )}
    </div>
  );
}
