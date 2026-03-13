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

function fmtTs(iso: string | null | undefined): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString("ru-RU", {
      day: "2-digit", month: "2-digit", year: "numeric",
      hour: "2-digit", minute: "2-digit", second: "2-digit",
    });
  } catch { return iso ?? "—"; }
}

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

// ── Phase sets for panel visibility ──────────────────────────────────────────

const BULK_PHASES      = new Set(["CHUNKING", "BULK_LOADING", "BULK_LOADED"]);
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

// ── Main component ────────────────────────────────────────────────────────────

export function MigrationDetailPanel({ migrationId, onClose, sseEvents = [] }: Props) {
  const [detail,  setDetail]  = useState<MigrationDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error,   setError]   = useState<string | null>(null);

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

  // Reload when a migration_phase event arrives for this migration
  useEffect(() => {
    const last = sseEvents[0];
    if (last?.type === "migration_phase" && last.migration_id === migrationId) {
      loadDetail();
    }
  }, [sseEvents]); // eslint-disable-line

  const phase = detail?.phase ?? "";

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
        padding: "12px 16px",
        borderBottom: "1px solid #1e293b",
        display: "flex",
        alignItems: "center",
        gap: 10,
        background: "#0a111f",
      }}>
        {detail && <PhaseBadge phase={detail.phase} />}
        <span style={{ fontWeight: 700, fontSize: 14, color: "#e2e8f0", flex: 1 }}>
          {detail?.migration_name ?? "Загрузка..."}
        </span>
        <button onClick={onClose} style={{
          background: "none", border: "none", color: "#475569",
          cursor: "pointer", fontSize: 18, lineHeight: 1, padding: "0 4px",
        }}>✕</button>
      </div>

      {loading && (
        <div style={{ padding: 24, color: "#475569", fontSize: 13 }}>Загрузка...</div>
      )}
      {error && (
        <div style={{ padding: 24, color: "#fca5a5", fontSize: 13 }}>Ошибка: {error}</div>
      )}

      {detail && !loading && (
        <div style={{ flex: 1, overflow: "auto", padding: 16 }}>

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

          {/* ── Phase-specific panels ── */}
          {CONNECTOR_PHASES.has(phase) && (
            <ConnectorPanel migrationId={migrationId} sseEvents={sseEvents} />
          )}
          {BULK_PHASES.has(phase) && (
            <BulkProgressPanel migrationId={migrationId} sseEvents={sseEvents} />
          )}
          {VALIDATION_PHASES.has(phase) && (
            <ValidationPanel migrationId={migrationId} />
          )}
          {LAG_PHASES.has(phase) && (
            <KafkaLagPanel migrationId={migrationId} sseEvents={sseEvents} />
          )}

          {/* ── Info grid ── */}
          <div style={{ fontSize: 12, marginBottom: 16 }}>
            <div style={{
              color: "#475569", fontSize: 11, fontWeight: 700,
              textTransform: "uppercase", letterSpacing: 0.8, marginBottom: 8,
            }}>
              Основное
            </div>
            <div style={{
              display: "grid",
              gridTemplateColumns: "minmax(160px, auto) 1fr",
              gap: "5px 16px",
            }}>
              <InfoRow label="ID" value={
                <span style={{ fontFamily: "monospace", fontSize: 11 }}>
                  {detail.migration_id}
                </span>
              } />
              <InfoRow label="Создана"       value={fmtTs(detail.created_at)} />
              <InfoRow label="Автор"         value={detail.created_by} />
              <InfoRow label="Описание"      value={detail.description} />
              <InfoRow label="Фаза изменена" value={fmtTs(detail.state_changed_at)} />
              <InfoRow label="Повторов"      value={
                detail.retry_count > 0
                  ? <span style={{ color: "#f59e0b" }}>{detail.retry_count}</span>
                  : "0"
              } />
            </div>
          </div>

          <div style={{ fontSize: 12, marginBottom: 16 }}>
            <div style={{
              color: "#475569", fontSize: 11, fontWeight: 700,
              textTransform: "uppercase", letterSpacing: 0.8, marginBottom: 8,
            }}>
              Источник → Цель
            </div>
            <div style={{
              display: "grid",
              gridTemplateColumns: "minmax(160px, auto) 1fr",
              gap: "5px 16px",
            }}>
              <InfoRow label="Source connection" value={detail.source_connection_id} />
              <InfoRow label="Source table"      value={`${detail.source_schema}.${detail.source_table}`} />
              <InfoRow label="Target connection" value={detail.target_connection_id} />
              <InfoRow label="Target table"      value={`${detail.target_schema}.${detail.target_table}`} />
              <InfoRow label="Stage table"       value={detail.stage_table_name} />
            </div>
          </div>

          <div style={{ fontSize: 12, marginBottom: 16 }}>
            <div style={{
              color: "#475569", fontSize: 11, fontWeight: 700,
              textTransform: "uppercase", letterSpacing: 0.8, marginBottom: 8,
            }}>
              Коннектор / Kafka
            </div>
            <div style={{
              display: "grid",
              gridTemplateColumns: "minmax(160px, auto) 1fr",
              gap: "5px 16px",
            }}>
              <InfoRow label="Connector"     value={detail.connector_name} />
              <InfoRow label="Topic prefix"  value={detail.topic_prefix} />
              <InfoRow label="Consumer group" value={detail.consumer_group} />
            </div>
          </div>

          <div style={{ fontSize: 12, marginBottom: 16 }}>
            <div style={{
              color: "#475569", fontSize: 11, fontWeight: 700,
              textTransform: "uppercase", letterSpacing: 0.8, marginBottom: 8,
            }}>
              Параметры загрузки
            </div>
            <div style={{
              display: "grid",
              gridTemplateColumns: "minmax(160px, auto) 1fr",
              gap: "5px 16px",
            }}>
              <InfoRow label="Chunk size"   value={detail.chunk_size?.toLocaleString()} />
              <InfoRow label="Max workers"  value={detail.max_parallel_workers} />
              <InfoRow label="Total chunks" value={detail.total_chunks ?? "—"} />
              <InfoRow label="Chunks done"  value={detail.chunks_done} />
              <InfoRow label="Start SCN"    value={detail.start_scn} />
              <InfoRow label="SCN fixed at" value={fmtTs(detail.scn_fixed_at)} />
              <InfoRow label="Hash/sample validate" value={
                detail.validate_hash_sample ? "включено" : "выключено"
              } />
            </div>
          </div>

          <div style={{ fontSize: 12, marginBottom: 16 }}>
            <div style={{
              color: "#475569", fontSize: 11, fontWeight: 700,
              textTransform: "uppercase", letterSpacing: 0.8, marginBottom: 8,
            }}>
              Ключ
            </div>
            <div style={{
              display: "grid",
              gridTemplateColumns: "minmax(160px, auto) 1fr",
              gap: "5px 16px",
            }}>
              <InfoRow label="Key type"   value={detail.effective_key_type} />
              <InfoRow label="Key source" value={detail.effective_key_source} />
              <InfoRow label="Key columns" value={
                <span style={{ fontFamily: "monospace", fontSize: 11 }}>
                  {detail.effective_key_columns_json}
                </span>
              } />
              <InfoRow label="PK exists" value={detail.source_pk_exists ? "да" : "нет"} />
              <InfoRow label="UK exists" value={detail.source_uk_exists ? "да" : "нет"} />
            </div>
          </div>

          {/* State history */}
          <div>
            <div style={{
              color: "#475569", fontSize: 11, fontWeight: 700,
              textTransform: "uppercase", letterSpacing: 0.8, marginBottom: 10,
            }}>
              История состояний ({detail.history.length})
            </div>
            {detail.history.length === 0 ? (
              <div style={{ color: "#334155", fontSize: 12 }}>Нет записей</div>
            ) : (
              <div style={{ display: "flex", flexDirection: "column", gap: 1 }}>
                {detail.history.map((h, i) => (
                  <HistoryRow key={h.id} entry={h} isFirst={i === 0} />
                ))}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

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
        }}>
          {entry.message}
        </div>
      )}
    </div>
  );
}
