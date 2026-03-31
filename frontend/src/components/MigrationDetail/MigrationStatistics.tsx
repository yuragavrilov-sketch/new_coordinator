import React from "react";
import type { MigrationDetail } from "../../types/migration";
import { fmtTs, fmtNum, fmtDuration } from "../../utils/format";
import { SectionHeader, StatTile, useNow, TERMINAL_PHASES } from "./helpers";

interface PhasePeriod {
  phase: string;
  startedAt: Date;
  endedAt: Date | null;
  durationMs: number;
}

export function StatisticsTab({ detail }: { detail: MigrationDetail }) {
  const now = useNow(5000);
  const isTerminal = TERMINAL_PHASES.has(detail.phase);

  // Build phase periods from history (history is newest-first -> reverse)
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
      {/* -- Total duration banner -- */}
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

      {/* -- Key metrics grid -- */}
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

      {/* -- Phase timeline -- */}
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
