import React, { useEffect, useState } from "react";
import type { MigrationDetail, StateHistoryEntry } from "../../types/migration";
import type { SSEEvent } from "../../hooks/useSSE";
import { PhaseBadge } from "../PhaseBadge";
import { fmtTs } from "../../utils/format";
import { StatusDot, SectionHeader, isCdcMode } from "./helpers";
import { OverviewTab } from "./MigrationProperties";
import { StatisticsTab } from "./MigrationStatistics";
import { ChunksTab } from "./ChunksTable";
import {
  EnableIndexesButton, EnableTriggersButton, RestartBaselineButton,
  DataMismatchButtons, StopDeleteButtons,
} from "./PhaseActions";

interface Props {
  migrationId: string;
  onClose: () => void;
  sseEvents?: SSEEvent[];
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
          {phase === "DATA_MISMATCH" && (
            <DataMismatchButtons migrationId={migrationId} onDone={loadDetail} />
          )}
          {(phase === "CDC_CATCHING_UP" || phase === "CDC_CAUGHT_UP" || phase === "STEADY_STATE") &&
            detail && isCdcMode(detail) && (
            <EnableTriggersButton migrationId={migrationId} onDone={loadDetail} />
          )}
          <StopDeleteButtons migrationId={migrationId} phase={phase} onDone={loadDetail} onDeleted={onClose} />
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
              loadDetail={loadDetail}
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
