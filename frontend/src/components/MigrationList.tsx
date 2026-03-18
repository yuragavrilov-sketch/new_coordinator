import React, { useEffect, useState, useCallback, useRef } from "react";
import type { MigrationSummary } from "../types/migration";
import type { SSEEvent } from "../hooks/useSSE";
import { PhaseBadge } from "./PhaseBadge";
import { MigrationDetailPanel } from "./MigrationDetail";
import { CreateMigrationModal } from "./CreateMigrationModal";

interface SpeedSnapshot { chunks_done: number; rows_loaded: number; ts: number }

function fmtSpeed(v: number): string {
  if (v >= 1000) return `${(v / 1000).toFixed(1)}k/s`;
  return `${v.toFixed(v < 10 ? 1 : 0)}/s`;
}

function fmtTs(iso: string): string {
  try {
    return new Date(iso).toLocaleString("ru-RU", {
      day: "2-digit", month: "2-digit", year: "numeric",
      hour: "2-digit", minute: "2-digit",
    });
  } catch { return iso; }
}

function EmptyState() {
  return (
    <div style={{
      display: "flex", flexDirection: "column", alignItems: "center",
      justifyContent: "center", padding: "64px 24px", color: "#334155",
    }}>
      <div style={{ fontSize: 40, marginBottom: 12, opacity: 0.4 }}>⇄</div>
      <div style={{ fontSize: 14, fontWeight: 600, color: "#475569" }}>Нет миграций</div>
      <div style={{ fontSize: 12, marginTop: 6, color: "#334155" }}>
        Миграции будут отображаться здесь после добавления
      </div>
    </div>
  );
}

const ACTIVE_PHASES = new Set([
  "NEW", "PREPARING", "SCN_FIXED", "CONNECTOR_STARTING", "CDC_BUFFERING",
  "CHUNKING", "BULK_LOADING", "BULK_LOADED",
  "STAGE_VALIDATING", "STAGE_VALIDATED",
  "BASELINE_PUBLISHING", "BASELINE_LOADING", "BASELINE_PUBLISHED",
  "STAGE_DROPPING", "INDEXES_ENABLING",
  "CDC_APPLY_STARTING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP",
  "STEADY_STATE",
]);
const DELETABLE_PHASES = new Set(["DRAFT", "CANCELLING", "CANCELLED", "FAILED"]);

type FilterKey = "all" | "active" | "done" | "error" | "draft";

const FILTER_LABELS: { key: FilterKey; label: string }[] = [
  { key: "all",    label: "Все"         },
  { key: "active", label: "Активные"    },
  { key: "done",   label: "Завершённые" },
  { key: "error",  label: "Ошибки"      },
  { key: "draft",  label: "Черновики"   },
];

const DONE_PHASES = new Set(["COMPLETED", "STEADY_STATE"]);

export function MigrationList({ refreshSignal, sseEvents }: { refreshSignal?: number; sseEvents?: SSEEvent[] }) {
  const [migrations,  setMigrations]  = useState<MigrationSummary[]>([]);
  const [loading,     setLoading]     = useState(true);
  const [error,       setError]       = useState<string | null>(null);
  const [selectedId,  setSelectedId]  = useState<string | null>(null);
  const [showCreate,  setShowCreate]  = useState(false);
  const [actionBusy,  setActionBusy]  = useState<string | null>(null); // migration_id
  const [speeds,      setSpeeds]      = useState<Record<string, { chunksSec: number; rowsSec: number }>>({});
  const [filter,      setFilter]      = useState<FilterKey>("all");
  const [search,      setSearch]      = useState("");
  const snapRef = useRef<Record<string, SpeedSnapshot>>({});

  const load = useCallback(() => {
    fetch("/api/migrations")
      .then(r => r.ok ? r.json() : Promise.reject(`HTTP ${r.status}`))
      .then((data: MigrationSummary[]) => {
        const now = Date.now();
        const newSpeeds: Record<string, { chunksSec: number; rowsSec: number }> = {};
        data.forEach(m => {
          const prev = snapRef.current[m.migration_id];
          if (prev && (m.chunks_done > prev.chunks_done || m.rows_loaded > prev.rows_loaded)) {
            const dt = (now - prev.ts) / 1000;
            const chunksSec = (m.chunks_done - prev.chunks_done) / dt;
            const rowsSec   = (m.rows_loaded - prev.rows_loaded) / dt;
            newSpeeds[m.migration_id] = { chunksSec, rowsSec };
          } else if (prev) {
            newSpeeds[m.migration_id] = speeds[m.migration_id] ?? { chunksSec: 0, rowsSec: 0 };
          }
          snapRef.current[m.migration_id] = { chunks_done: m.chunks_done, rows_loaded: m.rows_loaded, ts: now };
        });
        setSpeeds(newSpeeds);
        setMigrations(data);
        setLoading(false);
        setError(null);
      })
      .catch(e => { setError(String(e)); setLoading(false); });
  }, []); // eslint-disable-line

  useEffect(() => { load(); }, [load, refreshSignal]);

  // Refresh on migration_phase SSE events
  useEffect(() => {
    if (!sseEvents || sseEvents.length === 0) return;
    if (sseEvents[0].type === "migration_phase") load();
  }, [sseEvents]); // eslint-disable-line

  // Auto-refresh every 10s
  useEffect(() => {
    const id = setInterval(load, 10_000);
    return () => clearInterval(id);
  }, [load]);

  const visible = migrations.filter(m => {
    if (filter === "active") return ACTIVE_PHASES.has(m.phase);
    if (filter === "done")   return DONE_PHASES.has(m.phase);
    if (filter === "error")  return m.phase === "FAILED" || !!m.error_code;
    if (filter === "draft")  return m.phase === "DRAFT";
    return true;
  }).filter(m => {
    if (!search.trim()) return true;
    const q = search.toLowerCase();
    return (
      m.migration_name.toLowerCase().includes(q) ||
      `${m.source_schema}.${m.source_table}`.toLowerCase().includes(q) ||
      `${m.target_schema}.${m.target_table}`.toLowerCase().includes(q)
    );
  });

  const selected = migrations.find(m => m.migration_id === selectedId) ?? null;

  const handleAction = useCallback(async (id: string, action: "run" | "stop" | "delete") => {
    if (action === "delete") {
      if (!window.confirm("Удалить миграцию? Это действие необратимо.")) return;
    }
    setActionBusy(id);
    try {
      if (action === "run") {
        await fetch(`/api/migrations/${id}/action`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ action: "run" }),
        });
      } else if (action === "stop") {
        await fetch(`/api/migrations/${id}/action`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ action: "cancel" }),
        });
      } else {
        await fetch(`/api/migrations/${id}`, { method: "DELETE" });
        if (selectedId === id) setSelectedId(null);
      }
      load();
    } finally {
      setActionBusy(null);
    }
  }, [load, selectedId]);

  return (
    <>
    {showCreate && (
      <CreateMigrationModal
        onClose={() => setShowCreate(false)}
        onCreated={() => { setShowCreate(false); load(); }}
      />
    )}
    <div style={{ display: "flex", gap: 12, height: "calc(100vh - 180px)", minHeight: 400 }}>
      {/* List panel */}
      <div style={{
        width: selected ? "38%" : "100%",
        minWidth: 0,
        display: "flex",
        flexDirection: "column",
        background: "#0a111f",
        border: "1px solid #1e293b",
        borderRadius: 8,
        overflow: "hidden",
        transition: "width 0.2s",
      }}>
        {/* Toolbar */}
        <div style={{
          padding: "8px 14px",
          borderBottom: "1px solid #1e293b",
          display: "flex",
          flexDirection: "column",
          gap: 6,
        }}>
          {/* Row 1: title + action buttons */}
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <span style={{ fontWeight: 700, fontSize: 13, color: "#94a3b8" }}>
              Миграции
            </span>
            {migrations.length > 0 && (
              <span style={{
                background: "#1e293b", color: "#64748b",
                borderRadius: 10, fontSize: 11, padding: "1px 7px", fontWeight: 600,
              }}>{migrations.length}</span>
            )}
            <button
              onClick={() => setShowCreate(true)}
              style={{
                marginLeft: "auto", background: "#1d4ed8", border: "none",
                borderRadius: 5, color: "#fff", fontSize: 11, padding: "4px 10px",
                cursor: "pointer", fontWeight: 700,
              }}
            >
              + Добавить
            </button>
            <button
              onClick={load}
              style={{
                background: "none", border: "1px solid #1e293b",
                borderRadius: 5, color: "#475569", fontSize: 11, padding: "3px 8px",
                cursor: "pointer",
              }}
            >
              ↺
            </button>
          </div>
          {/* Row 2: filter pills + search */}
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            {FILTER_LABELS.map(({ key, label }) => {
              const count = key === "all" ? migrations.length
                : key === "active" ? migrations.filter(m => ACTIVE_PHASES.has(m.phase)).length
                : key === "done"   ? migrations.filter(m => DONE_PHASES.has(m.phase)).length
                : key === "error"  ? migrations.filter(m => m.phase === "FAILED" || !!m.error_code).length
                : migrations.filter(m => m.phase === "DRAFT").length;
              if (count === 0 && key !== "all") return null;
              const active = filter === key;
              return (
                <button key={key} onClick={() => setFilter(key)} style={{
                  background: active ? "#1e3a5f" : "none",
                  border: `1px solid ${active ? "#3b82f6" : "#1e293b"}`,
                  borderRadius: 12, color: active ? "#93c5fd" : "#475569",
                  fontSize: 10, padding: "2px 8px", cursor: "pointer", fontWeight: 600,
                  whiteSpace: "nowrap",
                }}>
                  {label} {count > 0 && <span style={{ opacity: 0.7 }}>{count}</span>}
                </button>
              );
            })}
            <input
              value={search}
              onChange={e => setSearch(e.target.value)}
              placeholder="Поиск…"
              style={{
                marginLeft: "auto", background: "#1e293b", border: "1px solid #334155",
                borderRadius: 5, color: "#e2e8f0", padding: "3px 8px",
                fontSize: 11, width: 120, outline: "none",
              }}
            />
          </div>
        </div>

        {/* Content */}
        <div style={{ flex: 1, overflow: "auto" }}>
          {loading && (
            <div style={{ padding: 24, color: "#475569", fontSize: 13 }}>Загрузка...</div>
          )}
          {error && (
            <div style={{ padding: 16, color: "#fca5a5", fontSize: 12 }}>
              Ошибка загрузки: {error}
            </div>
          )}
          {!loading && !error && visible.length === 0 && <EmptyState />}
          {!loading && !error && visible.map(m => (
            <MigrationRow
              key={m.migration_id}
              m={m}
              selected={m.migration_id === selectedId}
              compact={!!selected}
              busy={actionBusy === m.migration_id}
              speed={speeds[m.migration_id]}
              onClick={() => setSelectedId(id => id === m.migration_id ? null : m.migration_id)}
              onAction={handleAction}
            />
          ))}
        </div>
      </div>

      {/* Detail panel */}
      {selected && (
        <div style={{ flex: 1, minWidth: 0, overflow: "hidden" }}>
          <MigrationDetailPanel
            migrationId={selected.migration_id}
            onClose={() => setSelectedId(null)}
            sseEvents={sseEvents}
          />
        </div>
      )}
    </div>
    </>
  );
}

function ActionBtn({ icon, title, color, bg, disabled, onClick }: {
  icon: string; title: string; color: string; bg: string;
  disabled?: boolean; onClick: (e: React.MouseEvent) => void;
}) {
  return (
    <button
      title={title}
      disabled={disabled}
      onClick={onClick}
      style={{
        background: bg, border: `1px solid ${color}44`,
        borderRadius: 4, color, fontSize: 11, fontWeight: 700,
        padding: "2px 7px", cursor: disabled ? "not-allowed" : "pointer",
        opacity: disabled ? 0.4 : 1, lineHeight: 1.4,
      }}
    >
      {icon}
    </button>
  );
}

const BULK_PHASES = new Set(["CHUNKING", "BULK_LOADING", "BULK_LOADED"]);

function MigrationRow({
  m, selected, compact, busy, speed, onClick, onAction,
}: {
  m: MigrationSummary;
  selected: boolean;
  compact: boolean;
  busy: boolean;
  speed?: { chunksSec: number; rowsSec: number };
  onClick: () => void;
  onAction: (id: string, action: "run" | "stop" | "delete") => void;
}) {
  const canRun    = m.phase === "DRAFT";
  const canStop   = ACTIVE_PHASES.has(m.phase);
  const canDelete = DELETABLE_PHASES.has(m.phase);

  const showProgress = BULK_PHASES.has(m.phase) && m.total_chunks != null && m.total_chunks > 0;
  const pct = showProgress ? Math.min(100, (m.chunks_done / m.total_chunks!) * 100) : 0;

  return (
    <div
      onClick={onClick}
      style={{
        padding: compact ? "10px 14px" : "12px 16px",
        borderBottom: "1px solid #0f172a",
        cursor: "pointer",
        background: selected ? "#0d1e35" : "transparent",
        borderLeft: `3px solid ${selected ? "#3b82f6" : "transparent"}`,
        transition: "background 0.1s",
      }}
      onMouseEnter={e => { if (!selected) (e.currentTarget as HTMLDivElement).style.background = "#0d1829"; }}
      onMouseLeave={e => { if (!selected) (e.currentTarget as HTMLDivElement).style.background = "transparent"; }}
    >
      {/* Name + phase + actions */}
      <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 5 }}>
        <span style={{
          fontWeight: 700, fontSize: 13, color: "#e2e8f0",
          overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1,
          minWidth: 0,
        }}>
          {m.migration_name}
        </span>
        <PhaseBadge phase={m.phase} size="sm" />
        {m.phase === "NEW" && m.queue_position != null && (
          <span style={{
            background: "#3b2000", color: "#fcd34d",
            border: "1px solid #d97706", borderRadius: 4,
            fontSize: 10, fontWeight: 700, padding: "1px 6px",
          }}>
            #{m.queue_position} в очереди
          </span>
        )}
        {/* Action buttons — stop click propagation */}
        <div
          style={{ display: "flex", gap: 4, flexShrink: 0 }}
          onClick={e => e.stopPropagation()}
        >
          {canRun && (
            <ActionBtn icon="▶" title="Запустить" color="#86efac" bg="#052e16"
              disabled={busy}
              onClick={() => onAction(m.migration_id, "run")} />
          )}
          {canStop && (
            <ActionBtn icon="⏹" title="Остановить" color="#fca5a5" bg="#450a0a"
              disabled={busy}
              onClick={() => onAction(m.migration_id, "stop")} />
          )}
          {canDelete && (
            <ActionBtn icon="✕" title="Удалить" color="#94a3b8" bg="#1e293b"
              disabled={busy}
              onClick={() => onAction(m.migration_id, "delete")} />
          )}
          {busy && (
            <span style={{ fontSize: 10, color: "#475569", alignSelf: "center" }}>…</span>
          )}
        </div>
      </div>

      {/* Source → Target */}
      <div style={{ fontSize: 11, color: "#475569", marginBottom: 4 }}>
        <span style={{ color: "#64748b" }}>{m.source_schema}.{m.source_table}</span>
        <span style={{ color: "#334155", margin: "0 6px" }}>→</span>
        <span style={{ color: "#64748b" }}>{m.target_schema}.{m.target_table}</span>
      </div>

      {/* Progress bar (bulk phases only) */}
      {showProgress && (
        <div style={{ marginBottom: 5 }}>
          <div style={{
            height: 4, borderRadius: 2,
            background: "#1e293b", overflow: "hidden",
          }}>
            <div style={{
              height: "100%", borderRadius: 2,
              width: `${pct}%`,
              background: m.chunks_failed > 0 ? "#dc2626" : "#3b82f6",
              transition: "width 0.4s ease",
            }} />
          </div>
          <div style={{ display: "flex", justifyContent: "space-between", marginTop: 3 }}>
            <span style={{ fontSize: 10, color: "#475569" }}>
              {m.chunks_done}
              {m.chunks_failed > 0 && <span style={{ color: "#ef4444" }}> / {m.chunks_failed} ✗</span>}
              {" "}/ {m.total_chunks} чанков
              {m.rows_loaded > 0 && (
                <span style={{ color: "#334155" }}>
                  {" "}· {m.rows_loaded.toLocaleString("ru-RU")} строк
                </span>
              )}
            </span>
            {speed && (speed.chunksSec > 0 || speed.rowsSec > 0) && (
              <span style={{ fontSize: 10, color: "#64748b" }}>
                {speed.chunksSec > 0 && <>{fmtSpeed(speed.chunksSec)} чанк</>}
                {speed.chunksSec > 0 && speed.rowsSec > 0 && " · "}
                {speed.rowsSec > 0 && <>{fmtSpeed(speed.rowsSec)} строк/с</>}
              </span>
            )}
            <span style={{ fontSize: 10, color: "#475569" }}>{pct.toFixed(1)}%</span>
          </div>
        </div>
      )}

      {/* Error + meta */}
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        {m.error_code && (
          <span style={{
            background: "#450a0a", color: "#fca5a5",
            border: "1px solid #7f1d1d", borderRadius: 3,
            fontSize: 10, padding: "1px 5px", fontWeight: 600,
          }}>
            {m.error_code}
          </span>
        )}
        {m.retry_count > 0 && (
          <span style={{ fontSize: 10, color: "#f59e0b" }}>↺ {m.retry_count}</span>
        )}
        <span style={{ marginLeft: "auto", fontSize: 10, color: "#334155" }}>
          {fmtTs(m.state_changed_at)}
        </span>
      </div>
    </div>
  );
}
