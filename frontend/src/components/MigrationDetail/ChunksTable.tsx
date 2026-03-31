import React, { useEffect, useState } from "react";
import type { SSEEvent } from "../../hooks/useSSE";
import { pgBtn } from "./helpers";

// ── Types ────────────────────────────────────────────────────────────────────

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

// ── ChunksSection ────────────────────────────────────────────────────────────

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
        Загрузка...
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
              {retrying ? "..." : `↺ Повторить (${stats.failed})`}
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

// ── ChunksTab ────────────────────────────────────────────────────────────────

export function ChunksTab({ migrationId, sseEvents }: { migrationId: string; sseEvents: SSEEvent[] }) {
  return (
    <div>
      <ChunksSection migrationId={migrationId} chunkType="BULK"     sseEvents={sseEvents} />
      <ChunksSection migrationId={migrationId} chunkType="BASELINE" sseEvents={sseEvents} />
    </div>
  );
}
