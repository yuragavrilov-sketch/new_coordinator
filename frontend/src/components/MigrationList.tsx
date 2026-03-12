import { useEffect, useState, useCallback } from "react";
import type { MigrationSummary } from "../types/migration";
import { PhaseBadge } from "./PhaseBadge";
import { MigrationDetailPanel } from "./MigrationDetail";
import { CreateMigrationModal } from "./CreateMigrationModal";

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

export function MigrationList({ refreshSignal }: { refreshSignal?: number }) {
  const [migrations,  setMigrations] = useState<MigrationSummary[]>([]);
  const [loading,     setLoading]    = useState(true);
  const [error,       setError]      = useState<string | null>(null);
  const [selectedId,  setSelectedId] = useState<string | null>(null);
  const [showCreate,  setShowCreate] = useState(false);

  const load = useCallback(() => {
    fetch("/api/migrations")
      .then(r => r.ok ? r.json() : Promise.reject(`HTTP ${r.status}`))
      .then(data => { setMigrations(data); setLoading(false); setError(null); })
      .catch(e => { setError(String(e)); setLoading(false); });
  }, []);

  useEffect(() => { load(); }, [load, refreshSignal]);

  // Auto-refresh every 10s
  useEffect(() => {
    const id = setInterval(load, 10_000);
    return () => clearInterval(id);
  }, [load]);

  const selected = migrations.find(m => m.migration_id === selectedId) ?? null;

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
          padding: "10px 14px",
          borderBottom: "1px solid #1e293b",
          display: "flex",
          alignItems: "center",
          gap: 8,
        }}>
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
            ↺ обновить
          </button>
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
          {!loading && !error && migrations.length === 0 && <EmptyState />}
          {!loading && !error && migrations.map(m => (
            <MigrationRow
              key={m.migration_id}
              m={m}
              selected={m.migration_id === selectedId}
              compact={!!selected}
              onClick={() => setSelectedId(id => id === m.migration_id ? null : m.migration_id)}
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
          />
        </div>
      )}
    </div>
    </>
  );
}

function MigrationRow({
  m, selected, compact, onClick,
}: {
  m: MigrationSummary;
  selected: boolean;
  compact: boolean;
  onClick: () => void;
}) {
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
      {/* Name + phase */}
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 5 }}>
        <span style={{
          fontWeight: 700, fontSize: 13, color: "#e2e8f0",
          overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1,
        }}>
          {m.migration_name}
        </span>
        <PhaseBadge phase={m.phase} size="sm" />
      </div>

      {/* Source → Target */}
      <div style={{ fontSize: 11, color: "#475569", marginBottom: 4 }}>
        <span style={{ color: "#64748b" }}>{m.source_schema}.{m.source_table}</span>
        <span style={{ color: "#334155", margin: "0 6px" }}>→</span>
        <span style={{ color: "#64748b" }}>{m.target_schema}.{m.target_table}</span>
      </div>

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
