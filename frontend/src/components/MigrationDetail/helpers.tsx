import React, { useEffect, useState } from "react";

// ── Shared small widgets ──────────────────────────────────────────────────────

export function InfoRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div style={{ display: "contents" }}>
      <span style={{ color: "#64748b", fontSize: 12 }}>{label}</span>
      <span style={{ color: "#e2e8f0", fontSize: 12, wordBreak: "break-all" }}>
        {value ?? <span style={{ color: "#334155" }}>—</span>}
      </span>
    </div>
  );
}

export function StatusDot({ status }: { status: string }) {
  const color = status === "SUCCESS" ? "#22c55e" : status === "FAILED" ? "#ef4444" : "#f59e0b";
  return (
    <span style={{
      width: 7, height: 7, borderRadius: "50%", background: color,
      display: "inline-block", marginRight: 6, flexShrink: 0,
    }} />
  );
}

export function SectionHeader({ children }: { children: React.ReactNode }) {
  return (
    <div style={{
      color: "#475569", fontSize: 11, fontWeight: 700,
      textTransform: "uppercase", letterSpacing: 0.8, marginBottom: 8,
    }}>
      {children}
    </div>
  );
}

export function InfoGrid({ title, children }: { title: string; children: React.ReactNode }) {
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

export function EnsureChip({ label, color, bg }: { label: string; color: string; bg: string }) {
  return (
    <span style={{
      background: bg, color, border: `1px solid ${color}44`,
      borderRadius: 4, fontSize: 10, fontWeight: 700,
      padding: "2px 8px", display: "inline-block",
    }}>{label}</span>
  );
}

export function StatTile({
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

export function pgBtn(disabled: boolean): React.CSSProperties {
  return {
    background: "none", border: "1px solid #1e293b", borderRadius: 3,
    color: disabled ? "#1e293b" : "#64748b", cursor: disabled ? "default" : "pointer",
    padding: "1px 5px", fontSize: 10, fontWeight: 700, lineHeight: 1.2,
  };
}

// ── WorkerCountEditor ─────────────────────────────────────────────────────────

export function WorkerCountEditor({
  migrationId, field, value, onSaved,
}: {
  migrationId: string;
  field: "max_parallel_workers" | "baseline_parallel_degree";
  value: number;
  onSaved: () => void;
}) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft]     = useState(value);
  const [saving, setSaving]   = useState(false);

  useEffect(() => { setDraft(value); }, [value]);

  async function save() {
    const v = Math.max(1, draft);
    if (v === value) { setEditing(false); return; }
    setSaving(true);
    try {
      const res = await fetch(`/api/migrations/${migrationId}/workers`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ [field]: v }),
      });
      if (res.ok) { onSaved(); setEditing(false); }
    } finally { setSaving(false); }
  }

  if (!editing) {
    return (
      <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
        <span style={{ color: "#e2e8f0" }}>{value}</span>
        <button
          onClick={() => { setDraft(value); setEditing(true); }}
          style={{
            background: "none", border: "1px solid #334155", borderRadius: 4,
            color: "#94a3b8", fontSize: 10, padding: "1px 6px", cursor: "pointer",
          }}
        >
          изменить
        </button>
      </span>
    );
  }

  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
      <input
        type="number" min={1} value={draft}
        onChange={e => setDraft(parseInt(e.target.value) || 1)}
        onKeyDown={e => { if (e.key === "Enter") save(); if (e.key === "Escape") setEditing(false); }}
        autoFocus
        style={{
          width: 60, background: "#0f172a", border: "1px solid #3b82f6",
          borderRadius: 4, color: "#e2e8f0", fontSize: 12, padding: "2px 6px",
          textAlign: "center",
        }}
      />
      <button
        onClick={save} disabled={saving}
        style={{
          background: "#1e3a5f", border: "1px solid #1d4ed8", borderRadius: 4,
          color: "#93c5fd", fontSize: 10, padding: "2px 8px", cursor: saving ? "not-allowed" : "pointer",
        }}
      >
        {saving ? "..." : "OK"}
      </button>
      <button
        onClick={() => setEditing(false)}
        style={{
          background: "none", border: "1px solid #334155", borderRadius: 4,
          color: "#64748b", fontSize: 10, padding: "2px 6px", cursor: "pointer",
        }}
      >
        ✕
      </button>
    </span>
  );
}

// ── useNow hook ──────────────────────────────────────────────────────────────

export function useNow(intervalMs = 5000) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(id);
  }, [intervalMs]);
  return now;
}

// ── Phase sets ────────────────────────────────────────────────────────────────

export const BULK_PHASES      = new Set(["CHUNKING", "BULK_LOADING", "BULK_LOADED", "BASELINE_LOADING", "DATA_VERIFYING"]);
export const CONNECTOR_PHASES = new Set([
  "SCN_FIXED", "CONNECTOR_STARTING", "CDC_BUFFERING",
  "CHUNKING", "BULK_LOADING", "BULK_LOADED",
  "STAGE_VALIDATING", "STAGE_VALIDATED",
  "BASELINE_PUBLISHING", "BASELINE_PUBLISHED",
  "CDC_APPLY_STARTING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP", "STEADY_STATE",
]);
export const LAG_PHASES       = new Set([
  "CDC_APPLY_STARTING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP", "STEADY_STATE",
]);

export function isCdcMode(detail: { migration_mode?: string }): boolean {
  return (detail.migration_mode ?? "CDC").toUpperCase() !== "BULK_ONLY";
}

export const VALIDATION_PHASES = new Set([
  "STAGE_VALIDATED", "BASELINE_PUBLISHING", "BASELINE_PUBLISHED",
  "CDC_APPLY_STARTING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP", "STEADY_STATE",
]);

export const TERMINAL_PHASES = new Set(["COMPLETED", "CANCELLED", "FAILED"]);
