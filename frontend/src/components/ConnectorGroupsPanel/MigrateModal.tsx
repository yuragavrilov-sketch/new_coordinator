import React, { useState } from "react";
import ReactDOM from "react-dom";
import { t } from "../../theme";
import type { GroupTable, MigrateParams } from "./types";
import { MIGRATE_DEFAULTS } from "./helpers";
import { StrategyPicker } from "../StrategyPicker";
import { usesStage } from "../../types/migration";

interface Props {
  groupId:   string;
  table:     GroupTable;
  onClose:   () => void;
  onCreated: () => void;
}

const inp: React.CSSProperties = {
  background: t.bg.s2, border: `1px solid ${t.border.base}`, borderRadius: t.radius.md,
  color: t.text.primary, padding: "6px 10px", fontSize: t.size.base, width: "100%",
};

const lbl: React.CSSProperties = {
  fontSize: t.size.sm, color: t.text.muted, fontWeight: 600,
  letterSpacing: 0.3, marginBottom: 3,
};

const hint: React.CSSProperties = { fontSize: t.size.xs, color: t.text.disabled, marginTop: 2 };

export function MigrateModal({ groupId, table, onClose, onCreated }: Props) {
  const [params,     setParams]     = useState<MigrateParams>(MIGRATE_DEFAULTS);
  const [submitting, setSubmitting] = useState(false);
  const [error,      setError]      = useState("");

  const set = (up: Partial<MigrateParams>) => setParams(p => ({ ...p, ...up }));

  const handleSubmit = async () => {
    setSubmitting(true);
    setError("");
    try {
      const r = await fetch(`/api/connector-groups/${groupId}/create-migration`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ table_id: table.id, ...params }),
      });
      const d = await r.json();
      if (!r.ok) { setError(d.error || "Ошибка создания миграции"); return; }
      onCreated();
    } catch (e) {
      setError(String(e));
    } finally {
      setSubmitting(false);
    }
  };

  return ReactDOM.createPortal(
    <div
      style={{
        position: "fixed", inset: 0, background: "rgba(0,0,0,.72)",
        display: "flex", alignItems: "center", justifyContent: "center",
        zIndex: 1000,
      }}
      onClick={e => { if (e.target === e.currentTarget) onClose(); }}
    >
      <div style={{
        background: t.bg.app, border: `1px solid ${t.border.subtle}`,
        borderRadius: t.radius.lg, width: "100%", maxWidth: 480,
        display: "flex", flexDirection: "column",
        boxShadow: "0 24px 48px rgba(0,0,0,.55)",
      }}>
        {/* Header */}
        <div style={{
          padding: "12px 20px", borderBottom: `1px solid ${t.border.subtle}`,
          display: "flex", alignItems: "center", gap: 10,
        }}>
          <span style={{ fontSize: t.size.lg, fontWeight: 700, color: t.text.primary }}>
            Создать миграцию
          </span>
          <span style={{ flex: 1 }} />
          <button onClick={onClose} style={{
            background: "none", border: "none", color: t.text.disabled,
            cursor: "pointer", fontSize: 18, lineHeight: 1, padding: "0 2px",
          }}>✕</button>
        </div>

        {/* Body */}
        <div style={{ padding: 20, display: "flex", flexDirection: "column", gap: 14 }}>
          {/* Table info */}
          <div style={{
            background: t.bg.s2, borderRadius: t.radius.md, padding: "10px 14px",
            display: "flex", flexDirection: "column", gap: 4,
          }}>
            <div style={{ fontSize: t.size.base, color: t.text.secondary }}>
              <span style={{ color: t.text.disabled }}>Source: </span>
              <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>
                {table.source_schema}.{table.source_table}
              </strong>
            </div>
            <div style={{ fontSize: t.size.base, color: t.text.secondary }}>
              <span style={{ color: t.text.disabled }}>Target: </span>
              <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>
                {table.target_schema}.{table.target_table}
              </strong>
            </div>
            <div style={{ fontSize: t.size.xs, color: t.text.disabled, marginTop: 2 }}>
              Key: {table.effective_key_type}
            </div>
          </div>

          {/* Strategy */}
          <StrategyPicker
            value={params.strategy}
            onChange={(s) => setParams({ ...params, strategy: s })}
          />

          {/* Stage tablespace */}
          {usesStage(params.strategy) && (
            <div>
              <div style={lbl}>Stage tablespace</div>
              <input
                style={inp} value={params.stage_tablespace}
                placeholder="например MIGRATION_DATA"
                onChange={e => set({ stage_tablespace: e.target.value })}
              />
              <div style={hint}>Если пусто — default tablespace схемы</div>
            </div>
          )}

          {/* Numeric params */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 10 }}>
            <div>
              <div style={lbl}>Chunk size</div>
              <input style={inp} type="number" value={params.chunk_size} min={1}
                onChange={e => set({ chunk_size: parseInt(e.target.value) || 0 })} />
              <div style={hint}>Строк на чанк</div>
            </div>
            <div>
              <div style={lbl}>Воркеры (bulk)</div>
              <input style={inp} type="number" value={params.max_parallel_workers} min={1}
                onChange={e => set({ max_parallel_workers: Math.max(1, parseInt(e.target.value) || 1) })} />
            </div>
            <div>
              <div style={lbl}>Воркеры (baseline)</div>
              <input style={inp} type="number" value={params.baseline_parallel_degree} min={1}
                onChange={e => set({ baseline_parallel_degree: Math.max(1, parseInt(e.target.value) || 4) })} />
            </div>
          </div>

          {/* Hash validation */}
          <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
            <input
              type="checkbox"
              checked={params.validate_hash_sample}
              onChange={e => set({ validate_hash_sample: e.target.checked })}
            />
            <span style={{ fontSize: t.size.base, color: t.text.secondary }}>
              Hash/sample валидация stage
            </span>
          </label>

          {/* Error */}
          {error && (
            <div style={{
              background: t.red.bg, border: `1px solid ${t.red.border}`,
              borderRadius: t.radius.md, padding: "8px 12px",
              fontSize: t.size.base, color: t.red.fg,
            }}>
              {error}
            </div>
          )}
        </div>

        {/* Footer */}
        <div style={{
          padding: "12px 20px", borderTop: `1px solid ${t.border.subtle}`,
          display: "flex", justifyContent: "flex-end", gap: 8,
        }}>
          <button onClick={onClose} style={{
            background: "none", border: `1px solid ${t.border.base}`,
            borderRadius: t.radius.md, color: t.text.muted,
            padding: "6px 16px", fontSize: t.size.base, cursor: "pointer",
          }}>
            Отмена
          </button>
          <button
            onClick={handleSubmit}
            disabled={submitting}
            style={{
              background:   submitting ? t.bg.s3 : t.green.dim,
              border:       "none",
              borderRadius: t.radius.md,
              color:        submitting ? t.text.muted : t.text.inverse,
              padding:      "6px 18px",
              fontSize:     t.size.base,
              fontWeight:   700,
              cursor:       submitting ? "not-allowed" : "pointer",
            }}
          >
            {submitting ? "Создание..." : "Создать и запустить"}
          </button>
        </div>
      </div>
    </div>,
    document.body,
  );
}
