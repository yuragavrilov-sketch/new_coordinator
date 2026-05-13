import React, { useEffect, useState } from "react";
import { t } from "../theme";
import { Icon } from "../components/ui";
import { useApi } from "../hooks/useApi";
import { SearchSelect } from "../components/TargetPrep/SearchSelect";

export interface WizardSubmit {
  sourceCluster: string;
  sourceVersion: string;
  sourceSchema:  string;
  targetCluster: string;
  targetVersion: string;
  targetSchema:  string;
}

interface Props {
  onClose:  () => void;
  onSubmit: (params: WizardSubmit) => void | Promise<void>;
}

interface DbInfo {
  host:           string;
  service_name:   string;
  configured:     boolean;
  version:        string;
  version_banner: string;
  ok:             boolean;
  error:          string | null;
}
type DbInfoResp = { source: DbInfo; target: DbInfo };

export function NewMigrationWizard({ onClose, onSubmit }: Props) {
  const [sourceSchema, setSourceSchema] = useState("");
  const [targetSchema, setTargetSchema] = useState("");
  const [targetEdited, setTargetEdited] = useState(false);
  const [submitting,   setSubmit]       = useState(false);

  // Pull host/version + schema lists
  const info        = useApi<DbInfoResp>("/api/db/info");
  const srcSchemas  = useApi<string[]>("/api/db/source/schemas");
  const tgtSchemas  = useApi<string[]>("/api/db/target/schemas");

  // Auto-mirror source → target until user explicitly picks a different target
  useEffect(() => {
    if (!targetEdited && sourceSchema && targetSchema !== sourceSchema) {
      setTargetSchema(sourceSchema);
    }
  }, [sourceSchema, targetEdited, targetSchema]);

  const src = info.data?.source;
  const tgt = info.data?.target;

  const ready = sourceSchema.length > 0 && targetSchema.length > 0 && !submitting;

  const handleSubmit = async () => {
    if (!ready) return;
    setSubmit(true);
    try {
      await onSubmit({
        sourceCluster: src?.host || "",
        sourceVersion: src?.version || "",
        sourceSchema,
        targetCluster: tgt?.host || "",
        targetVersion: tgt?.version || "",
        targetSchema,
      });
    } finally {
      setSubmit(false);
    }
  };

  return (
    <div onClick={onClose} style={{
      position: "fixed", inset: 0,
      background: "rgba(20,20,20,0.36)",
      backdropFilter: "blur(3px)",
      zIndex: 60,
      display: "grid", placeItems: "center",
      padding: 24,
      animation: "fadeIn 140ms",
    }}>
      <div onClick={e => e.stopPropagation()} style={{
        background:   t.bg.s1,
        borderRadius: t.radius.xl,
        border:       `1px solid ${t.border.subtle}`,
        width:        "min(520px, 100%)",
        maxHeight:    "90vh",
        display:      "flex",
        flexDirection:"column",
        boxShadow:    t.shadow.s3,
        animation:    "popIn 180ms cubic-bezier(.22,.61,.36,1)",
      }}>
        {/* Head */}
        <div style={{
          display: "flex", justifyContent: "space-between", alignItems: "flex-start",
          gap: 12, padding: "18px 22px 14px",
          borderBottom: `1px solid ${t.border.subtle}`,
        }}>
          <div>
            <div style={{
              fontSize: "10.5px", textTransform: "uppercase",
              letterSpacing: "0.07em", fontWeight: 600,
              marginBottom: 4, color: t.text.muted,
            }}>
              Новая миграция схемы
            </div>
            <h2 style={{ margin: 0, fontSize: 17, fontWeight: 600, letterSpacing: "-0.015em" }}>
              Создать
            </h2>
          </div>
          <button onClick={onClose} style={{
            border: "none", background: "transparent",
            color: t.text.muted, cursor: "pointer", padding: 4,
            display: "flex",
          }}>
            <Icon name="close" size={16}/>
          </button>
        </div>

        {/* Body */}
        <div style={{ flex: 1, overflowY: "auto", padding: "18px 22px", display: "flex", flexDirection: "column", gap: 14 }}>
          <EndpointBanner side="SOURCE" info={src} loading={info.loading}/>
          <Field label="Схема источника" loading={srcSchemas.loading} error={srcSchemas.error}>
            <SearchSelect
              value={sourceSchema}
              onChange={setSourceSchema}
              options={srcSchemas.data || []}
              placeholder="Выберите схему…"
              disabled={!src?.configured}
            />
          </Field>

          <EndpointBanner side="TARGET" info={tgt} loading={info.loading}/>
          <Field label="Схема таргета" hint="по умолчанию совпадает с источником" loading={tgtSchemas.loading} error={tgtSchemas.error}>
            <SearchSelect
              value={targetSchema}
              onChange={v => { setTargetSchema(v); setTargetEdited(true); }}
              options={tgtSchemas.data || []}
              placeholder="Выберите схему…"
              disabled={!tgt?.configured}
            />
          </Field>
        </div>

        {/* Foot */}
        <div style={{
          display: "flex", justifyContent: "flex-end", gap: 8,
          padding: "12px 22px",
          borderTop: `1px solid ${t.border.subtle}`,
          background: t.bg.s2,
          borderRadius: `0 0 ${t.radius.xl} ${t.radius.xl}`,
        }}>
          <FootBtn onClick={onClose}>Отмена</FootBtn>
          <FootBtn primary onClick={handleSubmit} disabled={!ready}>
            {submitting ? "Создаём…" : "Создать"}
          </FootBtn>
        </div>
      </div>
    </div>
  );
}

function EndpointBanner({ side, info, loading }: {
  side:    "SOURCE" | "TARGET";
  info?:   DbInfo;
  loading: boolean;
}) {
  const empty = !info || (!info.configured && !loading);
  return (
    <div style={{
      display: "flex", alignItems: "center", gap: 10,
      padding: "8px 12px",
      background: t.bg.s2,
      border: `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.sm,
    }}>
      <span style={{
        fontSize: "9.5px", fontWeight: 600,
        textTransform: "uppercase", letterSpacing: "0.08em",
        color: t.text.muted,
      }}>
        {side}
      </span>
      <span style={{ flex: 1, fontFamily: t.font.mono, fontSize: 12 }}>
        {loading && "загрузка…"}
        {!loading && empty && (
          <span style={{ color: t.tone.warn }}>
            не настроен — задай в «Настройках»
          </span>
        )}
        {!loading && info?.configured && (
          <>
            {info.host}
            {info.service_name && <span style={{ color: t.text.muted }}> · {info.service_name}</span>}
            {info.version && (
              <span style={{ color: t.text.muted }}> · {info.version}</span>
            )}
            {info.error && (
              <span style={{ color: t.tone.error, marginLeft: 8 }}>
                ⚠ {info.error}
              </span>
            )}
          </>
        )}
      </span>
    </div>
  );
}

function Field({ label, hint, loading, error, children }: {
  label:    string;
  hint?:    string;
  loading?: boolean;
  error?:   string | null;
  children: React.ReactNode;
}) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6, minWidth: 0 }}>
      <label style={{
        fontSize: 11, color: t.text.muted, fontWeight: 500,
        display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 8,
      }}>
        <span>{label}</span>
        {loading && <span style={{ fontSize: 10, color: t.text.faint }}>загрузка…</span>}
        {!loading && error && <span style={{ fontSize: 10, color: t.tone.error }}>⚠ {error}</span>}
        {!loading && !error && hint && <span style={{ fontSize: 10, color: t.text.faint }}>{hint}</span>}
      </label>
      {children}
    </div>
  );
}

function FootBtn({ children, onClick, primary, disabled }: {
  children:  React.ReactNode;
  onClick:   () => void;
  primary?:  boolean;
  disabled?: boolean;
}) {
  return (
    <button onClick={onClick} disabled={disabled} style={{
      display: "inline-flex", alignItems: "center", gap: 6,
      padding: "6px 14px",
      borderRadius: t.radius.sm,
      fontSize: "12.5px", fontWeight: 500,
      cursor: disabled ? "not-allowed" : "pointer",
      opacity: disabled ? 0.5 : 1,
      background: primary ? t.text.primary : t.bg.s1,
      color:      primary ? t.text.inverse : t.text.secondary,
      border:     `1px solid ${primary ? t.text.primary : t.border.subtle}`,
    }}>
      {children}
    </button>
  );
}
