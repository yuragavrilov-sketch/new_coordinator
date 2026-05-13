import React, { useEffect, useState } from "react";
import { t } from "../theme";
import { Icon } from "../components/ui";
import { useApi } from "../hooks/useApi";

export interface WizardSubmit {
  sourceCluster: string;
  sourceVersion: string;
  sourceSchema:  string;
  targetCluster: string;
  targetVersion: string;
  targetSchema:  string;
  priority:      "P0" | "P1" | "P2";
  cutoverAt:     string;
  description:   string;
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

interface FormState {
  sourceSchema:  string;
  targetSchema:  string;
  priority:      "P0" | "P1" | "P2";
  cutoverAt:     string;
  description:   string;
  targetEdited:  boolean;        // tracks whether user manually changed target
}

const INITIAL: FormState = {
  sourceSchema: "", targetSchema: "",
  priority: "P2", cutoverAt: "", description: "",
  targetEdited: false,
};

export function NewMigrationWizard({ onClose, onSubmit }: Props) {
  const [data, setData]         = useState<FormState>(INITIAL);
  const [submitting, setSubmit] = useState(false);

  // Pull source/target host+version from settings (one-shot fetch)
  const info = useApi<DbInfoResp>("/api/db/info");

  // Auto-mirror sourceSchema → targetSchema until user manually edits target
  useEffect(() => {
    if (!data.targetEdited && data.sourceSchema && data.targetSchema !== data.sourceSchema) {
      setData(s => ({ ...s, targetSchema: s.sourceSchema }));
    }
  }, [data.sourceSchema, data.targetEdited, data.targetSchema]);

  const update = (patch: Partial<FormState>) => setData(s => ({ ...s, ...patch }));

  const src = info.data?.source;
  const tgt = info.data?.target;

  const ready = data.sourceSchema.trim().length > 0
             && data.targetSchema.trim().length > 0
             && !submitting;

  const handleSubmit = async () => {
    if (!ready) return;
    setSubmit(true);
    try {
      await onSubmit({
        sourceCluster: src?.host || "",
        sourceVersion: src?.version || "",
        sourceSchema:  data.sourceSchema.trim().toUpperCase(),
        targetCluster: tgt?.host || "",
        targetVersion: tgt?.version || "",
        targetSchema:  data.targetSchema.trim().toUpperCase(),
        priority:      data.priority,
        cutoverAt:     data.cutoverAt,
        description:   data.description,
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
        width:        "min(560px, 100%)",
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
          {/* Source endpoint banner */}
          <EndpointBanner side="SOURCE" info={src} loading={info.loading}/>
          <Field label="Схема источника">
            <input
              autoFocus
              value={data.sourceSchema}
              onChange={e => update({ sourceSchema: e.target.value })}
              placeholder="BILLING"
              style={{ ...inputStyle, fontFamily: t.font.mono, textTransform: "uppercase" }}
            />
          </Field>

          {/* Target endpoint banner */}
          <EndpointBanner side="TARGET" info={tgt} loading={info.loading}/>
          <Field label="Схема таргета" hint="по умолчанию совпадает с источником">
            <input
              value={data.targetSchema}
              onChange={e => update({ targetSchema: e.target.value, targetEdited: true })}
              placeholder="BILLING_19"
              style={{ ...inputStyle, fontFamily: t.font.mono, textTransform: "uppercase" }}
            />
          </Field>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
            <Field label="Приоритет">
              <PrioritySelect value={data.priority} onChange={p => update({ priority: p })}/>
            </Field>
            <Field label="Окно cutover">
              <input
                type="datetime-local"
                value={data.cutoverAt}
                onChange={e => update({ cutoverAt: e.target.value })}
                style={inputStyle}
              />
            </Field>
          </div>

          <Field label="Описание" hint="опционально">
            <textarea
              value={data.description}
              onChange={e => update({ description: e.target.value })}
              placeholder="Зачем эта миграция, ссылки на задачи, ответственные…"
              rows={2}
              style={{ ...inputStyle, resize: "vertical", fontFamily: t.font.sans }}
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

function PrioritySelect({ value, onChange }: {
  value:    "P0" | "P1" | "P2";
  onChange: (v: "P0" | "P1" | "P2") => void;
}) {
  const OPTIONS: { v: "P0" | "P1" | "P2"; label: string; tone: string }[] = [
    { v: "P0", label: "P0 — критично",  tone: t.tone.error },
    { v: "P1", label: "P1 — высокий",   tone: t.tone.warn  },
    { v: "P2", label: "P2 — обычный",   tone: t.text.muted },
  ];
  return (
    <div style={{
      display: "inline-flex", width: "100%",
      background: t.bg.s1,
      border: `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.sm,
      padding: 2,
    }}>
      {OPTIONS.map(o => (
        <button key={o.v} onClick={() => onChange(o.v)} style={{
          flex: 1, padding: "6px 8px", fontSize: 12,
          border: "none", borderRadius: 4, cursor: "pointer",
          background: value === o.v ? t.bg.s3 : "transparent",
          color: value === o.v ? t.text.primary : o.tone,
          fontWeight: value === o.v ? 600 : 500,
        }}>{o.label}</button>
      ))}
    </div>
  );
}

function Field({ label, hint, children }: {
  label:    string;
  hint?:    string;
  children: React.ReactNode;
}) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6, minWidth: 0 }}>
      <label style={{
        fontSize: 11, color: t.text.muted, fontWeight: 500,
        display: "flex", justifyContent: "space-between", alignItems: "baseline",
      }}>
        <span>{label}</span>
        {hint && <span style={{ fontSize: 10, color: t.text.faint }}>{hint}</span>}
      </label>
      {children}
    </div>
  );
}

const inputStyle: React.CSSProperties = {
  padding: "7px 10px",
  border: `1px solid ${t.border.subtle}`,
  borderRadius: t.radius.sm,
  background: t.bg.s1,
  fontSize: "12.5px",
  outline: 0,
};

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
