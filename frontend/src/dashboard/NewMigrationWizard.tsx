import React, { useState } from "react";
import { t } from "../theme";
import { Icon } from "../components/ui";

interface Props {
  onClose:  () => void;
  onSubmit: (params: WizardData) => void;
}

interface WizardData {
  sourceCluster: string;
  sourceVersion: string;
  sourceSchema:  string;
  priority:      "P0" | "P1" | "P2";
  targetCluster: string;
  targetVersion: string;
  targetSchema:  string;
  mode:          "bulk" | "bulk+cdc" | "cdc";
  workers:       number;
  cutoverAt:     string;
  validate:      boolean;
}

const STEPS = ["Источник", "Цель", "Объём", "Подтверждение"];

const INITIAL: WizardData = {
  sourceCluster: "", sourceVersion: "12.2.0.1", sourceSchema: "",
  priority: "P1",
  targetCluster: "", targetVersion: "19.21", targetSchema: "",
  mode: "bulk+cdc", workers: 8, cutoverAt: "", validate: true,
};

export function NewMigrationWizard({ onClose, onSubmit }: Props) {
  const [step, setStep] = useState(0);
  const [data, setData] = useState<WizardData>(INITIAL);
  const update = (patch: Partial<WizardData>) => setData(s => ({ ...s, ...patch }));

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
        width:        "min(720px, 100%)",
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
            <h2 style={{
              margin: 0, fontSize: 17, fontWeight: 600, letterSpacing: "-0.015em",
              fontFamily: t.font.mono,
            }}>
              {STEPS[step]}
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

        {/* Steps */}
        <div style={{
          display: "flex", gap: 0,
          padding: "12px 22px",
          borderBottom: `1px solid ${t.border.subtle}`,
          background: t.bg.s2,
        }}>
          {STEPS.map((s, i) => {
            const state = i < step ? "done" : i === step ? "on" : "idle";
            return (
              <div key={s} style={{
                flex: 1, display: "flex", alignItems: "center", gap: 8,
                padding: "2px 0", position: "relative",
              }}>
                {i > 0 && (
                  <span aria-hidden style={{
                    position: "absolute", left: 0, top: "50%",
                    width: 12, height: 1, background: t.border.subtle,
                    transform: "translateX(-100%)",
                  }}/>
                )}
                <span style={{
                  width: 20, height: 20, borderRadius: "50%",
                  display: "grid", placeItems: "center",
                  fontSize: "10.5px", fontWeight: 600,
                  background:
                    state === "on"   ? t.text.primary :
                    state === "done" ? t.tone.ok :
                                       t.bg.s3,
                  color:
                    state === "on"   ? t.text.inverse :
                    state === "done" ? t.text.inverse :
                                       t.text.muted,
                  flexShrink: 0,
                }}>
                  {state === "done" ? "✓" : i + 1}
                </span>
                <span style={{
                  fontSize: "11.5px", fontWeight: 500,
                  color: state === "on" ? t.text.primary : t.text.muted,
                }}>{s}</span>
              </div>
            );
          })}
        </div>

        {/* Body */}
        <div style={{ flex: 1, overflowY: "auto", padding: "20px 22px" }}>
          {step === 0 && <SourceStep data={data} update={update}/>}
          {step === 1 && <TargetStep data={data} update={update}/>}
          {step === 2 && <ScopeStep data={data} update={update}/>}
          {step === 3 && <SummaryStep data={data}/>}
        </div>

        {/* Foot */}
        <div style={{
          display: "flex", justifyContent: "space-between", alignItems: "center",
          padding: "12px 22px",
          borderTop: `1px solid ${t.border.subtle}`,
          background: t.bg.s2,
          borderRadius: `0 0 ${t.radius.xl} ${t.radius.xl}`,
        }}>
          <span style={{ fontSize: 12, color: t.text.muted }}>
            Шаг {step + 1} из {STEPS.length}
          </span>
          <div style={{ display: "flex", gap: 8 }}>
            {step > 0 && (
              <FootBtn onClick={() => setStep(s => s - 1)}>Назад</FootBtn>
            )}
            {step < STEPS.length - 1 ? (
              <FootBtn primary onClick={() => setStep(s => s + 1)}>Далее</FootBtn>
            ) : (
              <FootBtn primary onClick={() => onSubmit(data)}>Создать миграцию</FootBtn>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

function FootBtn({ children, onClick, primary }: {
  children: React.ReactNode;
  onClick:  () => void;
  primary?: boolean;
}) {
  return (
    <button onClick={onClick} style={{
      display: "inline-flex", alignItems: "center", gap: 6,
      padding: "6px 14px",
      borderRadius: t.radius.sm,
      fontSize: "12.5px", fontWeight: 500,
      cursor: "pointer",
      background: primary ? t.text.primary : t.bg.s1,
      color:      primary ? t.text.inverse : t.text.secondary,
      border:     `1px solid ${primary ? t.text.primary : t.border.subtle}`,
    }}>
      {children}
    </button>
  );
}

function Field({ label, full, children }: { label: string; full?: boolean; children: React.ReactNode }) {
  return (
    <div style={{
      display: "flex", flexDirection: "column", gap: 6,
      minWidth: 0,
      gridColumn: full ? "1 / -1" : undefined,
    }}>
      <label style={{ fontSize: 11, color: t.text.muted, fontWeight: 500 }}>{label}</label>
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

function SourceStep({ data, update }: { data: WizardData; update: (p: Partial<WizardData>) => void }) {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
      <Field label="Source кластер">
        <input style={inputStyle} value={data.sourceCluster} onChange={e => update({ sourceCluster: e.target.value })} placeholder="ora-prod-01"/>
      </Field>
      <Field label="Версия Oracle">
        <select style={inputStyle} value={data.sourceVersion} onChange={e => update({ sourceVersion: e.target.value })}>
          <option>12.2.0.1</option><option>18.3</option><option>19.21</option>
        </select>
      </Field>
      <Field label="Схема">
        <input style={{ ...inputStyle, fontFamily: t.font.mono }} value={data.sourceSchema} onChange={e => update({ sourceSchema: e.target.value })} placeholder="BILLING"/>
      </Field>
      <Field label="Приоритет">
        <select style={inputStyle} value={data.priority} onChange={e => update({ priority: e.target.value as "P0" | "P1" | "P2" })}>
          <option>P0</option><option>P1</option><option>P2</option>
        </select>
      </Field>
      <Field label="Pre-check" full>
        <div style={{
          background: t.bg.s2,
          border: `1px solid ${t.border.subtle}`,
          borderRadius: t.radius.lg,
          padding: "10px 12px",
          fontSize: 12, color: t.text.muted,
        }}>
          После заполнения «Схема» здесь появится список найденных объектов и предупреждения о deprecated features.
        </div>
      </Field>
    </div>
  );
}

function TargetStep({ data, update }: { data: WizardData; update: (p: Partial<WizardData>) => void }) {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
      <Field label="Target кластер">
        <input style={inputStyle} value={data.targetCluster} onChange={e => update({ targetCluster: e.target.value })} placeholder="ora-rac-04"/>
      </Field>
      <Field label="Target версия">
        <select style={inputStyle} value={data.targetVersion} onChange={e => update({ targetVersion: e.target.value })}>
          <option>19.21</option><option>21c</option>
        </select>
      </Field>
      <Field label="Target schema" full>
        <input style={{ ...inputStyle, fontFamily: t.font.mono }} value={data.targetSchema} onChange={e => update({ targetSchema: e.target.value })} placeholder="BILLING_19"/>
      </Field>
    </div>
  );
}

function ScopeStep({ data, update }: { data: WizardData; update: (p: Partial<WizardData>) => void }) {
  const MODES: { v: WizardData["mode"]; l: string }[] = [
    { v: "bulk",     l: "Только bulk" },
    { v: "bulk+cdc", l: "Bulk + CDC" },
    { v: "cdc",      l: "Только CDC" },
  ];
  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
      <Field label="Режим переноса" full>
        <div style={{
          display: "inline-flex", width: "100%",
          background: t.bg.s1,
          border: `1px solid ${t.border.subtle}`,
          borderRadius: t.radius.sm,
          padding: 2,
        }}>
          {MODES.map(m => (
            <button key={m.v} onClick={() => update({ mode: m.v })} style={{
              flex: 1, padding: "8px 14px", fontSize: 13,
              border: "none", borderRadius: 4, cursor: "pointer",
              background: data.mode === m.v ? t.bg.s3 : "transparent",
              color: data.mode === m.v ? t.text.primary : t.text.muted,
              fontWeight: 500,
            }}>{m.l}</button>
          ))}
        </div>
      </Field>
      <Field label="Параллельных воркеров">
        <input type="number" style={{ ...inputStyle, fontFamily: t.font.mono }}
               value={data.workers} onChange={e => update({ workers: parseInt(e.target.value) || 1 })}/>
      </Field>
      <Field label="Окно cutover">
        <input type="datetime-local" style={inputStyle}
               value={data.cutoverAt} onChange={e => update({ cutoverAt: e.target.value })}/>
      </Field>
      <Field label="Валидация" full>
        <label style={{
          display: "inline-flex", alignItems: "center", gap: 8,
          fontSize: "12.5px", cursor: "pointer", padding: "6px 0",
        }}>
          <input
            type="checkbox" checked={data.validate}
            onChange={e => update({ validate: e.target.checked })}
            style={{ accentColor: t.tone.accent, width: 14, height: 14 }}
          />
          Хэш-сравнение + count rows
        </label>
      </Field>
    </div>
  );
}

function SummaryStep({ data }: { data: WizardData }) {
  const rows = [
    ["Source",    `${data.sourceCluster || "—"} · ${data.sourceVersion}`],
    ["Схема",     data.sourceSchema || "—"],
    ["Target",    `${data.targetCluster || "—"} · ${data.targetVersion}`],
    ["Target sc.", data.targetSchema || "—"],
    ["Приоритет", data.priority],
    ["Режим",     data.mode],
    ["Воркеров",  String(data.workers)],
    ["Cutover",   data.cutoverAt || "—"],
    ["Валидация", data.validate ? "хэш + count" : "выкл."],
  ];
  return (
    <>
      <div style={{
        display: "flex", flexDirection: "column", gap: 1,
        background: t.border.subtle, borderRadius: t.radius.sm,
        overflow: "hidden",
      }}>
        {rows.map(([l, v]) => (
          <div key={l} style={{
            display: "grid", gridTemplateColumns: "160px 1fr",
            gap: 16, padding: "9px 13px",
            background: t.bg.s1, fontSize: "12.5px",
          }}>
            <span style={{ fontSize: "11.5px", color: t.text.muted }}>{l}</span>
            <span style={{ fontFamily: t.font.mono }}>{v}</span>
          </div>
        ))}
      </div>
      <div style={{
        background: t.tone.accentSoft,
        color: t.tone.accent,
        padding: "10px 13px",
        fontSize: 12, lineHeight: 1.5,
        marginTop: 12, borderRadius: t.radius.sm,
      }}>
        После создания миграция уйдёт в DRAFT. Запуск — кнопкой «Старт» на дашборде новой миграции.
      </div>
    </>
  );
}
