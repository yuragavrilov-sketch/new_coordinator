import React from "react";
import { t } from "../../theme";
import { S } from "./styles";

// ── Section wrapper ───────────────────────────────────────────────────────────

export function Section({
  title, accent, children,
}: {
  title: string; accent?: string; children: React.ReactNode;
}) {
  return (
    <div style={S.secWrap(accent)}>
      <div style={S.secHead(accent)}>{title}</div>
      <div style={S.secBody}>{children}</div>
    </div>
  );
}

// ── Field (label + control + hint/error) ──────────────────────────────────────

export function Field({
  label, required, error, hint, children,
}: {
  label: string; required?: boolean; error?: string; hint?: string;
  children: React.ReactNode;
}) {
  return (
    <div style={S.field}>
      <label style={S.label}>
        {label}{required && <span style={S.req}>*</span>}
      </label>
      {children}
      {hint  && !error && <div style={S.hint}>{hint}</div>}
      {error && <div style={S.err}>{error}</div>}
    </div>
  );
}

// ── TextInput ─────────────────────────────────────────────────────────────────

export function TextInput({
  value, placeholder, hasError, onChange,
}: {
  value: string; placeholder?: string; hasError?: boolean;
  onChange: (v: string) => void;
}) {
  return (
    <input
      style={hasError ? S.inputErr : S.input}
      value={value}
      placeholder={placeholder}
      onChange={e => onChange(e.target.value)}
    />
  );
}

// ── Chip ──────────────────────────────────────────────────────────────────────

export function Chip({ label, color, bg }: { label: string; color: string; bg: string }) {
  return (
    <span style={{
      background: bg, color, border: `1px solid ${color}44`,
      borderRadius: t.radius.sm, fontSize: t.size.xs, fontWeight: 700,
      padding: "2px 8px", display: "inline-block",
    }}>{label}</span>
  );
}

// ── KeyTypeBtn ────────────────────────────────────────────────────────────────

export function KeyTypeBtn({
  label, active, disabled, onClick,
}: {
  label: string; active: boolean; disabled?: boolean; onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      style={{
        padding:      "5px 12px",
        fontSize:     t.size.sm,
        fontWeight:   700,
        borderRadius: t.radius.md,
        border:       `1px solid ${active ? t.blue.base : t.border.base}`,
        background:   active ? t.bg.s3 : t.bg.s2,
        color:        disabled ? t.text.faint : active ? t.blue.fg : t.text.muted,
        cursor:       disabled ? "not-allowed" : "pointer",
      }}
    >
      {label}
    </button>
  );
}
