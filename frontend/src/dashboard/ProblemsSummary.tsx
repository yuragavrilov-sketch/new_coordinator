import React, { useState } from "react";
import { t } from "../theme";
import { Icon } from "../components/ui";
import { OBJECT_TYPES, type SchemaObject } from "./types";
import { applyDdl, type DdlApplyAction } from "./api";

interface Props {
  missing:      SchemaObject[];
  diff:         SchemaObject[];
  srcInvalid:   SchemaObject[];
  tgtInvalid:   SchemaObject[];
  bothInvalid:  SchemaObject[];
  schemaMigrationId: string;
  onOpen:       (o: SchemaObject) => void;
  onApplied?:   () => void;          // called after a successful submit
}

/** Card above the object table summarising decision-required objects:
 *  - missing in target (must be created)
 *  - DDL differs (review)
 *  - INVALID in source (would propagate the error)
 *  - INVALID in target only (post-migration breakage)
 *  - INVALID in both (pre-existing — verify, don't auto-fail)
 */
/** Types reproducible via CREATE OR REPLACE (sync_diff). Others fall back to
 *  recreate (DROP+CREATE) when their DDL differs. */
const REPLACEABLE_TYPES = new Set([
  "VIEW", "PACKAGE", "PROCEDURE", "FUNCTION", "TRIGGER", "TYPE", "SYNONYM",
]);
const DATA_BEARING_TYPES = new Set(["TABLE", "MVIEW"]);

export function ProblemsSummary({
  missing, diff, srcInvalid, tgtInvalid, bothInvalid,
  schemaMigrationId, onOpen, onApplied,
}: Props) {
  const total = missing.length + diff.length + srcInvalid.length + tgtInvalid.length + bothInvalid.length;
  const [syncBusy,     setSyncBusy]     = useState(false);
  const [syncFeedback, setSyncFeedback] = useState<string | null>(null);

  if (total === 0) return null;

  // Plan a bulk sync: split current problems into action groups.
  const diffReplace = diff.filter(o => REPLACEABLE_TYPES.has(o.type));
  const diffRecreate = diff.filter(o => !REPLACEABLE_TYPES.has(o.type));
  const toRecreate = [...diffRecreate, ...tgtInvalid];
  const destructiveItems = toRecreate.filter(o => DATA_BEARING_TYPES.has(o.type));
  const actionable = missing.length + diffReplace.length + toRecreate.length;

  const runSyncAll = async () => {
    if (actionable === 0) return;
    const lines = [
      missing.length      && `создать на target: ${missing.length}`,
      diffReplace.length  && `CREATE OR REPLACE: ${diffReplace.length}`,
      toRecreate.length   && `пересоздать: ${toRecreate.length}`,
    ].filter(Boolean).join("\n  ");
    let confirmMsg = `Синхронизация ${actionable} объект(ов):\n  ${lines}`;
    if (destructiveItems.length) {
      confirmMsg += `\n\n⚠ Среди объектов есть ${destructiveItems.length} таблиц(ы)/MVIEW — будут DROP-нуты до пересоздания.`
                  + `\n   Данные в этих объектах будут потеряны.`;
    }
    if (srcInvalid.length + bothInvalid.length > 0) {
      confirmMsg += `\n\nINVALID в source (${srcInvalid.length}) и обоих (${bothInvalid.length}) пропущены — требуют ручного решения.`;
    }
    confirmMsg += `\n\nПродолжить?`;
    if (!window.confirm(confirmMsg)) return;

    setSyncBusy(true);
    setSyncFeedback(null);
    let queued = 0;
    let skipped = 0;
    let errors  = 0;
    const allCalls: Array<{ action: DdlApplyAction; items: SchemaObject[] }> = [
      { action: "create_missing", items: missing },
      { action: "sync_diff",      items: diffReplace },
      { action: "recreate",       items: toRecreate },
    ];
    const calls = allCalls.filter(c => c.items.length > 0);

    for (const c of calls) {
      try {
        const r = await applyDdl(schemaMigrationId, c.action,
          c.items.map(o => ({ type: o.type, name: o.name })));
        queued  += r.queued;
        skipped += r.skipped.length;
      } catch {
        errors += 1;
      }
    }

    const parts: string[] = [`в очередь: ${queued}`];
    if (skipped) parts.push(`пропущено: ${skipped}`);
    if (errors)  parts.push(`ошибок: ${errors}`);
    setSyncFeedback(parts.join(" · "));
    setSyncBusy(false);
    onApplied?.();
  };

  return (
    <div style={{
      background:   t.bg.s1,
      border:       `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.lg,
      padding:      14,
      marginBottom: 12,
      boxShadow:    t.shadow.s1,
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
        <span style={{
          display: "grid", placeItems: "center",
          width: 22, height: 22, borderRadius: 6,
          background: t.tone.warnSoft, color: t.tone.warn,
        }}>
          <Icon name="warn" size={14}/>
        </span>
        <span style={{ fontSize: "12.5px", fontWeight: 600 }}>
          Требуется решение
        </span>
        <span style={{ fontFamily: t.font.mono, fontSize: 11, color: t.text.muted }}>
          {total}
        </span>
        <span style={{ marginLeft: "auto", display: "flex", gap: 8, alignItems: "center" }}>
          {syncFeedback && (
            <span style={{ fontSize: 11, color: t.text.muted, fontFamily: t.font.mono }}>
              {syncFeedback}
            </span>
          )}
          {actionable > 0 && (
            <button
              onClick={runSyncAll}
              disabled={syncBusy}
              title={destructiveItems.length
                ? `Синхронизировать весь DDL — ${destructiveItems.length} объект(ов) с потерей данных`
                : `Синхронизировать весь DDL: ${actionable} объект(ов)`}
              style={{
                display: "inline-flex", alignItems: "center", gap: 6,
                padding: "7px 14px",
                borderRadius: t.radius.sm,
                fontSize: 12.5, fontWeight: 700,
                cursor: syncBusy ? "default" : "pointer",
                background: syncBusy ? "#cfcfcf" : "#2563eb",
                color:      syncBusy ? "#666" : "#ffffff",
                border:     `1px solid ${syncBusy ? "#cfcfcf" : "#1d4ed8"}`,
                boxShadow:  syncBusy
                  ? "none"
                  : "0 1px 0 rgba(37,99,235,.15), 0 4px 12px -2px rgba(37,99,235,.35)",
                opacity:    syncBusy ? 0.7 : 1,
              }}
            >
              <Icon name="rotate" size={13}/>
              {syncBusy ? "очередь…" : `Синхронизировать весь DDL · ${actionable}`}
            </button>
          )}
        </span>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        <Bucket
          label="Нет в target — нужно создать"
          tone="warn"
          items={missing}
          onOpen={onOpen}
          smId={schemaMigrationId}
          action="create_missing"
          actionLabel="Создать все"
          onApplied={onApplied}
        />
        <Bucket
          label="DDL отличается — проверить и засинкать"
          tone="warn"
          items={diff}
          onOpen={onOpen}
          smId={schemaMigrationId}
          action="sync_diff"
          actionLabel="Засинкать все"
          onApplied={onApplied}
        />
        <Bucket
          label="INVALID в source — миграция перенесёт ошибку"
          tone="error"
          items={srcInvalid}
          onOpen={onOpen}
        />
        <Bucket
          label="INVALID только в target — пересоздать"
          tone="warn"
          items={tgtInvalid}
          onOpen={onOpen}
          smId={schemaMigrationId}
          action="recreate"
          actionLabel="Пересоздать"
          onApplied={onApplied}
        />
        <Bucket
          label="INVALID в обоих — pre-existing, проверить"
          tone="info"
          items={bothInvalid}
          onOpen={onOpen}
        />
      </div>
    </div>
  );
}

function Bucket({
  label, tone, items, onOpen,
  smId, action, actionLabel, onApplied,
}: {
  label:   string;
  tone:    "info" | "warn" | "error";
  items:   SchemaObject[];
  onOpen:  (o: SchemaObject) => void;
  smId?:        string;
  action?:      DdlApplyAction;
  actionLabel?: string;
  onApplied?:   () => void;
}) {
  const [expanded, setExpanded] = useState(false);
  const [busy,     setBusy]     = useState(false);
  const [feedback, setFeedback] = useState<string | null>(null);
  if (items.length === 0) return null;
  const dotColor =
    tone === "error" ? t.tone.error :
    tone === "warn"  ? t.tone.warn  :
                       t.tone.info;
  const visible = expanded ? items : items.slice(0, 6);
  const canApply = !!(smId && action && actionLabel);

  const runApply = async () => {
    if (!smId || !action) return;
    if (!window.confirm(
      `${actionLabel}: будут отправлены ${items.length} объект(ов) в worker. Продолжить?`,
    )) return;
    setBusy(true);
    setFeedback(null);
    try {
      const r = await applyDdl(
        smId, action,
        items.map(o => ({ type: o.type, name: o.name })),
      );
      const msg = r.skipped.length
        ? `Очередь: ${r.queued}, пропущено: ${r.skipped.length}`
        : `Очередь: ${r.queued}`;
      setFeedback(msg);
      onApplied?.();
    } catch (e) {
      setFeedback(`Ошибка: ${(e as Error).message}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div>
      <button
        onClick={() => setExpanded(e => !e)}
        style={{
          display: "flex", alignItems: "center", gap: 8,
          background: "none", border: "none",
          padding: "4px 0", cursor: "pointer",
          textAlign: "left", color: t.text.primary,
          width: "100%",
        }}
      >
        <span aria-hidden style={{
          width: 6, height: 6, borderRadius: "50%",
          background: dotColor, flexShrink: 0,
        }}/>
        <span style={{ fontSize: 12, fontWeight: 500 }}>{label}</span>
        <span style={{ fontFamily: t.font.mono, fontSize: 11, color: t.text.muted }}>
          {items.length}
        </span>
        <span style={{ marginLeft: "auto", display: "flex", gap: 6, alignItems: "center" }}>
          {feedback && (
            <span style={{ fontSize: 11, color: t.text.muted, fontFamily: t.font.mono }}>
              {feedback}
            </span>
          )}
          {canApply && (
            <span
              role="button"
              tabIndex={0}
              onClick={e => { e.stopPropagation(); if (!busy) runApply(); }}
              onKeyDown={e => {
                if ((e.key === "Enter" || e.key === " ") && !busy) {
                  e.preventDefault(); e.stopPropagation(); runApply();
                }
              }}
              aria-disabled={busy}
              style={{
                fontSize: 11, padding: "3px 9px",
                borderRadius: t.radius.pill,
                background:  busy ? t.bg.s2 : t.tone.accentSoft,
                color:       busy ? t.text.muted : t.tone.accent,
                border:      `1px solid ${t.border.subtle}`,
                cursor:      busy ? "default" : "pointer",
                userSelect:  "none",
              }}
            >
              {busy ? "…" : actionLabel}
            </span>
          )}
          {items.length > 6 && (
            <span style={{ color: t.text.muted, fontSize: 11 }}>
              {expanded ? "свернуть" : `показать все`}
            </span>
          )}
        </span>
      </button>
      <div style={{
        display: "flex", flexWrap: "wrap", gap: 4,
        paddingLeft: 14,
      }}>
        {visible.map(o => (
          <button
            key={o.id}
            onClick={() => onOpen(o)}
            title={o.note}
            style={{
              display: "inline-flex", alignItems: "center", gap: 5,
              padding: "2px 8px",
              background: t.bg.s2,
              border: `1px solid ${t.border.subtle}`,
              borderRadius: t.radius.pill,
              fontSize: "11.5px",
              cursor: "pointer",
            }}
          >
            <span style={{
              fontFamily: t.font.mono,
              fontSize: "9.5px", fontWeight: 600,
              letterSpacing: "0.05em",
              color: t.text.muted,
              textTransform: "uppercase",
            }}>
              {OBJECT_TYPES[o.type]?.label || o.type}
            </span>
            <span style={{ fontFamily: t.font.mono, color: t.text.primary }}>
              {o.name}
            </span>
          </button>
        ))}
      </div>
    </div>
  );
}
