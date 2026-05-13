import React, { useEffect, useRef, useState } from "react";
import { t } from "../theme";
import { Icon } from "../components/ui";
import { OBJECT_TYPES, type SchemaObject } from "./types";
import { applyDdl, listDdlJobs, type DdlApplyAction } from "./api";
import { SyncDdlDialog, type SyncGroup } from "./SyncDdlDialog";

interface Props {
  missing:      SchemaObject[];
  diff:         SchemaObject[];
  srcInvalid:   SchemaObject[];
  tgtInvalid:   SchemaObject[];
  bothInvalid:  SchemaObject[];
  schemaMigrationId: string;
  srcSchema?:   string;
  tgtSchema?:   string;
  onOpen:       (o: SchemaObject) => void;
  onApplied?:   () => void;          // called after worker finishes + snapshot reloaded
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
  schemaMigrationId, srcSchema, tgtSchema, onOpen, onApplied,
}: Props) {
  // ── Hooks (must come BEFORE any early return to keep ordering stable) ──
  const [syncFeedback, setSyncFeedback] = useState<string | null>(null);
  const [dialogOpen,   setDialogOpen]   = useState(false);
  // Track running poll so a second submit can supersede the first.
  // Also cancelled on unmount to avoid setState-after-unmount warnings.
  const pollAbort = useRef<{ cancelled: boolean } | null>(null);
  useEffect(() => () => {
    if (pollAbort.current) pollAbort.current.cancelled = true;
  }, []);

  const total = missing.length + diff.length + srcInvalid.length + tgtInvalid.length + bothInvalid.length;
  if (total === 0) return null;

  // Plan a bulk sync: split current problems into action groups.
  const diffReplace = diff.filter(o => REPLACEABLE_TYPES.has(o.type));
  const diffRecreate = diff.filter(o => !REPLACEABLE_TYPES.has(o.type));
  const toRecreate = [...diffRecreate, ...tgtInvalid];
  const actionable = missing.length + diffReplace.length + toRecreate.length;

  const allGroups: SyncGroup[] = [
    {
      action:      "create_missing",
      title:       "Создать недостающие на target",
      description: "CREATE — объект отсутствует в target.",
      items:       missing,
      destructive: false,
    },
    {
      action:      "sync_diff",
      title:       "CREATE OR REPLACE",
      description: "VIEW / PACKAGE / PROCEDURE / FUNCTION / TRIGGER / TYPE / SYNONYM с расхождением DDL.",
      items:       diffReplace,
      destructive: false,
    },
    {
      action:      "recreate",
      title:       "DROP + CREATE",
      description: "Объекты с DDL-расхождением (не replaceable) или INVALID только в target.",
      items:       toRecreate,
      destructive: true,
    },
  ];
  const syncGroups = allGroups.filter(g => g.items.length > 0);

  const runApply = async (selections: { action: DdlApplyAction; items: SchemaObject[] }[]) => {
    setSyncFeedback("отправляю в очередь…");
    const allJobIds: string[] = [];
    let queued = 0;
    let skipped = 0;
    let errors  = 0;
    for (const sel of selections) {
      try {
        const r = await applyDdl(
          schemaMigrationId, sel.action,
          sel.items.map(o => ({ type: o.type, name: o.name })),
        );
        queued  += r.queued;
        skipped += r.skipped.length;
        allJobIds.push(...r.job_ids);
      } catch {
        errors += 1;
      }
    }
    const sub: string[] = [];
    if (skipped) sub.push(`пропущено: ${skipped}`);
    if (errors)  sub.push(`ошибок: ${errors}`);
    setSyncFeedback(`в очередь: ${queued}${sub.length ? " · " + sub.join(" · ") : ""}`);

    // Cancel any previous in-flight poll
    if (pollAbort.current) pollAbort.current.cancelled = true;
    const token = { cancelled: false };
    pollAbort.current = token;

    if (allJobIds.length > 0) {
      // Poll until all queued jobs reach a terminal state, then refresh snapshot
      void pollAndRefresh(allJobIds, token);
    } else {
      onApplied?.();
    }
  };

  const pollAndRefresh = async (
    jobIds:   string[],
    token:    { cancelled: boolean },
  ) => {
    const TERMINAL = new Set(["DONE", "FAILED", "CANCELLED"]);
    const wanted   = new Set(jobIds);
    const total    = jobIds.length;
    const start = Date.now();
    const TIMEOUT_MS = 10 * 60_000;

    while (!token.cancelled && Date.now() - start < TIMEOUT_MS) {
      await new Promise(r => setTimeout(r, 3000));
      if (token.cancelled) return;
      let jobs;
      try {
        jobs = await listDdlJobs(schemaMigrationId, 500);
      } catch {
        continue;
      }
      // Single pass: count states for our jobs only (O(N) vs O(N·M)).
      let activeCount = 0, doneCount = 0, failedCount = 0, ourCount = 0;
      for (const j of jobs) {
        if (!wanted.has(j.job_id)) continue;
        ourCount++;
        if (j.state === "DONE")        doneCount++;
        else if (j.state === "FAILED") failedCount++;
        else if (!TERMINAL.has(j.state)) activeCount++;
      }
      setSyncFeedback(activeCount
        ? `worker: ${doneCount}/${total} готово${failedCount ? `, ${failedCount} с ошибкой` : ""}`
        : `worker завершил: ${doneCount}/${total}${failedCount ? `, ${failedCount} с ошибкой` : ""} · обновляю snapshot…`);
      if (activeCount === 0 && ourCount === total) {
        // All done — refresh snapshot, then trigger parent reload
        if (srcSchema && tgtSchema) {
          try {
            await fetch("/api/catalog/load", {
              method:  "POST",
              headers: { "Content-Type": "application/json" },
              body:    JSON.stringify({ src_schema: srcSchema, tgt_schema: tgtSchema }),
            });
          } catch {}
        }
        if (token.cancelled) return;
        onApplied?.();
        setSyncFeedback(`готово: ${doneCount}/${total}${failedCount ? ` (${failedCount} с ошибкой)` : ""}`);
        return;
      }
    }
    if (!token.cancelled) {
      setSyncFeedback("timeout — снимок не обновлён, проверьте вручную");
    }
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
              onClick={() => setDialogOpen(true)}
              title={`Синхронизировать DDL: до ${actionable} объект(ов)`}
              style={{
                display: "inline-flex", alignItems: "center", gap: 6,
                padding: "7px 14px",
                borderRadius: t.radius.sm,
                fontSize: 12.5, fontWeight: 700,
                cursor: "pointer",
                background: "#2563eb",
                color:      "#ffffff",
                border:     `1px solid #1d4ed8`,
                boxShadow:  "0 1px 0 rgba(37,99,235,.15), 0 4px 12px -2px rgba(37,99,235,.35)",
              }}
            >
              <Icon name="rotate" size={13}/>
              Синхронизировать весь DDL · {actionable}
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
      {dialogOpen && (
        <SyncDdlDialog
          groups={syncGroups}
          onClose={() => setDialogOpen(false)}
          onSubmit={runApply}
        />
      )}
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
