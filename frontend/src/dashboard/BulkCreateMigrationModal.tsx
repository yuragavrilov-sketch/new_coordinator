import React, { useEffect, useMemo, useState } from "react";
import { t } from "../theme";
import { S } from "../components/CreateMigrationModal/styles";
import { Section, Field } from "../components/CreateMigrationModal/ui";
import { primaryActionStyle, secondaryActionStyle } from "./buttonStyles";
import type { ConnectorGroup, Strategy } from "../types/migration";

interface BulkTable {
  source_schema: string;
  source_table:  string;
  target_schema: string;
  target_table?: string;
}

interface BulkResult {
  created: { table: string; migration_id: string }[];
  failed:  { table: string; error: string }[];
  total:   number;
}

interface Props {
  tables:    BulkTable[];
  onClose:   () => void;
  onCreated: (r: BulkResult) => void;
}

const STRATEGIES: Strategy[] = ["CDC_STAGE", "CDC_DIRECT", "BULK_STAGE", "BULK_DIRECT"];

function isCdc(s: Strategy): boolean { return s.startsWith("CDC_"); }
function usesStage(s: Strategy): boolean { return s.endsWith("_STAGE"); }

export function BulkCreateMigrationModal({ tables, onClose, onCreated }: Props) {
  const [strategy,             setStrategy]            = useState<Strategy>("CDC_STAGE");
  const [groupId,              setGroupId]             = useState<string>("");
  const [truncateTarget,       setTruncateTarget]      = useState<boolean>(true);
  const [validateHashSample,   setValidateHashSample]  = useState<boolean>(false);
  const [stageTablespace,      setStageTablespace]     = useState<string>("PAYSTAGE");

  const [connGroups, setConnGroups] = useState<ConnectorGroup[]>([]);
  const [busy, setBusy]             = useState(false);
  const [err,  setErr]              = useState<string>("");
  const [result, setResult]         = useState<BulkResult | null>(null);

  // STAGE forces truncate=true
  useEffect(() => {
    if (usesStage(strategy) && !truncateTarget) setTruncateTarget(true);
  }, [strategy, truncateTarget]);

  // Load running CDC packs when CDC strategy is picked
  useEffect(() => {
    if (!isCdc(strategy)) return;
    fetch("/api/connector-groups")
      .then(r => r.ok ? r.json() : [])
      .then(setConnGroups)
      .catch(() => {});
  }, [strategy]);

  const runningGroups = useMemo(
    () => connGroups.filter(g => g.status === "RUNNING"),
    [connGroups],
  );

  // Auto-pick a default group when one becomes available
  useEffect(() => {
    if (isCdc(strategy) && !groupId && runningGroups.length > 0) {
      setGroupId(runningGroups[0].group_id);
    }
  }, [strategy, groupId, runningGroups]);

  async function submit() {
    if (busy) return;
    setErr("");
    if (isCdc(strategy) && !groupId) {
      setErr("Для CDC-стратегии нужно выбрать запущенную CDC-пачку.");
      return;
    }
    setBusy(true);
    try {
      const payload = {
        tables: tables.map(x => ({
          source_schema: x.source_schema,
          source_table:  x.source_table,
          target_schema: x.target_schema,
          target_table:  x.target_table || x.source_table,
        })),
        strategy,
        group_id:             isCdc(strategy) ? groupId : undefined,
        truncate_target:      truncateTarget,
        validate_hash_sample: validateHashSample,
        stage_tablespace:     usesStage(strategy) ? stageTablespace.trim().toUpperCase() : undefined,
      };
      const r = await fetch("/api/migrations/bulk", {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify(payload),
      });
      const d = await r.json();
      if (!r.ok && r.status !== 207) {
        setErr(d.error || `HTTP ${r.status}`);
        return;
      }
      setResult(d as BulkResult);
    } catch (e) {
      setErr(String(e));
    } finally {
      setBusy(false);
    }
  }

  function done() {
    if (result) onCreated(result);
    onClose();
  }

  // ── Result view ────────────────────────────────────────────────────────────
  if (result) {
    return (
      <div style={S.overlay} onClick={e => { if (e.target === e.currentTarget) done(); }}>
        <div style={S.modal}>
          <div style={S.header}>
            <span style={{ fontSize: 15, fontWeight: 700, color: t.text.primary }}>
              Результат массового создания
            </span>
            <span style={{ flex: 1 }} />
            <button onClick={done} style={{
              background: "none", border: "none", color: t.text.disabled,
              cursor: "pointer", fontSize: 20, lineHeight: 1, padding: "0 2px",
            }}>✕</button>
          </div>
          <div style={S.body}>
            <div style={{ display: "flex", gap: 16, fontSize: 13 }}>
              <ResultPill label="Создано"  count={result.created.length} tone="ok"/>
              <ResultPill label="Не удалось" count={result.failed.length}  tone={result.failed.length > 0 ? "error" : "muted"}/>
              <ResultPill label="Всего" count={result.total} tone="muted"/>
            </div>
            {result.failed.length > 0 && (
              <Section title="Ошибки">
                <div style={{ maxHeight: 240, overflowY: "auto", fontSize: 12 }}>
                  {result.failed.map((f, i) => (
                    <div key={i} style={{
                      padding: "6px 8px", borderBottom: `1px solid ${t.border.subtle}`,
                    }}>
                      <div style={{ fontFamily: t.font.mono, color: t.text.primary }}>{f.table}</div>
                      <div style={{ color: t.tone.error, fontSize: 11.5 }}>{f.error}</div>
                    </div>
                  ))}
                </div>
              </Section>
            )}
          </div>
          <div style={S.footer}>
            <button onClick={done} style={primaryActionStyle(false)}>Готово</button>
          </div>
        </div>
      </div>
    );
  }

  // ── Form view ──────────────────────────────────────────────────────────────
  return (
    <div style={S.overlay} onClick={e => { if (e.target === e.currentTarget) onClose(); }}>
      <div style={S.modal}>
        <div style={S.header}>
          <span style={{ fontSize: 15, fontWeight: 700, color: t.text.primary }}>
            Массовое создание миграций
          </span>
          <span style={{ fontSize: 12, color: t.text.muted, fontFamily: t.font.mono }}>
            ({tables.length} {plural(tables.length, "таблица", "таблицы", "таблиц")})
          </span>
          <span style={{ flex: 1 }} />
          <button onClick={onClose} style={{
            background: "none", border: "none", color: t.text.disabled,
            cursor: "pointer", fontSize: 20, lineHeight: 1, padding: "0 2px",
          }}>✕</button>
        </div>

        <div style={S.body}>
          <Section title="Стратегия">
            <Field label="Стратегия миграции" required>
              <select
                style={S.select}
                value={strategy}
                onChange={e => setStrategy(e.target.value as Strategy)}
              >
                {STRATEGIES.map(s => (
                  <option key={s} value={s}>{s}</option>
                ))}
              </select>
            </Field>
            {isCdc(strategy) && (
              <Field label="CDC-пачка (RUNNING)" required>
                <select
                  style={S.select}
                  value={groupId}
                  onChange={e => setGroupId(e.target.value)}
                  disabled={runningGroups.length === 0}
                >
                  {runningGroups.length === 0
                    ? <option value="">— нет запущенных CDC-пачек —</option>
                    : runningGroups.map(g => (
                        <option key={g.group_id} value={g.group_id}>
                          {g.group_name} · {g.topic_prefix}
                        </option>
                      ))
                  }
                </select>
              </Field>
            )}
            {usesStage(strategy) && (
              <Field label="Stage tablespace" hint="STAGE-стратегия использует общее tablespace для всех stage-таблиц">
                <input
                  style={S.input}
                  value={stageTablespace}
                  onChange={e => setStageTablespace(e.target.value)}
                  placeholder="PAYSTAGE"
                />
              </Field>
            )}
          </Section>

          <Section title="Опции">
            <label style={{ display: "flex", gap: 8, alignItems: "center", fontSize: 13, color: t.text.primary }}>
              <input
                type="checkbox"
                checked={truncateTarget}
                disabled={usesStage(strategy)}
                onChange={e => setTruncateTarget(e.target.checked)}
              />
              <span>
                TRUNCATE target перед загрузкой
                {usesStage(strategy) && (
                  <span style={{ color: t.text.muted, marginLeft: 6 }}>
                    (STAGE требует TRUNCATE)
                  </span>
                )}
              </span>
            </label>
            <label style={{ display: "flex", gap: 8, alignItems: "center", fontSize: 13, color: t.text.primary }}>
              <input
                type="checkbox"
                checked={validateHashSample}
                onChange={e => setValidateHashSample(e.target.checked)}
              />
              <span>Hash-sample валидация после bulk-load</span>
            </label>
            <div style={{ fontSize: 11.5, color: t.text.muted, marginTop: 4 }}>
              PK / UK подбирается автоматически по каждой таблице. chunk_size = 1M, workers = 1,
              baseline_pd = 4. Stage-таблицы: <code>STG_&lt;schema&gt;_&lt;table&gt;</code>.
            </div>
          </Section>

          <Section title="Выбранные таблицы">
            <div style={{
              maxHeight: 200, overflowY: "auto",
              fontFamily: t.font.mono, fontSize: 11.5,
              border: `1px solid ${t.border.subtle}`, borderRadius: t.radius.sm,
            }}>
              {tables.map((x, i) => (
                <div key={i} style={{
                  padding: "4px 8px",
                  borderBottom: i < tables.length - 1 ? `1px solid ${t.border.subtle}` : undefined,
                  color: t.text.primary,
                }}>
                  {x.source_schema}.{x.source_table}
                  <span style={{ color: t.text.muted }}>
                    {" → "}{x.target_schema}.{x.target_table || x.source_table}
                  </span>
                </div>
              ))}
            </div>
          </Section>

          {err && <div style={S.err}>{err}</div>}
        </div>

        <div style={S.footer}>
          <button onClick={onClose} style={secondaryActionStyle()} disabled={busy}>
            Отмена
          </button>
          <button onClick={submit} style={primaryActionStyle(busy)} disabled={busy}>
            {busy ? "Создание…" : `Создать ${tables.length}`}
          </button>
        </div>
      </div>
    </div>
  );
}

function ResultPill({ label, count, tone }: {
  label: string; count: number;
  tone: "ok" | "error" | "muted";
}) {
  const color = tone === "ok" ? t.tone.ok : tone === "error" ? t.tone.error : t.text.muted;
  return (
    <div style={{
      display: "inline-flex", gap: 6, alignItems: "baseline",
      padding: "4px 10px", borderRadius: t.radius.sm,
      background: t.bg.s2, border: `1px solid ${t.border.subtle}`,
    }}>
      <span style={{ color: t.text.muted, fontSize: 11 }}>{label}</span>
      <span style={{ color, fontWeight: 700, fontFamily: t.font.mono }}>{count}</span>
    </div>
  );
}

function plural(n: number, one: string, few: string, many: string): string {
  const n10 = n % 10;
  const n100 = n % 100;
  if (n10 === 1 && n100 !== 11) return one;
  if (n10 >= 2 && n10 <= 4 && (n100 < 12 || n100 > 14)) return few;
  return many;
}
