import React, { useEffect, useMemo, useState } from "react";
import { t } from "../theme";
import { S } from "../components/CreateMigrationModal/styles";
import { Section, Field } from "../components/CreateMigrationModal/ui";
import { primaryActionStyle, secondaryActionStyle } from "./buttonStyles";
import { toSnake, shortId } from "../components/CreateMigrationModal/helpers";
import type { ConnectorGroup, Strategy } from "../types/migration";
import type { TableInfo } from "../components/CreateMigrationModal/types";

const STRATEGIES: Strategy[] = ["CDC_STAGE", "CDC_DIRECT", "BULK_STAGE", "BULK_DIRECT"];
const usesStage = (s: Strategy): boolean => s.endsWith("_STAGE");

export interface CdcGroupTable {
  source_schema: string;
  source_table:  string;
  target_schema: string;
  target_table?: string;
}

interface ReadinessRow {
  source_schema: string;
  source_table:  string;
  supp_log:      boolean;
}
interface ReadinessResp {
  archivelog:    boolean;
  db_level_supp: boolean;
  tables:        ReadinessRow[];
}

interface TableState {
  schema:                string;
  table:                 string;
  target_schema:         string;
  target_table:          string;
  loading:               boolean;
  info:                  TableInfo | null;
  effective_key_type:    string;
  effective_key_columns: string[];
  infoError:             string;
}

interface Props {
  tables:    CdcGroupTable[];
  onClose:   () => void;
  onDone:    (msg: string) => void;
}

type Mode = "new" | "existing";

const KEY_LABEL: Record<string, string> = {
  PRIMARY_KEY:  "PK",
  UNIQUE_KEY:   "UK",
  USER_DEFINED: "USER",
  ROWID:        "ROWID",
  NONE:         "NO KEY",
};

export function AddToCdcGroupModal({ tables: inputTables, onClose, onDone }: Props) {
  const [mode, setMode] = useState<Mode>("new");

  // ── New group form ───────────────────────────────────────────────────────
  const [groupName,     setGroupName]     = useState("");
  const [connectorName, setConnectorName] = useState("");
  const [topicPrefix,   setTopicPrefix]   = useState("");
  const [autoStart,     setAutoStart]     = useState(true);
  const [nameTouched,   setNameTouched]   = useState(false);

  // ── Migrations params ─────────────────────────────────────────────────────
  const [createMigrations,       setCreateMigrations]       = useState(true);
  const [strategy,               setStrategy]               = useState<Strategy>("CDC_STAGE");
  const [stageTablespace,        setStageTablespace]        = useState("PAYSTAGE");
  const [truncateTarget,         setTruncateTarget]         = useState(true);
  const [maxParallelWorkers,     setMaxParallelWorkers]     = useState(1);
  const [baselineParallelDegree, setBaselineParallelDegree] = useState(4);
  const [chunkSize,              setChunkSize]              = useState(1_000_000);
  const [baselineBatchSize,      setBaselineBatchSize]      = useState(500_000);

  // ── Existing groups ──────────────────────────────────────────────────────
  const [groups,        setGroups]        = useState<ConnectorGroup[]>([]);
  const [groupsLoading, setGroupsLoading] = useState(true);
  const [selectedGroup, setSelectedGroup] = useState("");

  // ── Per-table state (key info) ──────────────────────────────────────────
  const [tableStates, setTableStates] = useState<TableState[]>(() =>
    inputTables.map(x => ({
      schema:                x.source_schema,
      table:                 x.source_table,
      target_schema:         x.target_schema,
      target_table:          x.target_table || x.source_table,
      loading:               true,
      info:                  null,
      effective_key_type:    "",
      effective_key_columns: [],
      infoError:             "",
    })),
  );

  // ── Readiness ────────────────────────────────────────────────────────────
  const [readiness, setReadiness] = useState<ReadinessResp | null>(null);
  const [readinessLoading, setReadinessLoading] = useState(true);
  const [readinessError,   setReadinessError]   = useState("");

  // ── Submit state ─────────────────────────────────────────────────────────
  const [busy, setBusy]     = useState(false);
  const [err,  setErr]      = useState("");
  const [phase, setPhase]   = useState<string>("");

  // ── Auto-fill group name → connector/topic_prefix ────────────────────────
  useEffect(() => {
    if (nameTouched && !groupName) return;
    if (!groupName) {
      setConnectorName(""); setTopicPrefix(""); return;
    }
    const base = toSnake(groupName);
    const id   = shortId();
    setConnectorName(`grp_${base}_${id}_connector`);
    setTopicPrefix(`grp.${base}.${id}`);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [groupName]);

  // ── Load existing groups ─────────────────────────────────────────────────
  useEffect(() => {
    fetch("/api/connector-groups")
      .then(r => r.ok ? r.json() : [])
      .then((d: ConnectorGroup[]) => setGroups(d))
      .catch(() => setGroups([]))
      .finally(() => setGroupsLoading(false));
  }, []);

  // ── Load readiness + per-table info (parallel) ───────────────────────────
  useEffect(() => {
    setReadinessLoading(true);
    setReadinessError("");
    fetch("/api/connector-groups/check-readiness", {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({
        source_connection_id: "oracle_source",
        tables: inputTables.map(x => ({
          source_schema: x.source_schema,
          source_table:  x.source_table,
        })),
      }),
    })
      .then(r => r.json())
      .then(d => {
        if (d.error) setReadinessError(d.error);
        else setReadiness(d as ReadinessResp);
      })
      .catch(e => setReadinessError(String(e)))
      .finally(() => setReadinessLoading(false));

    // Per-table info
    inputTables.forEach(x => {
      const p = `schema=${encodeURIComponent(x.source_schema)}&table=${encodeURIComponent(x.source_table)}`;
      fetch(`/api/db/source/table-info?${p}`)
        .then(r => r.json())
        .then((d: TableInfo & { error?: string }) => {
          setTableStates(prev => prev.map(s => {
            if (s.schema !== x.source_schema || s.table !== x.source_table) return s;
            if (d.error) return { ...s, loading: false, infoError: d.error };
            let kt = "USER_DEFINED";
            let kc: string[] = [];
            if (d.pk_columns.length > 0) { kt = "PRIMARY_KEY"; kc = d.pk_columns; }
            else if (d.uk_constraints.length > 0) { kt = "UNIQUE_KEY"; kc = d.uk_constraints[0].columns; }
            return {
              ...s, loading: false, info: d,
              effective_key_type:    kt,
              effective_key_columns: kc,
            };
          }));
        })
        .catch(e => {
          setTableStates(prev => prev.map(s =>
            s.schema === x.source_schema && s.table === x.source_table
              ? { ...s, loading: false, infoError: String(e) }
              : s));
        });
    });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const anyLoading = tableStates.some(s => s.loading) || readinessLoading;

  const suppMap = useMemo(() => {
    const m = new Map<string, boolean>();
    if (readiness) for (const r of readiness.tables) {
      m.set(`${r.source_schema.toUpperCase()}.${r.source_table.toUpperCase()}`, r.supp_log);
    }
    return m;
  }, [readiness]);

  const allReady = !!readiness
    && readiness.archivelog
    && readiness.tables.every(r => r.supp_log);

  function setKeyType(schema: string, table: string, kt: string) {
    setTableStates(prev => prev.map(s => {
      if (s.schema !== schema || s.table !== table) return s;
      let kc = s.effective_key_columns;
      if (kt === "PRIMARY_KEY")  kc = s.info?.pk_columns ?? [];
      if (kt === "UNIQUE_KEY")   kc = s.info?.uk_constraints[0]?.columns ?? [];
      if (kt === "NONE")         kc = [];
      return { ...s, effective_key_type: kt, effective_key_columns: kc };
    }));
  }

  function buildPayloadTables() {
    return tableStates.map(s => ({
      source_schema:         s.schema,
      source_table:          s.table,
      target_schema:         s.target_schema,
      target_table:          s.target_table,
      effective_key_type:    s.effective_key_type || "USER_DEFINED",
      effective_key_columns: s.effective_key_columns,
      source_pk_exists:      (s.info?.pk_columns.length ?? 0) > 0,
      source_uk_exists:      (s.info?.uk_constraints.length ?? 0) > 0,
    }));
  }

  function validate(): boolean {
    setErr("");
    if (mode === "new") {
      if (!groupName.trim())     { setErr("Введите имя группы"); return false; }
      if (!connectorName.trim()) { setErr("connector_name обязателен"); return false; }
      if (!topicPrefix.trim())   { setErr("topic_prefix обязателен"); return false; }
    } else {
      if (!selectedGroup) { setErr("Выберите группу"); return false; }
    }
    for (const s of tableStates) {
      if (s.loading)  { setErr(`Подождите — загружается ${s.schema}.${s.table}`); return false; }
      if (s.infoError) { setErr(`Не удалось получить info по ${s.schema}.${s.table}: ${s.infoError}`); return false; }
      if (s.effective_key_type === "USER_DEFINED" && s.effective_key_columns.length === 0) {
        setErr(`Не выбраны колонки ключа для ${s.schema}.${s.table}`);
        return false;
      }
    }
    return true;
  }

  function migrationParams() {
    return createMigrations
      ? {
          create_migrations:        true,
          strategy,
          stage_tablespace:         usesStage(strategy) ? stageTablespace.trim().toUpperCase() : "",
          truncate_target:          truncateTarget,
          max_parallel_workers:     Math.max(1, maxParallelWorkers),
          baseline_parallel_degree: Math.max(1, baselineParallelDegree),
          chunk_size:               Math.max(1_000, chunkSize),
          baseline_batch_size:      Math.max(1_000, baselineBatchSize),
        }
      : { create_migrations: false };
  }

  async function submit() {
    if (busy || !validate()) return;
    setBusy(true);
    try {
      if (mode === "new") {
        setPhase("Создание группы...");
        const r = await fetch("/api/connector-groups/wizard", {
          method:  "POST",
          headers: { "Content-Type": "application/json" },
          body:    JSON.stringify({
            group_name:            groupName.trim(),
            connector_name:        connectorName.trim(),
            topic_prefix:          topicPrefix.trim(),
            consumer_group_prefix: topicPrefix.trim(),
            source_connection_id:  "oracle_source",
            tables: buildPayloadTables(),
            ...migrationParams(),
          }),
        });
        const d = await r.json();
        if (!r.ok && r.status !== 207) { setErr(d.error || `HTTP ${r.status}`); return; }
        if (d.migrations_error) { setErr(`Группа создана, но миграции не создались: ${d.migrations_error}`); return; }
        const gid = d.group?.group_id;
        if (autoStart && gid) {
          setPhase("Запуск коннектора...");
          const sr = await fetch(`/api/connector-groups/${gid}/start`, { method: "POST" });
          if (!sr.ok) {
            const sd = await sr.json().catch(() => ({}));
            setErr(`Группа создана, но запуск не удался: ${sd.error || sr.status}`);
            return;
          }
        }
        const mCount = (d.migrations || []).filter((x: any) => !x.skipped).length;
        onDone(buildDoneMsg(`Группа «${groupName}»`, autoStart, mCount, tableStates.length));
      } else {
        setPhase("Добавление таблиц в группу...");
        const r = await fetch(`/api/connector-groups/${selectedGroup}/tables`, {
          method:  "POST",
          headers: { "Content-Type": "application/json" },
          body:    JSON.stringify({
            tables: buildPayloadTables(),
            ...migrationParams(),
          }),
        });
        const d = await r.json();
        if (!r.ok) { setErr(d.error || `HTTP ${r.status}`); return; }
        if (d.migrations_error) { setErr(`Таблицы добавлены, но миграции не создались: ${d.migrations_error}`); return; }
        const addedTables = Array.isArray(d.tables) ? d.tables.length : tableStates.length;
        const mCount = (d.migrations || []).filter((x: any) => !x.skipped).length;
        const grp = groups.find(g => g.group_id === selectedGroup);
        onDone(`Добавлено ${addedTables} табл. в «${grp?.group_name ?? "группу"}»`
               + (createMigrations ? `, миграций создано: ${mCount}.` : "."));
      }
    } catch (e) {
      setErr(String(e));
    } finally {
      setBusy(false);
      setPhase("");
    }
  }

  return (
    <div style={S.overlay} onClick={e => { if (e.target === e.currentTarget) onClose(); }}>
      <div style={{ ...S.modal, maxWidth: 780, maxHeight: "calc(100vh - 80px)" }}>
        <div style={S.header}>
          <span style={{ fontSize: 15, fontWeight: 700, color: t.text.primary }}>
            CDC-группа · {inputTables.length} {plural(inputTables.length, "таблица", "таблицы", "таблиц")}
          </span>
          <span style={{ flex: 1 }} />
          <button onClick={onClose} style={{
            background: "none", border: "none", color: t.text.disabled,
            cursor: "pointer", fontSize: 20, lineHeight: 1, padding: "0 2px",
          }}>✕</button>
        </div>

        <div style={{ ...S.body, flex: "1 1 auto", minHeight: 0, overflowY: "auto" }}>

          {/* Mode switch */}
          <div style={{ display: "flex", gap: 8 }}>
            <ModeBtn active={mode === "new"} onClick={() => setMode("new")}
              label="Новая группа"/>
            <ModeBtn active={mode === "existing"} onClick={() => setMode("existing")}
              label={`В существующую (${groups.length})`}
              disabled={groupsLoading || groups.length === 0}/>
          </div>

          {/* Readiness */}
          <Section title="Готовность Oracle к CDC" accent={allReady ? t.green.dim : t.amber.dim}>
            {readinessLoading && (
              <div style={{ color: t.text.muted, fontSize: t.size.sm }}>Проверка…</div>
            )}
            {readinessError && (
              <div style={{ color: t.red.fg, fontSize: t.size.sm }}>{readinessError}</div>
            )}
            {readiness && (
              <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                <ReadinessLine
                  label="ARCHIVELOG"
                  ok={readiness.archivelog}
                  hint="v$database.log_mode"/>
                <ReadinessLine
                  label="DB-уровень SUPP LOG (ALL)"
                  ok={readiness.db_level_supp}
                  hint="v$database.supplemental_log_data_all = YES"
                  optional/>
                <div style={{ fontSize: t.size.xs, color: t.text.muted, marginTop: 2 }}>
                  Без DB-уровня нужна supp-logging для каждой таблицы (ALL COLUMN LOGGING):
                </div>
                <div style={{
                  display: "grid",
                  gridTemplateColumns: "1fr auto",
                  gap: "2px 12px",
                  fontFamily: t.font.mono, fontSize: 11.5,
                }}>
                  {readiness.tables.map(r => (
                    <React.Fragment key={`${r.source_schema}.${r.source_table}`}>
                      <span style={{ color: t.text.primary }}>
                        {r.source_schema}.{r.source_table}
                      </span>
                      <span style={{ color: r.supp_log ? t.green.fg : t.red.fg, fontWeight: 700 }}>
                        {r.supp_log ? "OK" : "—"}
                      </span>
                    </React.Fragment>
                  ))}
                </div>
              </div>
            )}
          </Section>

          {/* Mode-specific form */}
          {mode === "new" ? (
            <Section title="Новая группа" accent={t.blue.dim}>
              <Field label="Имя группы" required>
                <input
                  style={S.input}
                  value={groupName}
                  onChange={e => { setGroupName(e.target.value); setNameTouched(true); }}
                  placeholder="например: payments_cdc"
                />
              </Field>
              <div style={S.row2}>
                <Field label="connector_name" required hint="Авто из имени группы">
                  <input style={S.input}
                    value={connectorName}
                    onChange={e => setConnectorName(e.target.value)}/>
                </Field>
                <Field label="topic_prefix" required hint="Авто из имени группы">
                  <input style={S.input}
                    value={topicPrefix}
                    onChange={e => setTopicPrefix(e.target.value)}/>
                </Field>
              </div>
              <label style={{ display: "flex", gap: 8, alignItems: "center",
                              fontSize: 13, color: t.text.primary }}>
                <input type="checkbox" checked={autoStart}
                  onChange={e => setAutoStart(e.target.checked)}/>
                <span>Запустить коннектор сразу после создания</span>
              </label>
            </Section>
          ) : (
            <Section title="Целевая группа" accent={t.blue.dim}>
              <Field label="Группа" required>
                <select
                  style={S.select}
                  value={selectedGroup}
                  onChange={e => setSelectedGroup(e.target.value)}
                  disabled={groupsLoading || groups.length === 0}
                >
                  <option value="">— выберите —</option>
                  {groups.map(g => (
                    <option key={g.group_id} value={g.group_id}>
                      {g.group_name} · {g.status} · {g.topic_prefix}
                    </option>
                  ))}
                </select>
              </Field>
              <div style={{ fontSize: t.size.xs, color: t.text.muted }}>
                Если коннектор RUNNING, table.include.list будет обновлён автоматически.
              </div>
            </Section>
          )}

          {/* Migrations options */}
          <Section title="Миграции таблиц" accent={t.purple.base}>
            <label style={{ display: "flex", gap: 8, alignItems: "center",
                            fontSize: 13, color: t.text.primary }}>
              <input type="checkbox" checked={createMigrations}
                onChange={e => setCreateMigrations(e.target.checked)}/>
              <span>Создать миграции и поставить в очередь</span>
            </label>
            {createMigrations && (
              <>
                <div style={S.row2}>
                  <Field label="Стратегия" required>
                    <select style={S.select} value={strategy}
                      onChange={e => {
                        const s = e.target.value as Strategy;
                        setStrategy(s);
                        if (usesStage(s)) setTruncateTarget(true);
                      }}>
                      {STRATEGIES.map(s => <option key={s} value={s}>{s}</option>)}
                    </select>
                  </Field>
                  <Field label="Stage tablespace"
                    hint={usesStage(strategy) ? "Общий tablespace для STG_-таблиц" : "Только для STAGE-стратегий"}>
                    <input style={S.input} value={stageTablespace}
                      disabled={!usesStage(strategy)}
                      onChange={e => setStageTablespace(e.target.value)}/>
                  </Field>
                </div>
                <label style={{ display: "flex", gap: 8, alignItems: "center",
                                fontSize: 13, color: t.text.primary }}>
                  <input type="checkbox" checked={truncateTarget}
                    disabled={usesStage(strategy)}
                    onChange={e => setTruncateTarget(e.target.checked)}/>
                  <span>
                    TRUNCATE target перед загрузкой
                    {usesStage(strategy) && (
                      <span style={{ color: t.text.muted, marginLeft: 6 }}>
                        (STAGE требует TRUNCATE)
                      </span>
                    )}
                  </span>
                </label>
                <div style={{
                  display: "grid",
                  gridTemplateColumns: "1fr 1fr", gap: 10,
                }}>
                  <Field label="max_parallel_workers"
                    hint="Параллельные BULK-чанки на одну миграцию (внешние воркеры)">
                    <input style={S.input} type="number" min={1}
                      value={maxParallelWorkers}
                      onChange={e => setMaxParallelWorkers(
                        Math.max(1, parseInt(e.target.value) || 1))}/>
                  </Field>
                  <Field label="baseline_parallel_degree"
                    hint="Параллельные BASELINE-чанки воркерам">
                    <input style={S.input} type="number" min={1}
                      value={baselineParallelDegree}
                      onChange={e => setBaselineParallelDegree(
                        Math.max(1, parseInt(e.target.value) || 1))}/>
                  </Field>
                  <Field label="chunk_size (BULK)"
                    hint="Строк в одном чанке BULK_LOADING">
                    <input style={S.input} type="number" min={1000} step={1000}
                      value={chunkSize}
                      onChange={e => setChunkSize(
                        Math.max(1000, parseInt(e.target.value) || 1_000_000))}/>
                  </Field>
                  <Field label="baseline_batch_size"
                    hint="Строк в одном чанке BASELINE_LOADING">
                    <input style={S.input} type="number" min={1000} step={1000}
                      value={baselineBatchSize}
                      onChange={e => setBaselineBatchSize(
                        Math.max(1000, parseInt(e.target.value) || 500_000))}/>
                  </Field>
                </div>
                <div style={{ fontSize: 11.5, color: t.text.muted }}>
                  Все миграции создаются в фазе <code>NEW</code> с привязкой к группе.
                  Оркестратор берёт их по одной (FIFO) после Start группы.
                  Внешние воркеры внутри одной миграции могут одновременно тянуть до
                  <code> max_parallel_workers</code> чанков (фаза BULK_LOADING).
                </div>
              </>
            )}
          </Section>

          {/* Selected tables — keys preview */}
          <Section title="Таблицы и ключи" accent={t.green.dim}>
            {anyLoading && (
              <div style={{ fontSize: t.size.sm, color: t.text.muted }}>
                Подгружаются метаданные…
              </div>
            )}
            <div style={{
              maxHeight: 220, overflowY: "auto",
              border: `1px solid ${t.border.subtle}`, borderRadius: t.radius.sm,
            }}>
              {tableStates.map(s => {
                const k = `${s.schema}.${s.table}`.toUpperCase();
                const supp = suppMap.get(k);
                return (
                  <div key={k} style={{
                    padding: "6px 8px",
                    borderBottom: `1px solid ${t.border.subtle}`,
                    display: "flex", alignItems: "center", gap: 8,
                  }}>
                    <span style={{ fontFamily: t.font.mono, fontSize: 12,
                                   color: t.text.primary, minWidth: 220 }}>
                      {s.schema}.{s.table}
                    </span>
                    {s.loading
                      ? <span style={{ color: t.text.muted, fontSize: 11 }}>загрузка…</span>
                      : s.infoError
                        ? <span style={{ color: t.red.fg, fontSize: 11 }}>{s.infoError}</span>
                        : (
                          <>
                            <KeyChip kt={s.effective_key_type}/>
                            {s.effective_key_columns.length > 0 && (
                              <span style={{ fontFamily: t.font.mono, fontSize: 11, color: t.text.muted }}>
                                ({s.effective_key_columns.join(",")})
                              </span>
                            )}
                          </>
                        )
                    }
                    <span style={{ flex: 1 }}/>
                    {!s.loading && !s.infoError && s.info && (
                      <KeyTypeSwitch
                        info={s.info}
                        current={s.effective_key_type}
                        onChange={kt => setKeyType(s.schema, s.table, kt)}
                      />
                    )}
                    {supp !== undefined && (
                      <span style={{
                        fontFamily: t.font.mono, fontSize: 9.5, fontWeight: 700,
                        padding: "1px 6px", borderRadius: t.radius.sm,
                        background: supp ? t.green.bg : t.red.bg,
                        color:      supp ? t.green.fg : t.red.fg,
                        border: `1px solid ${(supp ? t.green.dim : t.red.dim)}40`,
                      }}>
                        {supp ? "SUPP" : "NO SUPP"}
                      </span>
                    )}
                  </div>
                );
              })}
            </div>
          </Section>

          {err && <div style={S.err}>{err}</div>}
          {phase && !err && (
            <div style={{ fontSize: t.size.sm, color: t.text.muted }}>{phase}</div>
          )}
        </div>

        <div style={S.footer}>
          <button onClick={onClose} style={secondaryActionStyle()} disabled={busy}>
            Отмена
          </button>
          <button onClick={submit} style={primaryActionStyle(busy)} disabled={busy || anyLoading}>
            {busy
              ? "…"
              : mode === "new"
                ? `Создать группу${autoStart ? " и запустить" : ""}${createMigrations ? " (+ миграции)" : ""}`
                : `Добавить ${tableStates.length}${createMigrations ? " (+ миграции)" : ""}`}
          </button>
        </div>
      </div>
    </div>
  );
}

function ModeBtn({ label, active, disabled, onClick }: {
  label: string; active: boolean; disabled?: boolean; onClick: () => void;
}) {
  return (
    <button onClick={onClick} disabled={disabled} style={{
      padding: "6px 14px", fontSize: t.size.sm, fontWeight: 700,
      borderRadius: t.radius.md,
      border: `1px solid ${active ? t.blue.base : t.border.base}`,
      background: active ? t.bg.s3 : t.bg.s2,
      color: disabled ? t.text.faint : active ? t.blue.fg : t.text.muted,
      cursor: disabled ? "not-allowed" : "pointer",
    }}>{label}</button>
  );
}

function ReadinessLine({ label, ok, hint, optional }: {
  label: string; ok: boolean; hint?: string; optional?: boolean;
}) {
  const color = ok ? t.green.fg : optional ? t.amber.fg : t.red.fg;
  const sym   = ok ? "✓" : optional ? "~" : "✕";
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 12.5 }}>
      <span style={{ color, fontWeight: 700, width: 14, textAlign: "center" }}>{sym}</span>
      <span style={{ color: t.text.primary }}>{label}</span>
      {hint && <span style={{ color: t.text.muted, fontSize: 11 }}>{hint}</span>}
    </div>
  );
}

function KeyChip({ kt }: { kt: string }) {
  const tone =
    kt === "PRIMARY_KEY" ? { bg: t.green.bg, fg: t.green.fg, br: t.green.dim }
    : kt === "UNIQUE_KEY" ? { bg: t.blue.bg, fg: t.blue.fg, br: t.blue.dim }
    : kt === "USER_DEFINED" ? { bg: t.amber.bg, fg: t.amber.fg, br: t.amber.dim }
    : kt === "ROWID" ? { bg: t.bg.s3, fg: t.text.muted, br: t.border.base }
    :  { bg: t.red.bg, fg: t.red.fg, br: t.red.dim };
  return (
    <span style={{
      fontFamily: t.font.mono, fontSize: 9.5, fontWeight: 700,
      padding: "1px 6px", borderRadius: t.radius.sm,
      background: tone.bg, color: tone.fg, border: `1px solid ${tone.br}40`,
    }}>
      {KEY_LABEL[kt] ?? kt}
    </span>
  );
}

function KeyTypeSwitch({ info, current, onChange }: {
  info: TableInfo;
  current: string;
  onChange: (kt: string) => void;
}) {
  const opts: { label: string; value: string; enabled: boolean }[] = [
    { label: "PK",   value: "PRIMARY_KEY",  enabled: info.pk_columns.length > 0 },
    { label: "UK",   value: "UNIQUE_KEY",   enabled: info.uk_constraints.length > 0 },
    { label: "USER", value: "USER_DEFINED", enabled: true },
    { label: "NONE", value: "NONE",         enabled: true },
  ];
  return (
    <span style={{ display: "inline-flex", gap: 4 }}>
      {opts.map(o => (
        <button key={o.value} onClick={() => o.enabled && onChange(o.value)}
          disabled={!o.enabled}
          style={{
            padding: "1px 6px", fontSize: 10, fontWeight: 700,
            borderRadius: t.radius.sm,
            border: `1px solid ${current === o.value ? t.blue.base : t.border.subtle}`,
            background: current === o.value ? t.bg.s3 : "transparent",
            color: !o.enabled ? t.text.faint
                   : current === o.value ? t.blue.fg : t.text.muted,
            cursor: o.enabled ? "pointer" : "not-allowed",
          }}>{o.label}</button>
      ))}
    </span>
  );
}

function buildDoneMsg(prefix: string, autoStart: boolean, migrationsCount: number, tablesCount: number): string {
  const head = autoStart ? `${prefix} создана и запущена` : `${prefix} создана`;
  if (migrationsCount === 0) return head + ".";
  if (migrationsCount === tablesCount) {
    return autoStart
      ? `${head}; ${migrationsCount} миграций в очереди — первая стартует автоматически.`
      : `${head}; ${migrationsCount} миграций в фазе NEW (ждут Start группы).`;
  }
  return `${head}; миграций создано: ${migrationsCount} из ${tablesCount}.`;
}

function plural(n: number, one: string, few: string, many: string): string {
  const n10 = n % 10;
  const n100 = n % 100;
  if (n10 === 1 && n100 !== 11) return one;
  if (n10 >= 2 && n10 <= 4 && (n100 < 12 || n100 > 14)) return few;
  return many;
}
