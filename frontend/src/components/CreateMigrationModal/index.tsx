import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { ConnectorGroup } from "../../types/migration";
import { hasCdc, usesStage } from "../../types/migration";
import { t } from "../../theme";
import { StrategyPicker } from "../StrategyPicker";
import { S } from "./styles";
import type { FormData, TableInfo, EnsureResult, MigrationPrefill } from "./types";
import { INIT, KEY_SOURCE_MAP, autoFields } from "./helpers";
import { Section, Field, TextInput, Chip, KeyTypeBtn } from "./ui";
import { SchemaTablePair } from "./SchemaTablePair";
import { EnsureChips } from "./EnsureChips";

interface Props {
  onClose: () => void;
  onCreated: () => void;
  prefill?: MigrationPrefill;
}

export function CreateMigrationModal({ onClose, onCreated, prefill }: Props) {
  const [form,         setFormRaw]    = useState<FormData>(() => ({ ...INIT, ...prefill }));
  const [tableInfo,    setTableInfo]  = useState<TableInfo | null>(null);
  const [loadingInfo,  setLoadInfo]   = useState(false);
  const [infoErr,      setInfoErr]    = useState("");
  const [submitting,   setSubmit]     = useState(false);
  const [submitErr,    setSubmitErr]  = useState("");
  const [fieldErrs,    setFieldErrs]  = useState<Partial<Record<keyof FormData, string>>>({});
  const [ensureBusy,   setEnsureBusy] = useState(false);
  const [ensureResult, setEnsureResult] = useState<EnsureResult | null>(null);
  const [ensureErr,    setEnsureErr]  = useState("");
  const [existingMigrations, setExistingMigrations] = useState<{
    migration_name: string; phase: string;
    source_schema: string; source_table: string;
    target_schema: string; target_table: string;
  }[]>([]);
  const [connGroups, setConnGroups] = useState<ConnectorGroup[]>([]);
  const nameTouched = useRef(false);

  // Load connector groups
  useEffect(() => {
    fetch("/api/connector-groups")
      .then(r => r.ok ? r.json() : [])
      .then(setConnGroups)
      .catch(() => {});
  }, []);

  const setF = useCallback((up: Partial<FormData>) =>
    setFormRaw(f => ({ ...f, ...up })), []);

  // Load existing migrations to exclude already-used tables and detect duplicates
  useEffect(() => {
    fetch("/api/migrations")
      .then(r => r.json())
      .then((data: {
        migration_name: string; phase: string;
        source_schema: string; source_table: string;
        target_schema: string; target_table: string;
      }[]) =>
        setExistingMigrations(data.filter(m =>
          m.phase !== "CANCELLED" && m.phase !== "FAILED" && m.phase !== "COMPLETED"
        ))
      )
      .catch(() => {});
  }, []);

  const excludeSourceTables = useMemo(() => {
    if (!form.source_schema) return undefined;
    const set = new Set<string>();
    for (const m of existingMigrations) {
      if (m.source_schema === form.source_schema) set.add(m.source_table);
    }
    return set.size > 0 ? set : undefined;
  }, [form.source_schema, existingMigrations]);

  // Load table info when source schema+table are both selected
  useEffect(() => {
    if (!form.source_schema || !form.source_table) {
      setTableInfo(null);
      return;
    }
    setLoadInfo(true);
    setInfoErr("");
    setTableInfo(null);
    const p = `schema=${encodeURIComponent(form.source_schema)}&table=${encodeURIComponent(form.source_table)}`;
    fetch(`/api/db/source/table-info?${p}`)
      .then(r => r.json())
      .then((d: TableInfo & { error?: string }) => {
        if (d.error) { setInfoErr(d.error); return; }
        setTableInfo(d);
        let keyType = "USER_DEFINED";
        let keyCols: string[] = [];
        if (d.pk_columns.length > 0) {
          keyType = "PRIMARY_KEY"; keyCols = d.pk_columns;
        } else if (d.uk_constraints.length > 0) {
          keyType = "UNIQUE_KEY"; keyCols = d.uk_constraints[0].columns;
        }
        setF({ effective_key_type: keyType, effective_key_columns: keyCols, selected_uk_index: 0 });
      })
      .catch(e => setInfoErr(String(e)))
      .finally(() => setLoadInfo(false));
  }, [form.source_schema, form.source_table, setF]);

  // Auto-fill generated names when source table chosen
  useEffect(() => {
    if (!form.source_schema || !form.source_table) return;
    setF(autoFields(form.source_schema, form.source_table));
  }, [form.source_schema, form.source_table, setF]);

  // Auto-fill migration name
  useEffect(() => {
    if (nameTouched.current) return;
    if (!form.source_schema || !form.source_table || !form.target_schema || !form.target_table) return;
    setF({
      migration_name:
        `${form.source_schema}.${form.source_table} → ${form.target_schema}.${form.target_table}`,
    });
  }, [form.source_schema, form.source_table, form.target_schema, form.target_table, setF]);

  // Duplicate detection (derived from existingMigrations — already loaded above)
  const dupWarning = useMemo(() => {
    if (!form.source_schema || !form.source_table || !form.target_schema || !form.target_table) return null;
    const dup = existingMigrations.find(m =>
      m.source_schema === form.source_schema && m.source_table === form.source_table &&
      m.target_schema === form.target_schema && m.target_table === form.target_table
    );
    return dup
      ? `Уже есть активная миграция "${dup.migration_name}" (${dup.phase}) для этой пары`
      : null;
  }, [form.source_schema, form.source_table, form.target_schema, form.target_table, existingMigrations]);

  // Auto-suggest target schema from source
  const onSourceSchema = useCallback((v: string) => {
    setFormRaw(f => ({ ...f, source_schema: v, source_table: "", target_schema: f.target_schema || v }));
    setEnsureResult(null); setEnsureErr("");
  }, []);

  const onSourceTable = useCallback((v: string) => {
    setFormRaw(f => ({ ...f, source_table: v, target_table: f.target_table || v }));
    setEnsureResult(null); setEnsureErr("");
  }, []);

  // Ensure target table matches source
  const ensureTargetTable = useCallback(() => {
    if (!form.source_schema || !form.source_table || !form.target_schema || !form.target_table) return;
    setEnsureBusy(true);
    setEnsureErr("");
    setEnsureResult(null);
    fetch("/api/target-prep/ensure-table", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        src_schema: form.source_schema,
        src_table:  form.source_table,
        tgt_schema: form.target_schema,
        tgt_table:  form.target_table,
      }),
    })
      .then(r => r.json())
      .then(d => {
        if (d.error) setEnsureErr(d.error);
        else setEnsureResult(d);
      })
      .catch(e => setEnsureErr(String(e)))
      .finally(() => setEnsureBusy(false));
  }, [form.source_schema, form.source_table, form.target_schema, form.target_table]);

  // Validation
  function validate(): boolean {
    const e: Partial<Record<keyof FormData, string>> = {};
    if (!form.migration_name.trim())   e.migration_name = "Обязательное поле";
    if (!form.source_schema)           e.source_schema  = "Выберите схему";
    if (!form.source_table)            e.source_table   = "Выберите таблицу";
    if (!form.target_schema)           e.target_schema  = "Выберите схему";
    if (!form.target_table)            e.target_table   = "Выберите таблицу";
    if (hasCdc(form.strategy) && !form.group_id) {
      if (!form.connector_name.trim()) e.connector_name = "Обязательное поле";
      if (!form.topic_prefix.trim())   e.topic_prefix   = "Обязательное поле";
      if (!form.consumer_group.trim()) e.consumer_group = "Обязательное поле";
    }
    if (usesStage(form.strategy) && !form.stage_table_name.trim())
      e.stage_table_name = "Обязательное поле";
    if (form.chunk_size <= 0)          e.chunk_size = "Должно быть > 0";
    if (!form.effective_key_type)      e.effective_key_type = "Выберите тип ключа";
    if (form.effective_key_type === "USER_DEFINED" && form.effective_key_columns.length === 0)
      e.effective_key_columns = "Выберите хотя бы один столбец";
    setFieldErrs(e);
    return Object.keys(e).length === 0;
  }

  // Submit
  async function handleSubmit() {
    if (!validate()) return;
    setSubmit(true);
    setSubmitErr("");
    const truncateTarget = usesStage(form.strategy) ? true : form.truncate_target;
    const payload = {
      initial_phase:              "DRAFT",
      migration_name:             form.migration_name.trim(),
      strategy:                   form.strategy,
      truncate_target:            truncateTarget,
      source_connection_id:       "oracle_source",
      target_connection_id:       "oracle_target",
      source_schema:              form.source_schema,
      source_table:               form.source_table,
      target_schema:              form.target_schema,
      target_table:               form.target_table,
      stage_table_name:           usesStage(form.strategy) ? form.stage_table_name.trim() : "",
      stage_tablespace:           usesStage(form.strategy) ? form.stage_tablespace.trim() : "",
      group_id:                   form.group_id || undefined,
      connector_name:             hasCdc(form.strategy) && !form.group_id ? form.connector_name.trim() : "",
      topic_prefix:               hasCdc(form.strategy) && !form.group_id ? form.topic_prefix.trim() : "",
      consumer_group:             hasCdc(form.strategy) && !form.group_id ? form.consumer_group.trim() : "",
      chunk_size:                 form.chunk_size,
      max_parallel_workers:       form.max_parallel_workers,
      baseline_parallel_degree:   form.baseline_parallel_degree,
      validate_hash_sample:       form.validate_hash_sample,
      source_pk_exists:           (tableInfo?.pk_columns.length ?? 0) > 0,
      source_uk_exists:           (tableInfo?.uk_constraints.length ?? 0) > 0,
      effective_key_type:         form.effective_key_type,
      effective_key_source:       KEY_SOURCE_MAP[form.effective_key_type] ?? "NONE",
      effective_key_columns_json: JSON.stringify(form.effective_key_columns),
    };
    try {
      const r = await fetch("/api/migrations", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const d = await r.json();
      if (!r.ok) { setSubmitErr(d.error ?? "Ошибка сервера"); return; }
      onCreated();
      onClose();
    } catch (e) {
      setSubmitErr(String(e));
    } finally {
      setSubmit(false);
    }
  }

  // Key section helpers
  function setKeyType(kt: string) {
    let cols: string[] = [];
    if (kt === "PRIMARY_KEY") cols = tableInfo?.pk_columns ?? [];
    if (kt === "UNIQUE_KEY")  cols = tableInfo?.uk_constraints[form.selected_uk_index]?.columns ?? [];
    setF({ effective_key_type: kt, effective_key_columns: cols });
  }

  function toggleKeyCol(col: string, checked: boolean) {
    const cols = checked
      ? [...form.effective_key_columns, col]
      : form.effective_key_columns.filter(c => c !== col);
    setF({ effective_key_columns: cols });
  }

  return (
    <div style={S.overlay} onClick={e => { if (e.target === e.currentTarget) onClose(); }}>
      <div style={S.modal}>
        {/* Header */}
        <div style={S.header}>
          <span style={{ fontSize: 15, fontWeight: 700, color: t.text.primary }}>
            Новая миграция
          </span>
          <span style={{ flex: 1 }} />
          <button onClick={onClose} style={{
            background: "none", border: "none", color: t.text.disabled,
            cursor: "pointer", fontSize: 20, lineHeight: 1, padding: "0 2px",
          }}>✕</button>
        </div>

        {/* Body */}
        <div style={S.body}>
          {/* ── Source ── */}
          <Section title="Источник (Oracle Source)" accent={t.blue.dim}>
            <SchemaTablePair
              db="source"
              schema={form.source_schema}
              table={form.source_table}
              onSchema={onSourceSchema}
              onTable={onSourceTable}
              schemaErr={fieldErrs.source_schema}
              tableErr={fieldErrs.source_table}
              excludeTables={excludeSourceTables}
            />
            {loadingInfo && (
              <div style={{ fontSize: t.size.sm, color: t.text.disabled }}>
                Загрузка информации о таблице…
              </div>
            )}
            {infoErr && <div style={S.err}>{infoErr}</div>}
            {tableInfo && !loadingInfo && (
              <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                <Chip label={`${tableInfo.columns.length} колонок`} color={t.text.secondary} bg={t.bg.s2} />
                {tableInfo.pk_columns.length > 0
                  ? <Chip label={`PK: ${tableInfo.pk_columns.join(", ")}`}      color={t.green.fg}   bg={t.green.bg} />
                  : <Chip label="Нет PK"                                         color={t.red.fg}     bg={t.red.bg} />}
                {tableInfo.uk_constraints.length > 0 && (
                  <Chip label={`UK: ${tableInfo.uk_constraints.length} constraint(s)`} color={t.purple.fg} bg={t.purple.bg} />
                )}
              </div>
            )}
          </Section>

          {/* ── Target ── */}
          <Section title="Цель (Oracle Target)" accent={t.green.dim}>
            <SchemaTablePair
              db="target"
              schema={form.target_schema}
              table={form.target_table}
              onSchema={v => { setF({ target_schema: v, target_table: "" }); setEnsureResult(null); setEnsureErr(""); }}
              onTable={v => { setF({ target_table: v }); setEnsureResult(null); setEnsureErr(""); }}
              schemaErr={fieldErrs.target_schema}
              tableErr={fieldErrs.target_table}
            />
            {form.source_schema && form.source_table && form.target_schema && (
              <div style={{ marginTop: 8, display: "flex", flexDirection: "column", gap: 6 }}>
                <button
                  disabled={ensureBusy || !form.target_table}
                  onClick={ensureTargetTable}
                  style={{
                    padding: "6px 14px", borderRadius: t.radius.md,
                    cursor: (ensureBusy || !form.target_table) ? "not-allowed" : "pointer",
                    border: `1px solid ${t.green.dim}`, background: t.green.bg, color: t.green.fg,
                    fontSize: t.size.base, fontWeight: 600,
                    opacity: (ensureBusy || !form.target_table) ? 0.5 : 1,
                  }}
                >
                  {ensureBusy ? "Синхронизация…" : "Привести target в соответствие source"}
                </button>
                {ensureErr && <div style={S.err}>{ensureErr}</div>}
                {ensureResult && <EnsureChips result={ensureResult} />}
              </div>
            )}
          </Section>

          {/* ── Mode + Strategy ── */}
          <Section title="Стратегия миграции" accent={t.purple.base}>
            <StrategyPicker
              value={form.strategy}
              onChange={(s) => setF({ strategy: s })}
              truncateTarget={form.truncate_target}
              onTruncateChange={(b) => setF({ truncate_target: b })}
            />
          </Section>

          {/* ── Config ── */}
          <Section title="Параметры миграции">
            {hasCdc(form.strategy) && (
              <>
                <Field label="Группа коннектора" hint="Выберите группу для общего Debezium-коннектора или оставьте пустым для отдельного">
                  <select
                    value={form.group_id}
                    onChange={e => setF({ group_id: e.target.value })}
                    style={{ ...S.input, cursor: "pointer", appearance: "auto" }}
                  >
                    <option value="">-- Без группы (отдельный коннектор) --</option>
                    {connGroups.map(g => (
                      <option key={g.group_id} value={g.group_id}>
                        {g.group_name} ({g.status}) — {g.connector_name}
                      </option>
                    ))}
                  </select>
                </Field>
                {!form.group_id && (
                  <>
                    <div style={S.row2}>
                      <Field label="Connector name" required error={fieldErrs.connector_name}>
                        <TextInput value={form.connector_name} hasError={!!fieldErrs.connector_name}
                          onChange={v => setF({ connector_name: v })} />
                      </Field>
                      <Field label="Topic prefix" required error={fieldErrs.topic_prefix}>
                        <TextInput value={form.topic_prefix} hasError={!!fieldErrs.topic_prefix}
                          onChange={v => setF({ topic_prefix: v })} />
                      </Field>
                    </div>
                    <Field label="Consumer group" required error={fieldErrs.consumer_group}>
                      <TextInput value={form.consumer_group} hasError={!!fieldErrs.consumer_group}
                        onChange={v => setF({ consumer_group: v })} />
                    </Field>
                  </>
                )}
                {form.group_id && (
                  <div style={{ fontSize: t.size.xs, color: t.green.fg, padding: "4px 0" }}>
                    Connector, topic prefix и consumer group будут унаследованы от группы
                  </div>
                )}
              </>
            )}

            <div style={S.row2}>
              {usesStage(form.strategy) && (<>
                <Field label="Stage table name" required error={fieldErrs.stage_table_name}>
                  <TextInput value={form.stage_table_name} hasError={!!fieldErrs.stage_table_name}
                    onChange={v => setF({ stage_table_name: v })} />
                </Field>
                <Field label="Stage tablespace" hint="Необязательно. Если пусто — default tablespace схемы">
                  <TextInput value={form.stage_tablespace} placeholder="например MIGRATION_DATA"
                    onChange={v => setF({ stage_tablespace: v })} />
                </Field>
              </>)}
            </div>

            <div style={S.row2}>
              <Field label="Chunk size" required error={fieldErrs.chunk_size} hint="Строк на чанк (рекомендуется 500k–2M)">
                <input style={S.input} type="number" value={form.chunk_size} min={1}
                  onChange={e => setF({ chunk_size: parseInt(e.target.value) || 0 })} />
              </Field>
              <Field label="Воркеры (bulk)" hint="Параллельных воркеров для bulk-загрузки">
                <input style={S.input} type="number" value={form.max_parallel_workers} min={1}
                  onChange={e => setF({ max_parallel_workers: Math.max(1, parseInt(e.target.value) || 1) })} />
              </Field>
              <Field label="Воркеры (baseline)" hint="Параллельных воркеров для baseline-загрузки">
                <input style={S.input} type="number" value={form.baseline_parallel_degree} min={1}
                  onChange={e => setF({ baseline_parallel_degree: Math.max(1, parseInt(e.target.value) || 4) })} />
              </Field>
            </div>

            <Field label="Валидация stage">
              <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
                <input
                  type="checkbox"
                  checked={form.validate_hash_sample}
                  onChange={e => setF({ validate_hash_sample: e.target.checked })}
                />
                <span style={{ fontSize: t.size.base, color: t.text.secondary }}>
                  Hash/sample проверка (сравнивает выборку строк source vs stage)
                </span>
              </label>
            </Field>
          </Section>

          {/* ── Key ── */}
          <Section title="Конфигурация ключа">
            {!form.source_schema || !form.source_table ? (
              <div style={{ fontSize: t.size.base, color: t.text.disabled }}>
                Выберите таблицу источника для определения ключа
              </div>
            ) : (
              <>
                <Field label="Тип ключа" required error={fieldErrs.effective_key_type}>
                  <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                    <KeyTypeBtn label="PRIMARY KEY"
                      active={form.effective_key_type === "PRIMARY_KEY"}
                      disabled={!tableInfo || tableInfo.pk_columns.length === 0}
                      onClick={() => setKeyType("PRIMARY_KEY")} />
                    <KeyTypeBtn label="UNIQUE KEY"
                      active={form.effective_key_type === "UNIQUE_KEY"}
                      disabled={!tableInfo || tableInfo.uk_constraints.length === 0}
                      onClick={() => setKeyType("UNIQUE_KEY")} />
                    <KeyTypeBtn label="USER DEFINED"
                      active={form.effective_key_type === "USER_DEFINED"}
                      onClick={() => setKeyType("USER_DEFINED")} />
                    <KeyTypeBtn label="NONE"
                      active={form.effective_key_type === "NONE"}
                      onClick={() => setKeyType("NONE")} />
                  </div>
                </Field>

                {form.effective_key_type === "PRIMARY_KEY" && tableInfo && (
                  <div style={{ fontSize: t.size.base, color: t.green.fg }}>
                    Колонки: <strong>{tableInfo.pk_columns.join(", ")}</strong>
                  </div>
                )}

                {form.effective_key_type === "UNIQUE_KEY" && tableInfo && (
                  <>
                    {tableInfo.uk_constraints.length > 1 && (
                      <Field label="Уникальный индекс">
                        <select
                          style={S.select}
                          value={form.selected_uk_index}
                          onChange={e => {
                            const idx = parseInt(e.target.value);
                            setF({
                              selected_uk_index: idx,
                              effective_key_columns: tableInfo.uk_constraints[idx]?.columns ?? [],
                            });
                          }}
                        >
                          {tableInfo.uk_constraints.map((uk, i) => (
                            <option key={uk.name} value={i}>
                              {uk.name} ({uk.columns.join(", ")})
                            </option>
                          ))}
                        </select>
                      </Field>
                    )}
                    <div style={{ fontSize: t.size.base, color: t.purple.fg }}>
                      Колонки: <strong>
                        {tableInfo.uk_constraints[form.selected_uk_index]?.columns.join(", ")}
                      </strong>
                    </div>
                  </>
                )}

                {form.effective_key_type === "USER_DEFINED" && tableInfo && (
                  <Field label="Выберите колонки ключа" required
                    error={fieldErrs.effective_key_columns as string}>
                    <div style={{
                      maxHeight: 180, overflowY: "auto",
                      border: `1px solid ${t.border.base}`, borderRadius: t.radius.md, background: t.bg.s2,
                    }}>
                      {tableInfo.columns.map(col => (
                        <label key={col.name} style={{
                          display: "flex", alignItems: "center", gap: 8,
                          padding: "5px 10px", cursor: "pointer",
                          borderBottom: `1px solid ${t.bg.app}`,
                        }}>
                          <input
                            type="checkbox"
                            checked={form.effective_key_columns.includes(col.name)}
                            onChange={e => toggleKeyCol(col.name, e.target.checked)}
                          />
                          <span style={{ fontSize: t.size.base, color: t.text.primary, fontFamily: t.font.mono }}>
                            {col.name}
                          </span>
                          <span style={{ fontSize: t.size.xs, color: t.text.disabled }}>{col.type}</span>
                          {!col.nullable && (
                            <span style={{ fontSize: t.size.xs, color: t.red.base, marginLeft: "auto" }}>
                              NOT NULL
                            </span>
                          )}
                        </label>
                      ))}
                    </div>
                    {form.effective_key_columns.length > 0 && (
                      <div style={S.hint}>Выбрано: {form.effective_key_columns.join(", ")}</div>
                    )}
                  </Field>
                )}
              </>
            )}
          </Section>

          {/* ── General ── */}
          <Section title="Общее">
            <Field
              label="Название миграции" required error={fieldErrs.migration_name}
              hint={!nameTouched.current ? "Автоматически заполняется из источника и цели" : undefined}
            >
              <input
                style={fieldErrs.migration_name ? S.inputErr : S.input}
                value={form.migration_name}
                placeholder="Выберите таблицы для автозаполнения"
                onChange={e => { nameTouched.current = true; setF({ migration_name: e.target.value }); }}
              />
            </Field>
          </Section>

          {/* Duplicate warning */}
          {dupWarning && (
            <div style={{
              background: t.amber.bg, border: `1px solid ${t.amber.dim}`, borderRadius: t.radius.md,
              padding: "10px 14px", fontSize: t.size.base, color: t.amber.fg,
            }}>
              ⚠ {dupWarning}
            </div>
          )}

          {/* Submit error */}
          {submitErr && (
            <div style={{
              background: t.red.bg, border: `1px solid ${t.red.border}`, borderRadius: t.radius.md,
              padding: "10px 14px", fontSize: t.size.base, color: t.red.fg,
            }}>
              {submitErr}
            </div>
          )}
        </div>

        {/* Footer */}
        <div style={S.footer}>
          <button onClick={onClose} style={{
            background: "none", border: `1px solid ${t.border.base}`, borderRadius: t.radius.md,
            color: t.text.muted, padding: "7px 18px", fontSize: t.size.md, cursor: "pointer",
          }}>
            Отмена
          </button>
          <button onClick={handleSubmit} disabled={submitting} style={{
            background: submitting ? t.bg.s3 : t.blue.dim,
            border: "none", borderRadius: t.radius.md,
            color: submitting ? t.text.muted : t.text.inverse,
            padding: "7px 22px", fontSize: t.size.md, fontWeight: 700,
            cursor: submitting ? "not-allowed" : "pointer",
          }}>
            {submitting ? "Сохранение…" : "Создать миграцию"}
          </button>
        </div>
      </div>
    </div>
  );
}
