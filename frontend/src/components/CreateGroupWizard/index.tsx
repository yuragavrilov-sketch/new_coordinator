import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { t } from "../../theme";
import { S } from "../CreateMigrationModal/styles";
import { Section, Field } from "../CreateMigrationModal/ui";
import { SearchableSelect } from "../CreateMigrationModal/SearchableSelect";
import type { GroupForm, TableEntry, TableInfo } from "./types";
import { INIT_FORM, STEP_LABELS, matchesFilter, toSnake, shortId } from "./helpers";
import { StepIndicator } from "./StepIndicator";
import { TableKeyConfig } from "./TableKeyConfig";
import { AvailableTablesList } from "./AvailableTablesList";

interface Props { onClose: () => void; onCreated: () => void }

export function CreateGroupWizard({ onClose, onCreated }: Props) {
  const [step,       setStep]       = useState(0);
  const [form,       setFormRaw]    = useState<GroupForm>(INIT_FORM);
  const [tables,     setTables]     = useState<TableEntry[]>([]);
  const [submitting, setSubmitting] = useState(false);
  const [submitErr,  setSubmitErr]  = useState("");
  const [fieldErrs,  setFieldErrs]  = useState<Record<string, string>>({});

  // schema / table loading
  const [schemas,         setSchemas]        = useState<string[]>([]);
  const [loadingSchemas,  setLoadingSchemas] = useState(true);
  const [selectedSchema,  setSelectedSchema] = useState("");
  const [schemaTables,    setSchemaTables]   = useState<string[]>([]);
  const [loadingTables,   setLoadingTables]  = useState(false);
  const [tableFilter,     setTableFilter]    = useState("");

  // map "SCHEMA.TABLE" → group_name for tables already in some group
  const [tableGroupMap, setTableGroupMap] = useState<Map<string, string>>(new Map());

  const setF = useCallback((up: Partial<GroupForm>) =>
    setFormRaw(f => ({ ...f, ...up })), []);

  // Auto-generate connector_name and topic_prefix from group_name
  const nameGenerated = useRef(false);
  useEffect(() => {
    if (!form.group_name) return;
    const base = toSnake(form.group_name);
    const id = nameGenerated.current ? "" : shortId();
    if (!nameGenerated.current) nameGenerated.current = true;
    setF({
      connector_name: `grp_${base}${id ? "_" + id : ""}_connector`,
      topic_prefix:   `grp.${base}${id ? "." + id : ""}`,
    });
  }, [form.group_name, setF]);

  // Load schemas
  useEffect(() => {
    fetch("/api/db/source/schemas")
      .then(r => r.json())
      .then(d => { if (!d.error) setSchemas(d); })
      .catch(() => {})
      .finally(() => setLoadingSchemas(false));
  }, []);

  // Build table→group map from existing group_tables
  useEffect(() => {
    fetch("/api/connector-groups")
      .then(r => r.ok ? r.json() : [])
      .then(async (groups: any[]) => {
        const tgMap = new Map<string, string>();
        for (const g of groups) {
          try {
            const resp = await fetch(`/api/connector-groups/${g.group_id}/tables`);
            if (!resp.ok) continue;
            const gTables: any[] = await resp.json();
            for (const tbl of gTables) {
              tgMap.set(`${tbl.source_schema}.${tbl.source_table}`, g.group_name);
            }
          } catch {}
        }
        setTableGroupMap(tgMap);
      })
      .catch(() => {});
  }, []);

  // Load tables when schema selected
  useEffect(() => {
    if (!selectedSchema) { setSchemaTables([]); return; }
    setLoadingTables(true);
    setSchemaTables([]);
    fetch(`/api/db/source/tables?schema=${encodeURIComponent(selectedSchema)}`)
      .then(r => r.json())
      .then(d => { if (!d.error) setSchemaTables(d); })
      .catch(() => {})
      .finally(() => setLoadingTables(false));
  }, [selectedSchema]);

  // Filter out already-selected tables
  const availableTables = useMemo(() => {
    const alreadySelected = new Set<string>();
    for (const tbl of tables) {
      if (tbl.schema === selectedSchema) alreadySelected.add(tbl.table);
    }
    let list = schemaTables.filter(tbl => !alreadySelected.has(tbl));
    if (tableFilter) {
      list = list.filter(tbl => matchesFilter(tbl, tableFilter));
    }
    return list;
  }, [schemaTables, selectedSchema, tables, tableFilter]);

  // Add table to selection
  function addTable(tableName: string) {
    const entry: TableEntry = {
      schema:                selectedSchema,
      table:                 tableName,
      tableInfo:             null,
      loadingInfo:           true,
      infoError:             "",
      effective_key_type:    "",
      effective_key_columns: [],
      selected_uk_index:     0,
      target_schema:         selectedSchema,
      target_table:          tableName,
    };
    setTables(prev => [...prev, entry]);

    const p = `schema=${encodeURIComponent(selectedSchema)}&table=${encodeURIComponent(tableName)}`;
    fetch(`/api/db/source/table-info?${p}`)
      .then(r => r.json())
      .then((d: TableInfo & { error?: string }) => {
        setTables(prev => prev.map(tbl => {
          if (tbl.schema !== selectedSchema || tbl.table !== tableName) return tbl;
          if (d.error) return { ...tbl, loadingInfo: false, infoError: d.error };
          let keyType = "USER_DEFINED";
          let keyCols: string[] = [];
          if (d.pk_columns.length > 0) {
            keyType = "PRIMARY_KEY"; keyCols = d.pk_columns;
          } else if (d.uk_constraints.length > 0) {
            keyType = "UNIQUE_KEY"; keyCols = d.uk_constraints[0].columns;
          }
          return {
            ...tbl,
            tableInfo: d,
            loadingInfo: false,
            effective_key_type: keyType,
            effective_key_columns: keyCols,
          };
        }));
      })
      .catch(e => {
        setTables(prev => prev.map(tbl => {
          if (tbl.schema !== selectedSchema || tbl.table !== tableName) return tbl;
          return { ...tbl, loadingInfo: false, infoError: String(e) };
        }));
      });
  }

  function removeTable(schema: string, table: string) {
    setTables(prev => prev.filter(tbl => !(tbl.schema === schema && tbl.table === table)));
  }

  function updateTable(schema: string, table: string, upd: Partial<TableEntry>) {
    setTables(prev => prev.map(tbl =>
      tbl.schema === schema && tbl.table === table ? { ...tbl, ...upd } : tbl
    ));
  }

  // ── Validation ──────────────────────────────────────────────────────────────

  function validateStep0(): boolean {
    const e: Record<string, string> = {};
    if (!form.group_name.trim())     e.group_name     = "Обязательное поле";
    if (!form.connector_name.trim()) e.connector_name = "Обязательное поле";
    if (!form.topic_prefix.trim())   e.topic_prefix   = "Обязательное поле";
    setFieldErrs(e);
    return Object.keys(e).length === 0;
  }

  function validateStep1(): boolean {
    if (tables.length === 0) {
      setSubmitErr("Выберите хотя бы одну таблицу");
      return false;
    }
    for (const tbl of tables) {
      if (!tbl.effective_key_type) {
        setSubmitErr(`Не задан тип ключа для ${tbl.schema}.${tbl.table}`);
        return false;
      }
      if (tbl.effective_key_type === "USER_DEFINED" && tbl.effective_key_columns.length === 0) {
        setSubmitErr(`Не выбраны колонки ключа для ${tbl.schema}.${tbl.table}`);
        return false;
      }
    }
    setSubmitErr("");
    return true;
  }

  // ── Navigation ──────────────────────────────────────────────────────────────

  function next() { if (step === 0 && validateStep0()) setStep(1); }
  function back() {
    setFieldErrs({});
    setSubmitErr("");
    if (step > 0) setStep(step - 1);
  }

  // ── Submit ──────────────────────────────────────────────────────────────────

  async function handleSubmit() {
    if (!validateStep1()) return;
    setSubmitting(true);
    setSubmitErr("");

    const payload = {
      group_name:            form.group_name.trim(),
      connector_name:        form.connector_name.trim(),
      topic_prefix:          form.topic_prefix.trim(),
      consumer_group_prefix: form.topic_prefix.trim(),
      source_connection_id:  "oracle_source",
      tables: tables.map(tbl => ({
        source_schema:         tbl.schema,
        source_table:          tbl.table,
        target_schema:         tbl.target_schema,
        target_table:          tbl.target_table,
        effective_key_type:    tbl.effective_key_type,
        effective_key_columns: tbl.effective_key_columns,
        source_pk_exists:      (tbl.tableInfo?.pk_columns.length ?? 0) > 0,
        source_uk_exists:      (tbl.tableInfo?.uk_constraints.length ?? 0) > 0,
      })),
    };

    try {
      const r = await fetch("/api/connector-groups/wizard", {
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
      setSubmitting(false);
    }
  }

  return (
    <div style={S.overlay} onClick={e => { if (e.target === e.currentTarget) onClose(); }}>
      <div style={{ ...S.modal, maxWidth: 780, maxHeight: "calc(100vh - 80px)" }}>

        {/* Header */}
        <div style={S.header}>
          <span style={{ fontSize: 15, fontWeight: 700, color: t.text.primary }}>
            Новая группа коннекторов
          </span>
          <span style={{ flex: 1 }} />
          <StepIndicator step={step} total={2} labels={STEP_LABELS} />
          <span style={{ flex: 1 }} />
          <button onClick={onClose} style={{
            background: "none", border: "none", color: t.text.disabled,
            cursor: "pointer", fontSize: 20, lineHeight: 1, padding: "0 2px",
          }}>✕</button>
        </div>

        {/* Body */}
        <div style={{ ...S.body, flex: "1 1 auto", minHeight: 0, overflowY: "auto" }}>

          {/* STEP 0: Group info */}
          {step === 0 && (
            <Section title="Настройки группы" accent={t.blue.dim}>
              <Field label="Имя группы" required error={fieldErrs.group_name}>
                <input
                  style={fieldErrs.group_name ? S.inputErr : S.input}
                  value={form.group_name}
                  placeholder="prod_batch_1"
                  onChange={e => setF({ group_name: e.target.value })}
                />
              </Field>
              <div style={S.row2}>
                <Field label="Имя коннектора (Debezium)" required error={fieldErrs.connector_name}
                  hint="Автозаполняется из имени группы">
                  <input
                    style={fieldErrs.connector_name ? S.inputErr : S.input}
                    value={form.connector_name}
                    onChange={e => setF({ connector_name: e.target.value })}
                  />
                </Field>
                <Field label="Topic prefix (Kafka)" required error={fieldErrs.topic_prefix}
                  hint="Автозаполняется из имени группы">
                  <input
                    style={fieldErrs.topic_prefix ? S.inputErr : S.input}
                    value={form.topic_prefix}
                    onChange={e => setF({ topic_prefix: e.target.value })}
                  />
                </Field>
              </div>
            </Section>
          )}

          {/* STEP 1: Tables + key config */}
          {step === 1 && (
            <>
              {/* Available tables — fixed height */}
              <div style={{ flexShrink: 0 }}>
                <Section title="Выбор таблиц" accent={t.blue.dim}>
                  <div style={S.row2}>
                    <Field label="Схема источника" required>
                      <SearchableSelect
                        items={schemas}
                        value={selectedSchema}
                        onChange={v => { setSelectedSchema(v); setTableFilter(""); }}
                        disabled={loadingSchemas}
                        placeholder={loadingSchemas ? "Загрузка…" : "Выберите схему"}
                      />
                    </Field>
                    <Field label="Фильтр таблиц" hint="PAY* или *LOG* или точное имя">
                      <input
                        style={S.input} value={tableFilter}
                        onChange={e => setTableFilter(e.target.value)}
                        placeholder="PAY* или *AUDIT*…"
                        disabled={!selectedSchema}
                      />
                    </Field>
                  </div>

                  {loadingTables && (
                    <div style={{ fontSize: t.size.sm, color: t.text.disabled }}>
                      Загрузка таблиц...
                    </div>
                  )}

                  {!loadingTables && selectedSchema && (
                    <AvailableTablesList
                      tables={availableTables}
                      schema={selectedSchema}
                      groupMap={tableGroupMap}
                      onAdd={addTable}
                      emptyText={tableFilter ? "Ничего не найдено" : "Нет доступных таблиц"}
                    />
                  )}
                </Section>
              </div>

              {/* Selected tables with key config — scrollable */}
              {tables.length > 0 && (
                <div style={{
                  border: `1px solid ${t.green.dim}50`, borderRadius: t.radius.md,
                  display: "flex", flexDirection: "column",
                  minHeight: 0, flex: "1 1 auto",
                }}>
                  <div style={{
                    padding: "7px 14px", background: t.bg.s1,
                    borderBottom: `1px solid ${t.green.dim}40`,
                    fontSize: t.size.sm, fontWeight: 700, color: t.green.dim,
                    textTransform: "uppercase", letterSpacing: 0.8,
                    flexShrink: 0,
                  }}>
                    {`Выбранные таблицы (${tables.length})`}
                  </div>
                  <div style={{
                    padding: 12, overflowY: "auto",
                    display: "flex", flexDirection: "column", gap: 10,
                    minHeight: 0,
                  }}>
                    {tables.map(tbl => (
                      <div key={`${tbl.schema}.${tbl.table}`} style={{
                        border: `1px solid ${t.border.subtle}`, borderRadius: t.radius.md,
                        background: t.bg.s1, overflow: "hidden",
                        flexShrink: 0,
                      }}>
                        <div style={{
                          display: "flex", alignItems: "center", gap: 8,
                          padding: "8px 12px", borderBottom: `1px solid ${t.border.subtle}`,
                        }}>
                          <span style={{
                            fontSize: t.size.base, fontWeight: 700, color: t.text.primary,
                            fontFamily: t.font.mono,
                          }}>
                            {tbl.schema}.{tbl.table}
                          </span>
                          <span style={{ fontSize: t.size.xs, color: t.text.disabled }}>
                            → {tbl.target_schema}.{tbl.target_table}
                          </span>
                          <span style={{ flex: 1 }} />
                          <button
                            onClick={() => removeTable(tbl.schema, tbl.table)}
                            style={{
                              background: "none", border: "none", color: t.red.dim,
                              cursor: "pointer", fontSize: t.size.lg, lineHeight: 1, padding: "0 2px",
                            }}
                          >✕</button>
                        </div>
                        <div style={{ padding: "8px 12px" }}>
                          <TableKeyConfig
                            entry={tbl}
                            onChange={upd => updateTable(tbl.schema, tbl.table, upd)}
                          />
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </>
          )}

          {submitErr && (
            <div style={{
              padding: "8px 12px", background: t.red.bg,
              border: `1px solid ${t.red.border}`, borderRadius: t.radius.md,
              color: t.red.fg, fontSize: t.size.base,
            }}>
              {submitErr}
            </div>
          )}
        </div>

        {/* Footer */}
        <div style={S.footer}>
          {step > 0 && (
            <button onClick={back} style={{
              background: "none", border: `1px solid ${t.border.base}`, borderRadius: t.radius.md,
              color: t.text.secondary, padding: "7px 16px", fontSize: t.size.base, cursor: "pointer",
            }}>Назад</button>
          )}
          <button onClick={onClose} style={{
            background: "none", border: `1px solid ${t.border.base}`, borderRadius: t.radius.md,
            color: t.text.muted, padding: "7px 16px", fontSize: t.size.base, cursor: "pointer",
          }}>Отмена</button>
          {step < STEP_LABELS.length - 1 ? (
            <button onClick={next} style={{
              background: t.blue.dim, border: "none", borderRadius: t.radius.md,
              color: t.text.inverse, padding: "7px 20px", fontSize: t.size.base, cursor: "pointer",
              fontWeight: 600,
            }}>Далее</button>
          ) : (
            <button
              onClick={handleSubmit}
              disabled={submitting}
              style={{
                background: t.green.dim, border: "none", borderRadius: t.radius.md,
                color: t.text.inverse, padding: "7px 20px", fontSize: t.size.base, cursor: "pointer",
                fontWeight: 600, opacity: submitting ? 0.6 : 1,
              }}
            >
              {submitting ? "Создание..." : `Создать группу (${tables.length} табл.)`}
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
