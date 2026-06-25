import React, { useEffect, useMemo, useState } from "react";
import { t } from "../theme";
import { S } from "../components/CreateMigrationModal/styles";
import { Section, Field } from "../components/CreateMigrationModal/ui";
import { primaryActionStyle, secondaryActionStyle } from "./buttonStyles";
import {
  addSchemaPlanItems,
  type AddPlanItemsPayload,
  type AddPlanItemsResp,
  type MigrationPlanCdcGroup,
  type MigrationPlanCdcTable,
} from "./api";

interface BulkTable {
  source_schema: string;
  source_table:  string;
  target_schema: string;
  target_table?: string;
}

interface TableInfo {
  columns:        Array<{ name: string; type: string; nullable: boolean }>;
  pk_columns:     string[];
  uk_constraints: Array<{ name: string; columns: string[] }>;
  supplemental_log_data_all?: string | null;
  error?:         string;
}

interface Props {
  schemaMigrationId: string;
  tables: BulkTable[];
  initialMode?: "historical" | "cdc";
  cdcGroup?: MigrationPlanCdcGroup | null;
  cdcGroupLoading?: boolean;
  cdcGroupError?: string | null;
  onClose: () => void;
  onReloadCdcGroup?: () => void | Promise<void>;
  onDone: (planId: number, count: number, response: AddPlanItemsResp) => void | Promise<void>;
}

export function AddToPlanModal({
  schemaMigrationId,
  tables,
  initialMode = "historical",
  cdcGroup,
  cdcGroupLoading = false,
  cdcGroupError = null,
  onClose,
  onReloadCdcGroup,
  onDone,
}: Props) {
  const [mode, setMode] = useState<"historical" | "cdc">(initialMode);
  const [strategy, setStrategy] = useState<AddPlanItemsPayload["strategy"]>(
    initialMode === "cdc" ? "CDC_DIRECT" : "BULK_DIRECT",
  );
  const [sequential, setSequential] = useState(true);
  const [truncateTarget, setTruncateTarget] = useState(true);
  const [chunkSize, setChunkSize] = useState(1_000_000);
  const [workers, setWorkers] = useState(1);
  const [baselinePd, setBaselinePd] = useState(4);
  const [stageTablespace, setStageTablespace] = useState("PAYSTAGE");
  const [tableInfos, setTableInfos] = useState<Record<string, TableInfo>>({});
  const [keyColumns, setKeyColumns] = useState<Record<string, string[]>>({});
  const [infoLoading, setInfoLoading] = useState(false);
  const [busy, setBusy] = useState(false);
  const [cdcRemoveBusy, setCdcRemoveBusy] = useState("");
  const [hiddenCdcTableKeys, setHiddenCdcTableKeys] = useState<Set<string>>(() => new Set());
  const [err, setErr] = useState("");

  const usesStage = strategy.endsWith("_STAGE");
  const tablesKey = useMemo(
    () => tables.map(x => `${x.source_schema}.${x.source_table}`).join("|"),
    [tables],
  );
  const selectedKeys = useMemo(
    () => new Set(tables.map(x => `${x.source_schema.toUpperCase()}.${x.source_table.toUpperCase()}`)),
    [tables],
  );
  const rawConnectorTables = cdcGroup?.tables || [];
  const connectorTables = rawConnectorTables.filter(t => !hiddenCdcTableKeys.has(cdcTableKey(t)));
  const connectorTableKeys = new Set(connectorTables.map(cdcTableKey));
  const connectorSelectedTables = connectorTables.filter(t => selectedKeys.has(cdcTableKey(t)));
  const connectorOtherTables = connectorTables.filter(t => !selectedKeys.has(cdcTableKey(t)));
  const connectorNewTables = tables.filter(t => !connectorTableKeys.has(rowKey(t)));
  const projectedConnectorLabels = [
    ...connectorTables.map(cdcTableLabel),
    ...connectorNewTables.map(rowKey),
  ];
  const projectedPreview = projectedConnectorLabels.slice(0, 8);
  const projectedRest = Math.max(0, projectedConnectorLabels.length - projectedPreview.length);
  const projectedConnectorCount = projectedConnectorLabels.length;
  const cdcSubmitLabel = projectedConnectorCount > tables.length
    ? `Добавить ${tables.length}, синхронизировать ${projectedConnectorCount} в Debezium`
    : `Добавить ${tables.length} в CDC-коннектор`;
  const submitDisabled = busy || !!cdcRemoveBusy || (mode === "cdc" && (infoLoading || cdcGroupLoading || !!cdcGroupError));

  function rowKey(x: BulkTable) {
    return `${x.source_schema.toUpperCase()}.${x.source_table.toUpperCase()}`;
  }

  function hasSuppLog(info?: TableInfo) {
    const raw = String(info?.supplemental_log_data_all ?? "").trim().toUpperCase();
    if (!raw) return null;
    return raw === "YES" || raw === "TRUE" || raw === "1";
  }

  function cdcTableKey(x: MigrationPlanCdcTable) {
    return `${x.source_schema.toUpperCase()}.${x.source_table.toUpperCase()}`;
  }

  function cdcTableLabel(x: MigrationPlanCdcTable) {
    return cdcTableKey(x);
  }

  useEffect(() => {
    setPackMode(initialMode);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialMode]);

  useEffect(() => {
    setHiddenCdcTableKeys(prev => {
      if (prev.size === 0) return prev;
      const actualKeys = new Set(rawConnectorTables.map(cdcTableKey));
      const next = new Set([...prev].filter(key => actualKeys.has(key)));
      return next.size === prev.size ? prev : next;
    });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cdcGroup?.group_id, rawConnectorTables.length, rawConnectorTables.map(cdcTableKey).join("|")]);

  useEffect(() => {
    if (usesStage && !truncateTarget) setTruncateTarget(true);
  }, [usesStage, truncateTarget]);

  useEffect(() => {
    if (mode !== "cdc") {
      setTableInfos({});
      setKeyColumns({});
      setInfoLoading(false);
      return;
    }

    let cancelled = false;
    setInfoLoading(true);
    setErr("");

    Promise.all(tables.map(async table => {
      const key = rowKey(table);
      const p = new URLSearchParams({
        schema: table.source_schema,
        table:  table.source_table,
      });
      try {
        const r = await fetch(`/api/db/source/table-info?${p.toString()}`);
        const d = await r.json().catch(() => ({}));
        if (!r.ok || d.error) {
          return [key, {
            columns: [],
            pk_columns: [],
            uk_constraints: [],
            error: d.error || `HTTP ${r.status}`,
          } as TableInfo] as const;
        }
        return [key, d as TableInfo] as const;
      } catch (e) {
        return [key, {
          columns: [],
          pk_columns: [],
          uk_constraints: [],
          error: e instanceof Error ? e.message : String(e),
        } as TableInfo] as const;
      }
    })).then(entries => {
      if (cancelled) return;
      const nextInfos = Object.fromEntries(entries);
      setTableInfos(nextInfos);
      setKeyColumns(prev => {
        const next: Record<string, string[]> = {};
        for (const table of tables) {
          const key = rowKey(table);
          const info = nextInfos[key];
          if (!info || info.pk_columns.length > 0) continue;
          next[key] = prev[key] ?? info.uk_constraints[0]?.columns ?? [];
        }
        return next;
      });
    }).finally(() => {
      if (!cancelled) setInfoLoading(false);
    });

    return () => { cancelled = true; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode, tablesKey]);

  function setPackMode(next: "historical" | "cdc") {
    setMode(next);
    setErr("");
    if (next === "historical") {
      setStrategy("BULK_DIRECT");
      setWorkers(1);
    } else {
      setStrategy("CDC_DIRECT");
      setWorkers(4);
      setSequential(true);
    }
  }

  async function removeExistingCdcTable(table: MigrationPlanCdcTable) {
    if (!cdcGroup || busy || cdcRemoveBusy) return;
    const label = cdcTableLabel(table);
    if (!window.confirm(`Убрать ${label} из CDC-коннектора? Debezium table.include.list будет обновлен.`)) return;
    setCdcRemoveBusy(label);
    setErr("");
    try {
      const res = await fetch(
        `/api/connector-groups/${cdcGroup.group_id}/tables/${encodeURIComponent(table.source_schema)}/${encodeURIComponent(table.source_table)}`,
        { method: "DELETE" },
      );
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error || `HTTP ${res.status}`);
      }
      const body = await res.json().catch(() => ({}));
      setHiddenCdcTableKeys(prev => new Set(prev).add(label));
      await onReloadCdcGroup?.();
      if (body.sync_error) {
        setErr(`Таблица убрана из пачки, но Debezium не синхронизирован: ${body.sync_error}`);
      }
    } catch (e) {
      setErr(String(e instanceof Error ? e.message : e));
    } finally {
      setCdcRemoveBusy("");
    }
  }

  async function submit() {
    if (busy) return;
    setBusy(true);
    setErr("");
    try {
      if (mode === "cdc") {
        if (cdcGroupLoading) {
          setErr("Дождитесь загрузки состава CDC-коннектора.");
          return;
        }
        if (cdcGroupError) {
          setErr(`Не удалось загрузить состав CDC-коннектора: ${cdcGroupError}`);
          return;
        }
        if (infoLoading) {
          setErr("Дождитесь загрузки ключей таблиц.");
          return;
        }
        for (const table of tables) {
          const key = rowKey(table);
          const info = tableInfos[key];
          if (!info) {
            setErr(`Не удалось получить ключи для ${key}.`);
            return;
          }
          if (info.error) {
            setErr(`${key}: ${info.error}`);
            return;
          }
          if (hasSuppLog(info) === false) {
            setErr(`${key}: для CDC нужен supplemental logging ALL COLUMNS.`);
            return;
          }
          if (info.pk_columns.length === 0 && info.uk_constraints.length === 0 && (keyColumns[key] ?? []).length === 0) {
            setErr(`Для ${key} без PK нужно выбрать колонки CDC-ключа.`);
            return;
          }
        }
      }

      const payload: AddPlanItemsPayload = {
        tables: tables.map(t => ({
          source_table: t.source_table,
          target_table: t.target_table || t.source_table,
          key_columns: mode === "cdc"
            && (tableInfos[rowKey(t)]?.pk_columns.length ?? 0) === 0
            && (tableInfos[rowKey(t)]?.uk_constraints.length ?? 0) === 0
            ? keyColumns[rowKey(t)] ?? []
            : undefined,
        })),
        strategy,
        sequential: mode === "cdc" ? true : sequential,
        truncate_target: truncateTarget,
        chunk_size: chunkSize,
        max_parallel_workers: workers,
        baseline_parallel_degree: baselinePd,
        stage_tablespace: usesStage ? stageTablespace.trim().toUpperCase() : undefined,
      };
      const res = await addSchemaPlanItems(schemaMigrationId, payload);
      await onDone(res.plan_id, res.items.length, res);
    } catch (e) {
      setErr(String(e instanceof Error ? e.message : e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div style={S.overlay} onClick={e => { if (e.target === e.currentTarget) onClose(); }}>
      <div style={{ ...S.modal, maxWidth: 640 }}>
        <div style={S.header}>
          <span style={{ fontSize: 15, fontWeight: 700, color: t.text.primary }}>
            {mode === "cdc" ? "Добавить в CDC-коннектор" : "Добавить в обычную пачку"}
          </span>
          <span style={{ fontSize: 12, color: t.text.muted, fontFamily: t.font.mono }}>
            {tables.length} таблиц
          </span>
          <span style={{ flex: 1 }} />
          <button onClick={onClose} style={{
            background: "none", border: "none", color: t.text.disabled,
            cursor: "pointer", fontSize: 20, lineHeight: 1, padding: "0 2px",
          }}>x</button>
        </div>

        <div style={S.body}>
          {err && (
            <div style={{
              padding: "8px 10px", borderRadius: t.radius.sm,
              background: `${t.red.border}22`, border: `1px solid ${t.red.border}`,
              color: t.red.fg, fontSize: 12,
            }}>
              {err}
            </div>
          )}

          <Section title="Режим">
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
              <button
                type="button"
                onClick={() => setPackMode("historical")}
                style={{
                  ...secondaryActionStyle(false),
                  justifyContent: "center",
                  borderColor: mode === "historical" ? t.green.base : t.border.subtle,
                  background: mode === "historical" ? t.green.bg : t.bg.s2,
                  color: mode === "historical" ? t.green.fg : t.text.primary,
                }}
              >
                Обычная пачка
              </button>
              <button
                type="button"
                onClick={() => setPackMode("cdc")}
                style={{
                  ...secondaryActionStyle(false),
                  justifyContent: "center",
                  borderColor: mode === "cdc" ? t.blue.base : t.border.subtle,
                  background: mode === "cdc" ? t.blue.bg : t.bg.s2,
                  color: mode === "cdc" ? t.blue.fg : t.text.primary,
                }}
              >
                CDC-коннектор
              </button>
            </div>
            <div style={{
              padding: "9px 12px",
              borderRadius: t.radius.md,
              background: mode === "cdc" ? t.blue.bg : t.amber.bg,
              border: `1px solid ${mode === "cdc" ? t.blue.dim : t.amber.dim}`,
              color: mode === "cdc" ? t.blue.fg : t.amber.fg,
              fontSize: 12,
              lineHeight: 1.45,
            }}>
              {mode === "cdc"
                ? "Таблицы попадут в единственный CDC-коннектор этой миграции. Если его ещё нет, coordinator создаст его автоматически. После добавления коннектор и перенос данных стартуют автоматически."
                : "SCN не фиксируется. Используйте только для таблиц, которые уже не меняются на source. Для DIRECT target будет подготовлен перед загрузкой: триггеры отключаются, вторичные индексы пересчитываются после переноса."}
            </div>
            <Field label="Стратегия">
              <select value={strategy} onChange={e => setStrategy(e.target.value as AddPlanItemsPayload["strategy"])} style={S.select}>
                {mode === "historical" ? (
                  <>
                    <option value="BULK_DIRECT">BULK_DIRECT</option>
                    <option value="BULK_STAGE">BULK_STAGE</option>
                  </>
                ) : (
                  <>
                    <option value="CDC_DIRECT">CDC_DIRECT</option>
                    <option value="CDC_STAGE">CDC_STAGE</option>
                  </>
                )}
              </select>
            </Field>
            {usesStage && (
              <Field label="Stage tablespace">
                <input value={stageTablespace} onChange={e => setStageTablespace(e.target.value)} style={S.input}/>
              </Field>
            )}
          </Section>

          {mode === "cdc" && (
            <Section title="CDC-коннектор">
              {cdcGroupLoading ? (
                <div style={{
                  padding: "9px 10px",
                  border: `1px solid ${t.border.subtle}`,
                  borderRadius: t.radius.sm,
                  background: t.bg.s1,
                  color: t.text.muted,
                  fontSize: 12,
                }}>
                  Загружаю текущий состав CDC-коннектора...
                </div>
              ) : cdcGroupError ? (
                <div style={{
                  padding: "9px 10px",
                  border: `1px solid ${t.red.border}`,
                  borderRadius: t.radius.sm,
                  background: `${t.red.border}22`,
                  color: t.red.fg,
                  fontSize: 12,
                }}>
                  Не удалось загрузить состав CDC-коннектора: {cdcGroupError}
                </div>
              ) : cdcGroup ? (
                <div style={{
                  display: "grid",
                  gap: 8,
                  padding: "9px 10px",
                  border: `1px solid ${t.border.subtle}`,
                  borderRadius: t.radius.sm,
                  background: t.bg.s1,
                  fontSize: 12,
                  color: t.text.secondary,
                }}>
                  <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                    <span>Статус: <strong style={{ color: t.text.primary }}>{cdcGroup.status}</strong></span>
                    <span>Сейчас в Debezium: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{connectorTables.length}</strong></span>
                    <span>Добавится новых: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{connectorNewTables.length}</strong></span>
                    <span>Будет в table.include.list: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{projectedConnectorCount}</strong></span>
                  </div>
                  <div style={{
                    padding: "7px 8px",
                    borderRadius: t.radius.sm,
                    border: `1px solid ${connectorOtherTables.length ? t.amber.dim : t.blue.dim}`,
                    background: connectorOtherTables.length ? t.amber.bg : t.blue.bg,
                    color: connectorOtherTables.length ? t.amber.fg : t.blue.fg,
                    lineHeight: 1.45,
                  }}>
                    Debezium-коннектор один на всю CDC-пачку. После сохранения в Kafka Connect уйдет полный список,
                    не только выбранные сейчас таблицы.
                    <div style={{ marginTop: 4, fontFamily: t.font.mono, color: t.text.primary, overflowWrap: "anywhere" }}>
                      {projectedPreview.length > 0 ? projectedPreview.join(", ") : "пока нет таблиц"}
                      {projectedRest > 0 && <span style={{ color: t.text.muted }}> +{projectedRest} еще</span>}
                    </div>
                  </div>
                  {connectorSelectedTables.length > 0 && (
                    <div style={{ color: t.amber.fg }}>
                      Уже в коннекторе из выбранных: {connectorSelectedTables.map(cdcTableLabel).join(", ")}
                    </div>
                  )}
                  {connectorOtherTables.length > 0 && (
                    <div style={{ color: t.amber.fg }}>
                      Это не новый пустой коннектор: выбранные таблицы добавятся к уже существующим: {connectorOtherTables.map(cdcTableLabel).join(", ")}
                    </div>
                  )}
                  {connectorOtherTables.length > 0 && (
                    <div style={{
                      display: "grid",
                      gap: 4,
                      borderTop: `1px solid ${t.border.subtle}`,
                      paddingTop: 7,
                    }}>
                      {connectorOtherTables.map(table => {
                        const label = cdcTableLabel(table);
                        const rowBusy = cdcRemoveBusy === label;
                        return (
                          <div key={table.id || label} style={{
                            display: "grid",
                            gridTemplateColumns: "minmax(0, 1fr) auto",
                            gap: 8,
                            alignItems: "center",
                          }}>
                            <span style={{
                              fontFamily: t.font.mono,
                              color: t.text.primary,
                              overflow: "hidden",
                              textOverflow: "ellipsis",
                              whiteSpace: "nowrap",
                            }}>
                              {label}
                            </span>
                            <button
                              type="button"
                              onClick={() => removeExistingCdcTable(table)}
                              disabled={!!cdcRemoveBusy || busy}
                              style={{
                                ...secondaryActionStyle(false),
                                padding: "3px 8px",
                                fontSize: 11,
                                opacity: rowBusy ? 0.55 : 1,
                              }}
                            >
                              {rowBusy ? "Убираю..." : "Убрать"}
                            </button>
                          </div>
                        );
                      })}
                    </div>
                  )}
                  {cdcGroup.table_include_list && (
                    <div style={{
                      fontFamily: t.font.mono,
                      fontSize: 11,
                      color: t.text.muted,
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}>
                      table.include.list: {cdcGroup.table_include_list}
                    </div>
                  )}
                </div>
              ) : (
                <div style={{
                  padding: "9px 10px",
                  border: `1px solid ${t.blue.dim}`,
                  borderRadius: t.radius.sm,
                  background: t.blue.bg,
                  color: t.blue.fg,
                  fontSize: 12,
                }}>
                  CDC-коннектор еще не создан. Он будет создан автоматически для этой миграции.
                </div>
              )}
            </Section>
          )}

          {mode === "cdc" && (
            <Section title="Ключи CDC">
              <div style={{ display: "grid", gap: 8 }}>
                {infoLoading && (
                  <div style={{ fontSize: 12, color: t.text.muted }}>
                    Загружаю PK/UK и колонки выбранных таблиц...
                  </div>
                )}
                {!infoLoading && tables.map(table => {
                  const key = rowKey(table);
                  const info = tableInfos[key];
                  const selected = keyColumns[key] ?? [];
                  const supp = hasSuppLog(info);
                  if (!info) {
                    return (
                      <div key={key} style={{ fontSize: 12, color: t.text.muted }}>
                        {key}: metadata не загружена
                      </div>
                    );
                  }
                  if (info.error) {
                    return (
                      <div key={key} style={{
                        padding: "8px 10px",
                        borderRadius: t.radius.sm,
                        border: `1px solid ${t.red.border}`,
                        background: `${t.red.border}22`,
                        color: t.red.fg,
                        fontSize: 12,
                      }}>
                        {key}: {info.error}
                      </div>
                    );
                  }
                  if (info.pk_columns.length > 0) {
                    return (
                      <div key={key} style={{
                        padding: "8px 10px",
                        borderRadius: t.radius.sm,
                        border: `1px solid ${t.green.dim}`,
                        background: t.green.bg,
                        color: t.green.fg,
                        fontSize: 12,
                      }}>
                        <strong style={{ fontFamily: t.font.mono }}>{key}</strong>
                        <span style={{ marginLeft: 8 }}>PK: {info.pk_columns.join(", ")}</span>
                        <span style={{ marginLeft: 8, color: supp === false ? t.red.fg : t.green.fg }}>
                          {supp === false ? "NO SUPP" : supp === true ? "SUPP" : ""}
                        </span>
                      </div>
                    );
                  }
                  if (info.uk_constraints.length > 0) {
                    const uk = info.uk_constraints[0];
                    return (
                      <div key={key} style={{
                        padding: "8px 10px",
                        borderRadius: t.radius.sm,
                        border: `1px solid ${t.purple.base}`,
                        background: t.purple.bg,
                        color: t.purple.fg,
                        fontSize: 12,
                      }}>
                        <strong style={{ fontFamily: t.font.mono }}>{key}</strong>
                        <span style={{ marginLeft: 8 }}>UK: {uk.name} ({uk.columns.join(", ")})</span>
                        <span style={{ marginLeft: 8, color: supp === false ? t.red.fg : t.green.fg }}>
                          {supp === false ? "NO SUPP" : supp === true ? "SUPP" : ""}
                        </span>
                      </div>
                    );
                  }
                  return (
                    <div key={key} style={{
                      border: `1px solid ${selected.length ? t.blue.dim : t.red.border}`,
                      borderRadius: t.radius.sm,
                      background: t.bg.s1,
                      padding: 10,
                    }}>
                      <div style={{
                        display: "flex",
                        alignItems: "center",
                        gap: 8,
                        marginBottom: 8,
                        color: t.text.primary,
                        fontSize: 12,
                      }}>
                        <strong style={{ fontFamily: t.font.mono }}>{key}</strong>
                        <span style={{ color: selected.length ? t.blue.fg : t.red.fg }}>
                          PK нет, выберите CDC-ключ
                        </span>
                        {supp !== null && (
                          <span style={{ color: supp ? t.green.fg : t.red.fg }}>
                            {supp ? "SUPP" : "NO SUPP"}
                          </span>
                        )}
                      </div>

                      <div style={{
                        display: "grid",
                        gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
                        gap: 6,
                      }}>
                        {info.columns.map(col => {
                          const checked = selected.includes(col.name);
                          return (
                            <label key={col.name} style={{
                              display: "flex",
                              alignItems: "center",
                              gap: 6,
                              padding: "5px 7px",
                              borderRadius: t.radius.sm,
                              border: `1px solid ${checked ? t.blue.dim : t.border.subtle}`,
                              background: checked ? t.blue.bg : t.bg.s2,
                              color: checked ? t.blue.fg : t.text.secondary,
                              fontSize: 11.5,
                              fontFamily: t.font.mono,
                              minWidth: 0,
                            }}>
                              <input
                                type="checkbox"
                                checked={checked}
                                onChange={e => {
                                  setKeyColumns(prev => {
                                    const cur = prev[key] ?? [];
                                    const next = e.target.checked
                                      ? [...cur, col.name]
                                      : cur.filter(x => x !== col.name);
                                    return { ...prev, [key]: next };
                                  });
                                }}
                              />
                              <span style={{ overflow: "hidden", textOverflow: "ellipsis" }}>
                                {col.name}
                              </span>
                            </label>
                          );
                        })}
                      </div>
                    </div>
                  );
                })}
              </div>
            </Section>
          )}

          <Section title="Порядок и нагрузка">
            <label style={{ display: "flex", gap: 8, alignItems: "center", fontSize: 13, color: t.text.primary }}>
              <input
                type="checkbox"
                checked={mode === "cdc" ? true : sequential}
                disabled={mode === "cdc"}
                onChange={e => setSequential(e.target.checked)}
              />
              <span>Каждую таблицу отдельной позицией очереди</span>
            </label>
            <label style={{ display: "flex", gap: 8, alignItems: "center", fontSize: 13, color: t.text.primary }}>
              <input
                type="checkbox"
                checked={truncateTarget}
                disabled={usesStage}
                onChange={e => setTruncateTarget(e.target.checked)}
              />
              <span>TRUNCATE target перед загрузкой{usesStage ? " (обязательно для STAGE)" : ""}</span>
            </label>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 10 }}>
              <Field label="Chunk size">
                <input type="number" value={chunkSize} onChange={e => setChunkSize(Number(e.target.value) || 1_000_000)} style={S.input}/>
              </Field>
              <Field label="Workers">
                <input type="number" value={workers} onChange={e => setWorkers(Number(e.target.value) || 1)} style={S.input}/>
              </Field>
              <Field label="Baseline PD">
                <input type="number" value={baselinePd} onChange={e => setBaselinePd(Number(e.target.value) || 4)} style={S.input}/>
              </Field>
            </div>
          </Section>

          <Section title="Таблицы">
            <div style={{
              maxHeight: 180, overflowY: "auto",
              border: `1px solid ${t.border.subtle}`, borderRadius: t.radius.sm,
              fontFamily: t.font.mono, fontSize: 11.5,
            }}>
              {tables.map((x, i) => (
                <div key={`${x.source_table}-${i}`} style={{
                  padding: "5px 8px",
                  borderBottom: i === tables.length - 1 ? "none" : `1px solid ${t.bg.s2}`,
                  color: t.text.secondary,
                }}>
                  {x.source_schema}.{x.source_table} -&gt; {x.target_schema}.{x.target_table || x.source_table}
                </div>
              ))}
            </div>
          </Section>
        </div>

        <div style={S.footer}>
          <button onClick={onClose} disabled={busy} style={secondaryActionStyle(busy)}>Отмена</button>
          <button onClick={submit} disabled={submitDisabled} style={primaryActionStyle(submitDisabled)}>
            {busy ? "Добавление..." : mode === "cdc" ? cdcSubmitLabel : "Добавить в обычную пачку"}
          </button>
        </div>
      </div>
    </div>
  );
}
