import React, { useEffect, useMemo, useState } from "react";
import { t } from "../theme";
import { S } from "../components/CreateMigrationModal/styles";
import { Section, Field } from "../components/CreateMigrationModal/ui";
import { primaryActionStyle, secondaryActionStyle } from "./buttonStyles";
import { addSchemaPlanItems, type AddPlanItemsPayload, type AddPlanItemsResp } from "./api";

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
  error?:         string;
}

interface Props {
  schemaMigrationId: string;
  tables: BulkTable[];
  initialMode?: "historical" | "cdc";
  onClose: () => void;
  onDone: (planId: number, count: number, response: AddPlanItemsResp) => void | Promise<void>;
}

export function AddToPlanModal({ schemaMigrationId, tables, initialMode = "historical", onClose, onDone }: Props) {
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
  const [err, setErr] = useState("");

  const usesStage = strategy.endsWith("_STAGE");
  const tablesKey = useMemo(
    () => tables.map(x => `${x.source_schema}.${x.source_table}`).join("|"),
    [tables],
  );

  function rowKey(x: BulkTable) {
    return `${x.source_schema.toUpperCase()}.${x.source_table.toUpperCase()}`;
  }

  useEffect(() => {
    setPackMode(initialMode);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialMode]);

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
    }
  }

  async function submit() {
    if (busy) return;
    setBusy(true);
    setErr("");
    try {
      if (mode === "cdc") {
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
          if (info.pk_columns.length === 0 && (keyColumns[key] ?? []).length === 0) {
            setErr(`Для ${key} без PK нужно выбрать колонки CDC-ключа.`);
            return;
          }
        }
      }

      const payload: AddPlanItemsPayload = {
        tables: tables.map(t => ({
          source_table: t.source_table,
          target_table: t.target_table || t.source_table,
          key_columns: mode === "cdc" && (tableInfos[rowKey(t)]?.pk_columns.length ?? 0) === 0
            ? keyColumns[rowKey(t)] ?? []
            : undefined,
        })),
        strategy,
        sequential,
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
                      </div>

                      {info.uk_constraints.length > 0 && (
                        <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginBottom: 8 }}>
                          {info.uk_constraints.map(uk => (
                            <button
                              key={uk.name}
                              type="button"
                              onClick={() => setKeyColumns(prev => ({ ...prev, [key]: uk.columns }))}
                              style={{
                                ...secondaryActionStyle(false),
                                padding: "4px 7px",
                                fontSize: 11,
                                minHeight: 0,
                              }}
                            >
                              {uk.name}: {uk.columns.join(", ")}
                            </button>
                          ))}
                        </div>
                      )}

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
              <input type="checkbox" checked={sequential} onChange={e => setSequential(e.target.checked)} />
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
          <button onClick={submit} disabled={busy || (mode === "cdc" && infoLoading)} style={primaryActionStyle(busy || (mode === "cdc" && infoLoading))}>
            {busy ? "Добавление..." : mode === "cdc" ? "Добавить в CDC-коннектор" : "Добавить в обычную пачку"}
          </button>
        </div>
      </div>
    </div>
  );
}
