import React, { useEffect, useState } from "react";
import { t } from "../theme";
import { S } from "../components/CreateMigrationModal/styles";
import { Section, Field } from "../components/CreateMigrationModal/ui";
import { primaryActionStyle, secondaryActionStyle } from "./buttonStyles";
import { addSchemaPlanItems, type AddPlanItemsPayload } from "./api";
import type { ConnectorGroup } from "../types/migration";

interface BulkTable {
  source_schema: string;
  source_table:  string;
  target_schema: string;
  target_table?: string;
}

interface Props {
  schemaMigrationId: string;
  tables: BulkTable[];
  onClose: () => void;
  onDone: (planId: number, count: number) => void;
}

export function AddToPlanModal({ schemaMigrationId, tables, onClose, onDone }: Props) {
  const [mode, setMode] = useState<"historical" | "cdc">("historical");
  const [strategy, setStrategy] = useState<AddPlanItemsPayload["strategy"]>("BULK_DIRECT");
  const [connectorGroupId, setConnectorGroupId] = useState("");
  const [sequential, setSequential] = useState(true);
  const [truncateTarget, setTruncateTarget] = useState(true);
  const [chunkSize, setChunkSize] = useState(1_000_000);
  const [workers, setWorkers] = useState(1);
  const [baselinePd, setBaselinePd] = useState(4);
  const [stageTablespace, setStageTablespace] = useState("PAYSTAGE");
  const [groups, setGroups] = useState<ConnectorGroup[]>([]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");

  const usesStage = strategy.endsWith("_STAGE");
  const runningGroups = groups.filter(g => g.status === "RUNNING");

  useEffect(() => {
    fetch("/api/connector-groups")
      .then(r => r.ok ? r.json() : [])
      .then((data: ConnectorGroup[]) => setGroups(data))
      .catch(() => {});
  }, []);

  useEffect(() => {
    if (mode !== "cdc") return;
    if (!connectorGroupId && runningGroups.length > 0) {
      setConnectorGroupId(runningGroups[0].group_id);
    }
  }, [mode, connectorGroupId, runningGroups]);

  useEffect(() => {
    if (usesStage && !truncateTarget) setTruncateTarget(true);
  }, [usesStage, truncateTarget]);

  function setPackMode(next: "historical" | "cdc") {
    setMode(next);
    setErr("");
    if (next === "historical") {
      setStrategy("BULK_DIRECT");
      setWorkers(1);
      setConnectorGroupId("");
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
      if (mode === "cdc" && !connectorGroupId) {
        setErr("Для CDC-пачки выберите RUNNING CDC-пачку.");
        setBusy(false);
        return;
      }
      const payload: AddPlanItemsPayload = {
        tables: tables.map(t => ({
          source_table: t.source_table,
          target_table: t.target_table || t.source_table,
        })),
        strategy,
        connector_group_id: mode === "cdc" ? connectorGroupId : undefined,
        sequential,
        truncate_target: truncateTarget,
        chunk_size: chunkSize,
        max_parallel_workers: workers,
        baseline_parallel_degree: baselinePd,
        stage_tablespace: usesStage ? stageTablespace.trim().toUpperCase() : undefined,
      };
      const res = await addSchemaPlanItems(schemaMigrationId, payload);
      onDone(res.plan_id, res.items.length);
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
            Добавить в пачку таблиц
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
                Исторические без CDC
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
                CDC пачка
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
                ? "CDC-пачка создаёт DRAFT-миграции с выбранной CDC-пачкой. После старта batch coordinator запустит bulk и CDC apply по обычному CDC flow."
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
            {mode === "cdc" && (
              <Field label="CDC-пачка (RUNNING)" required>
                <select
                  value={connectorGroupId}
                  onChange={e => setConnectorGroupId(e.target.value)}
                  style={S.select}
                  disabled={runningGroups.length === 0}
                >
                  {runningGroups.length === 0 ? (
                    <option value="">нет запущенных CDC-пачек</option>
                  ) : runningGroups.map(g => (
                    <option key={g.group_id} value={g.group_id}>
                      {g.group_name} · {g.topic_prefix}
                    </option>
                  ))}
                </select>
              </Field>
            )}
            {usesStage && (
              <Field label="Stage tablespace">
                <input value={stageTablespace} onChange={e => setStageTablespace(e.target.value)} style={S.input}/>
              </Field>
            )}
          </Section>

          <Section title="Порядок и нагрузка">
            <label style={{ display: "flex", gap: 8, alignItems: "center", fontSize: 13, color: t.text.primary }}>
              <input type="checkbox" checked={sequential} onChange={e => setSequential(e.target.checked)} />
              <span>Каждую таблицу отдельным batch, переносить по очереди</span>
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
          <button onClick={submit} disabled={busy} style={primaryActionStyle(busy)}>
            {busy ? "Добавление..." : "Добавить в пачку"}
          </button>
        </div>
      </div>
    </div>
  );
}
