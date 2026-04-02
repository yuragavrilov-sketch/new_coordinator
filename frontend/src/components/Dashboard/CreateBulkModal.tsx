import { useState, useEffect } from "react";
import { SearchSelect } from "../ui/SearchSelect";
import type { EnrichedTable } from "./TableList";

interface Props {
  schema: string;
  tables: string[];
  tablesMeta?: EnrichedTable[];
  createGroup?: boolean;
  onClose: () => void;
  onCreated: () => void;
}

function shortId() { return Math.random().toString(36).slice(2, 8); }

export function CreateBulkModal({ schema, tables, tablesMeta, createGroup, onClose, onCreated }: Props) {
  const isMulti = tables.length > 1;

  const [targetSchema, setTargetSchema] = useState(schema.toLowerCase());
  const [targetSchemas, setTargetSchemas] = useState<string[]>([]);
  const [migrationMode, setMigrationMode] = useState<"CDC" | "BULK_ONLY">(createGroup ? "CDC" : "BULK_ONLY");
  const [strategy, setStrategy] = useState<"STAGE" | "DIRECT">("STAGE");
  const [chunkSize, setChunkSize] = useState(500_000);
  const [maxWorkers, setMaxWorkers] = useState(10);
  const [baselineParallel, setBaselineParallel] = useState(10);
  const [stageTablespace, setStageTablespace] = useState("PAYSTAGE");
  const [migrationName, setMigrationName] = useState(
    isMulti ? `bulk-${schema.toLowerCase()}` : `${schema}.${tables[0]}`,
  );

  // Group fields (CDC mode)
  const id = shortId();
  const [groupName, setGroupName] = useState(`grp-${schema.toLowerCase()}-${id}`);
  const [connectorName, setConnectorName] = useState(`${schema.toLowerCase()}_${id}_connector`);
  const [topicPrefix, setTopicPrefix] = useState(`grp.${schema.toLowerCase()}.${id}`);

  // Load target schemas
  useEffect(() => {
    fetch("/api/db/target/schemas")
      .then(r => r.ok ? r.json() : [])
      .then((list: string[]) => {
        setTargetSchemas(list);
        if (list.length > 0 && !list.includes(targetSchema)) {
          // Keep current default if it looks valid
        }
      })
      .catch(() => {});
  }, []);

  const [sourceFilter, setSourceFilter] = useState("");

  const [creating, setCreating] = useState(false);
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState("");

  const _resolveKey = (table: string) => {
    const meta = tablesMeta?.find(
      t => t.object_name.toUpperCase() === table.toUpperCase(),
    )?.metadata;
    const pkCols = meta?.pk_columns ?? [];
    const ukConstraints = meta?.uk_constraints ?? [];
    const hasPk = pkCols.length > 0;
    const hasUk = ukConstraints.length > 0;
    let keyType = "NONE", keySource = "NONE", keyCols: string[] = [];
    if (hasPk) { keyType = "PRIMARY_KEY"; keySource = "PK"; keyCols = pkCols; }
    else if (hasUk) { keyType = "UNIQUE_KEY"; keySource = "UK"; keyCols = ukConstraints[0]?.columns ?? []; }
    return { hasPk, hasUk, keyType, keySource, keyCols };
  };

  const handleCreate = async () => {
    setCreating(true);
    setError("");
    setProgress(0);
    try {
      // CDC + group mode → use wizard API
      if (migrationMode === "CDC" && createGroup && isMulti) {
        const wizardTables = tables.map(table => {
          const k = _resolveKey(table);
          return {
            source_schema: schema.toUpperCase(),
            source_table: table.toUpperCase(),
            target_schema: targetSchema.toLowerCase(),
            target_table: table.toLowerCase(),
            effective_key_type: k.keyType,
            effective_key_columns: k.keyCols,
            source_pk_exists: k.hasPk,
            source_uk_exists: k.hasUk,
          };
        });
        const resp = await fetch("/api/connector-groups/wizard", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            group_name: groupName,
            connector_name: connectorName,
            topic_prefix: topicPrefix,
            source_connection_id: "oracle_source",
            tables: wizardTables,
          }),
        });
        if (!resp.ok) {
          const data = await resp.json().catch(() => ({ error: resp.statusText }));
          throw new Error(data.error || `HTTP ${resp.status}`);
        }
        // Group created — orchestrator will start Debezium connector.
        // Migrations are created later from Connector Groups page
        // once the connector is RUNNING.
        onCreated();
        return;
      }

      // Individual migrations (BULK_ONLY or single CDC)
      for (let i = 0; i < tables.length; i++) {
        const table = tables[i];
        const name = isMulti ? `${schema}.${table}` : migrationName;
        const k = _resolveKey(table);

        const resp = await fetch("/api/migrations", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            initial_phase: "NEW",
            migration_name: name,
            source_connection_id: "oracle_source",
            target_connection_id: "oracle_target",
            source_schema: schema.toUpperCase(),
            source_table: table.toUpperCase(),
            target_schema: targetSchema.toLowerCase(),
            target_table: table.toLowerCase(),
            stage_table_name: strategy === "STAGE" ? `STG_${table.toUpperCase()}` : "",
            stage_tablespace: strategy === "STAGE" ? stageTablespace : "",
            migration_strategy: strategy,
            migration_mode: migrationMode,
            chunk_size: chunkSize,
            max_parallel_workers: maxWorkers,
            baseline_parallel_degree: baselineParallel,
            source_pk_exists: k.hasPk,
            source_uk_exists: k.hasUk,
            effective_key_type: k.keyType,
            effective_key_source: k.keySource,
            effective_key_columns_json: JSON.stringify(k.keyCols),
            ...(sourceFilter.trim() ? { source_filter: sourceFilter.trim() } : {}),
          }),
        });
        if (!resp.ok) {
          const data = await resp.json().catch(() => ({ error: resp.statusText }));
          throw new Error(data.error || `HTTP ${resp.status}`);
        }
        setProgress(i + 1);
      }
      onCreated();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setCreating(false);
    }
  };

  return (
    <div
      onClick={(e) => { if (e.target === e.currentTarget && !creating) onClose(); }}
      style={S.overlay}
    >
      <div style={S.modal}>
        {/* Header */}
        <div style={S.header}>
          <span style={{ fontSize: 15, fontWeight: 700, color: "#e2e8f0" }}>
            {createGroup
              ? `Группа + миграции (${tables.length} таблиц)`
              : isMulti ? `Миграция (${tables.length} таблиц)` : "Миграция"}
          </span>
          <span style={{ flex: 1 }} />
          <button onClick={() => !creating && onClose()} style={S.closeBtn}>✕</button>
        </div>

        {/* Body */}
        <div style={S.body}>
          {/* Source */}
          <Section title="Источник (Oracle Source)" accent="#1d4ed8">
            <Field label="Schema">
              <div style={S.readOnly}>{schema}</div>
            </Field>
            {isMulti ? (
              <Field label={`Таблицы (${tables.length})`}>
                <div style={{ ...S.readOnly, maxHeight: 80, overflowY: "auto", fontFamily: "monospace", fontSize: 12 }}>
                  {tables.join(", ")}
                </div>
              </Field>
            ) : (
              <Field label="Table">
                <div style={S.readOnly}>{tables[0]}</div>
              </Field>
            )}
          </Section>

          {/* Target */}
          <Section title="Цель (Oracle Target)" accent="#047857">
            <Field label="Target schema">
              {targetSchemas.length > 0 ? (
                <SearchSelect
                  value={targetSchema}
                  onChange={setTargetSchema}
                  options={targetSchemas}
                  placeholder="Выберите схему..."
                  showClear={false}
                />
              ) : (
                <input value={targetSchema} onChange={e => setTargetSchema(e.target.value)}
                  style={S.input} placeholder="Схема на target" />
              )}
            </Field>
            {!isMulti && (
              <Field label="Target table">
                <div style={S.readOnly}>{tables[0].toLowerCase()}</div>
              </Field>
            )}
          </Section>

          {/* Mode */}
          <Section title="Режим миграции" accent="#7c3aed">
            <Field label="Тип">
              <div style={{ display: "flex", gap: 8 }}>
                <ToggleBtn
                  label="Разовая переливка"
                  hint="Однократный перенос данных без CDC"
                  active={migrationMode === "BULK_ONLY"}
                  activeColor="#6ee7b7"
                  activeBg="#064e3b"
                  onClick={() => setMigrationMode("BULK_ONLY")}
                />
                <ToggleBtn
                  label="CDC (Debezium)"
                  hint="Bulk + отслеживание изменений"
                  active={migrationMode === "CDC"}
                  activeColor="#c4b5fd"
                  activeBg="#2e1065"
                  onClick={() => setMigrationMode("CDC")}
                />
              </div>
            </Field>
          </Section>

          {/* Group params (CDC only) */}
          {migrationMode === "CDC" && createGroup && (
            <Section title="Группа коннектора" accent="#f59e0b">
              <Field label="Имя группы">
                <input value={groupName} onChange={e => setGroupName(e.target.value)} style={S.input} />
              </Field>
              <div style={S.row2}>
                <Field label="Connector name">
                  <input value={connectorName} onChange={e => setConnectorName(e.target.value)} style={S.input} />
                </Field>
                <Field label="Topic prefix">
                  <input value={topicPrefix} onChange={e => setTopicPrefix(e.target.value)} style={S.input} />
                </Field>
              </div>
            </Section>
          )}

          {/* Migration params */}
          <Section title="Параметры миграции">
            {!isMulti && (
              <Field label="Имя миграции">
                <input value={migrationName} onChange={e => setMigrationName(e.target.value)}
                  style={S.input} />
              </Field>
            )}

            <Field label="Стратегия загрузки">
              <div style={{ display: "flex", gap: 8 }}>
                <ToggleBtn
                  label="STAGE"
                  hint="Через промежуточную таблицу"
                  active={strategy === "STAGE"}
                  activeColor="#3b82f6"
                  activeBg="#1e3a5f"
                  onClick={() => setStrategy("STAGE")}
                />
                <ToggleBtn
                  label="DIRECT"
                  hint="Прямая загрузка"
                  active={strategy === "DIRECT"}
                  activeColor="#6ee7b7"
                  activeBg="#064e3b"
                  onClick={() => setStrategy("DIRECT")}
                />
              </div>
              <div style={{ fontSize: 10, color: "#475569", marginTop: 4 }}>
                {strategy === "STAGE"
                  ? "Загрузка через stage-таблицу → валидация → публикация в целевую"
                  : "Прямая загрузка в целевую таблицу, без stage"}
              </div>
            </Field>

            {strategy === "STAGE" && (
              <div style={S.row2}>
                <Field label="Stage table name" hint={`По умолчанию: STG_${isMulti ? "{TABLE}" : tables[0].toUpperCase()}`}>
                  <div style={S.readOnly}>STG_{isMulti ? "{TABLE}" : tables[0].toUpperCase()}</div>
                </Field>
                <Field label="Stage tablespace">
                  <input value={stageTablespace} onChange={e => setStageTablespace(e.target.value)}
                    style={S.input} placeholder="PAYSTAGE" />
                </Field>
              </div>
            )}

            <Field label="WHERE фильтр" hint="Условие для выборки строк из source (опционально)">
              <input
                value={sourceFilter}
                onChange={e => setSourceFilter(e.target.value)}
                style={{ ...S.input, fontFamily: "monospace", fontSize: 12 }}
                placeholder='например: STATUS = 1 AND CREATED_AT > DATE "2024-01-01"'
              />
              {sourceFilter.trim() && (
                <div style={{ fontSize: 10, color: "#f59e0b", marginTop: 3 }}>
                  SELECT * FROM {schema}.{tables[0]} WHERE ... AND ({sourceFilter.trim()})
                </div>
              )}
            </Field>

            <div style={S.row2}>
              <Field label="Chunk size" hint="Строк на чанк (500k–2M)">
                <input type="number" value={chunkSize} min={1}
                  onChange={e => setChunkSize(parseInt(e.target.value) || 500_000)}
                  style={S.input} />
              </Field>
              <Field label="Воркеры (bulk)" hint="Параллельных воркеров">
                <input type="number" value={maxWorkers} min={1}
                  onChange={e => setMaxWorkers(Math.max(1, parseInt(e.target.value) || 10))}
                  style={S.input} />
              </Field>
              {strategy === "STAGE" && (
                <Field label="Воркеры (baseline)" hint="Параллельных baseline-воркеров">
                  <input type="number" value={baselineParallel} min={1}
                    onChange={e => setBaselineParallel(Math.max(1, parseInt(e.target.value) || 10))}
                    style={S.input} />
                </Field>
              )}
            </div>
          </Section>

          {/* Error */}
          {error && <div style={S.err}>{error}</div>}

          {/* Progress */}
          {creating && isMulti && (
            <div style={{ fontSize: 13, color: "#94a3b8", marginBottom: 8 }}>
              Создано {progress} / {tables.length}…
            </div>
          )}
        </div>

        {/* Footer */}
        <div style={S.footer}>
          <button onClick={onClose} disabled={creating} style={S.btnSecondary}>
            Отмена
          </button>
          <button onClick={handleCreate} disabled={creating} style={S.btnPrimary}>
            {creating ? "Создание…" : isMulti ? `Создать ${tables.length} миграций` : "Создать и запустить"}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── UI primitives (matching CreateMigrationModal style) ──────────────────────

function Section({ title, accent, children }: { title: string; accent?: string; children: React.ReactNode }) {
  return (
    <div style={{ marginBottom: 18 }}>
      <div style={{
        fontSize: 11, fontWeight: 700, textTransform: "uppercase", letterSpacing: 0.5,
        color: accent || "#64748b", marginBottom: 8,
        borderBottom: `1px solid ${accent || "#334155"}22`, paddingBottom: 4,
      }}>
        {title}
      </div>
      {children}
    </div>
  );
}

function Field({ label, hint, children }: { label: string; hint?: string; children: React.ReactNode }) {
  return (
    <div style={{ marginBottom: 8 }}>
      <div style={{ fontSize: 11, color: "#94a3b8", fontWeight: 600, marginBottom: 3 }}>
        {label}
      </div>
      {children}
      {hint && <div style={{ fontSize: 10, color: "#475569", marginTop: 2 }}>{hint}</div>}
    </div>
  );
}

function ToggleBtn({ label, hint, active, activeColor, activeBg, onClick }: {
  label: string; hint: string; active: boolean;
  activeColor: string; activeBg: string; onClick: () => void;
}) {
  return (
    <button onClick={onClick} style={{
      flex: 1, padding: "8px 12px", borderRadius: 6, cursor: "pointer",
      border: `1px solid ${active ? activeColor : "#334155"}`,
      background: active ? activeBg : "#1e293b",
      color: active ? activeColor : "#64748b",
      fontWeight: 700, fontSize: 12, textAlign: "left",
    }}>
      <div>{label}</div>
      <div style={{ fontSize: 10, fontWeight: 400, marginTop: 2, opacity: 0.7 }}>{hint}</div>
    </button>
  );
}

// ── Style tokens ─────────────────────────────────────────────────────────────

const S = {
  overlay: {
    position: "fixed" as const, inset: 0, background: "rgba(0,0,0,.72)",
    display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1000,
  },
  modal: {
    background: "#0f172a", border: "1px solid #1e293b", borderRadius: 12,
    width: 560, maxHeight: "90vh", display: "flex", flexDirection: "column" as const,
    boxShadow: "0 20px 60px rgba(0,0,0,.6)",
  },
  header: {
    display: "flex", alignItems: "center", padding: "14px 20px",
    borderBottom: "1px solid #1e293b",
  },
  closeBtn: {
    background: "none", border: "none", color: "#475569",
    cursor: "pointer", fontSize: 20, lineHeight: 1, padding: "0 2px",
  } as React.CSSProperties,
  body: {
    padding: "16px 20px", overflowY: "auto" as const, flex: 1,
  },
  footer: {
    display: "flex", gap: 8, justifyContent: "flex-end",
    padding: "12px 20px", borderTop: "1px solid #1e293b",
  },
  input: {
    width: "100%", background: "#1e293b", border: "1px solid #334155",
    borderRadius: 6, color: "#e2e8f0", padding: "7px 10px", fontSize: 13,
  } as React.CSSProperties,
  readOnly: {
    padding: "7px 10px", background: "#1e293b", borderRadius: 6,
    border: "1px solid #334155", color: "#94a3b8", fontSize: 13,
  } as React.CSSProperties,
  row2: {
    display: "flex", gap: 12,
  } as React.CSSProperties,
  err: {
    background: "#450a0a", color: "#fca5a5", padding: "8px 12px",
    borderRadius: 6, fontSize: 13, marginBottom: 12,
  } as React.CSSProperties,
  btnSecondary: {
    padding: "8px 20px", borderRadius: 6, border: "1px solid #334155",
    background: "#1e293b", color: "#94a3b8", cursor: "pointer",
    fontSize: 13, fontWeight: 600,
  } as React.CSSProperties,
  btnPrimary: {
    padding: "8px 20px", borderRadius: 6, border: "1px solid #3b82f6",
    background: "#3b82f6", color: "#fff", cursor: "pointer",
    fontSize: 13, fontWeight: 600,
  } as React.CSSProperties,
};
