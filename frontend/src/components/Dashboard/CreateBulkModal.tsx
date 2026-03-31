import { useState } from "react";

interface Props {
  schema: string;
  tables: string[];  // one or more table names
  onClose: () => void;
  onCreated: () => void;
}

export function CreateBulkModal({ schema, tables, onClose, onCreated }: Props) {
  // Target defaults
  const [targetSchema, setTargetSchema] = useState(schema.toLowerCase());

  // Load parameters
  const [strategy, setStrategy] = useState<"STAGE" | "DIRECT">("STAGE");
  const [chunkSize, setChunkSize] = useState(500_000);
  const [maxWorkers, setMaxWorkers] = useState(10);
  const [baselineParallel, setBaselineParallel] = useState(10);
  const [stageTablespace, setStageTablespace] = useState("PAYSTAGE");

  // State
  const [creating, setCreating] = useState(false);
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState("");

  const handleCreate = async () => {
    setCreating(true);
    setError("");
    setProgress(0);

    try {
      for (let i = 0; i < tables.length; i++) {
        const table = tables[i];
        const resp = await fetch("/api/migrations", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            migration_name: `${schema}.${table}`,
            source_connection_id: "oracle_source",
            target_connection_id: "oracle_target",
            source_schema: schema.toUpperCase(),
            source_table: table.toUpperCase(),
            target_schema: targetSchema.toLowerCase(),
            target_table: table.toLowerCase(),
            stage_table_name: strategy === "STAGE" ? `STG_${table.toUpperCase()}` : "",
            stage_tablespace: strategy === "STAGE" ? stageTablespace : "",
            migration_strategy: strategy,
            migration_mode: "BULK_ONLY",
            chunk_size: chunkSize,
            max_parallel_workers: maxWorkers,
            baseline_parallel_degree: baselineParallel,
          }),
        });
        if (!resp.ok) {
          const data = await resp.json().catch(() => ({ error: resp.statusText }));
          throw new Error(data.error || `HTTP ${resp.status}`);
        }
        setProgress(i + 1);
      }
      onCreated();
    } catch (e: any) {
      setError(e.message || String(e));
    } finally {
      setCreating(false);
    }
  };

  const isMulti = tables.length > 1;

  return (
    <div
      onClick={(e) => { if (e.target === e.currentTarget && !creating) onClose(); }}
      style={{
        position: "fixed", inset: 0, background: "rgba(0,0,0,0.6)",
        display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1000,
      }}
    >
      <div style={{
        background: "#1e293b", border: "1px solid #334155", borderRadius: 10,
        padding: 24, width: 520, maxHeight: "85vh", overflowY: "auto", color: "#e2e8f0",
      }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
          <h3 style={{ margin: 0, fontSize: 16 }}>
            {isMulti ? `Создание миграций (${tables.length})` : `Миграция: ${tables[0]}`}
          </h3>
          <span onClick={() => !creating && onClose()} style={{ cursor: "pointer", color: "#64748b", fontSize: 18 }}>✕</span>
        </div>

        {/* Source */}
        <Section title="Source">
          <Row label="Schema">{schema}</Row>
          {isMulti ? (
            <Row label={`Таблицы (${tables.length})`}>
              <div style={{ maxHeight: 80, overflowY: "auto", fontSize: 12, fontFamily: "monospace" }}>
                {tables.join(", ")}
              </div>
            </Row>
          ) : (
            <Row label="Table">{tables[0]}</Row>
          )}
        </Section>

        {/* Target */}
        <Section title="Target">
          <Row label="Schema">
            <input value={targetSchema} onChange={e => setTargetSchema(e.target.value)}
              style={inputStyle} />
          </Row>
          {!isMulti && (
            <Row label="Table">
              <span style={{ fontSize: 13, color: "#94a3b8" }}>{tables[0].toLowerCase()}</span>
            </Row>
          )}
        </Section>

        {/* Load Parameters */}
        <Section title="Параметры загрузки">
          <Row label="Стратегия">
            <div style={{ display: "flex", gap: 8 }}>
              {(["STAGE", "DIRECT"] as const).map(s => (
                <label key={s} style={{ display: "flex", alignItems: "center", gap: 4, cursor: "pointer", fontSize: 13 }}>
                  <input type="radio" checked={strategy === s} onChange={() => setStrategy(s)} />
                  {s}
                </label>
              ))}
            </div>
          </Row>
          <Row label="Chunk size">
            <input type="number" value={chunkSize} onChange={e => setChunkSize(Number(e.target.value) || 500_000)}
              style={{ ...inputStyle, width: 120 }} />
          </Row>
          <Row label="Max workers">
            <input type="number" value={maxWorkers} onChange={e => setMaxWorkers(Number(e.target.value) || 10)}
              min={1} style={{ ...inputStyle, width: 80 }} />
          </Row>
          {strategy === "STAGE" && (
            <>
              <Row label="Baseline parallel">
                <input type="number" value={baselineParallel} onChange={e => setBaselineParallel(Number(e.target.value) || 10)}
                  min={1} style={{ ...inputStyle, width: 80 }} />
              </Row>
              <Row label="Stage tablespace">
                <input value={stageTablespace} onChange={e => setStageTablespace(e.target.value)}
                  style={{ ...inputStyle, width: 160 }} />
              </Row>
            </>
          )}
        </Section>

        {/* Error */}
        {error && (
          <div style={{ background: "#450a0a", color: "#fca5a5", padding: "8px 12px", borderRadius: 6, fontSize: 13, marginBottom: 12 }}>
            {error}
          </div>
        )}

        {/* Progress */}
        {creating && isMulti && (
          <div style={{ marginBottom: 12, fontSize: 13, color: "#94a3b8" }}>
            Создано {progress} / {tables.length}...
          </div>
        )}

        {/* Buttons */}
        <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
          <button onClick={onClose} disabled={creating}
            style={{ ...btnStyle, background: "#334155" }}>
            Отмена
          </button>
          <button onClick={handleCreate} disabled={creating}
            style={{ ...btnStyle, background: "#3b82f6", color: "#fff" }}>
            {creating ? "Создание..." : "Создать"}
          </button>
        </div>
      </div>
    </div>
  );
}

// --- Helper components ---

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div style={{ marginBottom: 16 }}>
      <div style={{ fontSize: 11, fontWeight: 700, textTransform: "uppercase", color: "#64748b", marginBottom: 8, letterSpacing: 0.5 }}>
        {title}
      </div>
      {children}
    </div>
  );
}

function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 6, fontSize: 13 }}>
      <span style={{ width: 140, color: "#94a3b8", flexShrink: 0 }}>{label}</span>
      <div style={{ flex: 1 }}>{children}</div>
    </div>
  );
}

const inputStyle: React.CSSProperties = {
  background: "#0f172a",
  border: "1px solid #334155",
  borderRadius: 4,
  color: "#e2e8f0",
  padding: "5px 10px",
  fontSize: 13,
  width: "100%",
};

const btnStyle: React.CSSProperties = {
  padding: "8px 20px",
  borderRadius: 6,
  border: "1px solid #334155",
  color: "#e2e8f0",
  cursor: "pointer",
  fontSize: 13,
  fontWeight: 600,
};
