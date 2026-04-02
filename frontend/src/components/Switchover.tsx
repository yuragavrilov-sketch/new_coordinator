import { useState, useEffect, useCallback } from "react";
import { SearchSelect } from "./ui/SearchSelect";

interface SwitchoverStatus {
  schema: string;
  disabled_fks: { table: string; constraint: string }[];
  disabled_triggers: { table: string; trigger: string }[];
  identity_cols: { table: string; column: string; generation_type: string }[];
  unusable_indexes: { table: string; index: string; type: string }[];
  disabled_constraints: { table: string; constraint: string; type: string }[];
  nologging_tables: string[];
}

interface ActionResult {
  status: "ok" | "error";
  error?: string;
  [key: string]: unknown;
}

type CheckKey = "fks" | "triggers" | "identity" | "indexes" | "constraints" | "logging";

const CHECKS: { key: CheckKey; label: string; actionLabel: string; endpoint: string; statusKey: keyof SwitchoverStatus; countFn: (s: SwitchoverStatus) => number }[] = [
  { key: "constraints", label: "Disabled PK/UK/CHECK", actionLabel: "Включить", endpoint: "/api/switchover/enable-constraints", statusKey: "disabled_constraints", countFn: s => s.disabled_constraints.length },
  { key: "indexes", label: "Unusable индексы", actionLabel: "Rebuild", endpoint: "/api/switchover/rebuild-indexes", statusKey: "unusable_indexes", countFn: s => s.unusable_indexes.length },
  { key: "fks", label: "Disabled FK", actionLabel: "Включить FK", endpoint: "/api/switchover/enable-fks", statusKey: "disabled_fks", countFn: s => s.disabled_fks.length },
  { key: "triggers", label: "Disabled триггеры", actionLabel: "Включить триггеры", endpoint: "/api/switchover/enable-triggers", statusKey: "disabled_triggers", countFn: s => s.disabled_triggers.length },
  { key: "identity", label: "Identity BY DEFAULT", actionLabel: "Восстановить ALWAYS", endpoint: "/api/switchover/restore-identity", statusKey: "identity_cols", countFn: s => s.identity_cols.filter(c => c.generation_type !== "ALWAYS").length },
  { key: "logging", label: "NOLOGGING таблицы", actionLabel: "Включить LOGGING", endpoint: "/api/switchover/set-logging", statusKey: "nologging_tables", countFn: s => s.nologging_tables.length },
];

export function Switchover() {
  const [schemas, setSchemas] = useState<string[]>([]);
  const [schema, setSchema] = useState("");
  const [status, setStatus] = useState<SwitchoverStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState<CheckKey | null>(null);
  const [results, setResults] = useState<Record<string, ActionResult[]>>({});
  const [expanded, setExpanded] = useState<CheckKey | null>(null);

  useEffect(() => {
    fetch("/api/db/target/schemas").then(r => r.ok ? r.json() : []).then(setSchemas).catch(() => {});
  }, []);

  const loadStatus = useCallback(() => {
    if (!schema) return;
    setLoading(true);
    setError(null);
    setResults({});
    fetch(`/api/switchover/status?schema=${schema}`)
      .then(r => r.ok ? r.json() : r.json().then(d => Promise.reject(d.error)))
      .then(setStatus)
      .catch(e => setError(String(e)))
      .finally(() => setLoading(false));
  }, [schema]);

  useEffect(() => { if (schema) loadStatus(); }, [schema, loadStatus]);

  const handleAction = async (check: typeof CHECKS[number]) => {
    setRunning(check.key);
    try {
      const r = await fetch(check.endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ schema }),
      });
      const data = await r.json();
      setResults(prev => ({ ...prev, [check.key]: Array.isArray(data) ? data : [] }));
      loadStatus(); // refresh
    } catch (e: any) {
      setResults(prev => ({ ...prev, [check.key]: [{ status: "error", error: e.message }] }));
    } finally {
      setRunning(null);
    }
  };

  const handleFixAll = async () => {
    if (!status) return;
    for (const check of CHECKS) {
      if (check.countFn(status) > 0) {
        await handleAction(check);
      }
    }
  };

  const totalIssues = status ? CHECKS.reduce((sum, c) => sum + c.countFn(status), 0) : 0;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* Toolbar */}
      <div style={{
        display: "flex", alignItems: "center", gap: 12,
        background: "#0f172a", border: "1px solid #1e293b", borderRadius: 8,
        padding: "12px 16px",
      }}>
        <span style={{ fontSize: 12, color: "#94a3b8" }}>Target schema:</span>
        <SearchSelect value={schema} onChange={setSchema} options={schemas} placeholder="Выберите..." />
        <button onClick={loadStatus} disabled={!schema || loading} style={btn("#1e293b")}>
          {loading ? "..." : "Проверить"}
        </button>
        {status && totalIssues > 0 && (
          <button
            onClick={handleFixAll}
            disabled={running !== null}
            style={{ ...btn("#1d4ed8"), color: "#e2e8f0", fontWeight: 600 }}
          >
            Исправить все ({totalIssues})
          </button>
        )}
        {status && totalIssues === 0 && (
          <span style={{ fontSize: 13, fontWeight: 700, color: "#4ade80", marginLeft: 8 }}>
            Готово к переключению
          </span>
        )}
      </div>

      {error && (
        <div style={{ background: "#7f1d1d", color: "#fca5a5", padding: "8px 12px", borderRadius: 6, fontSize: 12 }}>
          {error}
        </div>
      )}

      {/* Checks */}
      {status && (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {CHECKS.map(check => {
            const count = check.countFn(status);
            const isOk = count === 0;
            const isRunning = running === check.key;
            const actionResults = results[check.key];
            const isExpanded = expanded === check.key;

            return (
              <div key={check.key} style={{
                background: "#0f172a", border: `1px solid ${isOk ? "#166534" : "#78350f"}`,
                borderRadius: 8, overflow: "hidden",
              }}>
                <div
                  style={{
                    display: "flex", alignItems: "center", gap: 12,
                    padding: "10px 16px", cursor: count > 0 ? "pointer" : "default",
                  }}
                  onClick={() => count > 0 && setExpanded(isExpanded ? null : check.key)}
                >
                  {/* Status dot */}
                  <span style={{
                    width: 10, height: 10, borderRadius: "50%", flexShrink: 0,
                    background: isOk ? "#4ade80" : "#f59e0b",
                  }} />

                  {/* Label */}
                  <span style={{ fontSize: 13, color: "#e2e8f0", fontWeight: 600, flex: 1 }}>
                    {check.label}
                  </span>

                  {/* Count */}
                  <span style={{
                    fontSize: 12, fontWeight: 700,
                    color: isOk ? "#4ade80" : "#f59e0b",
                    minWidth: 40,
                  }}>
                    {isOk ? "OK" : count}
                  </span>

                  {/* Action */}
                  {!isOk && (
                    <button
                      onClick={(e) => { e.stopPropagation(); handleAction(check); }}
                      disabled={isRunning}
                      style={{ ...btn("#1e293b"), color: "#93c5fd", fontWeight: 600, fontSize: 11 }}
                    >
                      {isRunning ? "..." : check.actionLabel}
                    </button>
                  )}

                  {/* Expand arrow */}
                  {count > 0 && (
                    <span style={{ color: "#64748b", fontSize: 10 }}>
                      {isExpanded ? "\u25B2" : "\u25BC"}
                    </span>
                  )}
                </div>

                {/* Action results */}
                {actionResults && actionResults.length > 0 && (
                  <div style={{ padding: "0 16px 8px", fontSize: 11 }}>
                    {actionResults.map((r, i) => (
                      <div key={i} style={{
                        color: r.status === "ok" ? "#4ade80" : "#fca5a5",
                        marginBottom: 1,
                      }}>
                        {r.status === "ok" ? "\u2713" : "\u2717"}{" "}
                        {Object.entries(r)
                          .filter(([k]) => k !== "status" && k !== "error")
                          .map(([, v]) => String(v))
                          .join(" ")}
                        {r.error && <span style={{ color: "#fca5a5" }}> — {r.error}</span>}
                      </div>
                    ))}
                  </div>
                )}

                {/* Expanded detail */}
                {isExpanded && (
                  <div style={{
                    borderTop: "1px solid #1e293b", padding: "8px 16px",
                    maxHeight: 250, overflowY: "auto",
                  }}>
                    <DetailTable checkKey={check.key} status={status} />
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}

      {!schema && (
        <div style={{ color: "#475569", textAlign: "center", padding: 50, fontSize: 13 }}>
          Выберите target-схему для проверки готовности к переключению
        </div>
      )}
    </div>
  );
}

// ── Detail tables ───────────────────────────────────────────────────────────

function DetailTable({ checkKey, status }: { checkKey: CheckKey; status: SwitchoverStatus }) {
  switch (checkKey) {
    case "fks":
      return (
        <table style={tblStyle}>
          <thead><tr><Th>Таблица</Th><Th>Constraint</Th></tr></thead>
          <tbody>
            {status.disabled_fks.map((r, i) => (
              <tr key={i}><Td>{r.table}</Td><Td mono>{r.constraint}</Td></tr>
            ))}
          </tbody>
        </table>
      );
    case "triggers":
      return (
        <table style={tblStyle}>
          <thead><tr><Th>Таблица</Th><Th>Триггер</Th></tr></thead>
          <tbody>
            {status.disabled_triggers.map((r, i) => (
              <tr key={i}><Td>{r.table}</Td><Td mono>{r.trigger}</Td></tr>
            ))}
          </tbody>
        </table>
      );
    case "identity":
      return (
        <table style={tblStyle}>
          <thead><tr><Th>Таблица</Th><Th>Колонка</Th><Th>Тип</Th></tr></thead>
          <tbody>
            {status.identity_cols.filter(c => c.generation_type !== "ALWAYS").map((r, i) => (
              <tr key={i}><Td>{r.table}</Td><Td mono>{r.column}</Td><Td>{r.generation_type}</Td></tr>
            ))}
          </tbody>
        </table>
      );
    case "indexes":
      return (
        <table style={tblStyle}>
          <thead><tr><Th>Таблица</Th><Th>Индекс</Th><Th>Тип</Th></tr></thead>
          <tbody>
            {status.unusable_indexes.map((r, i) => (
              <tr key={i}><Td>{r.table}</Td><Td mono>{r.index}</Td><Td>{r.type}</Td></tr>
            ))}
          </tbody>
        </table>
      );
    case "constraints":
      return (
        <table style={tblStyle}>
          <thead><tr><Th>Таблица</Th><Th>Constraint</Th><Th>Тип</Th></tr></thead>
          <tbody>
            {status.disabled_constraints.map((r, i) => (
              <tr key={i}><Td>{r.table}</Td><Td mono>{r.constraint}</Td><Td>{r.type}</Td></tr>
            ))}
          </tbody>
        </table>
      );
    case "logging":
      return (
        <table style={tblStyle}>
          <thead><tr><Th>Таблица</Th></tr></thead>
          <tbody>
            {status.nologging_tables.map((t, i) => (
              <tr key={i}><Td mono>{t}</Td></tr>
            ))}
          </tbody>
        </table>
      );
  }
}

// ── Styles ──────────────────────────────────────────────────────────────────

function btn(bg: string): React.CSSProperties {
  return {
    background: bg, border: "1px solid #334155", borderRadius: 4,
    color: "#94a3b8", padding: "5px 12px", fontSize: 12, cursor: "pointer", fontWeight: 500,
  };
}

const tblStyle: React.CSSProperties = {
  width: "100%", borderCollapse: "collapse", fontSize: 11,
};

function Th({ children }: { children: React.ReactNode }) {
  return (
    <th style={{
      padding: "3px 8px", textAlign: "left", color: "#64748b",
      fontWeight: 600, fontSize: 10, textTransform: "uppercase",
      borderBottom: "1px solid #1e293b",
    }}>
      {children}
    </th>
  );
}

function Td({ children, mono }: { children: React.ReactNode; mono?: boolean }) {
  return (
    <td style={{
      padding: "2px 8px", color: "#94a3b8",
      fontFamily: mono ? "monospace" : "inherit",
      borderBottom: "1px solid #0f172a",
    }}>
      {children}
    </td>
  );
}
