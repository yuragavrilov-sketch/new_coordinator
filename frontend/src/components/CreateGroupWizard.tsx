import React, { useCallback, useEffect, useRef, useState } from "react";
import ReactDOM from "react-dom";

// ── Types ─────────────────────────────────────────────────────────────────────

interface Column { name: string; type: string; nullable: boolean }
interface UkConstraint { name: string; columns: string[] }
interface TableInfo {
  columns: Column[];
  pk_columns: string[];
  uk_constraints: UkConstraint[];
}

interface TableEntry {
  schema: string;
  table: string;
  tableInfo: TableInfo | null;
  loadingInfo: boolean;
  infoError: string;
  effective_key_type: string;
  effective_key_columns: string[];
  selected_uk_index: number;
  target_schema: string;
  target_table: string;
  migration_strategy: "STAGE" | "DIRECT";
  stage_table_name: string;
}

interface GroupForm {
  group_name: string;
  connector_name: string;
  topic_prefix: string;
  migration_strategy: "STAGE" | "DIRECT";
  chunk_size: number;
  max_parallel_workers: number;
  baseline_parallel_degree: number;
  validate_hash_sample: boolean;
  stage_tablespace: string;
}

// ── Style tokens ──────────────────────────────────────────────────────────────

const S = {
  overlay: {
    position: "fixed" as const, inset: 0,
    background: "rgba(0,0,0,.72)",
    display: "flex" as const, alignItems: "flex-start" as const,
    justifyContent: "center" as const, zIndex: 1000,
    overflowY: "auto" as const, padding: "40px 16px 60px",
  },
  modal: {
    background: "#0f172a", border: "1px solid #1e293b", borderRadius: 10,
    width: "100%", maxWidth: 780,
    display: "flex" as const, flexDirection: "column" as const,
    boxShadow: "0 24px 48px rgba(0,0,0,.55)",
  },
  header: {
    padding: "14px 20px", borderBottom: "1px solid #1e293b",
    display: "flex" as const, alignItems: "center" as const, gap: 12,
  },
  body: {
    padding: 20, display: "flex" as const, flexDirection: "column" as const, gap: 16,
    maxHeight: "70vh", overflowY: "auto" as const,
  },
  footer: {
    padding: "12px 20px", borderTop: "1px solid #1e293b",
    display: "flex" as const, justifyContent: "flex-end" as const, gap: 8,
  },
  secWrap: (accent?: string) => ({
    border: `1px solid ${accent ? accent + "50" : "#1e293b"}`,
    borderRadius: 7, overflow: "hidden" as const,
  }),
  secHead: (accent?: string) => ({
    padding: "7px 14px", background: "#0a111f",
    borderBottom: `1px solid ${accent ? accent + "40" : "#1e293b"}`,
    fontSize: 11, fontWeight: 700 as const, color: accent ?? "#475569",
    textTransform: "uppercase" as const, letterSpacing: 0.8,
  }),
  secBody: {
    padding: 12, display: "flex" as const, flexDirection: "column" as const, gap: 10,
  },
  row2: { display: "grid" as const, gridTemplateColumns: "1fr 1fr", gap: 10 },
  row3: { display: "grid" as const, gridTemplateColumns: "1fr 1fr 1fr", gap: 10 },
  field: { display: "flex" as const, flexDirection: "column" as const, gap: 4 },
  label: { fontSize: 11, color: "#64748b", fontWeight: 600 as const, letterSpacing: 0.3 },
  req: { color: "#ef4444", marginLeft: 2 },
  input: {
    background: "#1e293b", border: "1px solid #334155", borderRadius: 5,
    color: "#e2e8f0", padding: "7px 10px", fontSize: 13, width: "100%",
  },
  inputErr: {
    background: "#1e293b", border: "1px solid #7f1d1d", borderRadius: 5,
    color: "#e2e8f0", padding: "7px 10px", fontSize: 13, width: "100%",
  },
  select: {
    background: "#1e293b", border: "1px solid #334155", borderRadius: 5,
    color: "#e2e8f0", padding: "7px 10px", fontSize: 13, width: "100%", cursor: "pointer",
  },
  selectDis: {
    background: "#0f172a", border: "1px solid #1e293b", borderRadius: 5,
    color: "#334155", padding: "7px 10px", fontSize: 13, width: "100%", cursor: "not-allowed",
  },
  hint: { fontSize: 10, color: "#475569" },
  err: { fontSize: 10, color: "#fca5a5" },
};

// ── Helpers ───────────────────────────────────────────────────────────────────

function toSnake(s: string) { return s.replace(/[^a-zA-Z0-9]/g, "_").toLowerCase(); }
function shortId() { return Math.random().toString(36).slice(2, 8); }

// ── Small components ──────────────────────────────────────────────────────────

function Section({ title, accent, children }: {
  title: string; accent?: string; children: React.ReactNode;
}) {
  return (
    <div style={S.secWrap(accent)}>
      <div style={S.secHead(accent)}>{title}</div>
      <div style={S.secBody}>{children}</div>
    </div>
  );
}

function Field({ label, required, error, hint, children }: {
  label: string; required?: boolean; error?: string; hint?: string; children: React.ReactNode;
}) {
  return (
    <div style={S.field}>
      <label style={S.label}>
        {label}{required && <span style={S.req}>*</span>}
      </label>
      {children}
      {hint && !error && <div style={S.hint}>{hint}</div>}
      {error && <div style={S.err}>{error}</div>}
    </div>
  );
}

function Chip({ label, color, bg }: { label: string; color: string; bg: string }) {
  return (
    <span style={{
      background: bg, color, border: `1px solid ${color}44`,
      borderRadius: 4, fontSize: 10, fontWeight: 700, padding: "2px 8px",
      display: "inline-block",
    }}>{label}</span>
  );
}

function KeyTypeBtn({ label, active, disabled, onClick }: {
  label: string; active: boolean; disabled?: boolean; onClick: () => void;
}) {
  return (
    <button onClick={onClick} disabled={disabled} style={{
      padding: "4px 10px", fontSize: 10, fontWeight: 700, borderRadius: 5,
      border: `1px solid ${active ? "#3b82f6" : "#334155"}`,
      background: active ? "#1e3a5f" : "#1e293b",
      color: disabled ? "#334155" : active ? "#93c5fd" : "#64748b",
      cursor: disabled ? "not-allowed" : "pointer",
    }}>{label}</button>
  );
}

function SearchableSelect({ items, value, onChange, disabled, placeholder }: {
  items: string[]; value: string; onChange: (v: string) => void;
  disabled?: boolean; placeholder?: string;
}) {
  const [open, setOpen] = useState(false);
  const [filter, setFilter] = useState("");
  const [pos, setPos] = useState<{ top: number; left: number; width: number }>({ top: 0, left: 0, width: 0 });
  const triggerRef = useRef<HTMLDivElement>(null);
  const dropRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const filtered = filter
    ? items.filter(i => i.toLowerCase().includes(filter.toLowerCase()))
    : items;

  useEffect(() => {
    function onClickOutside(e: MouseEvent) {
      const t = e.target as Node;
      if (triggerRef.current?.contains(t)) return;
      if (dropRef.current?.contains(t)) return;
      setOpen(false);
    }
    document.addEventListener("mousedown", onClickOutside);
    return () => document.removeEventListener("mousedown", onClickOutside);
  }, []);

  useEffect(() => {
    if (open && triggerRef.current) {
      const r = triggerRef.current.getBoundingClientRect();
      setPos({ top: r.bottom + 2, left: r.left, width: r.width });
      setTimeout(() => inputRef.current?.focus(), 0);
    }
  }, [open]);

  function pick(v: string) { onChange(v); setFilter(""); setOpen(false); }

  return (
    <>
      <div ref={triggerRef}
        onClick={() => { if (!disabled) setOpen(!open); }}
        style={{
          ...(disabled ? S.selectDis : S.select),
          display: "flex", alignItems: "center", gap: 6, minHeight: 33,
        }}
      >
        {value
          ? <span style={{ flex: 1 }}>{value}</span>
          : <span style={{ flex: 1, color: "#475569" }}>{placeholder}</span>}
        <span style={{ color: "#475569", fontSize: 10 }}>{open ? "\u25B2" : "\u25BC"}</span>
      </div>
      {open && ReactDOM.createPortal(
        <div ref={dropRef} style={{
          position: "fixed", top: pos.top, left: pos.left, width: pos.width,
          zIndex: 9999, background: "#0f172a", border: "1px solid #334155",
          borderRadius: 5, maxHeight: 260, display: "flex", flexDirection: "column",
          boxShadow: "0 8px 24px rgba(0,0,0,.6)",
        }}>
          <input ref={inputRef} value={filter}
            onChange={e => setFilter(e.target.value)}
            placeholder="Поиск\u2026"
            style={{ ...S.input, borderRadius: 0, border: "none", borderBottom: "1px solid #1e293b", padding: "7px 10px" }}
          />
          <div style={{ overflowY: "auto", maxHeight: 220 }}>
            {filtered.length === 0 && (
              <div style={{ padding: "8px 10px", color: "#334155", fontSize: 12 }}>
                {filter ? "Ничего не найдено" : "Нет данных"}
              </div>
            )}
            {filtered.map(t => (
              <div key={t} onClick={() => pick(t)} style={{
                padding: "5px 10px", fontSize: 13, cursor: "pointer",
                color: t === value ? "#93c5fd" : "#e2e8f0",
                background: t === value ? "#1e3a5f" : "transparent",
              }}
                onMouseEnter={e => (e.currentTarget.style.background = "#1e293b")}
                onMouseLeave={e => (e.currentTarget.style.background = t === value ? "#1e3a5f" : "transparent")}
              >{t}</div>
            ))}
          </div>
          <div style={{
            padding: "3px 10px", fontSize: 10, color: "#475569",
            borderTop: "1px solid #1e293b", textAlign: "right",
          }}>{filtered.length} / {items.length}</div>
        </div>,
        document.body,
      )}
    </>
  );
}

// ── Step indicators ───────────────────────────────────────────────────────────

function StepIndicator({ step, total, labels }: { step: number; total: number; labels: string[] }) {
  return (
    <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
      {labels.map((l, i) => (
        <React.Fragment key={i}>
          {i > 0 && <div style={{ width: 24, height: 1, background: i <= step ? "#3b82f6" : "#334155" }} />}
          <div style={{
            display: "flex", alignItems: "center", gap: 6,
            opacity: i <= step ? 1 : 0.4,
          }}>
            <div style={{
              width: 22, height: 22, borderRadius: "50%",
              background: i < step ? "#1d4ed8" : i === step ? "#1e3a5f" : "#1e293b",
              border: `1px solid ${i <= step ? "#3b82f6" : "#334155"}`,
              color: i <= step ? "#93c5fd" : "#475569",
              display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: 10, fontWeight: 700,
            }}>
              {i < step ? "\u2713" : i + 1}
            </div>
            <span style={{ fontSize: 11, color: i <= step ? "#e2e8f0" : "#475569", fontWeight: 600 }}>
              {l}
            </span>
          </div>
        </React.Fragment>
      ))}
    </div>
  );
}

// ── Per-table key configuration ───────────────────────────────────────────────

function TableKeyConfig({ entry, onChange }: {
  entry: TableEntry;
  onChange: (upd: Partial<TableEntry>) => void;
}) {
  const info = entry.tableInfo;

  function setKeyType(kt: string) {
    let cols: string[] = [];
    if (kt === "PRIMARY_KEY") cols = info?.pk_columns ?? [];
    if (kt === "UNIQUE_KEY") cols = info?.uk_constraints[entry.selected_uk_index]?.columns ?? [];
    onChange({ effective_key_type: kt, effective_key_columns: cols });
  }

  function toggleKeyCol(col: string, checked: boolean) {
    const cols = checked
      ? [...entry.effective_key_columns, col]
      : entry.effective_key_columns.filter(c => c !== col);
    onChange({ effective_key_columns: cols });
  }

  if (entry.loadingInfo) {
    return <div style={{ fontSize: 11, color: "#475569", padding: "4px 0" }}>Загрузка информации...</div>;
  }
  if (entry.infoError) {
    return <div style={S.err}>{entry.infoError}</div>;
  }
  if (!info) return null;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      {/* chips */}
      <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
        <Chip label={`${info.columns.length} кол.`} color="#94a3b8" bg="#1e293b" />
        {info.pk_columns.length > 0
          ? <Chip label={`PK: ${info.pk_columns.join(", ")}`} color="#86efac" bg="#052e16" />
          : <Chip label="Нет PK" color="#fca5a5" bg="#450a0a" />}
        {info.uk_constraints.length > 0 && (
          <Chip label={`UK: ${info.uk_constraints.length}`} color="#c4b5fd" bg="#2e1065" />
        )}
      </div>

      {/* key type selector */}
      <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
        <KeyTypeBtn label="PRIMARY KEY"
          active={entry.effective_key_type === "PRIMARY_KEY"}
          disabled={info.pk_columns.length === 0}
          onClick={() => setKeyType("PRIMARY_KEY")} />
        <KeyTypeBtn label="UNIQUE KEY"
          active={entry.effective_key_type === "UNIQUE_KEY"}
          disabled={info.uk_constraints.length === 0}
          onClick={() => setKeyType("UNIQUE_KEY")} />
        <KeyTypeBtn label="USER DEFINED"
          active={entry.effective_key_type === "USER_DEFINED"}
          onClick={() => setKeyType("USER_DEFINED")} />
        <KeyTypeBtn label="NONE"
          active={entry.effective_key_type === "NONE"}
          onClick={() => setKeyType("NONE")} />
      </div>

      {/* PK display */}
      {entry.effective_key_type === "PRIMARY_KEY" && (
        <div style={{ fontSize: 11, color: "#86efac" }}>
          Ключ: <strong>{info.pk_columns.join(", ")}</strong>
        </div>
      )}

      {/* UK selector */}
      {entry.effective_key_type === "UNIQUE_KEY" && (
        <>
          {info.uk_constraints.length > 1 && (
            <select style={{ ...S.select, fontSize: 11 }} value={entry.selected_uk_index}
              onChange={e => {
                const idx = parseInt(e.target.value);
                onChange({
                  selected_uk_index: idx,
                  effective_key_columns: info.uk_constraints[idx]?.columns ?? [],
                });
              }}>
              {info.uk_constraints.map((uk, i) => (
                <option key={uk.name} value={i}>{uk.name} ({uk.columns.join(", ")})</option>
              ))}
            </select>
          )}
          <div style={{ fontSize: 11, color: "#c4b5fd" }}>
            Ключ: <strong>{info.uk_constraints[entry.selected_uk_index]?.columns.join(", ")}</strong>
          </div>
        </>
      )}

      {/* User-defined column picker */}
      {entry.effective_key_type === "USER_DEFINED" && (
        <div>
          <div style={{
            maxHeight: 140, overflowY: "auto",
            border: "1px solid #334155", borderRadius: 5, background: "#1e293b",
          }}>
            {info.columns.map(col => (
              <label key={col.name} style={{
                display: "flex", alignItems: "center", gap: 8,
                padding: "4px 10px", cursor: "pointer",
                borderBottom: "1px solid #0f172a",
              }}>
                <input type="checkbox"
                  checked={entry.effective_key_columns.includes(col.name)}
                  onChange={e => toggleKeyCol(col.name, e.target.checked)}
                />
                <span style={{ fontSize: 11, color: "#e2e8f0", fontFamily: "monospace" }}>{col.name}</span>
                <span style={{ fontSize: 9, color: "#475569" }}>{col.type}</span>
                {!col.nullable && (
                  <span style={{ fontSize: 9, color: "#ef4444", marginLeft: "auto" }}>NOT NULL</span>
                )}
              </label>
            ))}
          </div>
          {entry.effective_key_columns.length > 0 && (
            <div style={S.hint}>Выбрано: {entry.effective_key_columns.join(", ")}</div>
          )}
          {entry.effective_key_columns.length === 0 && (
            <div style={S.err}>Выберите хотя бы один столбец</div>
          )}
        </div>
      )}
    </div>
  );
}

// ── Main Wizard ───────────────────────────────────────────────────────────────

interface Props { onClose: () => void; onCreated: () => void }

const INIT_FORM: GroupForm = {
  group_name: "",
  connector_name: "",
  topic_prefix: "",
  migration_strategy: "STAGE",
  chunk_size: 1_000_000,
  max_parallel_workers: 1,
  baseline_parallel_degree: 4,
  validate_hash_sample: false,
  stage_tablespace: "PAYSTAGE",
};

const STEP_LABELS = ["Группа", "Таблицы и ключи"];

export function CreateGroupWizard({ onClose, onCreated }: Props) {
  const [step, setStep] = useState(0);
  const [form, setFormRaw] = useState<GroupForm>(INIT_FORM);
  const [tables, setTables] = useState<TableEntry[]>([]);
  const [submitting, setSubmitting] = useState(false);
  const [submitErr, setSubmitErr] = useState("");
  const [fieldErrs, setFieldErrs] = useState<Record<string, string>>({});

  // schema / table loading
  const [schemas, setSchemas] = useState<string[]>([]);
  const [loadingSchemas, setLoadingSchemas] = useState(true);
  const [selectedSchema, setSelectedSchema] = useState("");
  const [schemaTables, setSchemaTables] = useState<string[]>([]);
  const [loadingTables, setLoadingTables] = useState(false);
  const [tableFilter, setTableFilter] = useState("");

  // existing migrations (to exclude already-migrated)
  const [existingMigs, setExistingMigs] = useState<{ source_schema: string; source_table: string; phase: string }[]>([]);

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
      topic_prefix: `grp.${base}${id ? "." + id : ""}`,
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

  // Load existing migrations
  useEffect(() => {
    fetch("/api/migrations")
      .then(r => r.json())
      .then((data: any[]) =>
        setExistingMigs(data.filter(m =>
          m.phase !== "CANCELLED" && m.phase !== "FAILED" && m.phase !== "COMPLETED"
        ))
      )
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

  // Filter out already-migrated tables
  const availableTables = React.useMemo(() => {
    const excluded = new Set<string>();
    for (const m of existingMigs) {
      if (m.source_schema === selectedSchema) excluded.add(m.source_table);
    }
    // also exclude already-selected from other schemas? No — only same schema
    for (const t of tables) {
      if (t.schema === selectedSchema) excluded.add(t.table);
    }
    let list = schemaTables.filter(t => !excluded.has(t));
    if (tableFilter) {
      const f = tableFilter.toLowerCase();
      list = list.filter(t => t.toLowerCase().includes(f));
    }
    return list;
  }, [schemaTables, existingMigs, selectedSchema, tables, tableFilter]);

  // Add table to selection
  function addTable(tableName: string) {
    const entry: TableEntry = {
      schema: selectedSchema,
      table: tableName,
      tableInfo: null,
      loadingInfo: true,
      infoError: "",
      effective_key_type: "",
      effective_key_columns: [],
      selected_uk_index: 0,
      target_schema: selectedSchema,
      target_table: tableName,
      migration_strategy: form.migration_strategy,
      stage_table_name: `STG_${selectedSchema}_${tableName}`.toUpperCase(),
    };
    setTables(prev => [...prev, entry]);

    // Load table info
    const p = `schema=${encodeURIComponent(selectedSchema)}&table=${encodeURIComponent(tableName)}`;
    fetch(`/api/db/source/table-info?${p}`)
      .then(r => r.json())
      .then((d: TableInfo & { error?: string }) => {
        setTables(prev => prev.map(t => {
          if (t.schema !== selectedSchema || t.table !== tableName) return t;
          if (d.error) return { ...t, loadingInfo: false, infoError: d.error };
          let keyType = "USER_DEFINED";
          let keyCols: string[] = [];
          if (d.pk_columns.length > 0) {
            keyType = "PRIMARY_KEY"; keyCols = d.pk_columns;
          } else if (d.uk_constraints.length > 0) {
            keyType = "UNIQUE_KEY"; keyCols = d.uk_constraints[0].columns;
          }
          return {
            ...t,
            tableInfo: d,
            loadingInfo: false,
            effective_key_type: keyType,
            effective_key_columns: keyCols,
          };
        }));
      })
      .catch(e => {
        setTables(prev => prev.map(t => {
          if (t.schema !== selectedSchema || t.table !== tableName) return t;
          return { ...t, loadingInfo: false, infoError: String(e) };
        }));
      });
  }

  // Remove table from selection
  function removeTable(schema: string, table: string) {
    setTables(prev => prev.filter(t => !(t.schema === schema && t.table === table)));
  }

  // Update a table entry
  function updateTable(schema: string, table: string, upd: Partial<TableEntry>) {
    setTables(prev => prev.map(t =>
      t.schema === schema && t.table === table ? { ...t, ...upd } : t
    ));
  }

  // ── Validation ──────────────────────────────────────────────────────────────

  function validateStep0(): boolean {
    const e: Record<string, string> = {};
    if (!form.group_name.trim()) e.group_name = "Обязательное поле";
    if (!form.connector_name.trim()) e.connector_name = "Обязательное поле";
    if (!form.topic_prefix.trim()) e.topic_prefix = "Обязательное поле";
    if (form.chunk_size <= 0) e.chunk_size = "Должно быть > 0";
    setFieldErrs(e);
    return Object.keys(e).length === 0;
  }

  function validateStep1(): boolean {
    if (tables.length === 0) {
      setSubmitErr("Выберите хотя бы одну таблицу");
      return false;
    }
    for (const t of tables) {
      if (!t.effective_key_type) {
        setSubmitErr(`Не задан тип ключа для ${t.schema}.${t.table}`);
        return false;
      }
      if (t.effective_key_type === "USER_DEFINED" && t.effective_key_columns.length === 0) {
        setSubmitErr(`Не выбраны колонки ключа для ${t.schema}.${t.table}`);
        return false;
      }
    }
    setSubmitErr("");
    return true;
  }

  // ── Navigation ──────────────────────────────────────────────────────────────

  function next() {
    if (step === 0 && validateStep0()) setStep(1);
  }

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
      group_name: form.group_name.trim(),
      connector_name: form.connector_name.trim(),
      topic_prefix: form.topic_prefix.trim(),
      consumer_group_prefix: form.topic_prefix.trim(),
      source_connection_id: "oracle_source",
      migration_strategy: form.migration_strategy,
      chunk_size: form.chunk_size,
      max_parallel_workers: form.max_parallel_workers,
      baseline_parallel_degree: form.baseline_parallel_degree,
      validate_hash_sample: form.validate_hash_sample,
      stage_tablespace: form.stage_tablespace,
      tables: tables.map(t => ({
        source_schema: t.schema,
        source_table: t.table,
        target_schema: t.target_schema,
        target_table: t.target_table,
        effective_key_type: t.effective_key_type,
        effective_key_columns: t.effective_key_columns,
        source_pk_exists: (t.tableInfo?.pk_columns.length ?? 0) > 0,
        source_uk_exists: (t.tableInfo?.uk_constraints.length ?? 0) > 0,
        migration_strategy: t.migration_strategy,
        stage_table_name: t.stage_table_name,
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

  // ── Render ──────────────────────────────────────────────────────────────────

  return (
    <div style={S.overlay} onClick={e => { if (e.target === e.currentTarget) onClose(); }}>
      <div style={S.modal}>

        {/* Header */}
        <div style={S.header}>
          <span style={{ fontSize: 15, fontWeight: 700, color: "#e2e8f0" }}>
            Новая группа коннекторов
          </span>
          <span style={{ flex: 1 }} />
          <StepIndicator step={step} total={2} labels={STEP_LABELS} />
          <span style={{ flex: 1 }} />
          <button onClick={onClose} style={{
            background: "none", border: "none", color: "#475569",
            cursor: "pointer", fontSize: 20, lineHeight: 1, padding: "0 2px",
          }}>{"\u2715"}</button>
        </div>

        {/* Body */}
        <div style={S.body}>

          {/* ── STEP 0: Group Info ── */}
          {step === 0 && (
            <>
              <Section title="Настройки группы" accent="#1d4ed8">
                <Field label="Имя группы" required error={fieldErrs.group_name}>
                  <input style={fieldErrs.group_name ? S.inputErr : S.input}
                    value={form.group_name}
                    placeholder="prod_batch_1"
                    onChange={e => setF({ group_name: e.target.value })} />
                </Field>
                <div style={S.row2}>
                  <Field label="Имя коннектора (Debezium)" required error={fieldErrs.connector_name}
                    hint="Автозаполняется из имени группы">
                    <input style={fieldErrs.connector_name ? S.inputErr : S.input}
                      value={form.connector_name}
                      onChange={e => setF({ connector_name: e.target.value })} />
                  </Field>
                  <Field label="Topic prefix (Kafka)" required error={fieldErrs.topic_prefix}
                    hint="Автозаполняется из имени группы">
                    <input style={fieldErrs.topic_prefix ? S.inputErr : S.input}
                      value={form.topic_prefix}
                      onChange={e => setF({ topic_prefix: e.target.value })} />
                  </Field>
                </div>
              </Section>

              <Section title="Общие настройки миграций">
                <div style={S.row2}>
                  <Field label="Стратегия по умолчанию">
                    <div style={{ display: "flex", gap: 8 }}>
                      {(["STAGE", "DIRECT"] as const).map(s => (
                        <button key={s} onClick={() => setF({ migration_strategy: s })} style={{
                          padding: "5px 14px", fontSize: 11, fontWeight: 700, borderRadius: 5,
                          border: `1px solid ${form.migration_strategy === s ? "#3b82f6" : "#334155"}`,
                          background: form.migration_strategy === s ? "#1e3a5f" : "#1e293b",
                          color: form.migration_strategy === s ? "#93c5fd" : "#64748b",
                          cursor: "pointer",
                        }}>{s}</button>
                      ))}
                    </div>
                  </Field>
                  <Field label="Stage Tablespace">
                    <input style={S.input}
                      value={form.stage_tablespace}
                      onChange={e => setF({ stage_tablespace: e.target.value })} />
                  </Field>
                </div>
                <div style={S.row3}>
                  <Field label="Chunk size" error={fieldErrs.chunk_size}>
                    <input style={S.input} type="number"
                      value={form.chunk_size}
                      onChange={e => setF({ chunk_size: parseInt(e.target.value) || 0 })} />
                  </Field>
                  <Field label="Parallel workers">
                    <input style={S.input} type="number" min={1}
                      value={form.max_parallel_workers}
                      onChange={e => setF({ max_parallel_workers: parseInt(e.target.value) || 1 })} />
                  </Field>
                  <Field label="Baseline parallel">
                    <input style={S.input} type="number" min={1}
                      value={form.baseline_parallel_degree}
                      onChange={e => setF({ baseline_parallel_degree: parseInt(e.target.value) || 4 })} />
                  </Field>
                </div>
                <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
                  <input type="checkbox" checked={form.validate_hash_sample}
                    onChange={e => setF({ validate_hash_sample: e.target.checked })} />
                  <span style={{ fontSize: 12, color: "#94a3b8" }}>Валидация hash/sample</span>
                </label>
              </Section>
            </>
          )}

          {/* ── STEP 1: Tables + Key Config ── */}
          {step === 1 && (
            <>
              <Section title="Выбор таблиц" accent="#1d4ed8">
                <div style={S.row2}>
                  <Field label="Схема источника" required>
                    <SearchableSelect
                      items={schemas}
                      value={selectedSchema}
                      onChange={v => { setSelectedSchema(v); setTableFilter(""); }}
                      disabled={loadingSchemas}
                      placeholder={loadingSchemas ? "Загрузка\u2026" : "Выберите схему"}
                    />
                  </Field>
                  <Field label="Фильтр таблиц">
                    <input style={S.input} value={tableFilter}
                      onChange={e => setTableFilter(e.target.value)}
                      placeholder="Поиск по имени\u2026"
                      disabled={!selectedSchema} />
                  </Field>
                </div>

                {loadingTables && (
                  <div style={{ fontSize: 11, color: "#475569" }}>Загрузка таблиц...</div>
                )}

                {!loadingTables && selectedSchema && (
                  <div style={{
                    maxHeight: 220, overflowY: "auto",
                    border: "1px solid #334155", borderRadius: 5, background: "#1e293b",
                  }}>
                    {availableTables.length === 0 && (
                      <div style={{ padding: "8px 10px", color: "#334155", fontSize: 12 }}>
                        {tableFilter ? "Ничего не найдено" : "Нет доступных таблиц"}
                      </div>
                    )}
                    {availableTables.map(t => (
                      <div key={t}
                        onClick={() => addTable(t)}
                        style={{
                          padding: "6px 10px", fontSize: 12, cursor: "pointer",
                          color: "#e2e8f0", display: "flex", alignItems: "center", gap: 8,
                          borderBottom: "1px solid #0f172a",
                        }}
                        onMouseEnter={e => (e.currentTarget.style.background = "#334155")}
                        onMouseLeave={e => (e.currentTarget.style.background = "transparent")}
                      >
                        <span style={{ color: "#3b82f6", fontSize: 14, fontWeight: 700 }}>+</span>
                        <span style={{ fontFamily: "monospace" }}>{t}</span>
                      </div>
                    ))}
                  </div>
                )}
              </Section>

              {/* Selected tables with key config */}
              {tables.length > 0 && (
                <Section title={`Выбранные таблицы (${tables.length})`} accent="#16a34a">
                  {tables.map((t, idx) => (
                    <div key={`${t.schema}.${t.table}`} style={{
                      border: "1px solid #1e293b", borderRadius: 6,
                      background: "#0a111f", overflow: "hidden",
                    }}>
                      {/* table header */}
                      <div style={{
                        display: "flex", alignItems: "center", gap: 8,
                        padding: "8px 12px", borderBottom: "1px solid #1e293b",
                      }}>
                        <span style={{
                          fontSize: 12, fontWeight: 700, color: "#e2e8f0",
                          fontFamily: "monospace",
                        }}>
                          {t.schema}.{t.table}
                        </span>
                        <span style={{ fontSize: 10, color: "#475569" }}>
                          {"\u2192"} {t.target_schema}.{t.target_table}
                        </span>
                        <span style={{ flex: 1 }} />
                        <span style={{
                          fontSize: 10, padding: "2px 6px", borderRadius: 3,
                          background: t.migration_strategy === "STAGE" ? "#1e3a5f" : "#1e293b",
                          color: t.migration_strategy === "STAGE" ? "#93c5fd" : "#64748b",
                          border: `1px solid ${t.migration_strategy === "STAGE" ? "#3b82f6" : "#334155"}`,
                          cursor: "pointer",
                        }}
                          onClick={() => updateTable(t.schema, t.table, {
                            migration_strategy: t.migration_strategy === "STAGE" ? "DIRECT" : "STAGE",
                          })}
                        >
                          {t.migration_strategy}
                        </span>
                        <button onClick={() => removeTable(t.schema, t.table)} style={{
                          background: "none", border: "none", color: "#dc2626",
                          cursor: "pointer", fontSize: 14, lineHeight: 1, padding: "0 2px",
                        }}>{"\u2715"}</button>
                      </div>

                      {/* key config */}
                      <div style={{ padding: "8px 12px" }}>
                        <TableKeyConfig
                          entry={t}
                          onChange={upd => updateTable(t.schema, t.table, upd)}
                        />
                      </div>
                    </div>
                  ))}
                </Section>
              )}
            </>
          )}

          {submitErr && (
            <div style={{
              padding: "8px 12px", background: "#450a0a", border: "1px solid #7f1d1d",
              borderRadius: 6, color: "#fca5a5", fontSize: 12,
            }}>{submitErr}</div>
          )}
        </div>

        {/* Footer */}
        <div style={S.footer}>
          {step > 0 && (
            <button onClick={back} style={{
              background: "none", border: "1px solid #334155", borderRadius: 6,
              color: "#94a3b8", padding: "7px 16px", fontSize: 12, cursor: "pointer",
            }}>Назад</button>
          )}
          <button onClick={onClose} style={{
            background: "none", border: "1px solid #334155", borderRadius: 6,
            color: "#64748b", padding: "7px 16px", fontSize: 12, cursor: "pointer",
          }}>Отмена</button>
          {step < STEP_LABELS.length - 1 ? (
            <button onClick={next} style={{
              background: "#1d4ed8", border: "none", borderRadius: 6,
              color: "#fff", padding: "7px 20px", fontSize: 12, cursor: "pointer",
              fontWeight: 600,
            }}>Далее</button>
          ) : (
            <button onClick={handleSubmit} disabled={submitting} style={{
              background: "#16a34a", border: "none", borderRadius: 6,
              color: "#fff", padding: "7px 20px", fontSize: 12, cursor: "pointer",
              fontWeight: 600, opacity: submitting ? 0.6 : 1,
            }}>{submitting ? "Создание..." : `Создать группу (${tables.length} табл.)`}</button>
          )}
        </div>
      </div>
    </div>
  );
}
