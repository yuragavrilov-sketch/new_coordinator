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

interface FormData {
  migration_name: string;
  source_schema: string;
  source_table: string;
  target_schema: string;
  target_table: string;
  migration_strategy: "STAGE" | "DIRECT";
  connector_name: string;
  topic_prefix: string;
  consumer_group: string;
  stage_table_name: string;
  chunk_size: number;
  max_parallel_workers: number;
  baseline_parallel_degree: number;
  validate_hash_sample: boolean;
  effective_key_type: string;
  effective_key_columns: string[];
  selected_uk_index: number;
}

// ── Auto-generation helpers ───────────────────────────────────────────────────

function toSnake(s: string) { return s.replace(/[^a-zA-Z0-9]/g, "_").toLowerCase(); }

function shortId() { return Math.random().toString(36).slice(2, 8); }

function autoFields(ss: string, st: string) {
  const s = toSnake(ss), t = toSnake(st), id = shortId();
  return {
    connector_name:   `${s}_${t}_${id}_connector`,
    topic_prefix:     `${s}.${t}.${id}`,
    consumer_group:   `${s}_${t}_${id}_cg`,
    stage_table_name: `STG_${ss.toUpperCase()}_${st.toUpperCase()}`,
  };
}

// ── Style tokens ──────────────────────────────────────────────────────────────

const S = {
  overlay: {
    position:       "fixed"           as const,
    inset:          0,
    background:     "rgba(0,0,0,.72)",
    display:        "flex"            as const,
    alignItems:     "flex-start"      as const,
    justifyContent: "center"          as const,
    zIndex:         1000,
    overflowY:      "auto"            as const,
    padding:        "40px 16px 60px",
  },
  modal: {
    background:     "#0f172a",
    border:         "1px solid #1e293b",
    borderRadius:   10,
    width:          "100%",
    maxWidth:       700,
    display:        "flex"            as const,
    flexDirection:  "column"          as const,
    boxShadow:      "0 24px 48px rgba(0,0,0,.55)",
  },
  header: {
    padding:        "14px 20px",
    borderBottom:   "1px solid #1e293b",
    display:        "flex"            as const,
    alignItems:     "center"          as const,
    gap:            12,
  },
  body: {
    padding:        20,
    display:        "flex"            as const,
    flexDirection:  "column"          as const,
    gap:            16,
  },
  footer: {
    padding:        "12px 20px",
    borderTop:      "1px solid #1e293b",
    display:        "flex"            as const,
    justifyContent: "flex-end"        as const,
    gap:            8,
  },
  secWrap: (accent?: string) => ({
    border:       `1px solid ${accent ? accent + "50" : "#1e293b"}`,
    borderRadius: 7,
    overflow:     "hidden" as const,
  }),
  secHead: (accent?: string) => ({
    padding:         "7px 14px",
    background:      "#0a111f",
    borderBottom:    `1px solid ${accent ? accent + "40" : "#1e293b"}`,
    fontSize:        11,
    fontWeight:      700 as const,
    color:           accent ?? "#475569",
    textTransform:   "uppercase" as const,
    letterSpacing:   0.8,
  }),
  secBody: {
    padding:       12,
    display:       "flex"   as const,
    flexDirection: "column" as const,
    gap:           10,
  },
  row2: {
    display:             "grid" as const,
    gridTemplateColumns: "1fr 1fr",
    gap:                 10,
  },
  field: {
    display:       "flex"   as const,
    flexDirection: "column" as const,
    gap:           4,
  },
  label: { fontSize: 11, color: "#64748b", fontWeight: 600 as const, letterSpacing: 0.3 },
  req:   { color: "#ef4444", marginLeft: 2 },
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
    color: "#e2e8f0", padding: "7px 10px", fontSize: 13,
    width: "100%", cursor: "pointer",
  },
  selectDis: {
    background: "#0f172a", border: "1px solid #1e293b", borderRadius: 5,
    color: "#334155", padding: "7px 10px", fontSize: 13,
    width: "100%", cursor: "not-allowed",
  },
  hint:  { fontSize: 10, color: "#475569" },
  err:   { fontSize: 10, color: "#fca5a5" },
  textarea: {
    background: "#1e293b", border: "1px solid #334155", borderRadius: 5,
    color: "#e2e8f0", padding: "7px 10px", fontSize: 13,
    width: "100%", resize: "vertical" as const, minHeight: 52, fontFamily: "inherit",
  },
};

// ── Small helper components ───────────────────────────────────────────────────

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
      {hint  && !error && <div style={S.hint}>{hint}</div>}
      {error && <div style={S.err}>{error}</div>}
    </div>
  );
}

function TextInput({ value, placeholder, hasError, onChange }: {
  value: string; placeholder?: string; hasError?: boolean; onChange: (v: string) => void;
}) {
  return (
    <input
      style={hasError ? S.inputErr : S.input}
      value={value}
      placeholder={placeholder}
      onChange={e => onChange(e.target.value)}
    />
  );
}

function SearchableSelect({ items, value, onChange, disabled, placeholder }: {
  items: string[]; value: string; onChange: (v: string) => void;
  disabled?: boolean; placeholder?: string;
}) {
  const [open, setOpen]     = useState(false);
  const [filter, setFilter] = useState("");
  const [pos, setPos]       = useState<{ top: number; left: number; width: number }>({ top: 0, left: 0, width: 0 });
  const triggerRef = useRef<HTMLDivElement>(null);
  const dropRef   = useRef<HTMLDivElement>(null);
  const inputRef  = useRef<HTMLInputElement>(null);

  const filtered = filter
    ? items.filter(i => i.toLowerCase().includes(filter.toLowerCase()))
    : items;

  // Close on click outside (check both trigger and dropdown)
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

  // Position the dropdown and focus the search input when opened
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
        {value ? (
          <span style={{ flex: 1 }}>{value}</span>
        ) : (
          <span style={{ flex: 1, color: "#475569" }}>{placeholder}</span>
        )}
        <span style={{ color: "#475569", fontSize: 10 }}>{open ? "\u25B2" : "\u25BC"}</span>
      </div>
      {open && ReactDOM.createPortal(
        <div ref={dropRef} style={{
          position: "fixed", top: pos.top, left: pos.left, width: pos.width,
          zIndex: 9999,
          background: "#0f172a", border: "1px solid #334155", borderRadius: 5,
          maxHeight: 260, display: "flex", flexDirection: "column",
          boxShadow: "0 8px 24px rgba(0,0,0,.6)",
        }}>
          <input
            ref={inputRef}
            value={filter}
            onChange={e => setFilter(e.target.value)}
            placeholder="Поиск…"
            style={{
              ...S.input, borderRadius: 0, border: "none",
              borderBottom: "1px solid #1e293b", padding: "7px 10px",
            }}
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
              >
                {t}
              </div>
            ))}
          </div>
          <div style={{
            padding: "3px 10px", fontSize: 10, color: "#475569",
            borderTop: "1px solid #1e293b", textAlign: "right",
          }}>
            {filtered.length} / {items.length}
          </div>
        </div>,
        document.body,
      )}
    </>
  );
}

function SchemaTablePair({ db, schema, table, onSchema, onTable, schemaErr, tableErr }: {
  db: "source" | "target";
  schema: string; table: string;
  onSchema: (v: string) => void; onTable: (v: string) => void;
  schemaErr?: string; tableErr?: string;
}) {
  const [schemas, setSchemas] = useState<string[]>([]);
  const [tables,  setTables]  = useState<string[]>([]);
  const [lSch,    setLSch]    = useState(true);
  const [lTab,    setLTab]    = useState(false);
  const [eSch,    setESch]    = useState("");
  const [eTab,    setETab]    = useState("");

  useEffect(() => {
    setLSch(true);
    fetch(`/api/db/${db}/schemas`)
      .then(r => r.json())
      .then(d => { if (d.error) setESch(d.error); else setSchemas(d); })
      .catch(e => setESch(String(e)))
      .finally(() => setLSch(false));
  }, [db]);

  useEffect(() => {
    if (!schema) { setTables([]); return; }
    setLTab(true);
    setTables([]);
    fetch(`/api/db/${db}/tables?schema=${encodeURIComponent(schema)}`)
      .then(r => r.json())
      .then(d => { if (d.error) setETab(d.error); else setTables(d); })
      .catch(e => setETab(String(e)))
      .finally(() => setLTab(false));
  }, [db, schema]);

  const prevTables = useRef<string[]>([]);
  useEffect(() => {
    if (tables !== prevTables.current) {
      prevTables.current = tables;
      if (table && !tables.includes(table)) onTable("");
    }
  }, [tables]); // eslint-disable-line

  return (
    <div style={S.row2}>
      <Field label="Схема" required error={schemaErr || eSch}>
        <SearchableSelect
          items={schemas}
          value={schema}
          onChange={onSchema}
          disabled={lSch}
          placeholder={lSch ? "Загрузка…" : "Выберите схему"}
        />
      </Field>
      <Field label="Таблица" required error={tableErr || eTab}>
        <SearchableSelect
          items={tables}
          value={table}
          onChange={onTable}
          disabled={!schema || lTab}
          placeholder={!schema ? "Сначала схему" : lTab ? "Загрузка…" : "Выберите таблицу"}
        />
      </Field>
    </div>
  );
}

// ── Chips for detected key info ───────────────────────────────────────────────

function Chip({ label, color, bg }: { label: string; color: string; bg: string }) {
  return (
    <span style={{
      background: bg, color, border: `1px solid ${color}44`,
      borderRadius: 4, fontSize: 10, fontWeight: 700,
      padding: "2px 8px", display: "inline-block",
    }}>{label}</span>
  );
}

// ── Key type button ───────────────────────────────────────────────────────────

function KeyTypeBtn({ label, active, disabled, onClick }: {
  label: string; active: boolean; disabled?: boolean; onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      style={{
        padding: "5px 12px", fontSize: 11, fontWeight: 700, borderRadius: 5,
        border:     `1px solid ${active ? "#3b82f6" : "#334155"}`,
        background: active ? "#1e3a5f" : "#1e293b",
        color:      disabled ? "#334155" : active ? "#93c5fd" : "#64748b",
        cursor:     disabled ? "not-allowed" : "pointer",
      }}
    >{label}</button>
  );
}

// ── Main modal ────────────────────────────────────────────────────────────────

interface Props { onClose: () => void; onCreated: () => void }

const INIT: FormData = {
  migration_name: "",
  source_schema: "", source_table: "",
  target_schema: "", target_table: "",
  migration_strategy: "STAGE",
  connector_name: "", topic_prefix: "", consumer_group: "", stage_table_name: "",
  chunk_size: 1_000_000,
  max_parallel_workers: 1,
  baseline_parallel_degree: 4,
  validate_hash_sample: false,
  effective_key_type: "", effective_key_columns: [], selected_uk_index: 0,
};

export function CreateMigrationModal({ onClose, onCreated }: Props) {
  const [form,         setFormRaw]  = useState<FormData>(INIT);
  const [tableInfo,    setTableInfo] = useState<TableInfo | null>(null);
  const [loadingInfo,  setLoadInfo]  = useState(false);
  const [infoErr,      setInfoErr]   = useState("");
  const [submitting,   setSubmit]    = useState(false);
  const [submitErr,    setSubmitErr] = useState("");
  const [fieldErrs,    setFieldErrs] = useState<Partial<Record<keyof FormData, string>>>({});
  const [dupWarning,   setDupWarning] = useState<string | null>(null);
  const nameTouched = useRef(false);

  const setF = useCallback((up: Partial<FormData>) =>
    setFormRaw(f => ({ ...f, ...up })), []);

  // ── Load table info when source schema+table are both selected ────────────

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
        // Auto-detect key
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

  // ── Auto-fill generated names when source table chosen ───────────────────

  useEffect(() => {
    if (!form.source_schema || !form.source_table) return;
    setF(autoFields(form.source_schema, form.source_table));
  }, [form.source_schema, form.source_table, setF]);

  // ── Auto-fill migration name ──────────────────────────────────────────────

  useEffect(() => {
    if (nameTouched.current) return;
    if (!form.source_schema || !form.source_table || !form.target_schema || !form.target_table) return;
    setF({
      migration_name:
        `${form.source_schema}.${form.source_table} → ${form.target_schema}.${form.target_table}`,
    });
  }, [form.source_schema, form.source_table, form.target_schema, form.target_table, setF]);

  // ── Duplicate detection ───────────────────────────────────────────────────

  useEffect(() => {
    if (!form.source_schema || !form.source_table || !form.target_schema || !form.target_table) {
      setDupWarning(null);
      return;
    }
    fetch("/api/migrations")
      .then(r => r.json())
      .then((data: {
        migration_name: string; phase: string;
        source_schema: string; source_table: string;
        target_schema: string; target_table: string;
      }[]) => {
        const dup = data.find(m =>
          m.source_schema === form.source_schema && m.source_table === form.source_table &&
          m.target_schema === form.target_schema && m.target_table === form.target_table &&
          m.phase !== "CANCELLED" && m.phase !== "FAILED" && m.phase !== "COMPLETED"
        );
        setDupWarning(dup
          ? `Уже есть активная миграция "${dup.migration_name}" (${dup.phase}) для этой пары`
          : null
        );
      })
      .catch(() => setDupWarning(null));
  }, [form.source_schema, form.source_table, form.target_schema, form.target_table]);

  // ── Auto-suggest target schema from source ───────────────────────────────

  const onSourceSchema = useCallback((v: string) => {
    setF({ source_schema: v, source_table: "" });
    // Suggest same schema for target if not yet chosen
    setFormRaw(f => ({ ...f, source_schema: v, source_table: "", target_schema: f.target_schema || v }));
  }, []);

  // ── Validation ────────────────────────────────────────────────────────────

  function validate(): boolean {
    const e: Partial<Record<keyof FormData, string>> = {};
    if (!form.migration_name.trim())    e.migration_name      = "Обязательное поле";
    if (!form.source_schema)            e.source_schema       = "Выберите схему";
    if (!form.source_table)             e.source_table        = "Выберите таблицу";
    if (!form.target_schema)            e.target_schema       = "Выберите схему";
    if (!form.target_table)             e.target_table        = "Выберите таблицу";
    if (!form.connector_name.trim())    e.connector_name      = "Обязательное поле";
    if (!form.topic_prefix.trim())      e.topic_prefix        = "Обязательное поле";
    if (!form.consumer_group.trim())    e.consumer_group      = "Обязательное поле";
    if (form.migration_strategy === "STAGE" && !form.stage_table_name.trim())
      e.stage_table_name = "Обязательное поле";
    if (form.chunk_size <= 0)           e.chunk_size          = "Должно быть > 0";
    if (!form.effective_key_type)       e.effective_key_type  = "Выберите тип ключа";
    if (form.effective_key_type === "USER_DEFINED" && form.effective_key_columns.length === 0)
      e.effective_key_columns = "Выберите хотя бы один столбец";
    setFieldErrs(e);
    return Object.keys(e).length === 0;
  }

  // ── Submit ────────────────────────────────────────────────────────────────

  async function handleSubmit() {
    if (!validate()) return;
    setSubmit(true);
    setSubmitErr("");
    const keySourceMap: Record<string, string> = {
      PRIMARY_KEY: "PK", UNIQUE_KEY: "UK", USER_DEFINED: "USER", NONE: "NONE",
    };
    const payload = {
      initial_phase:              "DRAFT",
      migration_name:             form.migration_name.trim(),
      migration_strategy:         form.migration_strategy,
      source_connection_id:       "oracle_source",
      target_connection_id:       "oracle_target",
      source_schema:              form.source_schema,
      source_table:               form.source_table,
      target_schema:              form.target_schema,
      target_table:               form.target_table,
      stage_table_name:           form.migration_strategy === "STAGE" ? form.stage_table_name.trim() : "",
      connector_name:             form.connector_name.trim(),
      topic_prefix:               form.topic_prefix.trim(),
      consumer_group:             form.consumer_group.trim(),
      chunk_size:                 form.chunk_size,
      max_parallel_workers:       form.max_parallel_workers,
      baseline_parallel_degree:   form.baseline_parallel_degree,
      validate_hash_sample:       form.validate_hash_sample,
      source_pk_exists:           (tableInfo?.pk_columns.length ?? 0) > 0,
      source_uk_exists:           (tableInfo?.uk_constraints.length ?? 0) > 0,
      effective_key_type:         form.effective_key_type,
      effective_key_source:       keySourceMap[form.effective_key_type] ?? "NONE",
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

  // ── Key section helpers ───────────────────────────────────────────────────

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

  // ── Render ────────────────────────────────────────────────────────────────

  return (
    <div style={S.overlay} onClick={e => { if (e.target === e.currentTarget) onClose(); }}>
      <div style={S.modal}>

        {/* Header */}
        <div style={S.header}>
          <span style={{ fontSize: 15, fontWeight: 700, color: "#e2e8f0" }}>
            Новая миграция
          </span>
          <span style={{ flex: 1 }} />
          <button onClick={onClose} style={{
            background: "none", border: "none", color: "#475569",
            cursor: "pointer", fontSize: 20, lineHeight: 1, padding: "0 2px",
          }}>✕</button>
        </div>

        {/* Body */}
        <div style={S.body}>

          {/* ── Source ── */}
          <Section title="Источник (Oracle Source)" accent="#1d4ed8">
            <SchemaTablePair
              db="source"
              schema={form.source_schema}
              table={form.source_table}
              onSchema={onSourceSchema}
              onTable={v => setF({ source_table: v })}
              schemaErr={fieldErrs.source_schema}
              tableErr={fieldErrs.source_table}
            />
            {loadingInfo && (
              <div style={{ fontSize: 11, color: "#475569" }}>
                Загрузка информации о таблице…
              </div>
            )}
            {infoErr && <div style={S.err}>{infoErr}</div>}
            {tableInfo && !loadingInfo && (
              <div style={{ display: "flex", gap: 6, flexWrap: "wrap" as const }}>
                <Chip label={`${tableInfo.columns.length} колонок`}
                  color="#94a3b8" bg="#1e293b" />
                {tableInfo.pk_columns.length > 0
                  ? <Chip label={`PK: ${tableInfo.pk_columns.join(", ")}`}
                      color="#86efac" bg="#052e16" />
                  : <Chip label="Нет PK" color="#fca5a5" bg="#450a0a" />}
                {tableInfo.uk_constraints.length > 0 && (
                  <Chip label={`UK: ${tableInfo.uk_constraints.length} constraint(s)`}
                    color="#c4b5fd" bg="#2e1065" />
                )}
              </div>
            )}
          </Section>

          {/* ── Target ── */}
          <Section title="Цель (Oracle Target)" accent="#047857">
            <SchemaTablePair
              db="target"
              schema={form.target_schema}
              table={form.target_table}
              onSchema={v => setF({ target_schema: v, target_table: "" })}
              onTable={v => setF({ target_table: v })}
              schemaErr={fieldErrs.target_schema}
              tableErr={fieldErrs.target_table}
            />
          </Section>

          {/* ── Config ── */}
          <Section title="Параметры миграции">
            <Field label="Стратегия миграции" required>
              <div style={{ display: "flex", gap: 8 }}>
                <button
                  onClick={() => setF({ migration_strategy: "STAGE" })}
                  style={{
                    flex: 1, padding: "8px 12px", borderRadius: 6, cursor: "pointer",
                    border: `1px solid ${form.migration_strategy === "STAGE" ? "#3b82f6" : "#334155"}`,
                    background: form.migration_strategy === "STAGE" ? "#1e3a5f" : "#1e293b",
                    color: form.migration_strategy === "STAGE" ? "#93c5fd" : "#64748b",
                    fontWeight: 700, fontSize: 12,
                  }}
                >
                  STAGE
                </button>
                <button
                  onClick={() => setF({ migration_strategy: "DIRECT" })}
                  style={{
                    flex: 1, padding: "8px 12px", borderRadius: 6, cursor: "pointer",
                    border: `1px solid ${form.migration_strategy === "DIRECT" ? "#059669" : "#334155"}`,
                    background: form.migration_strategy === "DIRECT" ? "#064e3b" : "#1e293b",
                    color: form.migration_strategy === "DIRECT" ? "#6ee7b7" : "#64748b",
                    fontWeight: 700, fontSize: 12,
                  }}
                >
                  DIRECT
                </button>
              </div>
              <div style={{ fontSize: 10, color: "#475569", marginTop: 4 }}>
                {form.migration_strategy === "STAGE"
                  ? "Загрузка через промежуточную stage-таблицу → валидация → публикация в целевую"
                  : "Прямая загрузка в целевую таблицу, без stage. Быстрее, но без возможности валидации stage"}
              </div>
            </Field>
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
            <div style={S.row2}>
              <Field label="Consumer group" required error={fieldErrs.consumer_group}>
                <TextInput value={form.consumer_group} hasError={!!fieldErrs.consumer_group}
                  onChange={v => setF({ consumer_group: v })} />
              </Field>
              {form.migration_strategy === "STAGE" && (
                <Field label="Stage table name" required error={fieldErrs.stage_table_name}>
                  <TextInput value={form.stage_table_name} hasError={!!fieldErrs.stage_table_name}
                    onChange={v => setF({ stage_table_name: v })} />
                </Field>
              )}
            </div>
            <div style={S.row2}>
              <Field label="Chunk size" required error={fieldErrs.chunk_size}
                hint="Строк на чанк (рекомендуется 500k–2M)">
                <input style={S.input} type="number" value={form.chunk_size} min={1}
                  onChange={e => setF({ chunk_size: parseInt(e.target.value) || 0 })} />
              </Field>
              <Field label="Воркеры (bulk)" hint="Параллельных воркеров для bulk-загрузки (1–16)">
                <input style={S.input} type="number" value={form.max_parallel_workers} min={1} max={16}
                  onChange={e => setF({ max_parallel_workers: Math.max(1, Math.min(16, parseInt(e.target.value) || 1)) })} />
              </Field>
              <Field label="Воркеры (baseline)" hint="Параллельных воркеров для baseline-загрузки (1–32)">
                <input style={S.input} type="number" value={form.baseline_parallel_degree} min={1} max={32}
                  onChange={e => setF({ baseline_parallel_degree: Math.max(1, Math.min(32, parseInt(e.target.value) || 4)) })} />
              </Field>
            </div>
            <Field label="Валидация stage">
              <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
                <input type="checkbox"
                  checked={form.validate_hash_sample}
                  onChange={e => setF({ validate_hash_sample: e.target.checked })}
                />
                <span style={{ fontSize: 12, color: "#94a3b8" }}>
                  Hash/sample проверка (сравнивает выборку строк source vs stage)
                </span>
              </label>
            </Field>
          </Section>

          {/* ── Key ── */}
          <Section title="Конфигурация ключа">
            {!form.source_schema || !form.source_table ? (
              <div style={{ fontSize: 12, color: "#475569" }}>
                Выберите таблицу источника для определения ключа
              </div>
            ) : (
              <>
                <Field label="Тип ключа" required error={fieldErrs.effective_key_type}>
                  <div style={{ display: "flex", gap: 8, flexWrap: "wrap" as const }}>
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

                {/* PK display */}
                {form.effective_key_type === "PRIMARY_KEY" && tableInfo && (
                  <div style={{ fontSize: 12, color: "#86efac" }}>
                    Колонки: <strong>{tableInfo.pk_columns.join(", ")}</strong>
                  </div>
                )}

                {/* UK selector */}
                {form.effective_key_type === "UNIQUE_KEY" && tableInfo && (
                  <>
                    {tableInfo.uk_constraints.length > 1 && (
                      <Field label="Уникальный индекс">
                        <select style={S.select} value={form.selected_uk_index}
                          onChange={e => {
                            const idx = parseInt(e.target.value);
                            setF({
                              selected_uk_index: idx,
                              effective_key_columns: tableInfo.uk_constraints[idx]?.columns ?? [],
                            });
                          }}>
                          {tableInfo.uk_constraints.map((uk, i) => (
                            <option key={uk.name} value={i}>
                              {uk.name} ({uk.columns.join(", ")})
                            </option>
                          ))}
                        </select>
                      </Field>
                    )}
                    <div style={{ fontSize: 12, color: "#c4b5fd" }}>
                      Колонки: <strong>
                        {tableInfo.uk_constraints[form.selected_uk_index]?.columns.join(", ")}
                      </strong>
                    </div>
                  </>
                )}

                {/* User-defined column picker */}
                {form.effective_key_type === "USER_DEFINED" && tableInfo && (
                  <Field label="Выберите колонки ключа" required
                    error={fieldErrs.effective_key_columns as string}>
                    <div style={{
                      maxHeight: 180, overflowY: "auto" as const,
                      border: "1px solid #334155", borderRadius: 5, background: "#1e293b",
                    }}>
                      {tableInfo.columns.map(col => (
                        <label key={col.name} style={{
                          display: "flex", alignItems: "center", gap: 8,
                          padding: "5px 10px", cursor: "pointer",
                          borderBottom: "1px solid #0f172a",
                        }}>
                          <input type="checkbox"
                            checked={form.effective_key_columns.includes(col.name)}
                            onChange={e => toggleKeyCol(col.name, e.target.checked)}
                          />
                          <span style={{ fontSize: 12, color: "#e2e8f0", fontFamily: "monospace" }}>
                            {col.name}
                          </span>
                          <span style={{ fontSize: 10, color: "#475569" }}>{col.type}</span>
                          {!col.nullable && (
                            <span style={{ fontSize: 10, color: "#ef4444", marginLeft: "auto" }}>
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
            <Field label="Название миграции" required error={fieldErrs.migration_name}
              hint={!nameTouched.current ? "Автоматически заполняется из источника и цели" : undefined}>
              <input style={fieldErrs.migration_name ? S.inputErr : S.input}
                value={form.migration_name}
                placeholder="Выберите таблицы для автозаполнения"
                onChange={e => { nameTouched.current = true; setF({ migration_name: e.target.value }); }}
              />
            </Field>
          </Section>

          {/* Duplicate warning */}
          {dupWarning && (
            <div style={{
              background: "#451a03", border: "1px solid #92400e", borderRadius: 6,
              padding: "10px 14px", fontSize: 12, color: "#fcd34d",
            }}>
              ⚠ {dupWarning}
            </div>
          )}

          {/* Submit error */}
          {submitErr && (
            <div style={{
              background: "#450a0a", border: "1px solid #7f1d1d", borderRadius: 6,
              padding: "10px 14px", fontSize: 12, color: "#fca5a5",
            }}>
              {submitErr}
            </div>
          )}
        </div>

        {/* Footer */}
        <div style={S.footer}>
          <button onClick={onClose} style={{
            background: "none", border: "1px solid #334155", borderRadius: 6,
            color: "#64748b", padding: "7px 18px", fontSize: 13, cursor: "pointer",
          }}>
            Отмена
          </button>
          <button onClick={handleSubmit} disabled={submitting} style={{
            background: submitting ? "#1e3a5f" : "#1d4ed8",
            border: "none", borderRadius: 6,
            color: submitting ? "#64748b" : "#fff",
            padding: "7px 22px", fontSize: 13, fontWeight: 700,
            cursor: submitting ? "not-allowed" : "pointer",
          }}>
            {submitting ? "Сохранение…" : "Создать миграцию"}
          </button>
        </div>
      </div>
    </div>
  );
}
