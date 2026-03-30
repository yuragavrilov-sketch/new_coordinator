import React, { useState, useEffect, useCallback, useMemo } from "react";
import { S } from "./styles";
import { ObjectTabs, ObjectTabId } from "./ObjectTabs";
import { TablesTab, CatalogObject } from "./TablesTab";
import { ViewsTab } from "./ViewsTab";
import { CodeTab } from "./CodeTab";
import { OtherTab } from "./OtherTab";
import { PlannerWizard } from "./PlannerWizard";

// ── SearchSelect ────────────────────────────────────────────────────────────

function SearchSelect({
  value, onChange, options, placeholder, disabled,
}: {
  value: string; onChange: (v: string) => void; options: string[];
  placeholder: string; disabled?: boolean;
}) {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const wrapRef = React.useRef<HTMLDivElement>(null);
  const inputRef = React.useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) { setOpen(false); setQuery(""); }
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [open]);
  useEffect(() => { if (open) inputRef.current?.focus(); }, [open]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    return q ? options.filter(o => o.toLowerCase().includes(q)) : options;
  }, [options, query]);

  return (
    <div ref={wrapRef} style={{ position: "relative", minWidth: 165 }}>
      <div onClick={() => !disabled && (setOpen(o => !o), setQuery(""))}
        style={{
          display: "flex", alignItems: "center", gap: 6,
          background: "#1e293b", border: `1px solid ${open ? "#3b82f6" : "#334155"}`,
          borderRadius: 4, padding: "0 8px", height: 30,
          cursor: disabled ? "not-allowed" : "pointer", opacity: disabled ? 0.5 : 1,
        }}>
        <span style={{ fontSize: 12, flex: 1, color: value ? "#e2e8f0" : "#475569", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
          {value || placeholder}
        </span>
        <span style={{ color: "#475569", fontSize: 9 }}>{open ? "\u25B2" : "\u25BC"}</span>
      </div>
      {open && (
        <div style={{ position: "absolute", top: "calc(100% + 3px)", left: 0, right: 0, background: "#1e293b", border: "1px solid #334155", borderRadius: 4, zIndex: 200, boxShadow: "0 6px 20px rgba(0,0,0,0.5)" }}>
          <div style={{ padding: "6px 8px", borderBottom: "1px solid #0f1e35", display: "flex", alignItems: "center", gap: 6 }}>
            <input ref={inputRef} value={query} onChange={e => setQuery(e.target.value)}
              onKeyDown={e => { if (e.key === "Escape") { setOpen(false); setQuery(""); } if (e.key === "Enter" && filtered.length === 1) { onChange(filtered[0]); setOpen(false); setQuery(""); } }}
              placeholder="Поиск..." style={{ background: "none", border: "none", color: "#e2e8f0", fontSize: 12, width: "100%", outline: "none" }} />
          </div>
          <div style={{ maxHeight: 200, overflowY: "auto" }}>
            {filtered.length === 0
              ? <div style={{ padding: "8px 10px", color: "#475569", fontSize: 12 }}>Нет совпадений</div>
              : filtered.map(o => (
                <div key={o} onMouseDown={() => { onChange(o); setOpen(false); setQuery(""); }}
                  style={{ padding: "6px 10px", fontSize: 12, cursor: "pointer", background: o === value ? "#1d3a5f" : "transparent", color: o === value ? "#93c5fd" : "#e2e8f0" }}
                  onMouseEnter={e => (e.currentTarget.style.background = o === value ? "#1d3a5f" : "#0f1624")}
                  onMouseLeave={e => (e.currentTarget.style.background = o === value ? "#1d3a5f" : "transparent")}>
                  {o}
                </div>
              ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Type grouping ───────────────────────────────────────────────────────────

const TABLE_TYPES = new Set(["TABLE"]);
const VIEW_TYPES = new Set(["VIEW", "MATERIALIZED VIEW"]);
const CODE_TYPES = new Set(["FUNCTION", "PROCEDURE", "PACKAGE"]);
const OTHER_TYPES = new Set(["SEQUENCE", "SYNONYM", "TYPE"]);

function groupObjects(objects: CatalogObject[], typeMap: Map<string, string>) {
  const tables: CatalogObject[] = [];
  const views: CatalogObject[] = [];
  const code: CatalogObject[] = [];
  const other: CatalogObject[] = [];
  for (const obj of objects) {
    const oType = typeMap.get(obj.object_name) || "TABLE";
    if (TABLE_TYPES.has(oType)) tables.push(obj);
    else if (VIEW_TYPES.has(oType)) views.push(obj);
    else if (CODE_TYPES.has(oType)) code.push(obj);
    else other.push(obj);
  }
  return { tables, views, code, other };
}

// ── Main Component ──────────────────────────────────────────────────────────

export function DDLCatalog() {
  const [srcSchema, setSrcSchema] = useState("");
  const [tgtSchema, setTgtSchema] = useState("");
  const [srcSchemas, setSrcSchemas] = useState<string[]>([]);
  const [tgtSchemas, setTgtSchemas] = useState<string[]>([]);

  const [snapshotId, setSnapshotId] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loadedAt, setLoadedAt] = useState<string | null>(null);
  const [objectCounts, setObjectCounts] = useState<Record<string, number>>({});

  const [allObjects, setAllObjects] = useState<CatalogObject[]>([]);
  const [objectTypeMap, setObjectTypeMap] = useState<Map<string, string>>(new Map());
  const [activeTab, setActiveTab] = useState<ObjectTabId>("tables");
  const [currentTypeLoading, setCurrentTypeLoading] = useState(false);

  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [syncBusy, setSyncBusy] = useState<Set<string>>(new Set());

  const [showWizard, setShowWizard] = useState(false);

  useEffect(() => {
    fetch("/api/db/source/schemas").then(r => r.json()).then(d => Array.isArray(d) && setSrcSchemas(d)).catch(() => {});
    fetch("/api/db/target/schemas").then(r => r.json()).then(d => Array.isArray(d) && setTgtSchemas(d)).catch(() => {});
  }, []);

  const doLoad = useCallback(() => {
    if (!srcSchema || !tgtSchema) return;
    setLoading(true); setLoadError(null);
    fetch("/api/catalog/load", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ src_schema: srcSchema, tgt_schema: tgtSchema }),
    })
      .then(r => r.ok ? r.json() : r.json().then(d => Promise.reject(d.error || "Ошибка")))
      .then(data => {
        setSnapshotId(data.snapshot_id);
        setObjectCounts(data.object_counts || {});
        setLoadedAt(new Date().toLocaleString());
        setAllObjects([]);
        setObjectTypeMap(new Map());
        setActiveTab("tables");
      })
      .catch(e => setLoadError(typeof e === "string" ? e : String(e)))
      .finally(() => setLoading(false));
  }, [srcSchema, tgtSchema]);

  const loadObjectsForTab = useCallback((tab: ObjectTabId) => {
    if (!snapshotId) return;
    setCurrentTypeLoading(true);

    const typesByTab: Record<ObjectTabId, string[]> = {
      tables: ["TABLE"],
      views: ["VIEW", "MATERIALIZED VIEW"],
      code: ["FUNCTION", "PROCEDURE", "PACKAGE"],
      other: ["SEQUENCE", "SYNONYM", "TYPE"],
    };

    const types = typesByTab[tab];
    const fetches = types.map(t =>
      fetch(`/api/catalog/objects?snapshot_id=${snapshotId}&type=${t === "MATERIALIZED VIEW" ? "MVIEW" : t}`)
        .then(r => r.ok ? r.json() : [])
        .then((objs: CatalogObject[]) => objs.map(o => ({ ...o, _type: t })))
    );

    Promise.all(fetches)
      .then(results => {
        const flat = results.flat();
        setAllObjects(flat);
        const tMap = new Map<string, string>();
        for (const obj of flat) tMap.set(obj.object_name, (obj as unknown as { _type: string })._type);
        setObjectTypeMap(tMap);
      })
      .catch(() => {})
      .finally(() => setCurrentTypeLoading(false));
  }, [snapshotId]);

  useEffect(() => {
    if (snapshotId) loadObjectsForTab(activeTab);
  }, [snapshotId, activeTab, loadObjectsForTab]);

  const grouped = useMemo(() => groupObjects(allObjects, objectTypeMap), [allObjects, objectTypeMap]);

  const counts = useMemo(() => ({
    tables: objectCounts["TABLE"] || 0,
    views: (objectCounts["VIEW"] || 0) + (objectCounts["MATERIALIZED VIEW"] || 0),
    code: (objectCounts["FUNCTION"] || 0) + (objectCounts["PROCEDURE"] || 0) + (objectCounts["PACKAGE"] || 0),
    other: (objectCounts["SEQUENCE"] || 0) + (objectCounts["SYNONYM"] || 0) + (objectCounts["TYPE"] || 0),
  }), [objectCounts]);

  const toggleTable = (name: string) => {
    setSelected(prev => { const n = new Set(prev); if (n.has(name)) n.delete(name); else n.add(name); return n; });
  };
  const toggleAllTables = () => {
    const tableNames = grouped.tables.map(t => t.object_name);
    if (tableNames.every(t => selected.has(t))) setSelected(new Set());
    else setSelected(new Set(tableNames));
  };

  const doCompare = useCallback((type: string, name: string) => {
    if (!snapshotId) return;
    setSyncBusy(prev => new Set(prev).add(name));
    fetch("/api/catalog/compare", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ snapshot_id: snapshotId, src_schema: srcSchema, tgt_schema: tgtSchema, objects: [`${type}:${name}`] }),
    })
      .then(() => loadObjectsForTab(activeTab))
      .catch(() => {})
      .finally(() => setSyncBusy(prev => { const n = new Set(prev); n.delete(name); return n; }));
  }, [snapshotId, srcSchema, tgtSchema, activeTab, loadObjectsForTab]);

  const doSync = useCallback((type: string, name: string, action: string) => {
    setSyncBusy(prev => new Set(prev).add(name));

    if (type === "TABLE") {
      let url = "";
      const body: Record<string, unknown> = {
        src_schema: srcSchema, src_table: name,
        tgt_schema: tgtSchema, tgt_table: name,
      };
      if (action === "create") url = "/api/target-prep/ensure-table";
      else if (action === "sync_cols") url = "/api/target-prep/sync-columns";
      else { url = "/api/target-prep/sync-objects"; body.types = ["constraints", "indexes", "triggers"]; }

      fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) })
        .then(() => doCompare(type, name))
        .catch(() => {})
        .finally(() => setSyncBusy(prev => { const n = new Set(prev); n.delete(name); return n; }));
      return;
    }

    fetch("/api/catalog/sync-to-target", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ src_schema: srcSchema, tgt_schema: tgtSchema, object_type: type, object_name: name, action }),
    })
      .then(() => doCompare(type, name))
      .catch(() => {})
      .finally(() => setSyncBusy(prev => { const n = new Set(prev); n.delete(name); return n; }));
  }, [srcSchema, tgtSchema, doCompare]);

  const selectedTables = useMemo(() => Array.from(selected), [selected]);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 0 }}>
      <div style={{ display: "flex", alignItems: "flex-end", gap: 12, marginBottom: 16 }}>
        <div style={S.field}>
          <label style={S.label}>Схема источника</label>
          <SearchSelect value={srcSchema} onChange={setSrcSchema} options={srcSchemas} placeholder="Выберите схему" />
        </div>
        <div style={S.field}>
          <label style={S.label}>Схема таргета</label>
          <SearchSelect value={tgtSchema} onChange={setTgtSchema} options={tgtSchemas} placeholder="Выберите схему" />
        </div>
        <button
          onClick={doLoad}
          disabled={!srcSchema || !tgtSchema || loading}
          style={{ ...S.btnPrimary, opacity: (!srcSchema || !tgtSchema || loading) ? 0.5 : 1, height: 30 }}
        >
          {loading ? "Загрузка каталога..." : "Загрузить каталог"}
        </button>
        {loadedAt && (
          <span style={{ fontSize: 11, color: "#475569" }}>Загружено: {loadedAt}</span>
        )}
      </div>

      {loadError && (
        <div style={{ background: "#7f1d1d22", border: "1px solid #7f1d1d", borderRadius: 6, color: "#fca5a5", padding: "8px 14px", fontSize: 12, marginBottom: 12 }}>
          {loadError}
        </div>
      )}

      {snapshotId && (
        <>
          <ObjectTabs active={activeTab} onChange={setActiveTab} counts={counts} />

          {currentTypeLoading ? (
            <div style={{ padding: 20, textAlign: "center", color: "#475569", fontSize: 12 }}>Загрузка объектов...</div>
          ) : (
            <>
              {activeTab === "tables" && (
                <TablesTab
                  objects={grouped.tables} selected={selected}
                  onToggle={toggleTable} onToggleAll={toggleAllTables}
                  syncBusy={syncBusy} onCompare={doCompare} onSync={doSync}
                />
              )}
              {activeTab === "views" && (
                <ViewsTab objects={grouped.views} syncBusy={syncBusy} onCompare={doCompare} onSync={doSync} />
              )}
              {activeTab === "code" && (
                <CodeTab objects={grouped.code} syncBusy={syncBusy} onCompare={doCompare} onSync={doSync} />
              )}
              {activeTab === "other" && (
                <OtherTab objects={grouped.other} syncBusy={syncBusy} onCompare={doCompare} onSync={doSync} />
              )}
            </>
          )}

          {selected.size > 0 && !showWizard && (
            <div style={{ marginTop: 16, textAlign: "center" }}>
              <button onClick={() => setShowWizard(true)} style={S.btnPrimary}>
                Запустить визард для выбранных ({selected.size})
              </button>
            </div>
          )}

          {showWizard && selectedTables.length > 0 && (
            <PlannerWizard
              selectedTables={selectedTables}
              srcSchema={srcSchema} tgtSchema={tgtSchema}
              onClose={() => setShowWizard(false)}
            />
          )}
        </>
      )}
    </div>
  );
}
