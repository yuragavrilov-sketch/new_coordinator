import React, { useState, useEffect, useMemo } from "react";
import { S } from "./styles";
import { MatchBadge, MigrationBadge } from "./StatusBadges";
import { ObjectActions } from "./ObjectActions";
import { Pagination, usePagination } from "./Pagination";

export interface CatalogObject {
  object_name: string;
  oracle_status: string;
  last_ddl_time: string | null;
  metadata: Record<string, unknown>;
  match_status: string;
  diff: Record<string, unknown>;
  migration_status: string;
  _type?: string;  // real Oracle object_type, set during load
}

interface Props {
  objects: CatalogObject[];
  snapshotId: number | null;
  selected: Set<string>;
  onToggle: (name: string) => void;
  onToggleAll: (names: string[]) => void;
  syncBusy: Set<string>;
  onCompare: (type: string, name: string) => void;
  onSync: (type: string, name: string, action: string) => void;
}

type StatusFilter = "ALL" | "OK" | "DIFF" | "MISSING";

function fmtType(c: Record<string, unknown>): string {
  const dt = (c.data_type as string) ?? "";
  const prec = c.data_precision as number | null;
  const scale = c.data_scale as number | null;
  const len = c.data_length as number | null;
  if (["NUMBER"].includes(dt)) {
    if (prec != null && scale != null) return `NUMBER(${prec},${scale})`;
    if (prec != null) return `NUMBER(${prec})`;
    return "NUMBER";
  }
  if (["VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"].includes(dt)) {
    return len != null ? `${dt}(${len})` : dt;
  }
  return dt;
}

function ColsTable({ cols }: { cols: Record<string, unknown>[] }) {
  return (
    <table style={{ width: "100%", borderCollapse: "collapse" }}>
      <thead>
        <tr>
          {["#", "Имя", "Тип", "Nullable", "Default"].map(h => (
            <th key={h} style={S.th}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {cols.map((c, i) => (
          <tr key={i} style={S.trBorder}>
            <td style={{ ...S.td, color: "#475569" }}>{(c.column_id as number) ?? i + 1}</td>
            <td style={{ ...S.td, color: "#e2e8f0" }}>{(c.name ?? c.column_name) as string}</td>
            <td style={{ ...S.td, color: "#94a3b8" }}>{fmtType(c)}</td>
            <td style={S.td}>
              {(c.nullable === true || c.nullable === "Y")
                ? <span style={S.badge("#22c55e22", "#22c55e")}>YES</span>
                : <span style={S.badge("#ef444422", "#ef4444")}>NO</span>}
            </td>
            <td style={{ ...S.td, color: "#64748b", fontFamily: "monospace" }}>
              {(c.data_default as string) ?? "—"}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function IdxTable({ idxs }: { idxs: Record<string, unknown>[] }) {
  return (
    <table style={{ width: "100%", borderCollapse: "collapse" }}>
      <thead>
        <tr>
          {["Имя", "Тип / Unique", "Колонки", "Статус"].map(h => (
            <th key={h} style={S.th}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {idxs.map((idx, i) => {
          // columns can be string[] or {column_name}[]
          const rawCols = (idx.columns ?? []) as unknown[];
          const colStr = rawCols.map((c: unknown) => typeof c === "string" ? c : (c as Record<string, unknown>).column_name as string).join(", ");
          return (
            <tr key={i} style={S.trBorder}>
              <td style={{ ...S.td, color: "#e2e8f0" }}>{(idx.name ?? idx.index_name) as string}</td>
              <td style={S.td}>
                <span style={S.badge("#3b82f622", "#3b82f6")}>{idx.index_type as string}</span>
                {(idx.unique === true || idx.uniqueness === "UNIQUE") && (
                  <span style={{ ...S.badge("#8b5cf622", "#8b5cf6"), marginLeft: 4 }}>UNIQUE</span>
                )}
              </td>
              <td style={{ ...S.td, color: "#94a3b8" }}>{colStr}</td>
              <td style={S.td}>
                <span style={S.badge(
                  idx.status === "VALID" ? "#22c55e22" : "#ef444422",
                  idx.status === "VALID" ? "#22c55e" : "#ef4444"
                )}>{idx.status as string}</span>
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function ConstrTable({ constrs }: { constrs: Record<string, unknown>[] }) {
  const typeColor: Record<string, [string, string]> = {
    P: ["#3b82f622", "#3b82f6"], "PRIMARY KEY": ["#3b82f622", "#3b82f6"],
    U: ["#8b5cf622", "#8b5cf6"], "UNIQUE": ["#8b5cf622", "#8b5cf6"],
    R: ["#eab30822", "#eab308"], "FOREIGN KEY": ["#eab30822", "#eab308"],
    C: ["#0ea5e922", "#0ea5e9"], "CHECK": ["#0ea5e922", "#0ea5e9"],
  };
  return (
    <table style={{ width: "100%", borderCollapse: "collapse" }}>
      <thead>
        <tr>
          {["Имя", "Тип", "Колонки", "Статус"].map(h => (
            <th key={h} style={S.th}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {constrs.map((c, i) => {
          // columns can be string[] or {column_name}[]
          const rawCols = (c.columns ?? []) as unknown[];
          const colStr = rawCols.map((x: unknown) => typeof x === "string" ? x : (x as Record<string, unknown>).column_name as string).join(", ");
          const ct = (c.type_code ?? c.type ?? c.constraint_type ?? "") as string;
          const label = (c.type ?? ct) as string;
          const [bg, fg] = typeColor[ct] ?? typeColor[label] ?? ["#33415522", "#475569"];
          return (
            <tr key={i} style={S.trBorder}>
              <td style={{ ...S.td, color: "#e2e8f0" }}>{(c.name ?? c.constraint_name) as string}</td>
              <td style={S.td}><span style={S.badge(bg, fg)}>{label}</span></td>
              <td style={{ ...S.td, color: "#94a3b8" }}>{colStr}</td>
              <td style={S.td}>
                <span style={S.badge(
                  c.status === "ENABLED" ? "#22c55e22" : "#ef444422",
                  c.status === "ENABLED" ? "#22c55e" : "#ef4444"
                )}>{c.status as string}</span>
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function TrigTable({ trigs }: { trigs: Record<string, unknown>[] }) {
  return (
    <table style={{ width: "100%", borderCollapse: "collapse" }}>
      <thead>
        <tr>
          {["Имя", "Тип", "Событие", "Статус"].map(h => (
            <th key={h} style={S.th}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {trigs.map((t, i) => (
          <tr key={i} style={S.trBorder}>
            <td style={{ ...S.td, color: "#e2e8f0" }}>{(t.name ?? t.trigger_name) as string}</td>
            <td style={{ ...S.td, color: "#94a3b8" }}>{t.trigger_type as string}</td>
            <td style={{ ...S.td, color: "#94a3b8" }}>{(t.event ?? t.triggering_event) as string}</td>
            <td style={S.td}>
              <span style={S.badge(
                t.status === "ENABLED" ? "#22c55e22" : "#ef444422",
                t.status === "ENABLED" ? "#22c55e" : "#ef4444"
              )}>{t.status as string}</span>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function DiffSummary({ diff }: { diff: Record<string, unknown> }) {
  if (!diff || diff.ok === true) return null;

  const items: { label: string; names: string[] }[] = [];
  const colsMissing = (diff.cols_missing ?? []) as string[];
  const colsExtra = (diff.cols_extra ?? []) as string[];
  const colsType = (diff.cols_type ?? []) as string[];
  const idxMissing = (diff.idx_missing ?? []) as string[];
  const idxDisabled = (diff.idx_disabled ?? []) as string[];
  const conMissing = (diff.con_missing ?? []) as string[];
  const conDisabled = (diff.con_disabled ?? []) as string[];
  const trgMissing = (diff.trg_missing ?? []) as string[];

  if (colsMissing.length) items.push({ label: "Колонки отсутствуют на таргете", names: colsMissing });
  if (colsExtra.length) items.push({ label: "Лишние колонки на таргете", names: colsExtra });
  if (colsType.length) items.push({ label: "Различие типов колонок", names: colsType });
  if (idxMissing.length) items.push({ label: "Индексы отсутствуют", names: idxMissing });
  if (idxDisabled.length) items.push({ label: "Индексы невалидные", names: idxDisabled });
  if (conMissing.length) items.push({ label: "Ограничения отсутствуют", names: conMissing });
  if (conDisabled.length) items.push({ label: "Ограничения отключены", names: conDisabled });
  if (trgMissing.length) items.push({ label: "Триггеры отсутствуют", names: trgMissing });

  if (items.length === 0) return null;

  return (
    <div style={{
      background: "#1c1007", border: "1px solid #854d0e44", borderRadius: 6,
      padding: "10px 14px", marginBottom: 6,
    }}>
      <div style={{ fontSize: 11, fontWeight: 700, color: "#eab308", marginBottom: 6 }}>РАЗЛИЧИЯ С ТАРГЕТОМ</div>
      {items.map((item, i) => (
        <div key={i} style={{ marginBottom: 4 }}>
          <span style={{ fontSize: 11, color: "#fbbf24", fontWeight: 600 }}>{item.label}: </span>
          <span style={{ fontSize: 11, color: "#fde68a", fontFamily: "monospace" }}>
            {item.names.join(", ")}
          </span>
        </div>
      ))}
    </div>
  );
}

function TargetCompare({ srcCols, tgtCols }: { srcCols: Record<string, unknown>[]; tgtCols: Record<string, unknown>[] }) {
  const tgtMap = new Map<string, Record<string, unknown>>();
  for (const c of tgtCols) tgtMap.set((c.name ?? c.column_name) as string, c);
  const srcMap = new Map<string, Record<string, unknown>>();
  for (const c of srcCols) srcMap.set((c.name ?? c.column_name) as string, c);

  const allNames = new Set([...srcMap.keys(), ...tgtMap.keys()]);
  const rows: { name: string; srcType: string; tgtType: string; status: string }[] = [];
  for (const name of allNames) {
    const s = srcMap.get(name);
    const t = tgtMap.get(name);
    if (s && !t) rows.push({ name, srcType: fmtType(s), tgtType: "—", status: "missing" });
    else if (!s && t) rows.push({ name, srcType: "—", tgtType: fmtType(t), status: "extra" });
    else if (s && t) {
      const st = fmtType(s), tt = fmtType(t);
      rows.push({ name, srcType: st, tgtType: tt, status: st === tt ? "ok" : "type" });
    }
  }
  const diffs = rows.filter(r => r.status !== "ok");
  if (diffs.length === 0) return null;

  const colors: Record<string, string> = { missing: "#ef4444", extra: "#8b5cf6", type: "#eab308", ok: "#22c55e" };
  const labels: Record<string, string> = { missing: "Нет на тгт", extra: "Лишняя", type: "Тип отлич.", ok: "OK" };

  return (
    <div style={{ border: "1px solid #854d0e44", borderRadius: 6, overflow: "hidden", marginBottom: 6 }}>
      <div style={{ padding: "5px 12px", fontSize: 11, fontWeight: 700, color: "#eab308", background: "#1c1007", borderBottom: "1px solid #854d0e44" }}>
        СРАВНЕНИЕ КОЛОНОК (только различия: {diffs.length})
      </div>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr>
            {["Колонка", "Source тип", "Target тип", "Статус"].map(h => <th key={h} style={S.th}>{h}</th>)}
          </tr>
        </thead>
        <tbody>
          {diffs.map(r => (
            <tr key={r.name} style={S.trBorder}>
              <td style={{ ...S.td, color: "#e2e8f0", fontFamily: "monospace" }}>{r.name}</td>
              <td style={{ ...S.td, color: "#94a3b8", fontFamily: "monospace" }}>{r.srcType}</td>
              <td style={{ ...S.td, color: colors[r.status], fontFamily: "monospace" }}>{r.tgtType}</td>
              <td style={S.td}><span style={S.badge(colors[r.status] + "22", colors[r.status])}>{labels[r.status]}</span></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function TableDetail({ obj, snapshotId }: { obj: CatalogObject; snapshotId: number | null }) {
  const [tgtMeta, setTgtMeta] = useState<Record<string, unknown> | null>(null);

  useEffect(() => {
    if (!snapshotId || obj.match_status !== "DIFF") return;
    fetch(`/api/catalog/objects/${obj.object_name}/detail?snapshot_id=${snapshotId}&type=TABLE`)
      .then(r => r.ok ? r.json() : null)
      .then(d => d && setTgtMeta(d.target ?? null))
      .catch(() => {});
  }, [obj.object_name, obj.match_status, snapshotId]);

  const meta = obj.metadata;
  const cols = (meta.columns as Record<string, unknown>[]) ?? [];
  const idxs = (meta.indexes as Record<string, unknown>[]) ?? [];
  const constrs = (meta.constraints as Record<string, unknown>[]) ?? [];
  const trigs = (meta.triggers as Record<string, unknown>[]) ?? [];

  const sectionStyle: React.CSSProperties = {
    background: "#07101e",
    border: "1px solid #1e293b",
    borderRadius: 6,
    marginTop: 4,
    overflow: "hidden",
  };
  const sectionHeader: React.CSSProperties = {
    padding: "5px 12px",
    fontSize: 11,
    fontWeight: 700,
    color: "#64748b",
    letterSpacing: 0.5,
    background: "#0a111f",
    borderBottom: "1px solid #1e293b",
  };

  return (
    <td colSpan={6} style={{ padding: "8px 16px 12px 32px", background: "#07101e" }}>
      {obj.match_status === "DIFF" && <DiffSummary diff={obj.diff} />}
      {obj.match_status === "DIFF" && tgtMeta && (
        <TargetCompare
          srcCols={cols}
          tgtCols={(tgtMeta.columns as Record<string, unknown>[]) ?? []}
        />
      )}
      {cols.length > 0 && (
        <div style={sectionStyle}>
          <div style={sectionHeader}>КОЛОНКИ ({cols.length})</div>
          <ColsTable cols={cols} />
        </div>
      )}
      {idxs.length > 0 && (
        <div style={sectionStyle}>
          <div style={sectionHeader}>ИНДЕКСЫ ({idxs.length})</div>
          <IdxTable idxs={idxs} />
        </div>
      )}
      {constrs.length > 0 && (
        <div style={sectionStyle}>
          <div style={sectionHeader}>ОГРАНИЧЕНИЯ ({constrs.length})</div>
          <ConstrTable constrs={constrs} />
        </div>
      )}
      {trigs.length > 0 && (
        <div style={sectionStyle}>
          <div style={sectionHeader}>ТРИГГЕРЫ ({trigs.length})</div>
          <TrigTable trigs={trigs} />
        </div>
      )}
    </td>
  );
}

export function TablesTab({ objects, snapshotId, selected, onToggle, onToggleAll, syncBusy, onCompare, onSync }: Props) {
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("ALL");
  const [expandedObj, setExpandedObj] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);

  // Reset page on filter change
  useEffect(() => { setPage(0); }, [search, statusFilter]);

  const filtered = useMemo(() => {
    return objects.filter(o => {
      const matchSearch = o.object_name.toLowerCase().includes(search.toLowerCase());
      const matchStatus =
        statusFilter === "ALL" ? true :
        statusFilter === "OK" ? o.match_status === "MATCH" :
        statusFilter === "DIFF" ? o.match_status === "DIFF" :
        statusFilter === "MISSING" ? o.match_status === "MISSING" : true;
      return matchSearch && matchStatus;
    });
  }, [objects, search, statusFilter]);

  const paged = usePagination(filtered, pageSize, page);
  const filteredNames = filtered.map(o => o.object_name);
  const allSelected = filteredNames.length > 0 && filteredNames.every(n => selected.has(n));

  const filterBtns: StatusFilter[] = ["ALL", "OK", "DIFF", "MISSING"];
  const filterLabels: Record<StatusFilter, string> = {
    ALL: "Все", OK: "OK", DIFF: "Diff", MISSING: "Missing",
  };

  return (
    <div style={S.card}>
      <div style={S.cardHeader}>
        <input
          value={search}
          onChange={e => setSearch(e.target.value)}
          placeholder="Поиск таблиц..."
          style={{ ...S.input, width: 220 }}
        />
        <div style={{ display: "flex", gap: 4 }}>
          {filterBtns.map(f => (
            <button
              key={f}
              onClick={() => setStatusFilter(f)}
              style={statusFilter === f ? S.btnPrimary : S.btnSecondary}
            >
              {filterLabels[f]}
            </button>
          ))}
        </div>
        <span style={{ marginLeft: "auto", fontSize: 12, color: "#475569" }}>
          {filtered.length} / {objects.length}
        </span>
      </div>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ background: "#0a111f" }}>
            <th style={{ ...S.th, width: 32 }}>
              <input
                type="checkbox"
                checked={allSelected}
                onChange={() => onToggleAll(filteredNames)}
              />
            </th>
            <th style={S.th}>Таблица</th>
            <th style={S.th}>Совпадение</th>
            <th style={S.th}>Миграция</th>
            <th style={S.th}>Действия</th>
          </tr>
        </thead>
        <tbody>
          {paged.map(obj => {
            const expanded = expandedObj === obj.object_name;
            return (
              <React.Fragment key={obj.object_name}>
                <tr style={S.trBorder}>
                  <td style={{ ...S.td, width: 32 }}>
                    <input
                      type="checkbox"
                      checked={selected.has(obj.object_name)}
                      onChange={() => onToggle(obj.object_name)}
                    />
                  </td>
                  <td style={S.td}>
                    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                      <button
                        onClick={() => setExpandedObj(expanded ? null : obj.object_name)}
                        style={{
                          background: "none", border: "none", color: "#475569",
                          cursor: "pointer", fontSize: 12, padding: "0 2px",
                        }}
                      >
                        {expanded ? "▼" : "▶"}
                      </button>
                      <span style={{ color: "#e2e8f0", fontFamily: "monospace" }}>
                        {obj.object_name}
                      </span>
                    </div>
                  </td>
                  <td style={S.td}><MatchBadge status={obj.match_status} /></td>
                  <td style={S.td}><MigrationBadge status={obj.migration_status} /></td>
                  <td style={S.td}>
                    <ObjectActions
                      objectType="TABLE"
                      objectName={obj.object_name}
                      matchStatus={obj.match_status}
                      syncBusy={syncBusy.has(obj.object_name)}
                      onCompare={onCompare}
                      onSync={onSync}
                      onShowDetail={() => setExpandedObj(expanded ? null : obj.object_name)}
                    />
                  </td>
                </tr>
                {expanded && (
                  <tr style={{ background: "#07101e" }}>
                    <TableDetail obj={obj} snapshotId={snapshotId} />
                  </tr>
                )}
              </React.Fragment>
            );
          })}
        </tbody>
      </table>
      <Pagination total={filtered.length} page={page} pageSize={pageSize} onPage={setPage} onPageSize={setPageSize} />
    </div>
  );
}
