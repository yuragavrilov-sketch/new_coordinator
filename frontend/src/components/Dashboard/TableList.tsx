import { useState, useMemo } from "react";
import { TableRow } from "./TableRow";
import { TableDetail } from "./TableDetail";

export interface EnrichedTable {
  object_name: string;
  oracle_status: string | null;
  migration_status: "NONE" | "PLANNED" | "IN_PROGRESS" | "COMPLETED" | "FAILED";
  match_status: string;
  metadata?: {
    pk_columns?: string[];
    uk_constraints?: { name: string; columns: string[] }[];
    num_rows?: number | null;
    columns?: { name: string; data_type: string }[];
  };
  migration?: {
    migration_id: string;
    migration_name: string;
    phase: string;
    chunks_done: number;
    total_chunks: number | null;
    rows_loaded: number;
    group_id: string | null;
    state_changed_at: string;
    error_text: string | null;
  };
}

type Filter = "all" | "none" | "active" | "completed" | "errors";
type SortKey = "name" | "status" | "rows" | "phase";
type SortDir = "asc" | "desc";

interface Props {
  tables: EnrichedTable[];
  selected: Set<string>;
  onToggleSelect: (name: string) => void;
  onToggleAll: () => void;
  expandedTable: string | null;
  onExpandTable: (name: string | null) => void;
  filter: Filter;
  onFilterChange: (f: Filter) => void;
  search: string;
  onSearchChange: (s: string) => void;
  schema: string;
  onCreateMigration: (tableName: string) => void;
  onMigrationChanged?: () => void;
}

const FILTER_BTNS: { key: Filter; label: string }[] = [
  { key: "all", label: "Все" },
  { key: "none", label: "Без миграции" },
  { key: "active", label: "Активные" },
  { key: "completed", label: "Завершённые" },
  { key: "errors", label: "Ошибки" },
];

const PAGE_SIZES = [25, 50, 100, 0]; // 0 = all

const STATUS_ORDER: Record<string, number> = {
  IN_PROGRESS: 0, PLANNED: 1, FAILED: 2, COMPLETED: 3, NONE: 4,
};

const LOB_TYPES = new Set(["CLOB", "BLOB", "NCLOB", "LONG", "LONG RAW"]);

function _hasLob(t: EnrichedTable): boolean {
  return t.metadata?.columns?.some(c => LOB_TYPES.has(c.data_type)) ?? false;
}
function _hasPk(t: EnrichedTable): boolean {
  return (t.metadata?.pk_columns?.length ?? 0) > 0;
}
function _hasUk(t: EnrichedTable): boolean {
  return (t.metadata?.uk_constraints?.length ?? 0) > 0;
}

// null = any, true = has, false = hasn't
type TriState = boolean | null;

interface StructFilters { pk: TriState; lob: TriState }

function applyFilters(tables: EnrichedTable[], filter: Filter, search: string, struct: StructFilters): EnrichedTable[] {
  let result = tables;
  if (filter === "none") result = result.filter(t => t.migration_status === "NONE");
  else if (filter === "active") result = result.filter(t => t.migration_status === "IN_PROGRESS" || t.migration_status === "PLANNED");
  else if (filter === "completed") result = result.filter(t => t.migration_status === "COMPLETED");
  else if (filter === "errors") result = result.filter(t => t.migration_status === "FAILED");
  if (struct.pk === true) result = result.filter(t => _hasPk(t) || _hasUk(t));
  else if (struct.pk === false) result = result.filter(t => !_hasPk(t) && !_hasUk(t));
  if (struct.lob === true) result = result.filter(_hasLob);
  else if (struct.lob === false) result = result.filter(t => !_hasLob(t));
  if (search.trim()) {
    const q = search.trim().toLowerCase();
    result = result.filter(t => t.object_name.toLowerCase().includes(q));
  }
  return result;
}

function sortTables(tables: EnrichedTable[], key: SortKey, dir: SortDir): EnrichedTable[] {
  const mul = dir === "asc" ? 1 : -1;
  return [...tables].sort((a, b) => {
    switch (key) {
      case "name":
        return mul * a.object_name.localeCompare(b.object_name);
      case "status":
        return mul * ((STATUS_ORDER[a.migration_status] ?? 9) - (STATUS_ORDER[b.migration_status] ?? 9));
      case "rows": {
        const ar = a.metadata?.num_rows ?? -1;
        const br = b.metadata?.num_rows ?? -1;
        return mul * (ar - br);
      }
      case "phase": {
        const ap = a.migration?.phase ?? "zzz";
        const bp = b.migration?.phase ?? "zzz";
        return mul * ap.localeCompare(bp);
      }
      default: return 0;
    }
  });
}

export function TableList({
  tables, selected, onToggleSelect, onToggleAll,
  expandedTable, onExpandTable,
  filter, onFilterChange, search, onSearchChange,
  schema, onCreateMigration, onMigrationChanged,
}: Props) {
  const [sortKey, setSortKey] = useState<SortKey>("name");
  const [sortDir, setSortDir] = useState<SortDir>("asc");
  const [pageSize, setPageSize] = useState(50);
  const [page, setPage] = useState(1);
  const [structFilters, setStructFilters] = useState<StructFilters>({ pk: null, lob: null });

  const toggleTri = (val: TriState): TriState => val === null ? true : val === true ? false : null;

  const filtered = useMemo(() => applyFilters(tables, filter, search, structFilters), [tables, filter, search, structFilters]);
  const sorted = useMemo(() => sortTables(filtered, sortKey, sortDir), [filtered, sortKey, sortDir]);

  const totalPages = pageSize === 0 ? 1 : Math.max(1, Math.ceil(sorted.length / pageSize));
  const currentPage = Math.min(page, totalPages);
  const paged = pageSize === 0 ? sorted : sorted.slice((currentPage - 1) * pageSize, currentPage * pageSize);

  const allSelected = paged.length > 0 && paged.every(t => selected.has(t.object_name));

  const handleSort = (key: SortKey) => {
    if (sortKey === key) setSortDir(d => d === "asc" ? "desc" : "asc");
    else { setSortKey(key); setSortDir("asc"); }
    setPage(1);
  };

  const arrow = (key: SortKey) => sortKey === key ? (sortDir === "asc" ? " ▲" : " ▼") : "";

  return (
    <div>
      {/* Filter + search bar */}
      <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 10, flexWrap: "wrap" }}>
        <div style={{ display: "flex", gap: 4 }}>
          {FILTER_BTNS.map(fb => (
            <button key={fb.key} onClick={() => { onFilterChange(fb.key); setPage(1); }}
              style={{
                background: filter === fb.key ? "#3b82f6" : "#1e293b",
                color: filter === fb.key ? "#fff" : "#94a3b8",
                border: `1px solid ${filter === fb.key ? "#3b82f6" : "#334155"}`,
                borderRadius: 6, padding: "5px 12px", fontSize: 12, fontWeight: 600, cursor: "pointer",
              }}>
              {fb.label}
            </button>
          ))}
        </div>
        <div style={{ display: "flex", gap: 4, marginLeft: 8, borderLeft: "1px solid #334155", paddingLeft: 8 }}>
          <TriBtn label="PK/UK" value={structFilters.pk}
            colors={{ on: "#86efac", off: "#f87171" }}
            onClick={() => { setStructFilters(s => ({ ...s, pk: toggleTri(s.pk) })); setPage(1); }} />
          <TriBtn label="LOB" value={structFilters.lob}
            colors={{ on: "#fbbf24", off: "#94a3b8" }}
            onClick={() => { setStructFilters(s => ({ ...s, lob: toggleTri(s.lob) })); setPage(1); }} />
        </div>

        <input type="text" placeholder="Поиск таблицы…" value={search}
          onChange={e => { onSearchChange(e.target.value); setPage(1); }}
          style={{
            marginLeft: "auto", background: "#1e293b", border: "1px solid #334155",
            borderRadius: 6, color: "#e2e8f0", padding: "6px 12px", fontSize: 13, width: 220,
          }} />
        <span style={{ fontSize: 12, color: "#64748b" }}>
          {filtered.length} из {tables.length}
        </span>
      </div>

      {/* Table header */}
      <div style={{
        display: "grid",
        gridTemplateColumns: "40px 1fr 60px 42px 100px 130px 130px 180px 110px",
        gap: 4, alignItems: "center", padding: "8px 12px",
        borderBottom: "1px solid #334155",
        color: "#64748b", fontSize: 11, fontWeight: 700,
        textTransform: "uppercase", letterSpacing: 0.5,
      }}>
        <div>
          <input type="checkbox" checked={allSelected} onChange={onToggleAll} style={{ cursor: "pointer" }} />
        </div>
        <SortHeader label="Таблица" sortKey="name" current={sortKey} dir={sortDir} onClick={handleSort} arrow={arrow} />
        <div>Ключ</div>
        <div>LOB</div>
        <SortHeader label="Строк" sortKey="rows" current={sortKey} dir={sortDir} onClick={handleSort} arrow={arrow} />
        <SortHeader label="Статус" sortKey="status" current={sortKey} dir={sortDir} onClick={handleSort} arrow={arrow} />
        <SortHeader label="Фаза" sortKey="phase" current={sortKey} dir={sortDir} onClick={handleSort} arrow={arrow} />
        <div>Прогресс</div>
        <div>Группа</div>
      </div>

      {/* Rows */}
      {paged.length === 0 && (
        <div style={{ padding: "24px 12px", color: "#64748b", fontSize: 13, textAlign: "center" }}>
          Таблицы не найдены
        </div>
      )}

      {paged.map(table => (
        <div key={table.object_name}>
          <TableRow
            table={table}
            isSelected={selected.has(table.object_name)}
            isExpanded={expandedTable === table.object_name}
            onToggleSelect={() => onToggleSelect(table.object_name)}
            onExpand={() => onExpandTable(expandedTable === table.object_name ? null : table.object_name)}
          />
          {expandedTable === table.object_name && (
            <TableDetail tableName={table.object_name} schema={schema}
              migration={table.migration} onCreateMigration={onCreateMigration}
              onMigrationChanged={onMigrationChanged} />
          )}
        </div>
      ))}

      {/* Pagination */}
      {sorted.length > 25 && (
        <div style={{
          display: "flex", alignItems: "center", justifyContent: "space-between",
          padding: "10px 12px", borderTop: "1px solid #1e293b", marginTop: 4,
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: "#64748b" }}>
            <span>Показать:</span>
            {PAGE_SIZES.map(ps => (
              <button key={ps} onClick={() => { setPageSize(ps); setPage(1); }}
                style={{
                  background: pageSize === ps ? "#3b82f6" : "#1e293b",
                  color: pageSize === ps ? "#fff" : "#94a3b8",
                  border: `1px solid ${pageSize === ps ? "#3b82f6" : "#334155"}`,
                  borderRadius: 4, padding: "3px 8px", fontSize: 11, cursor: "pointer",
                }}>
                {ps === 0 ? "Все" : ps}
              </button>
            ))}
          </div>

          {pageSize > 0 && totalPages > 1 && (
            <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: "#94a3b8" }}>
              <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={currentPage <= 1}
                style={pgBtn(currentPage <= 1)}>←</button>
              <span>{currentPage} / {totalPages}</span>
              <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={currentPage >= totalPages}
                style={pgBtn(currentPage >= totalPages)}>→</button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function SortHeader({ label, sortKey, current, dir, onClick, arrow }: {
  label: string; sortKey: SortKey; current: SortKey; dir: SortDir;
  onClick: (k: SortKey) => void; arrow: (k: SortKey) => string;
}) {
  return (
    <div onClick={() => onClick(sortKey)}
      style={{ cursor: "pointer", userSelect: "none", color: current === sortKey ? "#93c5fd" : "#64748b" }}>
      {label}{arrow(sortKey)}
    </div>
  );
}

const pgBtn = (disabled: boolean): React.CSSProperties => ({
  background: "#1e293b", border: "1px solid #334155", borderRadius: 4,
  color: disabled ? "#475569" : "#e2e8f0", padding: "3px 8px", fontSize: 12,
  cursor: disabled ? "not-allowed" : "pointer",
});

function TriBtn({ label, value, colors, onClick }: {
  label: string; value: TriState;
  colors: { on: string; off: string };
  onClick: () => void;
}) {
  const color = value === null ? "#64748b" : value ? colors.on : colors.off;
  const prefix = value === null ? "" : value ? "+" : "−";
  return (
    <button onClick={onClick} style={{
      background: value === null ? "#1e293b" : value ? "#1e293b" : "#1e293b",
      color, border: `1px solid ${value === null ? "#334155" : color}`,
      borderRadius: 4, padding: "3px 8px", fontSize: 11, fontWeight: 600,
      cursor: "pointer", minWidth: 50,
    }}>
      {prefix}{label}
    </button>
  );
}
