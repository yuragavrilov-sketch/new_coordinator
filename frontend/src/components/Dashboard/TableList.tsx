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
}

const filterButtons: { key: Filter; label: string }[] = [
  { key: "all", label: "Все" },
  { key: "none", label: "Без миграции" },
  { key: "active", label: "Активные" },
  { key: "completed", label: "Завершённые" },
  { key: "errors", label: "Ошибки" },
];

function applyFilters(
  tables: EnrichedTable[],
  filter: Filter,
  search: string,
): EnrichedTable[] {
  let result = tables;

  if (filter === "none") {
    result = result.filter((t) => t.migration_status === "NONE");
  } else if (filter === "active") {
    result = result.filter(
      (t) => t.migration_status === "IN_PROGRESS" || t.migration_status === "PLANNED",
    );
  } else if (filter === "completed") {
    result = result.filter((t) => t.migration_status === "COMPLETED");
  } else if (filter === "errors") {
    result = result.filter((t) => t.migration_status === "FAILED");
  }

  if (search.trim()) {
    const q = search.trim().toLowerCase();
    result = result.filter((t) => t.object_name.toLowerCase().includes(q));
  }

  return result;
}

export function TableList({
  tables,
  selected,
  onToggleSelect,
  onToggleAll,
  expandedTable,
  onExpandTable,
  filter,
  onFilterChange,
  search,
  onSearchChange,
  schema,
  onCreateMigration,
}: Props) {
  const filtered = applyFilters(tables, filter, search);
  const allSelected = filtered.length > 0 && filtered.every((t) => selected.has(t.object_name));

  return (
    <div>
      {/* Filter bar */}
      <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 12, flexWrap: "wrap" }}>
        <input
          type="text"
          placeholder="Поиск таблицы…"
          value={search}
          onChange={(e) => onSearchChange(e.target.value)}
          style={{
            background: "#1e293b",
            border: "1px solid #334155",
            borderRadius: 6,
            color: "#e2e8f0",
            padding: "6px 12px",
            fontSize: 13,
            outline: "none",
            minWidth: 200,
          }}
        />
        <div style={{ display: "flex", gap: 4 }}>
          {filterButtons.map((fb) => (
            <button
              key={fb.key}
              onClick={() => onFilterChange(fb.key)}
              style={{
                background: filter === fb.key ? "#3b82f6" : "#1e293b",
                color: filter === fb.key ? "#fff" : "#94a3b8",
                border: `1px solid ${filter === fb.key ? "#3b82f6" : "#334155"}`,
                borderRadius: 6,
                padding: "5px 12px",
                fontSize: 12,
                fontWeight: 600,
                cursor: "pointer",
              }}
            >
              {fb.label}
            </button>
          ))}
        </div>
      </div>

      {/* Table header */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "40px 1fr 60px 100px 130px 130px 180px 110px",
          gap: 4,
          alignItems: "center",
          padding: "8px 12px",
          borderBottom: "1px solid #334155",
          color: "#64748b",
          fontSize: 11,
          fontWeight: 700,
          textTransform: "uppercase",
          letterSpacing: 0.5,
        }}
      >
        <div>
          <input
            type="checkbox"
            checked={allSelected}
            onChange={onToggleAll}
            style={{ cursor: "pointer" }}
          />
        </div>
        <div>Таблица</div>
        <div>Ключ</div>
        <div>Строк</div>
        <div>Статус</div>
        <div>Фаза</div>
        <div>Прогресс</div>
        <div>Группа</div>
      </div>

      {/* Rows */}
      {filtered.length === 0 && (
        <div style={{ padding: "24px 12px", color: "#64748b", fontSize: 13, textAlign: "center" }}>
          Таблицы не найдены
        </div>
      )}

      {filtered.map((table) => (
        <div key={table.object_name}>
          <TableRow
            table={table}
            isSelected={selected.has(table.object_name)}
            isExpanded={expandedTable === table.object_name}
            onToggleSelect={() => onToggleSelect(table.object_name)}
            onExpand={() =>
              onExpandTable(expandedTable === table.object_name ? null : table.object_name)
            }
          />
          {expandedTable === table.object_name && (
            <TableDetail
              tableName={table.object_name}
              schema={schema}
              migration={table.migration}
              onCreateMigration={onCreateMigration}
            />
          )}
        </div>
      ))}
    </div>
  );
}
