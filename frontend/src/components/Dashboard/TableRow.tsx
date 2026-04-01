import { PhaseBadge } from "../PhaseBadge";
import type { EnrichedTable } from "./TableList";

interface Props {
  table: EnrichedTable;
  isSelected: boolean;
  isExpanded: boolean;
  onToggleSelect: () => void;
  onExpand: () => void;
}

const statusConfig: Record<
  EnrichedTable["migration_status"],
  { bg: string; color: string; label: string }
> = {
  NONE: { bg: "#1e293b", color: "#64748b", label: "—" },
  PLANNED: { bg: "#1e3a5f", color: "#60a5fa", label: "Запланирована" },
  IN_PROGRESS: { bg: "#422006", color: "#fbbf24", label: "В процессе" },
  COMPLETED: { bg: "#052e16", color: "#4ade80", label: "Завершена" },
  FAILED: { bg: "#450a0a", color: "#f87171", label: "Ошибка" },
};

export function TableRow({ table, isSelected, isExpanded, onToggleSelect, onExpand }: Props) {
  const status = statusConfig[table.migration_status];
  const m = table.migration;
  const progress =
    m && m.total_chunks && m.total_chunks > 0
      ? { done: m.chunks_done, total: m.total_chunks, pct: Math.round((m.chunks_done / m.total_chunks) * 100) }
      : null;

  return (
    <div
      onClick={(e) => {
        const target = e.target as HTMLElement;
        if (target.tagName === "INPUT" && (target as HTMLInputElement).type === "checkbox") return;
        onExpand();
      }}
      style={{
        display: "grid",
        gridTemplateColumns: "40px 1fr 60px 42px 100px 130px 130px 180px 110px",
        gap: 4,
        alignItems: "center",
        padding: "8px 12px",
        borderBottom: "1px solid #1e293b",
        cursor: "pointer",
        background: isExpanded ? "#0f1e35" : "transparent",
        fontSize: 13,
        color: "#e2e8f0",
        transition: "background 0.15s",
      }}
      onMouseEnter={(e) => {
        if (!isExpanded) e.currentTarget.style.background = "#0f1e35";
      }}
      onMouseLeave={(e) => {
        if (!isExpanded) e.currentTarget.style.background = "transparent";
      }}
    >
      {/* Checkbox */}
      <div>
        <input
          type="checkbox"
          checked={isSelected}
          onChange={onToggleSelect}
          style={{ cursor: "pointer" }}
        />
      </div>

      {/* Table name */}
      <div style={{ fontFamily: "monospace", fontWeight: 600, textTransform: "uppercase", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
        {table.object_name}
      </div>

      {/* Key indicator */}
      <div style={{ fontSize: 11 }}>
        {table.metadata?.pk_columns && table.metadata.pk_columns.length > 0
          ? <span style={{ color: "#86efac", fontWeight: 700 }}>PK</span>
          : table.metadata?.uk_constraints && table.metadata.uk_constraints.length > 0
            ? <span style={{ color: "#c4b5fd", fontWeight: 700 }}>UK</span>
            : <span style={{ color: "#475569" }}>—</span>}
      </div>

      {/* LOB indicator */}
      <div style={{ fontSize: 10, textAlign: "center" }}>
        {table.metadata?.columns?.some(c =>
          ["CLOB", "BLOB", "NCLOB", "LONG", "LONG RAW"].includes(c.data_type))
          ? <span style={{ color: "#fbbf24", fontWeight: 700 }}>LOB</span>
          : null}
      </div>

      {/* Estimated rows */}
      <div style={{ fontSize: 12, color: "#94a3b8", textAlign: "right" }}>
        {table.metadata?.num_rows != null
          ? table.metadata.num_rows.toLocaleString("ru-RU")
          : "—"}
      </div>

      {/* Status badge */}
      <div>
        <span
          style={{
            background: status.bg,
            color: status.color,
            borderRadius: 4,
            padding: "2px 8px",
            fontSize: 11,
            fontWeight: 700,
            whiteSpace: "nowrap",
          }}
        >
          {status.label}
        </span>
      </div>

      {/* Phase */}
      <div>{m ? <PhaseBadge phase={m.phase} size="sm" /> : <span style={{ color: "#64748b" }}>—</span>}</div>

      {/* Progress */}
      <div>
        {progress ? (
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <div
              style={{
                flex: 1,
                height: 6,
                background: "#334155",
                borderRadius: 3,
                overflow: "hidden",
              }}
            >
              <div
                style={{
                  width: `${progress.pct}%`,
                  height: "100%",
                  background: "#3b82f6",
                  borderRadius: 3,
                  transition: "width 0.3s",
                }}
              />
            </div>
            <span style={{ fontSize: 11, color: "#94a3b8", whiteSpace: "nowrap" }}>
              {progress.done}/{progress.total}
            </span>
          </div>
        ) : (
          <span style={{ color: "#64748b" }}>—</span>
        )}
      </div>

      {/* Group */}
      <div style={{ fontSize: 12, color: "#94a3b8", fontFamily: "monospace", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
        {m?.group_id ? m.group_id.substring(0, 8) : "—"}
      </div>
    </div>
  );
}
