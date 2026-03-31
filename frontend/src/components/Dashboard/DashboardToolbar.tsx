import { SearchSelect } from "../ui/SearchSelect";

interface Props {
  schemas: string[];
  selectedSchema: string;
  onSchemaChange: (schema: string) => void;
  onRefresh: () => void;
  refreshing: boolean;
  counts: { total: number; withMigration: number; noMigration: number; errors: number };
  selectedCount: number;
  onBulkCreate: () => void;
  onBulkGroup: () => void;
}

const badge = (label: string, value: number, color: string) => (
  <span
    style={{
      padding: "2px 10px",
      borderRadius: 6,
      fontSize: 13,
      fontWeight: 500,
      background: color,
      color: "#e2e8f0",
      marginRight: 6,
    }}
  >
    {label}: {value}
  </span>
);

export function DashboardToolbar({
  schemas,
  selectedSchema,
  onSchemaChange,
  onRefresh,
  refreshing,
  counts,
  selectedCount,
  onBulkCreate,
  onBulkGroup,
}: Props) {
  return (
    <div style={{ marginBottom: 12 }}>
      {/* Top row */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 12,
          padding: "10px 16px",
          background: "#1e293b",
          borderRadius: 8,
          border: "1px solid #334155",
        }}
      >
        <div style={{ width: 260 }}>
          <SearchSelect
            value={selectedSchema}
            onChange={onSchemaChange}
            options={schemas}
            placeholder="Схема..."
          />
        </div>

        <button
          onClick={onRefresh}
          disabled={refreshing}
          style={{
            padding: "6px 14px",
            borderRadius: 6,
            border: "1px solid #334155",
            background: refreshing ? "#334155" : "#3b82f6",
            color: "#e2e8f0",
            cursor: refreshing ? "not-allowed" : "pointer",
            fontSize: 13,
            fontWeight: 500,
            display: "flex",
            alignItems: "center",
            gap: 6,
          }}
        >
          {refreshing && (
            <span
              style={{
                display: "inline-block",
                width: 14,
                height: 14,
                border: "2px solid #e2e8f0",
                borderTopColor: "transparent",
                borderRadius: "50%",
                animation: "spin 0.8s linear infinite",
              }}
            />
          )}
          Обновить из Oracle
        </button>

        <div style={{ display: "flex", alignItems: "center", marginLeft: "auto" }}>
          {badge("Всего", counts.total, "#334155")}
          {badge("С миграцией", counts.withMigration, "#166534")}
          {badge("Без миграции", counts.noMigration, "#64748b")}
          {counts.errors > 0 && badge("Ошибки", counts.errors, "#991b1b")}
        </div>
      </div>

      {/* Bulk actions row */}
      {selectedCount > 0 && (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 10,
            padding: "8px 16px",
            marginTop: 6,
            background: "#1e293b",
            borderRadius: 8,
            border: "1px solid #334155",
          }}
        >
          <button
            onClick={onBulkCreate}
            style={{
              padding: "6px 14px",
              borderRadius: 6,
              border: "1px solid #334155",
              background: "#3b82f6",
              color: "#e2e8f0",
              cursor: "pointer",
              fontSize: 13,
              fontWeight: 500,
            }}
          >
            Создать миграции ({selectedCount})
          </button>
          <button
            onClick={onBulkGroup}
            style={{
              padding: "6px 14px",
              borderRadius: 6,
              border: "1px solid #334155",
              background: "#1e40af",
              color: "#e2e8f0",
              cursor: "pointer",
              fontSize: 13,
              fontWeight: 500,
            }}
          >
            Создать группу + миграции
          </button>
        </div>
      )}
    </div>
  );
}
