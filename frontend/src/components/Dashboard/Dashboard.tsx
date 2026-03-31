import { useState, useEffect, useCallback, useMemo } from "react";
import { DashboardToolbar } from "./DashboardToolbar";
import type { Migration } from "../../types/migration";

interface TableInfo {
  object_name: string;
  oracle_status: string | null;
  migration_status: "NONE" | "PLANNED" | "IN_PROGRESS" | "COMPLETED" | "FAILED";
  match_status: string;
  metadata: Record<string, unknown>;
}

type Filter = "all" | "none" | "active" | "completed" | "errors";

export function Dashboard() {
  const [schemas, setSchemas] = useState<string[]>([]);
  const [selectedSchema, setSelectedSchema] = useState("");
  const [snapshotId, setSnapshotId] = useState<number | null>(null);
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [migrations, setMigrations] = useState<Migration[]>([]);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [expandedTable, setExpandedTable] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [showBulkModal, setShowBulkModal] = useState(false);
  const [bulkMode, setBulkMode] = useState<"individual" | "group">("individual");
  const [filter, setFilter] = useState<Filter>("all");
  const [search, setSearch] = useState("");

  // Load snapshots on mount
  useEffect(() => {
    fetch("/api/catalog/snapshots")
      .then((r) => r.json())
      .then((snaps: { snapshot_id: number; src_schema: string; tgt_schema: string; loaded_at: string }[]) => {
        const unique = [...new Set(snaps.map((s) => s.src_schema))];
        setSchemas(unique);
        if (unique.length > 0 && !selectedSchema) setSelectedSchema(unique[0]);
      })
      .catch(console.error);
  }, []);

  // Load tables when schema changes
  useEffect(() => {
    if (!selectedSchema) return;
    fetch("/api/catalog/snapshots")
      .then((r) => r.json())
      .then((snaps: { snapshot_id: number; src_schema: string }[]) => {
        const matching = snaps
          .filter((s) => s.src_schema === selectedSchema)
          .sort((a, b) => b.snapshot_id - a.snapshot_id);
        if (matching.length === 0) return;
        const sid = matching[0].snapshot_id;
        setSnapshotId(sid);
        return fetch(`/api/catalog/objects?snapshot_id=${sid}&type=TABLE`);
      })
      .then((r) => r?.json())
      .then((data) => {
        if (data) setTables(data);
      })
      .catch(console.error);
  }, [selectedSchema]);

  // Poll migrations every 5s
  const loadMigrations = useCallback(() => {
    fetch("/api/migrations")
      .then((r) => r.json())
      .then(setMigrations)
      .catch(console.error);
  }, []);

  useEffect(() => {
    loadMigrations();
    const id = setInterval(loadMigrations, 5000);
    return () => clearInterval(id);
  }, [loadMigrations]);

  // Refresh from Oracle
  const handleRefresh = async () => {
    if (!selectedSchema) return;
    setRefreshing(true);
    try {
      const res = await fetch("/api/catalog/load", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ src_schema: selectedSchema, tgt_schema: selectedSchema.toLowerCase() }),
      });
      const data = await res.json();
      if (data.snapshot_id) {
        setSnapshotId(data.snapshot_id);
        const r = await fetch(`/api/catalog/objects?snapshot_id=${data.snapshot_id}&type=TABLE`);
        setTables(await r.json());
      }
    } catch (e) {
      console.error(e);
    } finally {
      setRefreshing(false);
    }
  };

  // Map migrations to tables
  const migrationMap = useMemo(() => {
    const m = new Map<string, Migration>();
    for (const mig of migrations) {
      const key = `${mig.source_schema.toUpperCase()}.${mig.source_table.toUpperCase()}`;
      m.set(key, mig);
    }
    return m;
  }, [migrations]);

  const enrichedTables = useMemo(() => {
    return tables.map((t) => {
      const key = `${selectedSchema.toUpperCase()}.${t.object_name.toUpperCase()}`;
      const mig = migrationMap.get(key) ?? null;
      return { ...t, migration: mig };
    });
  }, [tables, migrationMap, selectedSchema]);

  // Filter + search
  const filteredTables = useMemo(() => {
    let result = enrichedTables;
    if (search) {
      const q = search.toLowerCase();
      result = result.filter((t) => t.object_name.toLowerCase().includes(q));
    }
    switch (filter) {
      case "none":
        return result.filter((t) => !t.migration);
      case "active":
        return result.filter((t) => t.migration && !["COMPLETED", "FAILED", "CANCELLED"].includes(t.migration.phase));
      case "completed":
        return result.filter((t) => t.migration?.phase === "COMPLETED");
      case "errors":
        return result.filter((t) => t.migration?.phase === "FAILED");
      default:
        return result;
    }
  }, [enrichedTables, filter, search]);

  // Counts
  const counts = useMemo(() => {
    const total = enrichedTables.length;
    const withMigration = enrichedTables.filter((t) => t.migration).length;
    const errors = enrichedTables.filter((t) => t.migration?.phase === "FAILED").length;
    return { total, withMigration, noMigration: total - withMigration, errors };
  }, [enrichedTables]);

  // Selection handlers
  const toggleSelect = (name: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  };

  const toggleAll = () => {
    if (selected.size === filteredTables.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(filteredTables.map((t) => t.object_name)));
    }
  };

  const handleBulkCreate = () => {
    setBulkMode("individual");
    setShowBulkModal(true);
  };

  const handleBulkGroup = () => {
    setBulkMode("group");
    setShowBulkModal(true);
  };

  return (
    <div style={{ padding: 16, background: "#0f172a", minHeight: "100vh", color: "#e2e8f0" }}>
      <DashboardToolbar
        schemas={schemas}
        selectedSchema={selectedSchema}
        onSchemaChange={(s) => {
          setSelectedSchema(s);
          setSelected(new Set());
        }}
        onRefresh={handleRefresh}
        refreshing={refreshing}
        counts={counts}
        selectedCount={selected.size}
        onBulkCreate={handleBulkCreate}
        onBulkGroup={handleBulkGroup}
      />

      {/* Filter row */}
      <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 10, flexWrap: "wrap" }}>
        {(["all", "none", "active", "completed", "errors"] as Filter[]).map((f) => (
          <button
            key={f}
            onClick={() => setFilter(f)}
            style={{
              padding: "4px 12px",
              borderRadius: 6,
              border: filter === f ? "1px solid #3b82f6" : "1px solid #334155",
              background: filter === f ? "#1e3a5f" : "#1e293b",
              color: "#e2e8f0",
              cursor: "pointer",
              fontSize: 13,
            }}
          >
            {{ all: "Все", none: "Без миграции", active: "Активные", completed: "Завершённые", errors: "Ошибки" }[f]}
          </button>
        ))}
        <input
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Поиск таблицы..."
          style={{
            marginLeft: "auto",
            padding: "5px 12px",
            borderRadius: 6,
            border: "1px solid #334155",
            background: "#0f172a",
            color: "#e2e8f0",
            fontSize: 13,
            width: 220,
          }}
        />
      </div>

      {/* Table list placeholder — expects TableList component */}
      <div
        style={{
          background: "#1e293b",
          borderRadius: 8,
          border: "1px solid #334155",
          padding: 16,
          color: "#64748b",
          fontSize: 14,
        }}
      >
        {filteredTables.length === 0
          ? "Нет таблиц для отображения"
          : `Загружено таблиц: ${filteredTables.length}`}
        {/* TODO: Replace with <TableList /> component */}
      </div>

      {showBulkModal && (
        <div
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.6)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            zIndex: 100,
          }}
        >
          <div
            style={{
              background: "#1e293b",
              border: "1px solid #334155",
              borderRadius: 10,
              padding: 24,
              minWidth: 400,
              color: "#e2e8f0",
            }}
          >
            <h3 style={{ margin: "0 0 12px" }}>
              {bulkMode === "individual" ? "Создание миграций" : "Создание группы + миграций"}
            </h3>
            <p style={{ color: "#64748b", fontSize: 14 }}>
              Выбрано таблиц: {selected.size}
            </p>
            {/* TODO: BulkCreateModal content */}
            <div style={{ display: "flex", gap: 8, marginTop: 16, justifyContent: "flex-end" }}>
              <button
                onClick={() => setShowBulkModal(false)}
                style={{
                  padding: "6px 16px",
                  borderRadius: 6,
                  border: "1px solid #334155",
                  background: "#334155",
                  color: "#e2e8f0",
                  cursor: "pointer",
                  fontSize: 13,
                }}
              >
                Отмена
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
