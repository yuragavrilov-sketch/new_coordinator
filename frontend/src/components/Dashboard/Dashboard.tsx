import { useState, useEffect, useCallback, useMemo } from "react";
import { DashboardToolbar } from "./DashboardToolbar";
import { TableList, type EnrichedTable } from "./TableList";
import { CreateBulkModal } from "./CreateBulkModal";
import type { Migration } from "../../types/migration";

type Filter = "all" | "none" | "active" | "completed" | "errors";

export function Dashboard() {
  const [schemas, setSchemas] = useState<string[]>([]);
  const [selectedSchema, setSelectedSchema] = useState("");
  const [snapshotId, setSnapshotId] = useState<number | null>(null);
  const [tables, setTables] = useState<EnrichedTable[]>([]);
  const [migrations, setMigrations] = useState<Migration[]>([]);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [expandedTable, setExpandedTable] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [createTables, setCreateTables] = useState<string[]>([]);
  const [filter, setFilter] = useState<Filter>("all");
  const [search, setSearch] = useState("");

  // Load snapshots on mount
  useEffect(() => {
    fetch("/api/catalog/snapshots")
      .then((r) => r.json())
      .then((snaps: { snapshot_id: number; src_schema: string }[]) => {
        const unique = [...new Set(snaps.map((s) => s.src_schema))];
        setSchemas(unique);
        if (unique.length > 0 && !selectedSchema) setSelectedSchema(unique[0]);
      })
      .catch(console.error);
  }, []);

  // Load tables when schema changes
  const loadTables = useCallback((schema: string) => {
    if (!schema) return;
    fetch("/api/catalog/snapshots")
      .then((r) => r.json())
      .then((snaps: { snapshot_id: number; src_schema: string }[]) => {
        const matching = snaps
          .filter((s) => s.src_schema === schema)
          .sort((a, b) => b.snapshot_id - a.snapshot_id);
        if (matching.length === 0) {
          setSnapshotId(null);
          setTables([]);
          return;
        }
        const sid = matching[0].snapshot_id;
        setSnapshotId(sid);
        return fetch(`/api/catalog/objects?snapshot_id=${sid}&type=TABLE`);
      })
      .then((r) => r?.json())
      .then((data) => { if (data) setTables(data); })
      .catch(console.error);
  }, []);

  useEffect(() => { loadTables(selectedSchema); }, [selectedSchema, loadTables]);

  // Poll migrations every 5s
  const loadMigrations = useCallback(() => {
    fetch("/api/migrations")
      .then((r) => r.json())
      .then(setMigrations)
      .catch(console.error);
  }, []);

  useEffect(() => {
    loadMigrations();
    const id = setInterval(loadMigrations, 3000);
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

  // Enrich tables with migration data
  const migrationMap = useMemo(() => {
    const m = new Map<string, Migration>();
    for (const mig of migrations) {
      const key = `${mig.source_schema.toUpperCase()}.${mig.source_table.toUpperCase()}`;
      m.set(key, mig);
    }
    return m;
  }, [migrations]);

  const enrichedTables: EnrichedTable[] = useMemo(() => {
    return tables.map((t) => {
      const key = `${selectedSchema.toUpperCase()}.${t.object_name.toUpperCase()}`;
      const mig = migrationMap.get(key);
      if (!mig) return { ...t, migration_status: t.migration_status || "NONE" as const, migration: undefined };
      // Derive migration_status from live phase
      const phase = mig.phase;
      const liveStatus: EnrichedTable["migration_status"] =
        (phase === "COMPLETED") ? "COMPLETED"
        : (phase === "FAILED" || phase === "CANCELLED") ? "FAILED"
        : (phase === "DRAFT" || phase === "NEW") ? "PLANNED"
        : "IN_PROGRESS";
      return {
        ...t,
        migration_status: liveStatus,
        migration: {
          migration_id: mig.migration_id,
          migration_name: mig.migration_name,
          phase: mig.phase,
          chunks_done: mig.chunks_done,
          total_chunks: mig.total_chunks,
          rows_loaded: mig.rows_loaded,
          group_id: mig.group_id,
          state_changed_at: mig.state_changed_at,
          error_text: mig.error_text,
        },
      };
    });
  }, [tables, migrationMap, selectedSchema]);

  // Counts
  const counts = useMemo(() => {
    const total = enrichedTables.length;
    const withMigration = enrichedTables.filter(t => t.migration).length;
    const completed = enrichedTables.filter(t => t.migration_status === "COMPLETED").length;
    const errors = enrichedTables.filter(t => t.migration_status === "FAILED").length;
    const active = enrichedTables.filter(t => t.migration_status === "IN_PROGRESS" || t.migration_status === "PLANNED").length;
    return { total, withMigration, noMigration: total - withMigration, errors, completed, active };
  }, [enrichedTables]);

  // Selection
  const toggleSelect = (name: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name); else next.add(name);
      return next;
    });
  };

  const toggleAll = () => {
    if (selected.size === enrichedTables.length) setSelected(new Set());
    else setSelected(new Set(enrichedTables.map((t) => t.object_name)));
  };

  // Open CreateBulkModal for a single table (called from TableDetail)
  const handleOpenCreateModal = (tableName: string) => {
    setCreateTables([tableName]);
    setShowCreateModal(true);
  };

  return (
    <div>
      <DashboardToolbar
        schemas={schemas}
        selectedSchema={selectedSchema}
        onSchemaChange={(s) => { setSelectedSchema(s); setSelected(new Set()); }}
        onRefresh={handleRefresh}
        refreshing={refreshing}
        counts={counts}
        selectedCount={selected.size}
        onBulkCreate={() => { setCreateTables([...selected]); setShowCreateModal(true); }}
        onBulkGroup={() => { setCreateTables([...selected]); setShowCreateModal(true); }}
      />

      <TableList
        tables={enrichedTables}
        selected={selected}
        onToggleSelect={toggleSelect}
        onToggleAll={toggleAll}
        expandedTable={expandedTable}
        onExpandTable={(name) => setExpandedTable(expandedTable === name ? null : name)}
        filter={filter}
        onFilterChange={setFilter}
        search={search}
        onSearchChange={setSearch}
        schema={selectedSchema}
        onCreateMigration={handleOpenCreateModal}
        onMigrationChanged={loadMigrations}
      />

      {showCreateModal && (
        <CreateBulkModal
          schema={selectedSchema}
          tables={createTables}
          onClose={() => setShowCreateModal(false)}
          onCreated={() => {
            setShowCreateModal(false);
            setSelected(new Set());
            loadMigrations();
            loadTables(selectedSchema);
          }}
        />
      )}
    </div>
  );
}
