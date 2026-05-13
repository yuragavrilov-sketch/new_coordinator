import React, { useMemo, useState } from "react";
import { SchemaHeader } from "./SchemaHeader";
import { KpiRow, KpiCard } from "./KpiRow";
import { ObjectFilters, type SortKey, type StatusFilter } from "./ObjectFilters";
import { ObjectTable } from "./ObjectTable";
import { ObjectDrawer } from "./ObjectDrawer";
import { NewMigrationWizard } from "./NewMigrationWizard";
import { fmtCompactNum } from "../utils/format";
import { OBJECT_TYPES, type SchemaObject, type ObjectType } from "./types";
import { schemaInfo, initialObjects, initialEvents } from "./mockData";

export function Dashboard() {
  const [objects]   = useState<SchemaObject[]>(initialObjects);
  const [events]    = useState(initialEvents);
  const [typeFilter,   setTypeFilter]   = useState<ObjectType | "all">("all");
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");
  const [search,       setSearch]       = useState("");
  const [sort,         setSort]         = useState<SortKey>("priority");
  const [openObject,   setOpenObject]   = useState<SchemaObject | null>(null);
  const [wizardOpen,   setWizardOpen]   = useState(false);

  // Aggregate KPIs
  const overall = useMemo(() => {
    const done       = objects.filter(o => o.status === "done" || o.status === "skipped").length;
    const err        = objects.reduce((a, o) => a + o.err, 0);
    const warn       = objects.reduce((a, o) => a + o.warn, 0);
    const rowsPerSec = objects.reduce((a, o) => a + (o.rowsPerSec || 0), 0);
    const mbPerSec   = objects.reduce((a, o) => a + (o.mbPerSec   || 0), 0);
    const totalRows  = objects.reduce((a, o) => a + (o.rows     || 0), 0);
    const doneRows   = objects.reduce((a, o) => a + (o.rowsDone || 0), 0);
    const progress   = totalRows ? (doneRows / totalRows) * 100 : 0;
    return { done, total: objects.length, err, warn, rowsPerSec, mbPerSec, totalRows, doneRows, progress };
  }, [objects]);

  const statusCounts = useMemo(() => {
    const c = { all: objects.length, running: 0, queued: 0, done: 0 };
    objects.forEach(o => {
      if (o.status === "running") c.running++;
      else if (o.status === "queued") c.queued++;
      else if (o.status === "done") c.done++;
    });
    return c;
  }, [objects]);

  // Filtered + sorted objects
  const filtered = useMemo(() => {
    let arr = objects;
    if (typeFilter !== "all") arr = arr.filter(o => o.type === typeFilter);
    if (statusFilter !== "all") {
      arr = arr.filter(o => {
        if (statusFilter === "issues") return o.err > 0 || o.warn > 0 || o.status === "error" || o.status === "warn";
        return o.status === statusFilter;
      });
    }
    if (search) {
      const q = search.toLowerCase();
      arr = arr.filter(o => o.name.toLowerCase().includes(q));
    }
    arr = [...arr].sort((a, b) => {
      if (sort === "priority") {
        const rank = { error: 0, warn: 1, running: 2, validating: 3, paused: 4, queued: 5, done: 6, skipped: 7 } as const;
        const sa = rank[a.status] ?? 9;
        const sb = rank[b.status] ?? 9;
        if (sa !== sb) return sa - sb;
        return (b.sizeMb || 0) - (a.sizeMb || 0);
      }
      if (sort === "size")     return (b.sizeMb || 0) - (a.sizeMb || 0);
      if (sort === "progress") return b.progress - a.progress;
      if (sort === "name")     return a.name.localeCompare(b.name);
      if (sort === "type")     return OBJECT_TYPES[a.type].ord - OBJECT_TYPES[b.type].ord;
      return 0;
    });
    return arr;
  }, [objects, typeFilter, statusFilter, search, sort]);

  return (
    <>
      <SchemaHeader
        schema={schemaInfo}
        progress={overall.progress}
        onPause={()    => console.log("pause schema")}
        onRollback={() => console.log("rollback schema")}
        onNew={()      => setWizardOpen(true)}
      />

      <KpiRow>
        <KpiCard
          label="Прогресс схемы"
          value={`${overall.progress.toFixed(1)}%`}
          sub={`${overall.done}/${overall.total} объектов готово`}
          spark={[12, 18, 24, 32, 40, 45, 52, 58, 62, 66, 70, 72, 75, 78, 80, 82, 84, 86, 87, overall.progress]}
          tone="info"
          delta={+8}
        />
        <KpiCard
          label="Объектов в работе"
          value={statusCounts.running}
          sub={`${statusCounts.queued} в очереди · ${statusCounts.done} готовы`}
          spark={[2, 3, 4, 4, 3, 5, 6, 5, 4, 4, 5, 6, 7, 6, 5, 4, 3, 3, 2, statusCounts.running]}
          tone="info"
          mono={false}
        />
        <KpiCard
          label="Скорость"
          value={fmtCompactNum(overall.rowsPerSec)}
          sub={`rows/s · ${overall.mbPerSec} MB/s`}
          spark={[180, 210, 240, 260, 250, 280, 310, 290, 320, 340, 360, 355, 380, 400, 420, 415, 450, 470, 485, 490]}
          tone="ok"
          delta={+5}
        />
        <KpiCard
          label="Совместимость"
          value={`${schemaInfo.schemaCompat.toFixed(1)}%`}
          sub="по PL/SQL и view"
          spark={[88, 89, 90, 90, 91, 91, 92, 92, 93, 93, 93, 94, 94, 94, 94, 95, 95, 96, 96, 97]}
          tone="warn"
        />
        <KpiCard
          label="Ошибки"
          value={overall.err}
          sub={`${overall.warn} предупреждений`}
          spark={[0, 0, 0, 0, 0, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, overall.err]}
          tone="error"
          mono={false}
        />
      </KpiRow>

      <ObjectFilters
        objects={objects}
        filtered={filtered}
        typeFilter={typeFilter}     onTypeFilter={setTypeFilter}
        statusFilter={statusFilter} onStatusFilter={setStatusFilter}
        search={search}             onSearch={setSearch}
        sort={sort}                 onSort={setSort}
      />

      <ObjectTable
        objects={filtered}
        onOpen={o => setOpenObject(o)}
        onAction={(o, a) => console.log("object action", a, o.name)}
      />

      {openObject && (
        <ObjectDrawer
          object={openObject}
          events={events}
          onClose={() => setOpenObject(null)}
          onAction={(o, a) => console.log("drawer action", a, o.name)}
        />
      )}
      {wizardOpen && (
        <NewMigrationWizard
          onClose={() => setWizardOpen(false)}
          onSubmit={d => { console.log("create migration", d); setWizardOpen(false); }}
        />
      )}
    </>
  );
}
