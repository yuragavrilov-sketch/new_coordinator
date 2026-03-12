import React from "react";
import type { CdcEvent } from "../hooks/useSSE";

interface Props {
  events: CdcEvent[];
}

export function Stats({ events }: Props) {
  const counts = events.reduce<Record<string, number>>((acc, e) => {
    acc[e.operation] = (acc[e.operation] ?? 0) + 1;
    return acc;
  }, {});

  const tableSet = new Set(events.map((e) => `${e.schema}.${e.table}`));

  const tiles = [
    { label: "Total", value: events.length, color: "#e2e8f0" },
    { label: "INSERT", value: counts.INSERT ?? 0, color: "#22c55e" },
    { label: "UPDATE", value: counts.UPDATE ?? 0, color: "#3b82f6" },
    { label: "DELETE", value: counts.DELETE ?? 0, color: "#ef4444" },
    { label: "Tables", value: tableSet.size, color: "#a78bfa" },
  ];

  return (
    <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
      {tiles.map((t) => (
        <div
          key={t.label}
          style={{
            flex: "1 1 100px",
            background: "#1e293b",
            borderRadius: 8,
            padding: "12px 16px",
            minWidth: 80,
          }}
        >
          <div style={{ color: "#64748b", fontSize: 11, marginBottom: 4 }}>{t.label}</div>
          <div style={{ color: t.color, fontSize: 24, fontWeight: 700 }}>{t.value}</div>
        </div>
      ))}
    </div>
  );
}
