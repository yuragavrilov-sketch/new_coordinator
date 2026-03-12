import React from "react";
import type { CdcEvent } from "../hooks/useSSE";

const opColor: Record<string, string> = {
  INSERT: "#22c55e",
  UPDATE: "#3b82f6",
  DELETE: "#ef4444",
  UNKNOWN: "#6b7280",
};

function OpBadge({ op }: { op: string }) {
  const color = opColor[op] ?? opColor.UNKNOWN;
  return (
    <span
      style={{
        padding: "1px 8px",
        borderRadius: 4,
        background: color + "22",
        color,
        fontWeight: 700,
        fontSize: 11,
        letterSpacing: 1,
        border: `1px solid ${color}44`,
      }}
    >
      {op}
    </span>
  );
}

interface Props {
  events: CdcEvent[];
  filter: string;
}

export function EventTable({ events, filter }: Props) {
  const lower = filter.toLowerCase();
  const visible = filter
    ? events.filter(
        (e) =>
          e.table.toLowerCase().includes(lower) ||
          e.operation.toLowerCase().includes(lower) ||
          e.schema.toLowerCase().includes(lower)
      )
    : events;

  if (visible.length === 0) {
    return (
      <div style={{ textAlign: "center", color: "#6b7280", padding: 48 }}>
        No events yet — waiting for CDC stream…
      </div>
    );
  }

  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
        <thead>
          <tr style={{ background: "#1e293b", color: "#94a3b8", textAlign: "left" }}>
            {["Time", "Schema", "Table", "Op", "Data", "Old Data"].map((h) => (
              <th key={h} style={{ padding: "8px 12px", fontWeight: 600, whiteSpace: "nowrap" }}>
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {visible.map((e, i) => (
            <tr
              key={e.id + i}
              style={{
                background: i % 2 === 0 ? "#0f172a" : "#111827",
                borderBottom: "1px solid #1e293b",
                animation: i === 0 ? "fadeIn 0.3s ease" : undefined,
              }}
            >
              <td style={{ padding: "6px 12px", color: "#64748b", whiteSpace: "nowrap" }}>
                {new Date(e.ts).toLocaleTimeString()}
              </td>
              <td style={{ padding: "6px 12px", color: "#94a3b8" }}>{e.schema}</td>
              <td style={{ padding: "6px 12px", color: "#e2e8f0", fontWeight: 600 }}>{e.table}</td>
              <td style={{ padding: "6px 12px" }}>
                <OpBadge op={e.operation} />
              </td>
              <td style={{ padding: "6px 12px", color: "#94a3b8", fontFamily: "monospace" }}>
                <code style={{ fontSize: 11 }}>{JSON.stringify(e.data)}</code>
              </td>
              <td style={{ padding: "6px 12px", color: "#475569", fontFamily: "monospace" }}>
                {e.old_data ? (
                  <code style={{ fontSize: 11 }}>{JSON.stringify(e.old_data)}</code>
                ) : (
                  <span style={{ color: "#334155" }}>—</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
