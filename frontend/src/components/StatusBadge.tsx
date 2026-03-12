import React from "react";
import type { SSEStatus } from "../hooks/useSSE";

const colors: Record<SSEStatus, string> = {
  connecting: "#f59e0b",
  connected:  "#22c55e",
  error:      "#ef4444",
  closed:     "#6b7280",
};

export function StatusBadge({ status }: { status: SSEStatus }) {
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        padding: "2px 10px",
        borderRadius: 999,
        background: colors[status] + "22",
        color: colors[status],
        fontWeight: 600,
        fontSize: 13,
        border: `1px solid ${colors[status]}55`,
      }}
    >
      <span
        style={{
          width: 8,
          height: 8,
          borderRadius: "50%",
          background: colors[status],
          display: "inline-block",
          animation: status === "connected" ? "pulse 2s infinite" : "none",
        }}
      />
      {status}
    </span>
  );
}
