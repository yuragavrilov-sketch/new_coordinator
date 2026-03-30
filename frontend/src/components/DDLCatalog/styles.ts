import React from "react";

export const S = {
  card: {
    background: "#0f172a", border: "1px solid #1e293b", borderRadius: 8,
    overflow: "hidden" as const,
  },
  cardHeader: {
    padding: "10px 16px", background: "#0a111f",
    borderBottom: "1px solid #1e293b",
    display: "flex" as const, alignItems: "center" as const, gap: 10,
  },
  cardBody: {
    padding: 16, display: "flex" as const, flexDirection: "column" as const, gap: 12,
  },
  row2: { display: "grid" as const, gridTemplateColumns: "1fr 1fr", gap: 10 },
  row3: { display: "grid" as const, gridTemplateColumns: "1fr 1fr 1fr", gap: 10 },
  field: { display: "flex" as const, flexDirection: "column" as const, gap: 4 },
  label: { fontSize: 11, color: "#64748b", fontWeight: 600 as const, letterSpacing: 0.3 },
  input: {
    background: "#1e293b", border: "1px solid #334155", borderRadius: 5,
    color: "#e2e8f0", padding: "7px 10px", fontSize: 13, width: "100%",
  },
  select: {
    background: "#1e293b", border: "1px solid #334155", borderRadius: 5,
    color: "#e2e8f0", padding: "7px 10px", fontSize: 13, width: "100%", cursor: "pointer" as const,
  },
  btnPrimary: {
    background: "#3b82f6", border: "none", borderRadius: 6,
    color: "#fff", padding: "6px 14px", fontSize: 12, fontWeight: 600 as const,
    cursor: "pointer" as const,
  },
  btnSecondary: {
    background: "#1e293b", border: "1px solid #334155", borderRadius: 6,
    color: "#94a3b8", padding: "6px 14px", fontSize: 12, fontWeight: 600 as const,
    cursor: "pointer" as const,
  },
  btnDanger: {
    background: "#7f1d1d33", border: "1px solid #7f1d1d88", borderRadius: 6,
    color: "#fca5a5", padding: "6px 14px", fontSize: 12, fontWeight: 600 as const,
    cursor: "pointer" as const,
  },
  btnSuccess: {
    background: "#22c55e22", border: "1px solid #22c55e55", borderRadius: 6,
    color: "#86efac", padding: "6px 14px", fontSize: 12, fontWeight: 600 as const,
    cursor: "pointer" as const,
  },
  badge: (bg: string, fg: string): React.CSSProperties => ({
    padding: "2px 8px", borderRadius: 10, fontSize: 10, fontWeight: 600,
    background: bg, color: fg, whiteSpace: "nowrap",
  }),
  th: {
    padding: "6px 10px", textAlign: "left" as const,
    color: "#64748b", fontWeight: 500, fontSize: 12, whiteSpace: "nowrap" as const,
  },
  td: { padding: "5px 10px", fontSize: 12 },
  trBorder: { borderBottom: "1px solid #0f1624" },
};
