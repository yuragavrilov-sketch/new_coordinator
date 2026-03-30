import React from "react";

export type ObjectTabId = "tables" | "views" | "code" | "other";

interface Props {
  active: ObjectTabId;
  onChange: (tab: ObjectTabId) => void;
  counts: { tables: number; views: number; code: number; other: number };
}

const TABS: { id: ObjectTabId; label: string; countKey: keyof Props["counts"] }[] = [
  { id: "tables", label: "Таблицы", countKey: "tables" },
  { id: "views", label: "Views & MViews", countKey: "views" },
  { id: "code", label: "Code", countKey: "code" },
  { id: "other", label: "Другое", countKey: "other" },
];

export function ObjectTabs({ active, onChange, counts }: Props) {
  return (
    <div style={{ display: "flex", gap: 0, borderBottom: "1px solid #1e293b", marginBottom: 12 }}>
      {TABS.map(tab => {
        const isActive = active === tab.id;
        return (
          <button
            key={tab.id}
            onClick={() => onChange(tab.id)}
            style={{
              background: "none", border: "none",
              borderBottom: `2px solid ${isActive ? "#3b82f6" : "transparent"}`,
              color: isActive ? "#93c5fd" : "#475569",
              padding: "8px 16px", fontSize: 13,
              fontWeight: isActive ? 700 : 500,
              cursor: "pointer", marginBottom: -1,
              transition: "color 0.15s, border-color 0.15s",
            }}
          >
            {tab.label} ({counts[tab.countKey]})
          </button>
        );
      })}
    </div>
  );
}
