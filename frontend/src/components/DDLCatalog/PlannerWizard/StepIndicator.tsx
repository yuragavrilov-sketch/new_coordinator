import React from "react";

export const STEP_LABELS = ["Настройки таблиц", "Порядок загрузки", "Обзор и запуск"];

export function StepIndicator({ current }: { current: number }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 0, marginBottom: 20 }}>
      {STEP_LABELS.map((label, i) => {
        const done   = i < current;
        const active = i === current;
        const color     = done ? "#22c55e" : active ? "#3b82f6" : "#334155";
        const textColor = done ? "#86efac" : active ? "#93c5fd" : "#475569";
        return (
          <React.Fragment key={i}>
            {i > 0 && (
              <div style={{
                flex: 1, height: 2,
                background: done ? "#22c55e55" : "#1e293b",
                margin: "0 4px",
              }} />
            )}
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <div style={{
                width: 24, height: 24, borderRadius: "50%",
                border: `2px solid ${color}`,
                background: done ? "#052e16" : active ? "#1e3a5f" : "transparent",
                display: "flex", alignItems: "center", justifyContent: "center",
                fontSize: 11, fontWeight: 700, color: textColor,
              }}>
                {done ? "✓" : i + 1}
              </div>
              <span style={{
                fontSize: 12, fontWeight: active ? 700 : 500,
                color: textColor, whiteSpace: "nowrap",
              }}>
                {label}
              </span>
            </div>
          </React.Fragment>
        );
      })}
    </div>
  );
}
