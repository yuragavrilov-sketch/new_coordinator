import React from "react";
import { t } from "../../../theme";

export const STEP_LABELS = ["Настройки таблиц", "Порядок загрузки", "Обзор и запуск"];

export function StepIndicator({ current }: { current: number }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 0, marginBottom: 20 }}>
      {STEP_LABELS.map((label, i) => {
        const done   = i < current;
        const active = i === current;
        const color     = done ? t.green.base : active ? t.blue.base : t.border.base;
        const textColor = done ? t.green.fg   : active ? t.blue.fg   : t.text.disabled;
        return (
          <React.Fragment key={i}>
            {i > 0 && (
              <div style={{
                flex: 1, height: 2,
                background: done ? "#22c55e55" : t.border.subtle,
                margin: "0 4px",
              }} />
            )}
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <div style={{
                width: 24, height: 24, borderRadius: "50%",
                border: `2px solid ${color}`,
                background: done ? t.green.bg : active ? t.bg.s3 : "transparent",
                display: "flex", alignItems: "center", justifyContent: "center",
                fontSize: t.size.sm, fontWeight: 700, color: textColor,
              }}>
                {done ? "✓" : i + 1}
              </div>
              <span style={{
                fontSize: t.size.base, fontWeight: active ? 700 : 500,
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
