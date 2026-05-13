import React from "react";
import { t } from "../../theme";

interface Props {
  step:   number;
  total:  number;
  labels: string[];
}

export function StepIndicator({ step, labels }: Props) {
  return (
    <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
      {labels.map((l, i) => (
        <React.Fragment key={i}>
          {i > 0 && (
            <div style={{
              width: 24, height: 1,
              background: i <= step ? t.blue.base : t.border.base,
            }} />
          )}
          <div style={{
            display: "flex", alignItems: "center", gap: 6,
            opacity: i <= step ? 1 : 0.4,
          }}>
            <div style={{
              width: 22, height: 22, borderRadius: "50%",
              background:   i < step ? t.blue.dim : i === step ? t.bg.s3 : t.bg.s2,
              border:       `1px solid ${i <= step ? t.blue.base : t.border.base}`,
              color:        i <= step ? t.blue.fg : t.text.disabled,
              display:      "flex", alignItems: "center", justifyContent: "center",
              fontSize:     t.size.xs, fontWeight: 700,
            }}>
              {i < step ? "✓" : i + 1}
            </div>
            <span style={{
              fontSize: t.size.sm,
              color: i <= step ? t.text.primary : t.text.disabled,
              fontWeight: 600,
            }}>
              {l}
            </span>
          </div>
        </React.Fragment>
      ))}
    </div>
  );
}
