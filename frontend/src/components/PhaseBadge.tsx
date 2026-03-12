import { phaseColor } from "../types/migration";

interface Props {
  phase: string;
  size?: "sm" | "md";
}

export function PhaseBadge({ phase, size = "md" }: Props) {
  const c = phaseColor(phase);
  const fontSize = size === "sm" ? 10 : 11;
  const padding = size === "sm" ? "2px 7px" : "3px 10px";
  return (
    <span style={{
      background: c.bg,
      color: c.text,
      border: `1px solid ${c.border}`,
      borderRadius: 4,
      fontSize,
      fontWeight: 700,
      padding,
      letterSpacing: 0.3,
      display: "inline-block",
      whiteSpace: "nowrap",
    }}>
      {phase}
    </span>
  );
}
