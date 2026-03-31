/**
 * Centralized color palette. Import from here instead of hardcoding hex values.
 */
export const theme = {
  bg: {
    page: "#0a111f",
    primary: "#0f172a",
    secondary: "#1e293b",
    card: "#1e293b",
    hover: "#0f1624",
    hoverAlt: "#0f1e35",
    selected: "#1d3a5f",
    input: "#1e293b",
  },
  text: {
    primary: "#e2e8f0",
    secondary: "#94a3b8",
    muted: "#64748b",
    placeholder: "#475569",
    link: "#93c5fd",
  },
  border: {
    default: "#334155",
    subtle: "#1e293b",
    active: "#3b82f6",
    separator: "#0f1e35",
  },
  accent: {
    blue: "#3b82f6",
    blueLight: "#93c5fd",
    green: "#10b981",
    greenLight: "#86efac",
    red: "#ef4444",
    redLight: "#fca5a5",
    yellow: "#f59e0b",
    yellowLight: "#fcd34d",
    orange: "#ea580c",
    orangeLight: "#fdba74",
    purple: "#7c3aed",
    purpleLight: "#c4b5fd",
    cyan: "#0891b2",
    cyanLight: "#67e8f9",
  },
} as const;

export type Theme = typeof theme;
