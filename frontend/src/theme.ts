/**
 * Design tokens — single source of truth for colors, spacing, typography.
 *
 * Usage in inline styles:
 *   <div style={{ background: t.bg.s1, color: t.text.primary, padding: t.space[3] }}>
 *
 * Values resolve to `var(--token-name)`, so future palette swaps need only
 * change the CSS variables in `themeCss` below.
 */

export const t = {
  bg: {
    app: "var(--bg-app)",
    s1:  "var(--bg-s1)",   // primary surface (panels, list rows)
    s2:  "var(--bg-s2)",   // secondary surface (toolbar, inputs)
    s3:  "var(--bg-s3)",   // tertiary surface (hover)
    deep:"var(--bg-deep)", // deepest (inside panels)
  },
  border: {
    subtle:  "var(--border-subtle)",
    base:    "var(--border-base)",
    strong:  "var(--border-strong)",
  },
  text: {
    primary:   "var(--text-primary)",
    secondary: "var(--text-secondary)",
    muted:     "var(--text-muted)",
    disabled:  "var(--text-disabled)",
    faint:     "var(--text-faint)",
    inverse:   "var(--text-inverse)",
  },
  blue: {
    base:  "var(--blue)",
    dim:   "var(--blue-dim)",
    bg:    "var(--blue-bg)",
    fg:    "var(--blue-fg)",
  },
  green: {
    base:  "var(--green)",
    dim:   "var(--green-dim)",
    bg:    "var(--green-bg)",
    fg:    "var(--green-fg)",
    border:"var(--green-border)",
  },
  amber: {
    base:  "var(--amber)",
    dim:   "var(--amber-dim)",
    bg:    "var(--amber-bg)",
    fg:    "var(--amber-fg)",
  },
  red: {
    base:  "var(--red)",
    dim:   "var(--red-dim)",
    bg:    "var(--red-bg)",
    fg:    "var(--red-fg)",
    border:"var(--red-border)",
  },
  purple: {
    base:  "var(--purple)",
    fg:    "var(--purple-fg)",
    bg:    "var(--purple-bg)",
  },
  radius: {
    sm: "4px",
    md: "6px",
    lg: "8px",
    pill: "999px",
  },
  space: {
    1: "4px",
    2: "8px",
    3: "12px",
    4: "16px",
    5: "20px",
    6: "24px",
    8: "32px",
  },
  font: {
    sans: "'Inter', 'Segoe UI', sans-serif",
    mono: "'JetBrains Mono', ui-monospace, Menlo, monospace",
  },
  size: {
    xs:   "10px",
    sm:   "11px",
    base: "12px",
    md:   "13px",
    lg:   "14px",
    xl:   "18px",
    xxl:  "24px",
  },
} as const;

/**
 * Global stylesheet — injected once via main.tsx.
 * Keeps palette identical to legacy hardcoded hex values for backwards
 * compatibility while components are migrated piecemeal.
 */
export const themeCss = `
  :root {
    /* surfaces */
    --bg-app:    #0f172a;
    --bg-s1:     #0a111f;
    --bg-s2:     #1e293b;
    --bg-s3:     #1e3a5f;
    --bg-deep:   #060e1a;

    /* borders */
    --border-subtle: #1e293b;
    --border-base:   #334155;
    --border-strong: #475569;

    /* text */
    --text-primary:   #e2e8f0;
    --text-secondary: #94a3b8;
    --text-muted:     #64748b;
    --text-disabled:  #475569;
    --text-faint:     #334155;
    --text-inverse:   #ffffff;

    /* blue */
    --blue:     #3b82f6;
    --blue-dim: #1d4ed8;
    --blue-bg:  #1e3a5f;
    --blue-fg:  #93c5fd;

    /* green */
    --green:        #22c55e;
    --green-dim:    #16a34a;
    --green-bg:     #052e16;
    --green-fg:     #86efac;
    --green-border: #166534;

    /* amber */
    --amber:     #eab308;
    --amber-dim: #d97706;
    --amber-bg:  #3b2000;
    --amber-fg:  #fcd34d;

    /* red */
    --red:        #ef4444;
    --red-dim:    #dc2626;
    --red-bg:     #450a0a;
    --red-fg:     #fca5a5;
    --red-border: #7f1d1d;

    /* purple */
    --purple:    #7c3aed;
    --purple-fg: #c4b5fd;
    --purple-bg: #2e1065;

    color-scheme: dark;
  }

  * { box-sizing: border-box; }
  input, button, textarea, select { outline: none; }

  @keyframes pulse  { 0%,100%{opacity:1} 50%{opacity:.4} }
  @keyframes fadeIn { from{opacity:0;transform:translateY(-4px)} to{opacity:1;transform:none} }

  ::-webkit-scrollbar       { width: 6px; height: 6px; }
  ::-webkit-scrollbar-track { background: #0a111f; }
  ::-webkit-scrollbar-thumb { background: #1e293b; border-radius: 3px; }
  ::-webkit-scrollbar-thumb:hover { background: #334155; }
`;
