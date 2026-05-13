import React from "react";
import { t } from "../theme";
import { Icon } from "../components/ui";
import type { IconName } from "../components/ui";

export type NavKey =
  | "dashboard"   // Эта миграция
  | "history"     // История
  | "clusters"    // Кластеры
  | "rules"       // Правила conversion
  | "settings";

export const NAV_ITEMS: { key: NavKey; label: string; icon: IconName; badge?: number }[] = [
  { key: "dashboard", label: "Эта миграция", icon: "dashboard" },
  { key: "history",   label: "История",      icon: "history", badge: 8 },
  { key: "clusters",  label: "Кластеры",     icon: "clusters" },
  { key: "rules",     label: "Правила conversion", icon: "rules" },
  { key: "settings",  label: "Настройки",    icon: "settings" },
];

interface Props {
  active:        NavKey;
  onChange:      (key: NavKey) => void;
  schemaName:    string;
  migrationId:   string;
}

export function Sidebar({
  active, onChange, schemaName, migrationId,
}: Props) {
  return (
    <aside style={{
      background:    t.bg.s2,
      borderRight:   `1px solid ${t.border.subtle}`,
      padding:       "16px 12px",
      display:       "flex",
      flexDirection: "column",
      gap:           14,
      position:      "sticky",
      top:           0,
      height:        "100vh",
      minHeight:     0,
    }}>
      {/* Brand block */}
      <div style={{
        display:    "flex",
        gap:        10,
        alignItems: "center",
        padding:    "2px 6px 12px",
        borderBottom: `1px solid ${t.border.subtle}`,
      }}>
        <span style={{
          width: 30, height: 30,
          display: "grid", placeItems: "center",
          background: t.text.primary,
          color: t.text.inverse,
          borderRadius: t.radius.sm,
        }}>
          <svg width={16} height={16} viewBox="0 0 24 24" fill="none" stroke="currentColor"
               strokeWidth={1.6} strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
            <rect x="3" y="4" width="18" height="6" rx="1.5"/>
            <rect x="3" y="14" width="18" height="6" rx="1.5"/>
            <circle cx="7" cy="7" r="0.5" fill="currentColor"/>
            <circle cx="7" cy="17" r="0.5" fill="currentColor"/>
          </svg>
        </span>
        <div style={{ lineHeight: 1.15 }}>
          <div style={{ fontWeight: 600, fontSize: 13, letterSpacing: "-0.01em" }}>CDC·Migrator</div>
          <div style={{ fontSize: 11, color: t.text.muted }}>Oracle → Oracle</div>
        </div>
      </div>

      {/* Active migration picker */}
      <div style={{ padding: "0 2px" }}>
        <div style={{
          fontSize: "10.5px", textTransform: "uppercase",
          letterSpacing: "0.06em", fontWeight: 500,
          marginBottom: 6, padding: "0 6px",
          color: t.text.muted,
        }}>
          Активная миграция
        </div>
        <button style={{
          display: "flex", alignItems: "center", gap: 8,
          padding: "8px 10px", width: "100%",
          background: t.bg.s1,
          border: `1px solid ${t.border.subtle}`,
          borderRadius: t.radius.sm,
          textAlign: "left",
          cursor: "pointer",
        }}>
          <span style={{
            flex: 1,
            fontFamily: t.font.mono,
            fontSize: 13, fontWeight: 600,
            letterSpacing: "-0.01em",
          }}>
            {schemaName}
          </span>
          <span style={{ fontFamily: t.font.mono, fontSize: "10.5px", color: t.text.muted }}>
            {migrationId}
          </span>
          <span style={{ color: t.text.muted, display: "flex" }}>
            <Icon name="chevron" size={12}/>
          </span>
        </button>
      </div>

      {/* Nav */}
      <nav style={{ display: "flex", flexDirection: "column", gap: 1 }}>
        {NAV_ITEMS.map(item => {
          const isActive = item.key === active;
          return (
            <button
              key={item.key}
              onClick={() => onChange(item.key)}
              style={{
                display: "flex", alignItems: "center", gap: 9,
                padding: "6px 10px",
                borderRadius: t.radius.sm,
                fontSize: "12.5px", fontWeight: 500,
                textAlign: "left",
                border: "none", cursor: "pointer",
                background: isActive ? t.bg.s1 : "transparent",
                color: isActive ? t.text.primary : t.text.secondary,
                boxShadow: isActive ? t.shadow.s1 : "none",
              }}
            >
              <span style={{ color: isActive ? t.tone.accent : "currentColor", display: "flex" }}>
                <Icon name={item.icon} size={15}/>
              </span>
              <span style={{ flex: 1 }}>{item.label}</span>
              {item.badge != null && (
                <span style={{
                  background:   isActive ? t.tone.accentSoft : t.bg.s3,
                  color:        isActive ? t.tone.accent : t.text.secondary,
                  padding:      "0 6px",
                  borderRadius: t.radius.pill,
                  fontSize:     "10.5px",
                  fontFamily:   t.font.mono,
                }}>
                  {item.badge}
                </span>
              )}
            </button>
          );
        })}
      </nav>

    </aside>
  );
}
