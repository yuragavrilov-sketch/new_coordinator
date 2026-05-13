import React, { useState } from "react";
import { t } from "../theme";
import { Icon } from "../components/ui";
import { OBJECT_TYPES, type SchemaObject } from "./types";

interface Props {
  missing:      SchemaObject[];
  diff:         SchemaObject[];
  srcInvalid:   SchemaObject[];
  tgtInvalid:   SchemaObject[];
  bothInvalid:  SchemaObject[];
  onOpen:       (o: SchemaObject) => void;
}

/** Card above the object table summarising decision-required objects:
 *  - missing in target (must be created)
 *  - DDL differs (review)
 *  - INVALID in source (would propagate the error)
 *  - INVALID in target only (post-migration breakage)
 *  - INVALID in both (pre-existing — verify, don't auto-fail)
 */
export function ProblemsSummary({ missing, diff, srcInvalid, tgtInvalid, bothInvalid, onOpen }: Props) {
  const total = missing.length + diff.length + srcInvalid.length + tgtInvalid.length + bothInvalid.length;
  if (total === 0) return null;

  return (
    <div style={{
      background:   t.bg.s1,
      border:       `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.lg,
      padding:      14,
      marginBottom: 12,
      boxShadow:    t.shadow.s1,
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
        <span style={{
          display: "grid", placeItems: "center",
          width: 22, height: 22, borderRadius: 6,
          background: t.tone.warnSoft, color: t.tone.warn,
        }}>
          <Icon name="warn" size={14}/>
        </span>
        <span style={{ fontSize: "12.5px", fontWeight: 600 }}>
          Требуется решение
        </span>
        <span style={{ fontFamily: t.font.mono, fontSize: 11, color: t.text.muted }}>
          {total}
        </span>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        <Bucket
          label="Нет в target — нужно создать"
          tone="warn"
          items={missing}
          onOpen={onOpen}
        />
        <Bucket
          label="DDL отличается — проверить и засинкать"
          tone="warn"
          items={diff}
          onOpen={onOpen}
        />
        <Bucket
          label="INVALID в source — миграция перенесёт ошибку"
          tone="error"
          items={srcInvalid}
          onOpen={onOpen}
        />
        <Bucket
          label="INVALID только в target — пересоздать"
          tone="warn"
          items={tgtInvalid}
          onOpen={onOpen}
        />
        <Bucket
          label="INVALID в обоих — pre-existing, проверить"
          tone="info"
          items={bothInvalid}
          onOpen={onOpen}
        />
      </div>
    </div>
  );
}

function Bucket({ label, tone, items, onOpen }: {
  label:   string;
  tone:    "info" | "warn" | "error";
  items:   SchemaObject[];
  onOpen:  (o: SchemaObject) => void;
}) {
  const [expanded, setExpanded] = useState(false);
  if (items.length === 0) return null;
  const dotColor =
    tone === "error" ? t.tone.error :
    tone === "warn"  ? t.tone.warn  :
                       t.tone.info;
  const visible = expanded ? items : items.slice(0, 6);

  return (
    <div>
      <button
        onClick={() => setExpanded(e => !e)}
        style={{
          display: "flex", alignItems: "center", gap: 8,
          background: "none", border: "none",
          padding: "4px 0", cursor: "pointer",
          textAlign: "left", color: t.text.primary,
        }}
      >
        <span aria-hidden style={{
          width: 6, height: 6, borderRadius: "50%",
          background: dotColor, flexShrink: 0,
        }}/>
        <span style={{ fontSize: 12, fontWeight: 500 }}>{label}</span>
        <span style={{ fontFamily: t.font.mono, fontSize: 11, color: t.text.muted }}>
          {items.length}
        </span>
        {items.length > 6 && (
          <span style={{ color: t.text.muted, fontSize: 11, marginLeft: "auto" }}>
            {expanded ? "свернуть" : `показать все`}
          </span>
        )}
      </button>
      <div style={{
        display: "flex", flexWrap: "wrap", gap: 4,
        paddingLeft: 14,
      }}>
        {visible.map(o => (
          <button
            key={o.id}
            onClick={() => onOpen(o)}
            title={o.note}
            style={{
              display: "inline-flex", alignItems: "center", gap: 5,
              padding: "2px 8px",
              background: t.bg.s2,
              border: `1px solid ${t.border.subtle}`,
              borderRadius: t.radius.pill,
              fontSize: "11.5px",
              cursor: "pointer",
            }}
          >
            <span style={{
              fontFamily: t.font.mono,
              fontSize: "9.5px", fontWeight: 600,
              letterSpacing: "0.05em",
              color: t.text.muted,
              textTransform: "uppercase",
            }}>
              {OBJECT_TYPES[o.type]?.label || o.type}
            </span>
            <span style={{ fontFamily: t.font.mono, color: t.text.primary }}>
              {o.name}
            </span>
          </button>
        ))}
      </div>
    </div>
  );
}
