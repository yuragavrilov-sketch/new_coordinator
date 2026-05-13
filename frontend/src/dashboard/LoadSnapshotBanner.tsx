import React, { useState } from "react";
import { t } from "../theme";
import { Icon } from "../components/ui";

interface Props {
  srcSchema: string;
  tgtSchema: string;
  onLoaded:  () => void;
}

/** Inline CTA shown above the empty object table — fires POST /api/catalog/load
 *  to populate ddl_snapshots so the dashboard can show tables/views/PL-SQL. */
export function LoadSnapshotBanner({ srcSchema, tgtSchema, onLoaded }: Props) {
  const [loading, setLoading] = useState(false);
  const [error,   setError]   = useState<string | null>(null);
  const [done,    setDone]    = useState(false);

  const load = async () => {
    if (!srcSchema || !tgtSchema) {
      setError("src_schema / tgt_schema не заданы у миграции");
      return;
    }
    setLoading(true); setError(null);
    try {
      const r = await fetch("/api/catalog/load", {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify({ src_schema: srcSchema, tgt_schema: tgtSchema }),
      });
      if (!r.ok) {
        const body = await r.json().catch(() => ({}));
        throw new Error(body.error || `HTTP ${r.status}`);
      }
      setDone(true);
      onLoaded();
    } catch (e: unknown) {
      setError((e as Error).message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{
      background:   t.bg.s1,
      border:       `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.lg,
      padding:      "14px 18px",
      marginBottom: 12,
      display:      "flex",
      gap:          14,
      alignItems:   "center",
      boxShadow:    t.shadow.s1,
    }}>
      <div style={{
        width: 36, height: 36, borderRadius: 8,
        background: t.tone.accentSoft, color: t.tone.accent,
        display: "grid", placeItems: "center", flexShrink: 0,
      }}>
        <Icon name="db" size={18}/>
      </div>
      <div style={{ flex: 1 }}>
        <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 2 }}>
          {done ? "Snapshot загружен" : "Список объектов пуст"}
        </div>
        <div style={{ fontSize: 12, color: t.text.muted, lineHeight: 1.4 }}>
          {error ? (
            <span style={{ color: t.tone.error }}>⚠ {error}</span>
          ) : done ? (
            <>Объекты подтянутся через несколько секунд.</>
          ) : (
            <>
              Нет DDL snapshot и нет миграций таблиц для пары{" "}
              <span style={{ fontFamily: t.font.mono }}>{srcSchema || "?"}</span>
              {" → "}
              <span style={{ fontFamily: t.font.mono }}>{tgtSchema || "?"}</span>.
              Загрузите snapshot чтобы увидеть таблицы, views, PL/SQL.
            </>
          )}
        </div>
      </div>
      {!done && (
        <button
          onClick={load}
          disabled={loading || !srcSchema || !tgtSchema}
          style={{
            display: "inline-flex", alignItems: "center", gap: 6,
            padding: "7px 14px",
            background: loading ? t.bg.s3 : t.text.primary,
            color:      loading ? t.text.muted : t.text.inverse,
            border:     `1px solid ${loading ? t.border.subtle : t.text.primary}`,
            borderRadius: t.radius.sm,
            fontSize: 12, fontWeight: 500,
            cursor: loading ? "wait" : "pointer",
            flexShrink: 0,
          }}
        >
          <Icon name={loading ? "rotate" : "db"} size={14}/>
          {loading ? "Загружаем…" : "Загрузить snapshot"}
        </button>
      )}
    </div>
  );
}
