import React, { useState, useMemo } from "react";
import { S } from "./styles";
import { MatchBadge } from "./StatusBadges";
import { ObjectActions } from "./ObjectActions";
import { CatalogObject } from "./TablesTab";

interface Props {
  objects: CatalogObject[];
  syncBusy: Set<string>;
  onCompare: (type: string, name: string) => void;
  onSync: (type: string, name: string, action: string) => void;
}

function isMView(meta: Record<string, unknown>): boolean {
  return "refresh_type" in meta;
}

function ViewDiffSummary({ diff }: { diff: Record<string, unknown> }) {
  if (!diff || diff.ok === true) return null;
  const items: string[] = [];
  if (diff.sql_match === false) items.push("SQL-текст отличается");
  if (diff.status_match === false) items.push("Статус отличается (VALID/INVALID)");
  if (diff.refresh_match === false) items.push("Тип refresh отличается");
  if (items.length === 0) return null;
  return (
    <div style={{
      background: "#1c1007", border: "1px solid #854d0e44", borderRadius: 6,
      padding: "10px 14px", marginBottom: 6,
    }}>
      <div style={{ fontSize: 11, fontWeight: 700, color: "#eab308", marginBottom: 4 }}>РАЗЛИЧИЯ С ТАРГЕТОМ</div>
      {items.map((item, i) => (
        <div key={i} style={{ fontSize: 11, color: "#fde68a" }}>{item}</div>
      ))}
    </div>
  );
}

function ViewDetail({ obj }: { obj: CatalogObject }) {
  const meta = obj.metadata;
  const cols = (meta.columns as Record<string, unknown>[]) ?? [];
  const sql = (meta.sql_text as string) ?? "";
  const mview = isMView(meta);

  const sectionStyle: React.CSSProperties = {
    background: "#07101e",
    border: "1px solid #1e293b",
    borderRadius: 6,
    marginTop: 4,
    overflow: "hidden",
  };
  const sectionHeader: React.CSSProperties = {
    padding: "5px 12px",
    fontSize: 11,
    fontWeight: 700,
    color: "#64748b",
    letterSpacing: 0.5,
    background: "#0a111f",
    borderBottom: "1px solid #1e293b",
  };

  return (
    <td colSpan={5} style={{ padding: "8px 16px 12px 32px", background: "#07101e" }}>
      {obj.match_status === "DIFF" && <ViewDiffSummary diff={obj.diff} />}
      {mview && (
        <div style={{ fontSize: 11, color: "#64748b", marginBottom: 6 }}>
          Refresh: <span style={{ color: "#94a3b8" }}>{(meta.refresh_type as string) ?? "—"}</span>
          {meta.last_refresh ? <span style={{ marginLeft: 12 }}>Last: <span style={{ color: "#94a3b8" }}>{String(meta.last_refresh)}</span></span> : null}
        </div>
      )}
      {cols.length > 0 && (
        <div style={sectionStyle}>
          <div style={sectionHeader}>КОЛОНКИ ({cols.length})</div>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr>
                {["#", "Имя", "Тип", "Nullable"].map(h => (
                  <th key={h} style={S.th}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {cols.map((c, i) => (
                <tr key={i} style={S.trBorder}>
                  <td style={{ ...S.td, color: "#475569" }}>{i + 1}</td>
                  <td style={{ ...S.td, color: "#e2e8f0" }}>{(c.name ?? c.column_name) as string}</td>
                  <td style={{ ...S.td, color: "#94a3b8" }}>
                    {c.data_type as string}{c.data_length ? `(${c.data_length})` : ""}
                  </td>
                  <td style={S.td}>
                    {(c.nullable === true || c.nullable === "Y")
                      ? <span style={{ color: "#22c55e", fontSize: 11 }}>Y</span>
                      : <span style={{ color: "#ef4444", fontSize: 11 }}>N</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {sql && (
        <div style={{ ...sectionStyle, marginTop: 8 }}>
          <div style={sectionHeader}>SQL</div>
          <pre style={{
            margin: 0, padding: "10px 12px",
            fontSize: 11, color: "#94a3b8",
            fontFamily: "monospace", whiteSpace: "pre-wrap",
            overflowX: "auto", maxHeight: 300,
          }}>
            {sql}
          </pre>
        </div>
      )}
    </td>
  );
}

export function ViewsTab({ objects, syncBusy, onCompare, onSync }: Props) {
  const [search, setSearch] = useState("");
  const [expandedObj, setExpandedObj] = useState<string | null>(null);

  const filtered = useMemo(() => {
    return objects.filter(o =>
      o.object_name.toLowerCase().includes(search.toLowerCase())
    );
  }, [objects, search]);

  return (
    <div style={S.card}>
      <div style={S.cardHeader}>
        <input
          value={search}
          onChange={e => setSearch(e.target.value)}
          placeholder="Поиск views..."
          style={{ ...S.input, width: 220 }}
        />
        <span style={{ marginLeft: "auto", fontSize: 12, color: "#475569" }}>
          {filtered.length} / {objects.length}
        </span>
      </div>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ background: "#0a111f" }}>
            <th style={S.th}>Имя</th>
            <th style={S.th}>Тип</th>
            <th style={S.th}>Oracle статус</th>
            <th style={S.th}>Совпадение</th>
            <th style={S.th}>Действия</th>
          </tr>
        </thead>
        <tbody>
          {filtered.map(obj => {
            const expanded = expandedObj === obj.object_name;
            const mview = isMView(obj.metadata);
            const objType = mview ? "MVIEW" : "VIEW";
            return (
              <React.Fragment key={obj.object_name}>
                <tr style={S.trBorder}>
                  <td style={S.td}>
                    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                      <button
                        onClick={() => setExpandedObj(expanded ? null : obj.object_name)}
                        style={{
                          background: "none", border: "none", color: "#475569",
                          cursor: "pointer", fontSize: 12, padding: "0 2px",
                        }}
                      >
                        {expanded ? "▼" : "▶"}
                      </button>
                      <span style={{ color: "#e2e8f0", fontFamily: "monospace" }}>
                        {obj.object_name}
                      </span>
                    </div>
                  </td>
                  <td style={S.td}>
                    {mview
                      ? <span style={S.badge("#0ea5e922", "#0ea5e9")}>MVIEW</span>
                      : <span style={S.badge("#3b82f622", "#3b82f6")}>VIEW</span>
                    }
                  </td>
                  <td style={{ ...S.td, color: "#94a3b8" }}>{obj.oracle_status}</td>
                  <td style={S.td}><MatchBadge status={obj.match_status} /></td>
                  <td style={S.td}>
                    <ObjectActions
                      objectType={objType}
                      objectName={obj.object_name}
                      matchStatus={obj.match_status}
                      syncBusy={syncBusy.has(obj.object_name)}
                      onCompare={onCompare}
                      onSync={onSync}
                      onShowDetail={() => setExpandedObj(expanded ? null : obj.object_name)}
                    />
                  </td>
                </tr>
                {expanded && (
                  <tr style={{ background: "#07101e" }}>
                    <ViewDetail obj={obj} />
                  </tr>
                )}
              </React.Fragment>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
