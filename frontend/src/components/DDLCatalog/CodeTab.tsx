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

type CodeTypeFilter = "ALL" | "FUNCTION" | "PROCEDURE" | "PACKAGE";

function detectCodeType(meta: Record<string, unknown>): string {
  if ("spec_source" in meta) return "PACKAGE";
  if ("source" in meta && "body_source" in meta) return "TYPE";
  return (meta.object_type as string) ?? "FUNCTION";
}

function CodeDiffSummary({ diff, codeType }: { diff: Record<string, unknown>; codeType: string }) {
  if (!diff || diff.ok === true) return null;
  const items: string[] = [];
  if (codeType === "PACKAGE") {
    if (diff.spec_match === false) items.push("Спецификация отличается");
    if (diff.body_match === false) items.push("Тело пакета отличается");
  } else if (codeType === "TYPE") {
    if (diff.source_match === false) items.push("Спецификация типа отличается");
    if (diff.body_match === false) items.push("Тело типа отличается");
  } else {
    if (diff.code_match === false) items.push("Исходный код отличается");
  }
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

function CodeDetail({ obj }: { obj: CatalogObject }) {
  const [bodyOpen, setBodyOpen] = useState(false);
  const meta = obj.metadata;
  const codeType = detectCodeType(meta);

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
  const preStyle: React.CSSProperties = {
    margin: 0, padding: "10px 12px",
    fontSize: 11, color: "#94a3b8",
    fontFamily: "monospace", whiteSpace: "pre-wrap",
    overflowX: "auto", maxHeight: 400,
  };

  const argCount = meta.argument_count as number | undefined;

  return (
    <td colSpan={5} style={{ padding: "8px 16px 12px 32px", background: "#07101e" }}>
      {obj.match_status === "DIFF" && <CodeDiffSummary diff={obj.diff} codeType={codeType} />}
      {(codeType === "FUNCTION" || codeType === "PROCEDURE") && argCount != null && (
        <div style={{ marginBottom: 6, fontSize: 12, color: "#64748b" }}>
          Аргументов: <span style={{ color: "#94a3b8", fontWeight: 600 }}>{argCount}</span>
        </div>
      )}

      {codeType === "PACKAGE" ? (
        <>
          <div style={sectionStyle}>
            <div style={sectionHeader}>СПЕЦИФИКАЦИЯ</div>
            <pre style={preStyle}>{(meta.spec_source as string) ?? ""}</pre>
          </div>
          <div style={{ ...sectionStyle, marginTop: 8 }}>
            <div
              style={{ ...sectionHeader, cursor: "pointer", display: "flex", alignItems: "center", gap: 6 }}
              onClick={() => setBodyOpen(v => !v)}
            >
              <span>{bodyOpen ? "▼" : "▶"}</span>
              <span>ТЕЛО ПАКЕТА</span>
            </div>
            {bodyOpen && (
              <pre style={preStyle}>{(meta.body_source as string) ?? ""}</pre>
            )}
          </div>
        </>
      ) : codeType === "TYPE" ? (
        <>
          {meta.source && (
            <div style={sectionStyle}>
              <div style={sectionHeader}>СПЕЦИФИКАЦИЯ</div>
              <pre style={preStyle}>{meta.source as string}</pre>
            </div>
          )}
          {meta.body_source && (
            <div style={{ ...sectionStyle, marginTop: 8 }}>
              <div
                style={{ ...sectionHeader, cursor: "pointer", display: "flex", alignItems: "center", gap: 6 }}
                onClick={() => setBodyOpen(v => !v)}
              >
                <span>{bodyOpen ? "▼" : "▶"}</span>
                <span>ТЕЛО</span>
              </div>
              {bodyOpen && (
                <pre style={preStyle}>{meta.body_source as string}</pre>
              )}
            </div>
          )}
        </>
      ) : (
        <div style={sectionStyle}>
          <div style={sectionHeader}>ИСХОДНЫЙ КОД</div>
          <pre style={preStyle}>{(meta.source_code as string) ?? (meta.source as string) ?? ""}</pre>
        </div>
      )}
    </td>
  );
}

export function CodeTab({ objects, syncBusy, onCompare, onSync }: Props) {
  const [search, setSearch] = useState("");
  const [typeFilter, setTypeFilter] = useState<CodeTypeFilter>("ALL");
  const [expandedObj, setExpandedObj] = useState<string | null>(null);

  const filtered = useMemo(() => {
    return objects.filter(o => {
      const matchSearch = o.object_name.toLowerCase().includes(search.toLowerCase());
      if (!matchSearch) return false;
      if (typeFilter === "ALL") return true;
      const ct = detectCodeType(o.metadata);
      return ct === typeFilter;
    });
  }, [objects, search, typeFilter]);

  const typeFilterBtns: CodeTypeFilter[] = ["ALL", "FUNCTION", "PROCEDURE", "PACKAGE"];
  const typeFilterLabels: Record<CodeTypeFilter, string> = {
    ALL: "Все", FUNCTION: "Function", PROCEDURE: "Procedure", PACKAGE: "Package",
  };

  return (
    <div style={S.card}>
      <div style={S.cardHeader}>
        <input
          value={search}
          onChange={e => setSearch(e.target.value)}
          placeholder="Поиск объектов..."
          style={{ ...S.input, width: 220 }}
        />
        <div style={{ display: "flex", gap: 4 }}>
          {typeFilterBtns.map(f => (
            <button
              key={f}
              onClick={() => setTypeFilter(f)}
              style={typeFilter === f ? S.btnPrimary : S.btnSecondary}
            >
              {typeFilterLabels[f]}
            </button>
          ))}
        </div>
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
            const codeType = detectCodeType(obj.metadata);
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
                    <span style={S.badge("#8b5cf622", "#8b5cf6")}>{codeType}</span>
                  </td>
                  <td style={{ ...S.td, color: "#94a3b8" }}>{obj.oracle_status}</td>
                  <td style={S.td}><MatchBadge status={obj.match_status} /></td>
                  <td style={S.td}>
                    <ObjectActions
                      objectType={codeType}
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
                    <CodeDetail obj={obj} />
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
