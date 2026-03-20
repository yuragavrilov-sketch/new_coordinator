import React, { useEffect, useState } from "react";
import ReactDOM from "react-dom";
import type { ConnectorGroup, GroupStatus } from "../types/migration";
import { CreateGroupWizard } from "./CreateGroupWizard";

const STATUS_COLORS: Record<GroupStatus, { bg: string; text: string }> = {
  PENDING:  { bg: "#1e293b", text: "#94a3b8" },
  STARTING: { bg: "#1e3a5f", text: "#93c5fd" },
  RUNNING:  { bg: "#052e16", text: "#86efac" },
  STOPPING: { bg: "#431407", text: "#fdba74" },
  STOPPED:  { bg: "#1c1917", text: "#78716c" },
  FAILED:   { bg: "#450a0a", text: "#fca5a5" },
};

export function ConnectorGroupsPanel() {
  const [groups, setGroups] = useState<ConnectorGroup[]>([]);
  const [loading, setLoading] = useState(true);
  const [showCreate, setShowCreate] = useState(false);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [detail, setDetail] = useState<ConnectorGroup | null>(null);
  const [configModal, setConfigModal] = useState<{ json: string; name: string } | null>(null);
  const [configLoading, setConfigLoading] = useState(false);

  const load = () => {
    fetch("/api/connector-groups")
      .then(r => r.ok ? r.json() : Promise.reject())
      .then(setGroups)
      .catch(() => {})
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load();
    const id = setInterval(load, 10_000);
    return () => clearInterval(id);
  }, []);

  const toggleExpand = (gid: string) => {
    if (expanded === gid) {
      setExpanded(null);
      setDetail(null);
    } else {
      setExpanded(gid);
      fetch(`/api/connector-groups/${gid}`)
        .then(r => r.ok ? r.json() : Promise.reject())
        .then(setDetail)
        .catch(() => setDetail(null));
    }
  };

  const startGroup = (gid: string) => {
    fetch(`/api/connector-groups/${gid}/start`, { method: "POST" })
      .then(() => load())
      .catch(() => {});
  };

  const stopGroup = (gid: string) => {
    fetch(`/api/connector-groups/${gid}/stop`, { method: "POST" })
      .then(() => load())
      .catch(() => {});
  };

  const deleteGroup = (gid: string) => {
    if (!confirm("Удалить группу?")) return;
    fetch(`/api/connector-groups/${gid}`, { method: "DELETE" })
      .then(() => load())
      .catch(() => {});
  };

  const showConfig = (gid: string, groupName: string) => {
    setConfigLoading(true);
    fetch(`/api/connector-groups/${gid}/debezium-config`)
      .then(r => r.json())
      .then(d => {
        if (d.error) { alert(d.error); return; }
        setConfigModal({ json: JSON.stringify(d, null, 2), name: groupName });
      })
      .catch(e => alert(String(e)))
      .finally(() => setConfigLoading(false));
  };

  if (loading) return <div style={{ color: "#64748b", padding: 16 }}>Загрузка...</div>;

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
        <h2 style={{ margin: 0, fontSize: 15, fontWeight: 600, color: "#94a3b8" }}>
          Группы коннекторов
        </h2>
        <button
          onClick={() => setShowCreate(true)}
          style={{
            background: "#1e3a5f", border: "1px solid #1d4ed8", borderRadius: 6,
            color: "#93c5fd", padding: "5px 14px", fontSize: 12, cursor: "pointer",
          }}
        >
          + Создать группу
        </button>
      </div>

      {showCreate && <CreateGroupWizard onClose={() => setShowCreate(false)} onCreated={() => { setShowCreate(false); load(); }} />}

      {groups.length === 0 && !showCreate && (
        <div style={{ color: "#475569", padding: 24, textAlign: "center" }}>
          Нет групп коннекторов. Создайте группу для объединения таблиц в один Debezium-коннектор.
        </div>
      )}

      {groups.map(g => {
        const sc = STATUS_COLORS[g.status] || STATUS_COLORS.PENDING;
        const isExpanded = expanded === g.group_id;
        return (
          <div key={g.group_id} style={{
            background: "#0f172a", border: "1px solid #1e293b", borderRadius: 8,
            marginBottom: 8, overflow: "hidden",
          }}>
            <div
              onClick={() => toggleExpand(g.group_id)}
              style={{
                display: "flex", alignItems: "center", gap: 12, padding: "10px 14px",
                cursor: "pointer",
              }}
            >
              <span style={{
                background: sc.bg, color: sc.text,
                padding: "2px 8px", borderRadius: 4, fontSize: 11, fontWeight: 600,
              }}>
                {g.status}
              </span>
              <strong style={{ fontSize: 13, color: "#e2e8f0" }}>{g.group_name}</strong>
              <span style={{ fontSize: 11, color: "#475569" }}>
                {g.connector_name} | {g.topic_prefix}
              </span>
              <div style={{ marginLeft: "auto", display: "flex", gap: 6 }}>
                {(g.status === "PENDING" || g.status === "STOPPED" || g.status === "FAILED") && (
                  <button onClick={e => { e.stopPropagation(); startGroup(g.group_id); }} style={actionBtn("#052e16", "#16a34a")}>
                    Start
                  </button>
                )}
                {g.status === "RUNNING" && (
                  <button onClick={e => { e.stopPropagation(); stopGroup(g.group_id); }} style={actionBtn("#431407", "#ea580c")}>
                    Stop
                  </button>
                )}
                {(g.status === "PENDING" || g.status === "STOPPED") && (
                  <button onClick={e => { e.stopPropagation(); deleteGroup(g.group_id); }} style={actionBtn("#450a0a", "#dc2626")}>
                    Del
                  </button>
                )}
              </div>
            </div>

            {isExpanded && detail && detail.group_id === g.group_id && (
              <div style={{ padding: "0 14px 12px", borderTop: "1px solid #1e293b" }}>
                {g.error_text && (
                  <div style={{ color: "#fca5a5", fontSize: 11, marginTop: 8 }}>{g.error_text}</div>
                )}
                <div style={{ fontSize: 11, color: "#475569", marginTop: 8, display: "flex", alignItems: "center", gap: 8 }}>
                  <span>Source: {detail.source_connection_id} | Prefix: {detail.consumer_group_prefix || detail.topic_prefix}</span>
                  <button
                    onClick={() => showConfig(g.group_id, g.group_name)}
                    disabled={configLoading}
                    style={{
                      marginLeft: "auto",
                      background: "#1e293b", border: "1px solid #334155", borderRadius: 4,
                      color: "#94a3b8", padding: "2px 10px", fontSize: 10, cursor: "pointer",
                      fontWeight: 600,
                    }}
                  >
                    {configLoading ? "..." : "Debezium Config"}
                  </button>
                </div>
                <h4 style={{ color: "#64748b", fontSize: 12, margin: "12px 0 6px" }}>
                  Таблицы ({detail.migrations?.length || 0})
                </h4>
                {detail.migrations && detail.migrations.length > 0 ? (
                  <table style={{ width: "100%", fontSize: 11, borderCollapse: "collapse" }}>
                    <thead>
                      <tr style={{ color: "#475569", textAlign: "left" }}>
                        <th style={{ padding: "4px 8px" }}>Таблица</th>
                        <th style={{ padding: "4px 8px" }}>Фаза</th>
                        <th style={{ padding: "4px 8px" }}>Режим</th>
                      </tr>
                    </thead>
                    <tbody>
                      {detail.migrations.map(m => (
                        <tr key={m.migration_id} style={{ borderTop: "1px solid #1e293b" }}>
                          <td style={{ padding: "4px 8px", color: "#e2e8f0" }}>
                            {m.source_schema}.{m.source_table}
                          </td>
                          <td style={{ padding: "4px 8px", color: "#94a3b8" }}>{m.phase}</td>
                          <td style={{ padding: "4px 8px", color: "#64748b" }}>{m.migration_mode}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <div style={{ color: "#475569", fontSize: 11 }}>Нет миграций в группе</div>
                )}
              </div>
            )}
          </div>
        );
      })}

      {/* Debezium config modal */}
      {configModal && ReactDOM.createPortal(
        <div
          style={{
            position: "fixed", inset: 0, background: "rgba(0,0,0,.72)",
            display: "flex", alignItems: "center", justifyContent: "center",
            zIndex: 1000,
          }}
          onClick={e => { if (e.target === e.currentTarget) setConfigModal(null); }}
        >
          <div style={{
            background: "#0f172a", border: "1px solid #1e293b", borderRadius: 10,
            width: "100%", maxWidth: 720, maxHeight: "80vh",
            display: "flex", flexDirection: "column",
            boxShadow: "0 24px 48px rgba(0,0,0,.55)",
          }}>
            <div style={{
              padding: "12px 20px", borderBottom: "1px solid #1e293b",
              display: "flex", alignItems: "center", gap: 10,
            }}>
              <span style={{ fontSize: 14, fontWeight: 700, color: "#e2e8f0" }}>
                Debezium Config
              </span>
              <span style={{ fontSize: 11, color: "#475569" }}>{configModal.name}</span>
              <span style={{ flex: 1 }} />
              <button
                onClick={() => {
                  try {
                    if (navigator.clipboard?.writeText) {
                      navigator.clipboard.writeText(configModal.json);
                    } else {
                      const ta = document.createElement("textarea");
                      ta.value = configModal.json;
                      ta.style.position = "fixed";
                      ta.style.opacity = "0";
                      document.body.appendChild(ta);
                      ta.select();
                      document.execCommand("copy");
                      document.body.removeChild(ta);
                    }
                  } catch {}
                }}
                style={{
                  background: "#1e293b", border: "1px solid #334155", borderRadius: 4,
                  color: "#94a3b8", padding: "3px 10px", fontSize: 10, cursor: "pointer",
                }}
              >Copy</button>
              <button
                onClick={() => setConfigModal(null)}
                style={{
                  background: "none", border: "none", color: "#475569",
                  cursor: "pointer", fontSize: 18, lineHeight: 1, padding: "0 2px",
                }}
              >{"\u2715"}</button>
            </div>
            <pre style={{
              padding: 20, margin: 0, overflowY: "auto",
              fontSize: 11, lineHeight: 1.5, color: "#e2e8f0",
              fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
              whiteSpace: "pre-wrap", wordBreak: "break-all",
            }}>{configModal.json}</pre>
          </div>
        </div>,
        document.body,
      )}
    </div>
  );
}

function actionBtn(bg: string, border: string): React.CSSProperties {
  return {
    background: bg, border: `1px solid ${border}`, borderRadius: 4,
    color: border, padding: "2px 10px", fontSize: 11, cursor: "pointer",
  };
}
