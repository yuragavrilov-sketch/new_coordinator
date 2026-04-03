import React, { useEffect, useState } from "react";
import ReactDOM from "react-dom";
import type { ConnectorGroup, GroupStatus, MigrationSummary } from "../types/migration";
import { phaseColor } from "../types/migration";
import { CreateGroupWizard } from "./CreateGroupWizard";

const STATUS_COLORS: Record<GroupStatus, { bg: string; text: string }> = {
  PENDING:            { bg: "#1e293b", text: "#94a3b8" },
  TOPICS_CREATING:    { bg: "#1e3a5f", text: "#93c5fd" },
  CONNECTOR_STARTING: { bg: "#1e3a5f", text: "#93c5fd" },
  RUNNING:            { bg: "#052e16", text: "#86efac" },
  STOPPING:           { bg: "#431407", text: "#fdba74" },
  STOPPED:            { bg: "#1c1917", text: "#78716c" },
  FAILED:             { bg: "#450a0a", text: "#fca5a5" },
};

interface TopicCount {
  topic_name: string;
  count: number;
  exists: boolean;
}

interface GroupTable {
  id: string;
  source_schema: string;
  source_table: string;
  target_schema: string;
  target_table: string;
  effective_key_type: string;
  topic_name: string;
}

export function ConnectorGroupsPanel() {
  const [groups, setGroups] = useState<ConnectorGroup[]>([]);
  const [loading, setLoading] = useState(true);
  const [showCreate, setShowCreate] = useState(false);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [detail, setDetail] = useState<(ConnectorGroup & { tables?: GroupTable[] }) | null>(null);
  const [configModal, setConfigModal] = useState<{ json: string; name: string } | null>(null);
  const [configLoading, setConfigLoading] = useState(false);
  const [topicCounts, setTopicCounts] = useState<Map<string, TopicCount>>(new Map());
  const [topicLoading, setTopicLoading] = useState(false);
  const [history, setHistory] = useState<{ from_status: string | null; to_status: string; message: string | null; created_at: string }[]>([]);
  const [migrateModal, setMigrateModal] = useState<{ groupId: string; table: GroupTable } | null>(null);

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
      setTopicCounts(new Map());
      setHistory([]);
    } else {
      setExpanded(gid);
      fetch(`/api/connector-groups/${gid}`)
        .then(r => r.ok ? r.json() : Promise.reject())
        .then(d => {
          setDetail(d);
          loadTopicCounts(gid);
          loadHistory(gid);
        })
        .catch(() => setDetail(null));
    }
  };

  const loadHistory = (gid: string) => {
    fetch(`/api/connector-groups/${gid}/history`)
      .then(r => r.ok ? r.json() : [])
      .then(setHistory)
      .catch(() => setHistory([]));
  };

  const loadTopicCounts = (gid: string) => {
    setTopicLoading(true);
    fetch(`/api/connector-groups/${gid}/topic-counts`)
      .then(r => r.ok ? r.json() : [])
      .then((counts: TopicCount[]) => {
        const map = new Map<string, TopicCount>();
        for (const c of counts) map.set(c.topic_name, c);
        setTopicCounts(map);
      })
      .catch(() => {})
      .finally(() => setTopicLoading(false));
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
      .then(() => { load(); if (expanded === gid) { setExpanded(null); setDetail(null); } })
      .catch(() => {});
  };

  const createTopics = (gid: string) => {
    fetch(`/api/connector-groups/${gid}/create-topics`, { method: "POST" })
      .then(() => loadTopicCounts(gid))
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

  const onMigrationCreated = (gid: string) => {
    setMigrateModal(null);
    fetch(`/api/connector-groups/${gid}`)
      .then(r => r.ok ? r.json() : Promise.reject())
      .then(setDetail)
      .catch(() => {});
  };

  // Build a map: "SCHEMA.TABLE" -> migration summary for the current group
  const tableMigrationMap = new Map<string, MigrationSummary>();
  if (detail?.migrations) {
    for (const m of detail.migrations) {
      const key = `${m.source_schema}.${m.source_table}`;
      // Keep the most recent active migration (non-terminal)
      const existing = tableMigrationMap.get(key);
      if (!existing || (existing.phase === "CANCELLED" || existing.phase === "FAILED" || existing.phase === "COMPLETED")) {
        tableMigrationMap.set(key, m);
      }
    }
  }

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
                <div style={{ fontSize: 11, color: "#475569", marginTop: 8, display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                  <span>Source: {detail.source_connection_id} | Prefix: {detail.consumer_group_prefix || detail.topic_prefix}</span>
                  <span style={{ flex: 1 }} />
                  <button
                    onClick={() => createTopics(g.group_id)}
                    style={{
                      background: "#1e293b", border: "1px solid #334155", borderRadius: 4,
                      color: "#94a3b8", padding: "2px 10px", fontSize: 10, cursor: "pointer",
                      fontWeight: 600,
                    }}
                  >
                    Create Topics
                  </button>
                  <button
                    onClick={() => loadTopicCounts(g.group_id)}
                    disabled={topicLoading}
                    style={{
                      background: "#1e293b", border: "1px solid #334155", borderRadius: 4,
                      color: "#94a3b8", padding: "2px 10px", fontSize: 10, cursor: "pointer",
                      fontWeight: 600,
                    }}
                  >
                    {topicLoading ? "..." : "Refresh Counts"}
                  </button>
                  <button
                    onClick={() => showConfig(g.group_id, g.group_name)}
                    disabled={configLoading}
                    style={{
                      background: "#1e293b", border: "1px solid #334155", borderRadius: 4,
                      color: "#94a3b8", padding: "2px 10px", fontSize: 10, cursor: "pointer",
                      fontWeight: 600,
                    }}
                  >
                    {configLoading ? "..." : "Debezium Config"}
                  </button>
                </div>

                {/* Group tables */}
                <h4 style={{ color: "#64748b", fontSize: 12, margin: "12px 0 6px" }}>
                  Таблицы ({detail.tables?.length || 0})
                </h4>
                {detail.tables && detail.tables.length > 0 ? (
                  <table style={{ width: "100%", fontSize: 11, borderCollapse: "collapse" }}>
                    <thead>
                      <tr style={{ color: "#475569", textAlign: "left" }}>
                        <th style={{ padding: "4px 8px" }}>Таблица</th>
                        <th style={{ padding: "4px 8px" }}>Target</th>
                        <th style={{ padding: "4px 8px" }}>Ключ</th>
                        <th style={{ padding: "4px 8px" }}>Топик</th>
                        <th style={{ padding: "4px 8px", textAlign: "right" }}>Сообщений</th>
                        <th style={{ padding: "4px 8px", textAlign: "center" }}>Миграция</th>
                      </tr>
                    </thead>
                    <tbody>
                      {detail.tables.map(t => {
                        const tc = topicCounts.get(t.topic_name);
                        const tableKey = `${t.source_schema}.${t.source_table}`;
                        const mig = tableMigrationMap.get(tableKey);
                        const isTerminal = mig && (mig.phase === "CANCELLED" || mig.phase === "FAILED" || mig.phase === "COMPLETED");
                        const hasActive = mig && !isTerminal;
                        return (
                          <tr key={t.id} style={{ borderTop: "1px solid #1e293b" }}>
                            <td style={{ padding: "4px 8px", color: "#e2e8f0", fontFamily: "monospace" }}>
                              {t.source_schema}.{t.source_table}
                            </td>
                            <td style={{ padding: "4px 8px", color: "#64748b", fontFamily: "monospace" }}>
                              {t.target_schema}.{t.target_table}
                            </td>
                            <td style={{ padding: "4px 8px" }}>
                              <span style={{
                                fontSize: 9, padding: "1px 5px", borderRadius: 3,
                                background: t.effective_key_type === "PRIMARY_KEY" ? "#052e16" :
                                  t.effective_key_type === "UNIQUE_KEY" ? "#2e1065" :
                                  t.effective_key_type === "USER_DEFINED" ? "#1e3a5f" : "#1e293b",
                                color: t.effective_key_type === "PRIMARY_KEY" ? "#86efac" :
                                  t.effective_key_type === "UNIQUE_KEY" ? "#c4b5fd" :
                                  t.effective_key_type === "USER_DEFINED" ? "#93c5fd" : "#64748b",
                              }}>
                                {t.effective_key_type}
                              </span>
                            </td>
                            <td style={{ padding: "4px 8px", color: "#475569", fontSize: 10, fontFamily: "monospace" }}>
                              {t.topic_name}
                            </td>
                            <td style={{ padding: "4px 8px", textAlign: "right" }}>
                              {tc === undefined ? (
                                <span style={{ color: "#334155" }}>—</span>
                              ) : !tc.exists ? (
                                <span style={{ color: "#fca5a5", fontSize: 9 }}>no topic</span>
                              ) : (
                                <span style={{
                                  color: tc.count > 0 ? "#86efac" : "#64748b",
                                  fontWeight: tc.count > 0 ? 700 : 400,
                                  fontFamily: "monospace",
                                }}>
                                  {tc.count.toLocaleString()}
                                </span>
                              )}
                            </td>
                            <td style={{ padding: "4px 8px", textAlign: "center" }}>
                              {hasActive ? (() => {
                                const pc = phaseColor(mig.phase);
                                return (
                                  <span style={{
                                    fontSize: 9, padding: "1px 6px", borderRadius: 3,
                                    background: pc.bg, color: pc.text,
                                    border: `1px solid ${pc.border}`,
                                    fontWeight: 600,
                                  }}>
                                    {mig.phase}
                                  </span>
                                );
                              })() : (
                                <button
                                  onClick={() => setMigrateModal({ groupId: g.group_id, table: t })}
                                  style={{
                                    background: "#052e16", border: "1px solid #16a34a", borderRadius: 4,
                                    color: "#86efac", padding: "1px 8px", fontSize: 10,
                                    cursor: "pointer", fontWeight: 600,
                                  }}
                                >
                                  Migrate
                                </button>
                              )}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                ) : (
                  <div style={{ color: "#475569", fontSize: 11 }}>Нет таблиц в группе</div>
                )}

                {/* Migrations linked to this group */}
                {detail.migrations && detail.migrations.length > 0 && (
                  <>
                    <h4 style={{ color: "#64748b", fontSize: 12, margin: "12px 0 6px" }}>
                      Миграции ({detail.migrations.length})
                    </h4>
                    <table style={{ width: "100%", fontSize: 11, borderCollapse: "collapse" }}>
                      <thead>
                        <tr style={{ color: "#475569", textAlign: "left" }}>
                          <th style={{ padding: "4px 8px" }}>Таблица</th>
                          <th style={{ padding: "4px 8px" }}>Фаза</th>
                          <th style={{ padding: "4px 8px" }}>Режим</th>
                          <th style={{ padding: "4px 8px", textAlign: "right" }}>Действия</th>
                        </tr>
                      </thead>
                      <tbody>
                        {detail.migrations.map(m => (
                          <MigrationRow key={m.migration_id} m={m}
                            onChanged={() => toggleExpand(g.group_id)} />
                        ))}
                      </tbody>
                    </table>
                  </>
                )}

                {/* State history */}
                {history.length > 0 && (
                  <>
                    <h4 style={{ color: "#64748b", fontSize: 12, margin: "12px 0 6px" }}>
                      История ({history.length})
                    </h4>
                    <div style={{
                      maxHeight: 180, overflowY: "auto",
                      border: "1px solid #1e293b", borderRadius: 5,
                    }}>
                      {history.map((h, i) => {
                        const hsc = STATUS_COLORS[(h.to_status as GroupStatus)] || STATUS_COLORS.PENDING;
                        return (
                          <div key={i} style={{
                            display: "flex", alignItems: "center", gap: 8,
                            padding: "5px 10px", fontSize: 10,
                            borderBottom: i < history.length - 1 ? "1px solid #1e293b" : "none",
                          }}>
                            <span style={{ color: "#475569", fontFamily: "monospace", whiteSpace: "nowrap" }}>
                              {new Date(h.created_at).toLocaleString()}
                            </span>
                            {h.from_status && (
                              <span style={{ color: "#475569" }}>{h.from_status}</span>
                            )}
                            <span style={{ color: "#475569" }}>{"\u2192"}</span>
                            <span style={{
                              background: hsc.bg, color: hsc.text,
                              padding: "1px 6px", borderRadius: 3, fontWeight: 600,
                            }}>
                              {h.to_status}
                            </span>
                            {h.message && (
                              <span style={{ color: "#64748b", flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                                {h.message}
                              </span>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  </>
                )}
              </div>
            )}
          </div>
        );
      })}

      {/* Migrate modal */}
      {migrateModal && (
        <MigrateModal
          groupId={migrateModal.groupId}
          table={migrateModal.table}
          onClose={() => setMigrateModal(null)}
          onCreated={() => onMigrationCreated(migrateModal.groupId)}
        />
      )}

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

// ── Migrate modal ────────────────────────────────────────────────────────────

interface MigrateParams {
  migration_mode: "CDC" | "BULK_ONLY";
  migration_strategy: "STAGE" | "DIRECT";
  chunk_size: number;
  max_parallel_workers: number;
  baseline_parallel_degree: number;
  stage_tablespace: string;
  validate_hash_sample: boolean;
}

const MIGRATE_DEFAULTS: MigrateParams = {
  migration_mode: "CDC",
  migration_strategy: "STAGE",
  chunk_size: 1_000_000,
  max_parallel_workers: 1,
  baseline_parallel_degree: 4,
  stage_tablespace: "PAYSTAGE",
  validate_hash_sample: false,
};

function MigrateModal({ groupId, table, onClose, onCreated }: {
  groupId: string;
  table: GroupTable;
  onClose: () => void;
  onCreated: () => void;
}) {
  const [params, setParams] = useState<MigrateParams>(MIGRATE_DEFAULTS);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");

  const set = (up: Partial<MigrateParams>) => setParams(p => ({ ...p, ...up }));

  const handleSubmit = async () => {
    setSubmitting(true);
    setError("");
    try {
      const r = await fetch(`/api/connector-groups/${groupId}/create-migration`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ table_id: table.id, ...params }),
      });
      const d = await r.json();
      if (!r.ok) {
        setError(d.error || "Ошибка создания миграции");
        return;
      }
      onCreated();
    } catch (e) {
      setError(String(e));
    } finally {
      setSubmitting(false);
    }
  };

  const inp: React.CSSProperties = {
    background: "#1e293b", border: "1px solid #334155", borderRadius: 5,
    color: "#e2e8f0", padding: "6px 10px", fontSize: 12, width: "100%",
  };

  const lbl: React.CSSProperties = {
    fontSize: 11, color: "#64748b", fontWeight: 600, letterSpacing: 0.3,
    marginBottom: 3,
  };

  const hint: React.CSSProperties = { fontSize: 10, color: "#475569", marginTop: 2 };

  function ModeBtn({ value, label, activeColor }: {
    value: string; label: string; activeColor: string;
  }) {
    const field = value === "CDC" || value === "BULK_ONLY" ? "migration_mode" : "migration_strategy";
    const current = field === "migration_mode" ? params.migration_mode : params.migration_strategy;
    const active = current === value;
    return (
      <button
        onClick={() => set({ [field]: value } as Partial<MigrateParams>)}
        style={{
          flex: 1, padding: "7px 10px", borderRadius: 5, cursor: "pointer",
          border: `1px solid ${active ? activeColor : "#334155"}`,
          background: active ? activeColor + "20" : "#1e293b",
          color: active ? activeColor : "#64748b",
          fontWeight: 700, fontSize: 11,
        }}
      >{label}</button>
    );
  }

  return ReactDOM.createPortal(
    <div
      style={{
        position: "fixed", inset: 0, background: "rgba(0,0,0,.72)",
        display: "flex", alignItems: "center", justifyContent: "center",
        zIndex: 1000,
      }}
      onClick={e => { if (e.target === e.currentTarget) onClose(); }}
    >
      <div style={{
        background: "#0f172a", border: "1px solid #1e293b", borderRadius: 10,
        width: "100%", maxWidth: 480,
        display: "flex", flexDirection: "column",
        boxShadow: "0 24px 48px rgba(0,0,0,.55)",
      }}>
        {/* Header */}
        <div style={{
          padding: "12px 20px", borderBottom: "1px solid #1e293b",
          display: "flex", alignItems: "center", gap: 10,
        }}>
          <span style={{ fontSize: 14, fontWeight: 700, color: "#e2e8f0" }}>
            Создать миграцию
          </span>
          <span style={{ flex: 1 }} />
          <button onClick={onClose} style={{
            background: "none", border: "none", color: "#475569",
            cursor: "pointer", fontSize: 18, lineHeight: 1, padding: "0 2px",
          }}>{"\u2715"}</button>
        </div>

        {/* Body */}
        <div style={{ padding: 20, display: "flex", flexDirection: "column", gap: 14 }}>

          {/* Table info */}
          <div style={{
            background: "#1e293b", borderRadius: 6, padding: "10px 14px",
            display: "flex", flexDirection: "column", gap: 4,
          }}>
            <div style={{ fontSize: 12, color: "#94a3b8" }}>
              <span style={{ color: "#475569" }}>Source: </span>
              <strong style={{ color: "#e2e8f0", fontFamily: "monospace" }}>
                {table.source_schema}.{table.source_table}
              </strong>
            </div>
            <div style={{ fontSize: 12, color: "#94a3b8" }}>
              <span style={{ color: "#475569" }}>Target: </span>
              <strong style={{ color: "#e2e8f0", fontFamily: "monospace" }}>
                {table.target_schema}.{table.target_table}
              </strong>
            </div>
            <div style={{ fontSize: 10, color: "#475569", marginTop: 2 }}>
              Key: {table.effective_key_type}
            </div>
          </div>

          {/* Mode */}
          <div>
            <div style={lbl}>Режим</div>
            <div style={{ display: "flex", gap: 8 }}>
              <ModeBtn value="CDC" label="CDC (Debezium)" activeColor="#7c3aed" />
              <ModeBtn value="BULK_ONLY" label="Разовая переливка" activeColor="#059669" />
            </div>
            <div style={hint}>
              {params.migration_mode === "CDC"
                ? "Bulk-загрузка + CDC через Debezium до полного catchup"
                : "Однократная переливка без отслеживания изменений"}
            </div>
          </div>

          {/* Strategy */}
          <div>
            <div style={lbl}>Стратегия</div>
            <div style={{ display: "flex", gap: 8 }}>
              <ModeBtn value="STAGE" label="STAGE" activeColor="#3b82f6" />
              <ModeBtn value="DIRECT" label="DIRECT" activeColor="#059669" />
            </div>
            <div style={hint}>
              {params.migration_strategy === "STAGE"
                ? "Через промежуточную stage-таблицу с валидацией"
                : "Прямая загрузка в целевую таблицу"}
            </div>
          </div>

          {/* Stage tablespace */}
          {params.migration_strategy === "STAGE" && (
            <div>
              <div style={lbl}>Stage tablespace</div>
              <input style={inp} value={params.stage_tablespace}
                placeholder="например MIGRATION_DATA"
                onChange={e => set({ stage_tablespace: e.target.value })} />
              <div style={hint}>Если пусто — default tablespace схемы</div>
            </div>
          )}

          {/* Numeric params */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 10 }}>
            <div>
              <div style={lbl}>Chunk size</div>
              <input style={inp} type="number" value={params.chunk_size} min={1}
                onChange={e => set({ chunk_size: parseInt(e.target.value) || 0 })} />
              <div style={hint}>Строк на чанк</div>
            </div>
            <div>
              <div style={lbl}>Воркеры (bulk)</div>
              <input style={inp} type="number" value={params.max_parallel_workers} min={1}
                onChange={e => set({ max_parallel_workers: Math.max(1, parseInt(e.target.value) || 1) })} />
            </div>
            <div>
              <div style={lbl}>Воркеры (baseline)</div>
              <input style={inp} type="number" value={params.baseline_parallel_degree} min={1}
                onChange={e => set({ baseline_parallel_degree: Math.max(1, parseInt(e.target.value) || 4) })} />
            </div>
          </div>

          {/* Hash validation */}
          <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
            <input type="checkbox"
              checked={params.validate_hash_sample}
              onChange={e => set({ validate_hash_sample: e.target.checked })} />
            <span style={{ fontSize: 12, color: "#94a3b8" }}>
              Hash/sample валидация stage
            </span>
          </label>

          {/* Error */}
          {error && (
            <div style={{
              background: "#450a0a", border: "1px solid #7f1d1d", borderRadius: 6,
              padding: "8px 12px", fontSize: 12, color: "#fca5a5",
            }}>
              {error}
            </div>
          )}
        </div>

        {/* Footer */}
        <div style={{
          padding: "12px 20px", borderTop: "1px solid #1e293b",
          display: "flex", justifyContent: "flex-end", gap: 8,
        }}>
          <button onClick={onClose} style={{
            background: "none", border: "1px solid #334155", borderRadius: 6,
            color: "#64748b", padding: "6px 16px", fontSize: 12, cursor: "pointer",
          }}>
            Отмена
          </button>
          <button onClick={handleSubmit} disabled={submitting} style={{
            background: submitting ? "#1e3a5f" : "#16a34a",
            border: "none", borderRadius: 6,
            color: submitting ? "#64748b" : "#fff",
            padding: "6px 18px", fontSize: 12, fontWeight: 700,
            cursor: submitting ? "not-allowed" : "pointer",
          }}>
            {submitting ? "Создание..." : "Создать и запустить"}
          </button>
        </div>
      </div>
    </div>,
    document.body,
  );
}

const _ACTIVE_PHASES = new Set([
  "NEW", "PREPARING", "SCN_FIXED", "CONNECTOR_STARTING", "CDC_BUFFERING",
  "CHUNKING", "BULK_LOADING", "BULK_LOADED",
  "STAGE_VALIDATING", "STAGE_VALIDATED",
  "BASELINE_PUBLISHING", "BASELINE_LOADING", "BASELINE_PUBLISHED",
  "STAGE_DROPPING", "INDEXES_ENABLING",
  "CDC_APPLY_STARTING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP", "STEADY_STATE",
  "STRUCTURE_READY", "TARGET_CLEARING",
]);
const _DELETABLE_PHASES = new Set(["DRAFT", "CANCELLING", "CANCELLED", "FAILED"]);

function MigrationRow({ m, onChanged }: { m: MigrationSummary; onChanged: () => void }) {
  const [busy, setBusy] = useState(false);
  const pc = phaseColor(m.phase);
  const canStop = _ACTIVE_PHASES.has(m.phase);
  const canDelete = _DELETABLE_PHASES.has(m.phase);

  async function doStop() {
    if (!confirm(`Остановить миграцию ${m.source_schema}.${m.source_table}?`)) return;
    setBusy(true);
    try {
      await fetch(`/api/migrations/${m.migration_id}/action`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "cancel" }),
      });
      onChanged();
    } finally { setBusy(false); }
  }

  async function doDelete() {
    if (!confirm(`Удалить миграцию ${m.source_schema}.${m.source_table}? Это действие необратимо.`)) return;
    setBusy(true);
    try {
      await fetch(`/api/migrations/${m.migration_id}`, { method: "DELETE" });
      onChanged();
    } finally { setBusy(false); }
  }

  const smallBtn = (bg: string, border: string, color: string): React.CSSProperties => ({
    background: busy ? "#1e293b" : bg,
    color: busy ? "#475569" : color,
    border: `1px solid ${border}`,
    borderRadius: 4, padding: "1px 8px",
    fontSize: 10, fontWeight: 600,
    cursor: busy ? "not-allowed" : "pointer",
  });

  return (
    <tr style={{ borderTop: "1px solid #1e293b" }}>
      <td style={{ padding: "4px 8px", color: "#e2e8f0" }}>
        {m.source_schema}.{m.source_table}
      </td>
      <td style={{ padding: "4px 8px" }}>
        <span style={{
          fontSize: 9, padding: "1px 6px", borderRadius: 3,
          background: pc.bg, color: pc.text,
          border: `1px solid ${pc.border}`, fontWeight: 600,
        }}>
          {m.phase}
        </span>
      </td>
      <td style={{ padding: "4px 8px", color: "#64748b" }}>{m.migration_mode}</td>
      <td style={{ padding: "4px 8px", textAlign: "right" }}>
        <div style={{ display: "flex", gap: 4, justifyContent: "flex-end" }}>
          {canStop && (
            <button onClick={doStop} disabled={busy}
              style={smallBtn("#450a0a", "#7f1d1d", "#fca5a5")}>
              Stop
            </button>
          )}
          {canDelete && (
            <button onClick={doDelete} disabled={busy}
              style={smallBtn("#450a0a", "#7f1d1d", "#fca5a5")}>
              Del
            </button>
          )}
        </div>
      </td>
    </tr>
  );
}

function actionBtn(bg: string, border: string): React.CSSProperties {
  return {
    background: bg, border: `1px solid ${border}`, borderRadius: 4,
    color: border, padding: "2px 10px", fontSize: 11, cursor: "pointer",
  };
}
