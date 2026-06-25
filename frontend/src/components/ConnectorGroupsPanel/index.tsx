import { useEffect, useState } from "react";
import type { ConnectorGroup, MigrationSummary } from "../../types/migration";
import { t } from "../../theme";
import type { GroupTable, TopicCount, GroupHistoryEntry } from "./types";
import { STATUS_COLORS, actionBtn } from "./helpers";
import { GroupTablesTable } from "./GroupTablesTable";
import { GroupHistory } from "./GroupHistory";
import { DebeziumConfigModal } from "./DebeziumConfigModal";
import { MigrateModal } from "./MigrateModal";

export function ConnectorGroupsPanel() {
  const [groups,        setGroups]        = useState<ConnectorGroup[]>([]);
  const [loading,       setLoading]       = useState(true);
  const [expanded,      setExpanded]      = useState<string | null>(null);
  const [detail,        setDetail]        = useState<(ConnectorGroup & { tables?: GroupTable[] }) | null>(null);
  const [configModal,   setConfigModal]   = useState<{ json: string; name: string } | null>(null);
  const [configLoading, setConfigLoading] = useState(false);
  const [topicCounts,   setTopicCounts]   = useState<Map<string, TopicCount>>(new Map());
  const [topicLoading,  setTopicLoading]  = useState(false);
  const [history,       setHistory]       = useState<GroupHistoryEntry[]>([]);
  const [migrateModal,  setMigrateModal]  = useState<{ groupId: string; table: GroupTable } | null>(null);

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
      setExpanded(null); setDetail(null); setTopicCounts(new Map()); setHistory([]);
    } else {
      setExpanded(gid);
      fetch(`/api/connector-groups/${gid}`)
        .then(r => r.ok ? r.json() : Promise.reject())
        .then(d => { setDetail(d); loadTopicCounts(gid); loadHistory(gid); })
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

  const startGroup  = (gid: string) => fetch(`/api/connector-groups/${gid}/start`,  { method: "POST" }).then(load).catch(() => {});
  const stopGroup   = (gid: string) => fetch(`/api/connector-groups/${gid}/stop`,   { method: "POST" }).then(load).catch(() => {});
  const createTopics = (gid: string) => fetch(`/api/connector-groups/${gid}/create-topics`, { method: "POST" }).then(() => loadTopicCounts(gid)).catch(() => {});

  const deleteGroup = async (gid: string) => {
    if (!confirm("Удалить CDC-пачку?")) return;
    try {
      const r = await fetch(`/api/connector-groups/${gid}`, { method: "DELETE" });
      if (r.ok) {
        load();
        if (expanded === gid) { setExpanded(null); setDetail(null); }
        return;
      }
      const body = await r.json().catch(() => ({}));
      const msg  = body?.error || `HTTP ${r.status}`;
      // Backend отказался — обычно из-за активных миграций. Предлагаем force.
      if (r.status === 400 && /активных миграций/i.test(msg)) {
        if (!confirm(`${msg}.\n\nПеревести их в CANCELLED и удалить CDC-пачку?`)) return;
        const r2 = await fetch(`/api/connector-groups/${gid}?force=true`, { method: "DELETE" });
        if (r2.ok) {
          load();
          if (expanded === gid) { setExpanded(null); setDetail(null); }
          return;
        }
        const body2 = await r2.json().catch(() => ({}));
        alert(body2?.error || `Не удалось удалить (HTTP ${r2.status})`);
        return;
      }
      alert(msg);
    } catch (e) {
      alert(`Сеть: ${String(e)}`);
    }
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

  // "SCHEMA.TABLE" → most-recent-active migration in this group
  const tableMigrationMap = new Map<string, MigrationSummary>();
  if (detail?.migrations) {
    for (const m of detail.migrations) {
      const key = `${m.source_schema}.${m.source_table}`;
      const existing = tableMigrationMap.get(key);
      if (!existing || (existing.phase === "CANCELLED" || existing.phase === "FAILED" || existing.phase === "COMPLETED")) {
        tableMigrationMap.set(key, m);
      }
    }
  }

  if (loading) return <div style={{ color: t.text.muted, padding: 16 }}>Загрузка...</div>;

  return (
    <div>
      <div style={{ marginBottom: 12 }}>
        <h2 style={{ margin: 0, fontSize: 15, fontWeight: 600, color: t.text.secondary }}>
          CDC-пачки
        </h2>
      </div>

      {groups.length === 0 && (
        <div style={{ color: t.text.disabled, padding: 24, textAlign: "center" }}>
          Нет CDC-пачек. Добавьте таблицы в CDC-пачку на экране "Эта миграция".
        </div>
      )}

      {groups.map(g => {
        const sc         = STATUS_COLORS[g.status] || STATUS_COLORS.PENDING;
        const isExpanded = expanded === g.group_id;
        return (
          <div key={g.group_id} style={{
            background: t.bg.app, border: `1px solid ${t.border.subtle}`,
            borderRadius: t.radius.lg, marginBottom: 8, overflow: "hidden",
          }}>
            <div
              onClick={() => toggleExpand(g.group_id)}
              style={{
                display: "flex", alignItems: "center", gap: 12,
                padding: "10px 14px", cursor: "pointer",
              }}
            >
              <span style={{
                background: sc.bg, color: sc.text,
                padding: "2px 8px", borderRadius: t.radius.sm,
                fontSize: t.size.sm, fontWeight: 600,
              }}>
                {g.status}
              </span>
              <strong style={{ fontSize: t.size.md, color: t.text.primary }}>{g.group_name}</strong>
              <span style={{ fontSize: t.size.sm, color: t.text.disabled }}>
                {g.connector_name} | {g.topic_prefix}
              </span>
              <div style={{ marginLeft: "auto", display: "flex", gap: 6 }}>
                {(g.status === "PENDING" || g.status === "STOPPED" || g.status === "FAILED") && (
                  <button onClick={e => { e.stopPropagation(); startGroup(g.group_id); }}
                    style={actionBtn(t.green.bg, t.green.dim)}>
                    Start
                  </button>
                )}
                {g.status === "RUNNING" && (
                  <button onClick={e => { e.stopPropagation(); stopGroup(g.group_id); }}
                    style={actionBtn(t.red.bg, t.amber.dim)}>
                    Stop
                  </button>
                )}
                {(g.status === "PENDING" || g.status === "STOPPED") && (
                  <button onClick={e => { e.stopPropagation(); deleteGroup(g.group_id); }}
                    style={actionBtn(t.red.bg, t.red.dim)}>
                    Del
                  </button>
                )}
              </div>
            </div>

            {isExpanded && detail && detail.group_id === g.group_id && (
              <div style={{ padding: "0 14px 12px", borderTop: `1px solid ${t.border.subtle}` }}>
                {g.error_text && (
                  <div style={{ color: t.red.fg, fontSize: t.size.sm, marginTop: 8 }}>{g.error_text}</div>
                )}
                <div style={{
                  fontSize: t.size.sm, color: t.text.disabled, marginTop: 8,
                  display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap",
                }}>
                  <span>Source: {detail.source_connection_id} | Prefix: {detail.consumer_group_prefix || detail.topic_prefix}</span>
                  <span style={{ flex: 1 }} />
                  <button
                    onClick={() => createTopics(g.group_id)}
                    style={{
                      background: t.bg.s2, border: `1px solid ${t.border.base}`,
                      borderRadius: t.radius.sm, color: t.text.secondary,
                      padding: "2px 10px", fontSize: t.size.xs, cursor: "pointer", fontWeight: 600,
                    }}
                  >Create Topics</button>
                  <button
                    onClick={() => loadTopicCounts(g.group_id)}
                    disabled={topicLoading}
                    style={{
                      background: t.bg.s2, border: `1px solid ${t.border.base}`,
                      borderRadius: t.radius.sm, color: t.text.secondary,
                      padding: "2px 10px", fontSize: t.size.xs, cursor: "pointer", fontWeight: 600,
                    }}
                  >{topicLoading ? "..." : "Refresh Counts"}</button>
                  <button
                    onClick={() => showConfig(g.group_id, g.group_name)}
                    disabled={configLoading}
                    style={{
                      background: t.bg.s2, border: `1px solid ${t.border.base}`,
                      borderRadius: t.radius.sm, color: t.text.secondary,
                      padding: "2px 10px", fontSize: t.size.xs, cursor: "pointer", fontWeight: 600,
                    }}
                  >{configLoading ? "..." : "Debezium Config"}</button>
                </div>

                {/* Pack tables */}
                <h4 style={{ color: t.text.muted, fontSize: t.size.base, margin: "12px 0 6px" }}>
                  Таблицы ({detail.tables?.length || 0})
                </h4>
                <GroupTablesTable
                  tables={detail.tables ?? []}
                  topicCounts={topicCounts}
                  tableMigrationMap={tableMigrationMap}
                  onMigrate={(table) => setMigrateModal({ groupId: g.group_id, table })}
                />

                {/* Migrations linked to this group */}
                {detail.migrations && detail.migrations.length > 0 && (
                  <>
                    <h4 style={{ color: t.text.muted, fontSize: t.size.base, margin: "12px 0 6px" }}>
                      Миграции ({detail.migrations.length})
                    </h4>
                    <table style={{ width: "100%", fontSize: t.size.sm, borderCollapse: "collapse" }}>
                      <thead>
                        <tr style={{ color: t.text.disabled, textAlign: "left" }}>
                          <th style={{ padding: "4px 8px" }}>Таблица</th>
                          <th style={{ padding: "4px 8px" }}>Фаза</th>
                          <th style={{ padding: "4px 8px" }}>Режим</th>
                        </tr>
                      </thead>
                      <tbody>
                        {detail.migrations.map(m => (
                          <tr key={m.migration_id} style={{ borderTop: `1px solid ${t.border.subtle}` }}>
                            <td style={{ padding: "4px 8px", color: t.text.primary }}>
                              {m.source_schema}.{m.source_table}
                            </td>
                            <td style={{ padding: "4px 8px", color: t.text.secondary }}>{m.phase}</td>
                            <td style={{ padding: "4px 8px", color: t.text.muted }}>{m.strategy ?? "—"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </>
                )}

                {/* State history */}
                <GroupHistory history={history} />
              </div>
            )}
          </div>
        );
      })}

      {migrateModal && (
        <MigrateModal
          groupId={migrateModal.groupId}
          table={migrateModal.table}
          onClose={() => setMigrateModal(null)}
          onCreated={() => onMigrationCreated(migrateModal.groupId)}
        />
      )}

      {configModal && (
        <DebeziumConfigModal
          json={configModal.json}
          name={configModal.name}
          onClose={() => setConfigModal(null)}
        />
      )}
    </div>
  );
}
