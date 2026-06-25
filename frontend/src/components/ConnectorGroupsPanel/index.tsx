import { useCallback, useEffect, useState } from "react";
import type { ConnectorGroup, MigrationSummary } from "../../types/migration";
import type { SSEEvent } from "../../hooks/useSSE";
import { t } from "../../theme";
import type { GroupTable, TopicCount, GroupHistoryEntry } from "./types";
import { STATUS_COLORS, actionBtn } from "./helpers";
import { GroupTablesTable } from "./GroupTablesTable";
import { GroupHistory } from "./GroupHistory";
import { DebeziumConfigModal } from "./DebeziumConfigModal";

interface CdcContinuationResp {
  status?: string;
  error?: string;
  plan_starts?: Array<{ started?: unknown[] }>;
  plan_start_error?: string | null;
  cdc_queue_kicked?: boolean;
}

export function ConnectorGroupsPanel({ sseEvents = [] }: { sseEvents?: SSEEvent[] }) {
  const [groups,        setGroups]        = useState<ConnectorGroup[]>([]);
  const [loading,       setLoading]       = useState(true);
  const [expanded,      setExpanded]      = useState<string | null>(null);
  const [detail,        setDetail]        = useState<(ConnectorGroup & { tables?: GroupTable[] }) | null>(null);
  const [configModal,   setConfigModal]   = useState<{ json: string; name: string } | null>(null);
  const [configLoading, setConfigLoading] = useState(false);
  const [topicCounts,   setTopicCounts]   = useState<Map<string, TopicCount>>(new Map());
  const [topicLoading,  setTopicLoading]  = useState(false);
  const [history,       setHistory]       = useState<GroupHistoryEntry[]>([]);
  const [actionMsg,     setActionMsg]     = useState<{ tone: "ok" | "bad"; text: string } | null>(null);

  const load = useCallback(() => {
    fetch("/api/connector-groups")
      .then(r => r.ok ? r.json() : Promise.reject())
      .then(setGroups)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    load();
    const id = setInterval(load, 10_000);
    return () => clearInterval(id);
  }, [load]);

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

  const loadHistory = useCallback((gid: string) => {
    fetch(`/api/connector-groups/${gid}/history`)
      .then(r => r.ok ? r.json() : [])
      .then(setHistory)
      .catch(() => setHistory([]));
  }, []);

  const loadTopicCounts = useCallback((gid: string) => {
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
  }, []);

  useEffect(() => {
    const event = sseEvents[0];
    if (!event) return;
    if (
      event.type !== "connector_group_status"
      && event.type !== "schema_migration.plan_items_added"
    ) return;

    load();
    if (expanded) {
      fetch(`/api/connector-groups/${expanded}`)
        .then(r => r.ok ? r.json() : Promise.reject())
        .then(setDetail)
        .catch(() => setDetail(null));
      loadTopicCounts(expanded);
      loadHistory(expanded);
    }
  }, [sseEvents, expanded, load, loadHistory, loadTopicCounts]);

  const reloadGroupDetail = (gid: string) => {
    load();
    if (expanded !== gid) return;
    fetch(`/api/connector-groups/${gid}`)
      .then(r => r.ok ? r.json() : Promise.reject())
      .then(setDetail)
      .catch(() => setDetail(null));
    loadTopicCounts(gid);
    loadHistory(gid);
  };

  const continuationMessage = (body: CdcContinuationResp, prefix: string, runningVerb: string, waitingVerb: string) => {
    const startedCount = (body.plan_starts || []).reduce(
      (sum: number, item: { started?: unknown[] }) => sum + (item.started?.length || 0),
      0,
    );
    const status = String(body.status || "").toUpperCase();
    const rowText = status === "RUNNING" ? runningVerb : waitingVerb;
    if (body.plan_start_error) {
      return { tone: "bad" as const, text: `${prefix}, но очередь не продолжена: ${body.plan_start_error}` };
    }
    return {
      tone: "ok" as const,
      text: startedCount
        ? `${prefix}, ${rowText}: ${startedCount}`
        : body.cdc_queue_kicked
          ? `${prefix}, очередь CDC продолжена`
          : prefix,
    };
  };

  const startGroup = async (gid: string) => {
    setActionMsg(null);
    try {
      const res = await fetch(`/api/connector-groups/${gid}/start`, { method: "POST" });
      const body = await res.json().catch(() => ({})) as CdcContinuationResp;
      if (!res.ok) {
        setActionMsg({ tone: "bad", text: body?.error || `HTTP ${res.status}` });
        return;
      }
      const status = String(body.status || "").toUpperCase();
      const prefix = status === "RUNNING"
        ? "CDC-коннектор RUNNING"
        : status
          ? `Запуск CDC-коннектора: ${status}`
          : "Запуск CDC-коннектора запрошен";
      setActionMsg(continuationMessage(
        body,
        prefix,
        "запущено CDC строк",
        "CDC строк переведено в ожидание коннектора",
      ));
      reloadGroupDetail(gid);
    } catch (e) {
      setActionMsg({ tone: "bad", text: `Сеть: ${String(e)}` });
    }
  };
  const syncGroup = async (gid: string) => {
    setActionMsg(null);
    try {
      const res = await fetch(`/api/connector-groups/${gid}/refresh-tables`, { method: "POST" });
      const body = await res.json().catch(() => ({})) as CdcContinuationResp;
      if (!res.ok) {
        setActionMsg({ tone: "bad", text: body?.error || `HTTP ${res.status}` });
        return;
      }
      setActionMsg(continuationMessage(
        body,
        "Debezium синхронизирован",
        "запущено CDC строк",
        "CDC строк переведено в ожидание коннектора",
      ));
      reloadGroupDetail(gid);
    } catch (e) {
      setActionMsg({ tone: "bad", text: `Сеть: ${String(e)}` });
    }
  };
  const stopGroup   = (gid: string) => { setActionMsg(null); fetch(`/api/connector-groups/${gid}/stop`,   { method: "POST" }).then(load).catch(() => {}); };
  const createTopics = (gid: string) => fetch(`/api/connector-groups/${gid}/create-topics`, { method: "POST" }).then(() => loadTopicCounts(gid)).catch(() => {});

  const removeTable = async (gid: string, table: GroupTable) => {
    const label = `${table.source_schema}.${table.source_table}`;
    if (!confirm(`Удалить ${label} из CDC-коннектора? Debezium table.include.list будет обновлён.`)) return;
    try {
      const r = await fetch(
        `/api/connector-groups/${gid}/tables/${encodeURIComponent(table.source_schema)}/${encodeURIComponent(table.source_table)}`,
        { method: "DELETE" },
      );
      if (!r.ok) {
        const body = await r.json().catch(() => ({}));
        alert(body?.error || `HTTP ${r.status}`);
        return;
      }
      const body = await r.json().catch(() => ({}));
      const d = await fetch(`/api/connector-groups/${gid}`)
        .then(resp => resp.ok ? resp.json() : Promise.reject());
      setDetail(d);
      loadTopicCounts(gid);
      load();
      if (body?.sync_error) alert(`Таблица удалена из пачки, но Debezium не синхронизирован: ${body.sync_error}`);
    } catch (e) {
      alert(`Сеть: ${String(e)}`);
    }
  };

  const deleteGroup = async (gid: string) => {
    if (!confirm("Удалить CDC-коннектор?")) return;
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
        if (!confirm(`${msg}.\n\nПеревести их в CANCELLED и удалить CDC-коннектор?`)) return;
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
          CDC-коннекторы
        </h2>
      </div>
      {actionMsg && (
        <div style={{
          marginBottom: 10,
          padding: "7px 10px",
          borderRadius: t.radius.sm,
          background: actionMsg.tone === "ok" ? t.green.bg : `${t.red.border}22`,
          border: `1px solid ${actionMsg.tone === "ok" ? t.green.dim : t.red.border}`,
          color: actionMsg.tone === "ok" ? t.green.fg : t.red.fg,
          fontSize: 12,
        }}>
          {actionMsg.text}
        </div>
      )}

      {groups.length === 0 && (
        <div style={{ color: t.text.disabled, padding: 24, textAlign: "center" }}>
          Нет CDC-коннектора. Добавьте таблицы в CDC-коннектор на экране "Эта миграция".
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
                    onClick={() => syncGroup(g.group_id)}
                    style={{
                      background: t.bg.s2, border: `1px solid ${t.border.base}`,
                      borderRadius: t.radius.sm, color: t.text.secondary,
                      padding: "2px 10px", fontSize: t.size.xs, cursor: "pointer", fontWeight: 600,
                    }}
                  >Sync Debezium</button>
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
                  onRemove={(table) => removeTable(g.group_id, table)}
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
