import React from "react";
import { S } from "../../styles";
import type { Batch, FKDep } from "../types";
import { t } from "../../../../theme";

interface Props {
  batches:     Batch[];
  onBatches:   (b: Batch[]) => void;
  deps:        FKDep[];
  depsLoading: boolean;
  planMode:    "historical" | "cdc";
}

export function OrderingStep({ batches, onBatches, deps, depsLoading, planMode }: Props) {
  const moveItem = (fromBatch: number, table: string, toBatch: number) => {
    const next = batches.map(b => ({
      ...b,
      items: b.id === fromBatch
        ? b.items.filter(it => it.table !== table)
        : b.id === toBatch
          ? [...b.items, batches.find(bb => bb.id === fromBatch)!.items.find(it => it.table === table)!]
          : b.items,
    }));
    onBatches(next.filter(b => b.items.length > 0 || b.id === 1));
  };

  const addBatch = () => {
    const maxId = Math.max(...batches.map(b => b.id), 0);
    onBatches([...batches, { id: maxId + 1, items: [] }]);
  };

  const splitSequential = () => {
    const items = batches.flatMap(b => b.items);
    onBatches(items.map((item, idx) => ({ id: idx + 1, items: [item] })));
  };

  const moveUp = (batchId: number, table: string) => {
    const batch = batches.find(b => b.id === batchId);
    if (!batch) return;
    const idx = batch.items.findIndex(it => it.table === table);
    if (idx <= 0) return;
    const newItems = [...batch.items];
    [newItems[idx - 1], newItems[idx]] = [newItems[idx], newItems[idx - 1]];
    onBatches(batches.map(b => b.id === batchId ? { ...b, items: newItems } : b));
  };

  const moveDown = (batchId: number, table: string) => {
    const batch = batches.find(b => b.id === batchId);
    if (!batch) return;
    const idx = batch.items.findIndex(it => it.table === table);
    if (idx < 0 || idx >= batch.items.length - 1) return;
    const newItems = [...batch.items];
    [newItems[idx], newItems[idx + 1]] = [newItems[idx + 1], newItems[idx]];
    onBatches(batches.map(b => b.id === batchId ? { ...b, items: newItems } : b));
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* FK dependencies info */}
      <div style={S.card}>
        <div style={S.cardHeader}>
          <span style={{ fontSize: 13, fontWeight: 600, color: t.text.primary }}>FK зависимости</span>
          {depsLoading && <span style={{ fontSize: 11, color: t.text.muted }}>Загрузка...</span>}
        </div>
        <div style={S.cardBody}>
          {deps.length === 0 && !depsLoading && (
            <span style={{ fontSize: 12, color: t.text.disabled }}>Нет FK зависимостей между выбранными таблицами</span>
          )}
          {deps.length > 0 && (
            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              {deps.map(d => (
                <div key={d.table} style={{ fontSize: 12, color: t.text.secondary }}>
                  <code style={{ color: t.text.primary }}>{d.table}</code>
                  <span style={{ color: t.text.disabled, margin: "0 6px" }}>→</span>
                  {d.depends_on.map((dep, i) => (
                    <React.Fragment key={dep}>
                      {i > 0 && <span style={{ color: t.text.disabled }}>, </span>}
                      <code style={{ color: t.blue.fg }}>{dep}</code>
                    </React.Fragment>
                  ))}
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Batches */}
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
        <span style={{ fontSize: 13, fontWeight: 600, color: t.text.primary }}>Батчи ({batches.length})</span>
        {planMode === "historical" && (
          <button onClick={splitSequential} style={{ ...S.btnSuccess, fontSize: 11, padding: "3px 10px" }}>
            По одной таблице
          </button>
        )}
        <button onClick={addBatch} style={{ ...S.btnSecondary, fontSize: 11, padding: "3px 10px" }}>
          + Добавить батч
        </button>
      </div>

      {batches.map(batch => (
        <div key={batch.id} style={S.card}>
          <div style={{ ...S.cardHeader, justifyContent: "space-between" }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <span style={{ fontSize: 13, fontWeight: 600, color: t.text.primary }}>
                Батч #{batch.id}
              </span>
              <span style={S.badge(t.bg.s3, t.blue.fg)}>
                {batch.items.length} таблиц
              </span>
            </div>
          </div>
          {batch.items.length === 0 ? (
            <div style={{ padding: "16px", textAlign: "center", color: t.text.disabled, fontSize: 12 }}>
              Пустой батч — перетащите сюда таблицы
            </div>
          ) : (
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ borderBottom: `1px solid ${t.border.subtle}` }}>
                    {["#", "Таблица", "Стратегия", "Порядок", "Переместить"].map(h => (
                      <th key={h} style={S.th}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {batch.items.map((item, idx) => {
                    const hasDep = deps.some(d => d.table === item.table);
                    return (
                      <tr key={item.table} style={S.trBorder}>
                        <td style={{ ...S.td, color: t.text.disabled }}>{idx + 1}</td>
                        <td style={S.td}>
                          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                            <code style={{ color: t.text.primary, fontSize: 12 }}>{item.table}</code>
                            {hasDep && <span style={S.badge(`${t.amber.base}22`, t.amber.base)}>FK</span>}
                          </div>
                        </td>
                        <td style={S.td}>
                          <span style={{ fontSize: 11, color: t.text.secondary }}>{item.strategy}</span>
                        </td>
                        <td style={S.td}>
                          <div style={{ display: "flex", gap: 4 }}>
                            <button
                              onClick={() => moveUp(batch.id, item.table)}
                              disabled={idx === 0}
                              style={{
                                ...S.btnSecondary, fontSize: 10, padding: "2px 6px",
                                opacity: idx === 0 ? 0.3 : 1,
                              }}
                            >▲</button>
                            <button
                              onClick={() => moveDown(batch.id, item.table)}
                              disabled={idx === batch.items.length - 1}
                              style={{
                                ...S.btnSecondary, fontSize: 10, padding: "2px 6px",
                                opacity: idx === batch.items.length - 1 ? 0.3 : 1,
                              }}
                            >▼</button>
                          </div>
                        </td>
                        <td style={S.td}>
                          <div style={{ display: "flex", gap: 4 }}>
                            {batches.filter(b => b.id !== batch.id).map(b => (
                              <button
                                key={b.id}
                                onClick={() => moveItem(batch.id, item.table, b.id)}
                                style={{ ...S.btnSecondary, fontSize: 10, padding: "2px 8px" }}
                              >
                                → #{b.id}
                              </button>
                            ))}
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      ))}
    </div>
  );
}
