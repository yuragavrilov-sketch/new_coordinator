import React, { useState, useEffect, useCallback } from "react";

// ── Types ────────────────────────────────────────────────────────────────────

interface CheckItem {
  id: string;
  label: string;
  hint?: string;
}

interface CheckSection {
  title: string;
  color: string;
  items: CheckItem[];
}

// ── Checklist data ───────────────────────────────────────────────────────────

const SECTIONS: CheckSection[] = [
  {
    title: "1. Подготовка источника (Source)",
    color: "#3b82f6",
    items: [
      { id: "src_access",        label: "Доступ к source-схеме проверен",             hint: "SELECT, FLASHBACK на таблицу" },
      { id: "src_supplemental",  label: "Supplemental logging включён",               hint: "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS" },
      { id: "src_logminer",      label: "LogMiner-привилегии выданы",                 hint: "EXECUTE_CATALOG_ROLE или LOGMINING + SELECT ANY TRANSACTION" },
      { id: "src_archive",       label: "Archive log retention достаточен",            hint: "≥ 24 ч, проверить db_recovery_file_dest_size" },
      { id: "src_pk_uk",         label: "PK / UK ключ определён на таблице",          hint: "Или указан effective_key вручную" },
      { id: "src_row_count",     label: "Оценка количества строк получена",           hint: "SELECT COUNT(*) или DBA_TAB_STATISTICS" },
      { id: "src_lob",           label: "LOB / LONG / XMLType колонки учтены",        hint: "Проверить ограничения Debezium для LOB" },
      { id: "src_partitions",    label: "Partitioning учтён",                         hint: "Chunking может использовать partition pruning" },
    ],
  },
  {
    title: "2. Подготовка таргета (Target)",
    color: "#8b5cf6",
    items: [
      { id: "tgt_schema",       label: "Target-схема создана",                        hint: "CREATE USER ... IDENTIFIED BY ..." },
      { id: "tgt_table",        label: "Target-таблица создана (DDL совпадает)",      hint: "Используй вкладку «Подготовка таргета» для сравнения" },
      { id: "tgt_stage_tbs",    label: "Tablespace для stage-таблиц готов",           hint: "Отдельный tablespace: изоляция I/O, проще очистка" },
      { id: "tgt_indexes_off",  label: "Non-unique индексы будут отключены при bulk",  hint: "Система отключает автоматически, но проверь UNUSABLE" },
      { id: "tgt_triggers_off", label: "Триггеры на target отключены / учтены",       hint: "INSERT/UPDATE триггеры могут дублировать данные" },
      { id: "tgt_fk",           label: "Foreign key'и на target — disabled / deferrable", hint: "FK могут блокировать bulk INSERT" },
      { id: "tgt_grants",       label: "Привилегии на target-схему выданы",           hint: "INSERT, UPDATE, DELETE, ALTER, DROP для stage" },
      { id: "tgt_space",        label: "Достаточно места на target (datafiles)",      hint: "stage + target ≈ 2× объёма данных на время миграции" },
    ],
  },
  {
    title: "3. Инфраструктура (Kafka / Connect)",
    color: "#06b6d4",
    items: [
      { id: "inf_kafka",        label: "Kafka-кластер доступен",                     hint: "Broker bootstrap.servers проверены" },
      { id: "inf_connect",      label: "Kafka Connect запущен",                       hint: "REST API /connectors отвечает 200" },
      { id: "inf_debezium",     label: "Debezium Oracle connector plugin установлен", hint: "Плагин в plugin.path Connect-а" },
      { id: "inf_topic_config", label: "Topic retention / partitions настроены",      hint: "retention.ms, cleanup.policy=delete, partitions=1 (для ordering)" },
      { id: "inf_connect_heap", label: "JVM heap Kafka Connect достаточен",           hint: "≥ 2 GB для крупных таблиц" },
      { id: "inf_network",      label: "Сетевые правила: source → Kafka → target",   hint: "Порты: 1521, 9092, 8083" },
    ],
  },
  {
    title: "4. Конфигурация миграции",
    color: "#f59e0b",
    items: [
      { id: "cfg_name",         label: "Имя миграции задано (осмысленное)",           hint: "SCHEMA.TABLE_YYYYMMDD" },
      { id: "cfg_connections",   label: "Source / target connection_id проверены",     hint: "Тестовое подключение через Настройки" },
      { id: "cfg_chunk",        label: "Chunk strategy и chunk_size подобраны",        hint: "ROWID по умолчанию, PK range для числовых PK" },
      { id: "cfg_workers",      label: "max_parallel_workers подобрано",               hint: "Учитывай нагрузку на source и target" },
      { id: "cfg_mode",         label: "migration_mode выбран (FULL / BULK_ONLY)",     hint: "BULK_ONLY — без Debezium/CDC" },
      { id: "cfg_validate",     label: "Валидация включена (validate_hash_sample)",    hint: "Проверит хэши после bulk" },
      { id: "cfg_baseline",     label: "baseline_parallel_degree / batch_size заданы", hint: "Параллелизм INSERT /*+ APPEND */ в финальную таблицу" },
    ],
  },
  {
    title: "5. Запуск и мониторинг",
    color: "#22c55e",
    items: [
      { id: "run_created",      label: "Миграция создана (DRAFT → NEW)",              hint: "Кнопка «Создать миграцию»" },
      { id: "run_scn",          label: "SCN зафиксирован (SCN_FIXED)",                hint: "Система сама фиксирует start_scn" },
      { id: "run_connector",    label: "Debezium connector запущен",                   hint: "Статус RUNNING в панели connector" },
      { id: "run_bulk",         label: "Bulk loading завершён без ошибок",             hint: "chunks_failed = 0" },
      { id: "run_stage_valid",  label: "Stage validation пройдена",                    hint: "Хэш-сэмпл совпал" },
      { id: "run_baseline",     label: "Baseline publishing завершён",                 hint: "INSERT /*+ APPEND */ из stage в target" },
      { id: "run_cdc",          label: "CDC apply запущен, lag уменьшается",           hint: "kafka_lag → 0" },
      { id: "run_steady",       label: "STEADY_STATE достигнут",                       hint: "lag стабильно ≈ 0" },
    ],
  },
  {
    title: "6. Валидация после миграции",
    color: "#ec4899",
    items: [
      { id: "val_row_count",    label: "Количество строк source = target",            hint: "SELECT COUNT(*) на обоих" },
      { id: "val_hash",         label: "Хэш-сэмпл совпадает",                        hint: "Результат в панели Validation" },
      { id: "val_constraints",  label: "Все constraint'ы на target enabled",           hint: "ALTER TABLE ... ENABLE CONSTRAINT ..." },
      { id: "val_indexes",      label: "Все индексы VALID / USABLE",                  hint: "ALTER INDEX ... REBUILD если UNUSABLE" },
      { id: "val_triggers",     label: "Триггеры на target восстановлены",            hint: "Если были отключены перед миграцией" },
      { id: "val_app_test",     label: "Приложение протестировано на target",          hint: "Smoke test основных операций" },
      { id: "val_sequences",    label: "Sequence'ы на target обновлены",              hint: "ALTER SEQUENCE ... INCREMENT BY ... RESTART" },
    ],
  },
  {
    title: "7. Переключение (Cutover)",
    color: "#ef4444",
    items: [
      { id: "cut_lag_zero",     label: "Kafka lag = 0, STEADY_STATE подтверждён",     hint: "Финальная проверка перед переключением" },
      { id: "cut_app_stop",     label: "Приложение остановлено на source",            hint: "Нет новых DML к source-таблице" },
      { id: "cut_final_sync",   label: "Финальный lag = 0 после остановки",           hint: "Подождать пока CDC дольёт последние изменения" },
      { id: "cut_connector_rm", label: "Debezium connector удалён / остановлен",      hint: "DELETE /connectors/<name>" },
      { id: "cut_app_switch",   label: "Приложение переключено на target",            hint: "Обновить connection string" },
      { id: "cut_verify",       label: "Приложение работает штатно на target",        hint: "Мониторинг ошибок первые 30 минут" },
      { id: "cut_complete",     label: "Миграция переведена в COMPLETED",             hint: "Финальный статус" },
      { id: "cut_cleanup",      label: "Stage-таблицы и topic'и очищены",             hint: "DROP TABLE stage_*, удалить Kafka topic" },
    ],
  },
];

const ALL_IDS = SECTIONS.flatMap(s => s.items.map(i => i.id));

const LS_PREFIX = "mig_checklist_";

// ── Helpers ──────────────────────────────────────────────────────────────────

function loadProfiles(): string[] {
  try {
    return JSON.parse(localStorage.getItem(LS_PREFIX + "profiles") || "[]");
  } catch { return []; }
}

function saveProfiles(list: string[]) {
  localStorage.setItem(LS_PREFIX + "profiles", JSON.stringify(list));
}

function loadChecked(profile: string): Set<string> {
  try {
    return new Set(JSON.parse(localStorage.getItem(LS_PREFIX + "data_" + profile) || "[]"));
  } catch { return new Set(); }
}

function saveChecked(profile: string, set: Set<string>) {
  localStorage.setItem(LS_PREFIX + "data_" + profile, JSON.stringify([...set]));
}

// ── Component ────────────────────────────────────────────────────────────────

export function Checklist() {
  const [profiles, setProfiles] = useState<string[]>(loadProfiles);
  const [active, setActive]     = useState<string>(() => profiles[0] || "");
  const [checked, setChecked]   = useState<Set<string>>(() => active ? loadChecked(active) : new Set());
  const [newName, setNewName]   = useState("");
  const [showHints, setShowHints] = useState(true);
  const [filter, setFilter]     = useState<"all" | "pending" | "done">("all");

  // sync checked when profile changes
  useEffect(() => {
    if (active) setChecked(loadChecked(active));
    else setChecked(new Set());
  }, [active]);

  // persist
  useEffect(() => {
    if (active) saveChecked(active, checked);
  }, [active, checked]);

  const toggle = useCallback((id: string) => {
    setChecked(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  }, []);

  const addProfile = useCallback(() => {
    const name = newName.trim();
    if (!name || profiles.includes(name)) return;
    const next = [...profiles, name];
    setProfiles(next);
    saveProfiles(next);
    setActive(name);
    setNewName("");
  }, [newName, profiles]);

  const deleteProfile = useCallback(() => {
    if (!active) return;
    if (!confirm(`Удалить чек-лист «${active}»?`)) return;
    localStorage.removeItem(LS_PREFIX + "data_" + active);
    const next = profiles.filter(p => p !== active);
    setProfiles(next);
    saveProfiles(next);
    setActive(next[0] || "");
  }, [active, profiles]);

  const resetProfile = useCallback(() => {
    if (!active) return;
    if (!confirm("Сбросить все галочки?")) return;
    setChecked(new Set());
  }, [active]);

  const totalItems = ALL_IDS.length;
  const doneItems  = checked.size;
  const pct        = totalItems > 0 ? Math.round((doneItems / totalItems) * 100) : 0;

  const filteredSections = SECTIONS.map(sec => ({
    ...sec,
    items: sec.items.filter(item => {
      if (filter === "pending") return !checked.has(item.id);
      if (filter === "done")    return checked.has(item.id);
      return true;
    }),
  })).filter(sec => sec.items.length > 0);

  return (
    <div style={{ maxWidth: 960, margin: "0 auto" }}>
      {/* Profile selector */}
      <div style={{
        display: "flex", alignItems: "center", gap: 8, marginBottom: 16, flexWrap: "wrap",
      }}>
        <span style={{ fontSize: 12, color: "#64748b" }}>Чек-лист для:</span>
        <select
          value={active}
          onChange={e => setActive(e.target.value)}
          style={{
            background: "#1e293b", border: "1px solid #334155", borderRadius: 6,
            color: "#e2e8f0", padding: "5px 10px", fontSize: 12, minWidth: 180,
          }}
        >
          {profiles.length === 0 && <option value="">— создайте чек-лист —</option>}
          {profiles.map(p => <option key={p} value={p}>{p}</option>)}
        </select>
        <input
          placeholder="Новый (напр. ORDERS_20260319)"
          value={newName}
          onChange={e => setNewName(e.target.value)}
          onKeyDown={e => e.key === "Enter" && addProfile()}
          style={{
            background: "#0f172a", border: "1px solid #334155", borderRadius: 6,
            color: "#e2e8f0", padding: "5px 10px", fontSize: 12, width: 220,
          }}
        />
        <button onClick={addProfile} style={btnStyle("#1d4ed8")}>+ Создать</button>
        {active && (
          <>
            <button onClick={resetProfile} style={btnStyle("#334155")}>Сбросить</button>
            <button onClick={deleteProfile} style={btnStyle("#7f1d1d")}>Удалить</button>
          </>
        )}
      </div>

      {!active && (
        <div style={{
          textAlign: "center", padding: 48, color: "#475569", fontSize: 14,
        }}>
          Создайте чек-лист для конкретной миграции, чтобы отслеживать прогресс.
        </div>
      )}

      {active && (
        <>
          {/* Progress bar */}
          <div style={{
            background: "#1e293b", borderRadius: 8, padding: 16, marginBottom: 16,
            border: "1px solid #334155",
          }}>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 8 }}>
              <span style={{ fontSize: 13, fontWeight: 600 }}>
                Прогресс: {doneItems} / {totalItems}
              </span>
              <span style={{ fontSize: 13, fontWeight: 700, color: pct === 100 ? "#22c55e" : "#3b82f6" }}>
                {pct}%
              </span>
            </div>
            <div style={{
              background: "#0f172a", borderRadius: 4, height: 8, overflow: "hidden",
            }}>
              <div style={{
                width: `${pct}%`,
                height: "100%",
                background: pct === 100
                  ? "linear-gradient(90deg, #22c55e, #16a34a)"
                  : "linear-gradient(90deg, #3b82f6, #8b5cf6)",
                borderRadius: 4,
                transition: "width 0.3s ease",
              }} />
            </div>
          </div>

          {/* Controls */}
          <div style={{ display: "flex", gap: 8, marginBottom: 16, alignItems: "center" }}>
            <FilterBtn label="Все" value="all"     current={filter} onClick={setFilter} count={totalItems} />
            <FilterBtn label="Осталось" value="pending" current={filter} onClick={setFilter} count={totalItems - doneItems} />
            <FilterBtn label="Готово" value="done"    current={filter} onClick={setFilter} count={doneItems} />
            <div style={{ marginLeft: "auto" }}>
              <label style={{ fontSize: 12, color: "#64748b", cursor: "pointer", display: "flex", alignItems: "center", gap: 6 }}>
                <input
                  type="checkbox"
                  checked={showHints}
                  onChange={e => setShowHints(e.target.checked)}
                  style={{ accentColor: "#3b82f6" }}
                />
                Подсказки
              </label>
            </div>
          </div>

          {/* Sections */}
          {filteredSections.map(sec => (
            <SectionTable
              key={sec.title}
              section={sec}
              checked={checked}
              onToggle={toggle}
              showHints={showHints}
            />
          ))}
        </>
      )}
    </div>
  );
}

// ── Sub-components ───────────────────────────────────────────────────────────

function SectionTable({
  section, checked, onToggle, showHints,
}: {
  section: CheckSection;
  checked: Set<string>;
  onToggle: (id: string) => void;
  showHints: boolean;
}) {
  const done  = section.items.filter(i => checked.has(i.id)).length;
  const total = section.items.length;

  return (
    <div style={{ marginBottom: 20 }}>
      {/* Section header */}
      <div style={{
        display: "flex", alignItems: "center", gap: 10, marginBottom: 8,
      }}>
        <div style={{
          width: 4, height: 20, borderRadius: 2, background: section.color,
        }} />
        <span style={{ fontSize: 14, fontWeight: 700, color: "#e2e8f0" }}>
          {section.title}
        </span>
        <span style={{
          fontSize: 11, color: done === total ? "#22c55e" : "#64748b",
          fontWeight: done === total ? 700 : 400,
        }}>
          {done}/{total}
        </span>
      </div>

      {/* Table */}
      <table style={{
        width: "100%",
        borderCollapse: "collapse",
        fontSize: 13,
      }}>
        <thead>
          <tr style={{ borderBottom: "1px solid #1e293b" }}>
            <th style={{ ...thStyle, width: 40 }}></th>
            <th style={{ ...thStyle, textAlign: "left" }}>Пункт</th>
            {showHints && <th style={{ ...thStyle, textAlign: "left", width: "40%" }}>Подсказка</th>}
          </tr>
        </thead>
        <tbody>
          {section.items.map(item => {
            const isDone = checked.has(item.id);
            return (
              <tr
                key={item.id}
                onClick={() => onToggle(item.id)}
                style={{
                  borderBottom: "1px solid #1e293b",
                  cursor: "pointer",
                  background: isDone ? "rgba(34,197,94,0.05)" : "transparent",
                  transition: "background 0.15s",
                }}
                onMouseEnter={e => {
                  if (!isDone) (e.currentTarget as HTMLElement).style.background = "rgba(59,130,246,0.06)";
                }}
                onMouseLeave={e => {
                  (e.currentTarget as HTMLElement).style.background = isDone ? "rgba(34,197,94,0.05)" : "transparent";
                }}
              >
                <td style={{ ...tdStyle, textAlign: "center" }}>
                  <input
                    type="checkbox"
                    checked={isDone}
                    onChange={() => onToggle(item.id)}
                    onClick={e => e.stopPropagation()}
                    style={{ accentColor: "#22c55e", cursor: "pointer", width: 16, height: 16 }}
                  />
                </td>
                <td style={{
                  ...tdStyle,
                  color: isDone ? "#475569" : "#e2e8f0",
                  textDecoration: isDone ? "line-through" : "none",
                }}>
                  {item.label}
                </td>
                {showHints && (
                  <td style={{ ...tdStyle, color: "#64748b", fontSize: 12 }}>
                    {item.hint}
                  </td>
                )}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function FilterBtn({
  label, value, current, onClick, count,
}: {
  label: string;
  value: "all" | "pending" | "done";
  current: string;
  onClick: (v: "all" | "pending" | "done") => void;
  count: number;
}) {
  const isActive = value === current;
  return (
    <button
      onClick={() => onClick(value)}
      style={{
        background: isActive ? "#1e293b" : "transparent",
        border: `1px solid ${isActive ? "#334155" : "transparent"}`,
        borderRadius: 6,
        color: isActive ? "#e2e8f0" : "#475569",
        padding: "4px 12px",
        fontSize: 12,
        cursor: "pointer",
        fontWeight: isActive ? 600 : 400,
      }}
    >
      {label} <span style={{ color: "#64748b", fontWeight: 400 }}>({count})</span>
    </button>
  );
}

// ── Styles ───────────────────────────────────────────────────────────────────

const thStyle: React.CSSProperties = {
  padding: "8px 10px",
  fontSize: 11,
  fontWeight: 600,
  color: "#475569",
  textTransform: "uppercase",
  letterSpacing: 0.5,
};

const tdStyle: React.CSSProperties = {
  padding: "10px 10px",
};

function btnStyle(bg: string): React.CSSProperties {
  return {
    background: bg,
    border: "1px solid #334155",
    borderRadius: 6,
    color: "#e2e8f0",
    padding: "5px 12px",
    fontSize: 12,
    cursor: "pointer",
    fontWeight: 500,
  };
}
