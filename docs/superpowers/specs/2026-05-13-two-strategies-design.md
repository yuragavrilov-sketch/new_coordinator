# Two Migration Strategies — Design Spec

**Date:** 2026-05-13
**Status:** Approved (brainstorming)

## Problem

Текущая модель миграции таблиц комбинирует три ортогональные оси:

| Ось                  | Значения                                | Где живёт                                  |
|----------------------|------------------------------------------|---------------------------------------------|
| Класс машины         | Legacy (per-migration connector) / Group | `_LEGACY_HANDLERS` vs `_GROUP_HANDLERS`     |
| `migration_mode`     | CDC / BULK_ONLY                          | колонка `migrations.migration_mode`         |
| `migration_strategy` | STAGE / DIRECT                           | колонка `migrations.migration_strategy`     |

В сумме — 8 теоретических комбинаций и два пути диспетчеризации в оркестраторе (`_dispatch` ветвится по `group_id`). Реально используются 4: Legacy-ветка больше не нужна (групповой коннектор полностью заменил per-migration сценарий). Это даёт мёртвый код, лишние фазы (`PREPARING`, `SCN_FIXED`, `CONNECTOR_STARTING`, `CDC_BUFFERING`, `CDC_APPLY_STARTING`) и UX-сложность: пользователь выбирает два независимых dropdown'а вместо одной осмысленной стратегии.

## Solution (high-level)

1. На уровне UX — **2 стратегии**: «С CDC» и «Без CDC».
2. STAGE/DIRECT — advanced-настройка под обеими стратегиями (default = `STAGE`).
3. В БД — одно поле `strategy` с 4 enum-значениями: `CDC_STAGE`, `CDC_DIRECT`, `BULK_STAGE`, `BULK_DIRECT`.
4. Legacy-машина состояний и связанный код удаляются полностью (Big-bang refactor — живых Legacy-миграций в БД нет).
5. Group-машина переименовывается в единственный путь.

## Goals

- Свести user-facing выбор миграции к **2 стратегиям** (`С CDC` / `Без CDC`) + advanced toggle (STAGE/DIRECT).
- Удалить Legacy-ветку оркестратора (per-migration Debezium connector, SCN-фиксация на источнике, фазы `PREPARING`/`SCN_FIXED`/`CONNECTOR_STARTING`/`CDC_BUFFERING`/`CDC_APPLY_STARTING`).
- Привести БД к одному enum-полю `strategy`.
- Сделать `group_id` обязательным полем при создании миграции.

## Non-Goals

- Не меняем поведение фаз `DATA_VERIFYING` / `DATA_MISMATCH` (введены spec'ом 2026-03-30).
- Не вводим сверку HASH в CDC-стратегии (в CDC верификация остаётся косвенной — `STAGE_VALIDATING` лёгкая проверка row count'а).
- Не трогаем DDL-каталог (`ddl_objects`, `ddl_compare_results`, `schema_migrations` агрегатор).
- Не меняем формат SSE-событий и публичные API схема-миграций за пределами поля стратегии.
- Не вводим backward-compatibility для старого API (фронт и бэк деплоятся вместе).

## User-Facing Model

### UI

Один основной радиогруп: **«С CDC»** / **«Без CDC»**.
Один опциональный collapsible toggle «Дополнительно» с радиогрупом «Способ загрузки»: **STAGE** / **DIRECT** (default = STAGE независимо от стратегии).

```
┌─ Стратегия миграции ───────────────────────────────┐
│ (•) С CDC      — bulk-загрузка + apply из Kafka    │
│ ( ) Без CDC    — только bulk-загрузка               │
│                                                      │
│ ▶ Дополнительно                                    │
│   ┌─ Способ загрузки ──────────────────────────┐   │
│   │ (•) STAGE   — через stage-таблицу (надёжнее)│   │
│   │ ( ) DIRECT  — напрямую в target (быстрее)  │   │
│   └────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────┘
```

### Маппинг на DB enum

| UI выбор                         | DB `strategy`  |
|----------------------------------|----------------|
| С CDC + STAGE (default)          | `CDC_STAGE`    |
| С CDC + DIRECT                   | `CDC_DIRECT`   |
| Без CDC + STAGE (default «без CDC») | `BULK_STAGE` |
| Без CDC + DIRECT                 | `BULK_DIRECT`  |

### Где меняется UI

- `CreateMigrationModal`
- `PlannerWizard/steps/TableSelectionStep` (глобальный picker + per-table override)
- `ConnectorGroupsPanel/MigrateModal`
- `MigrationDetail/OverviewTab`, `MigrationList/MigrationRow` — отображение лейбла (`strategyLabel(s)` = «С CDC (stage)» и т.п.)

Унифицируем через новый компонент `<StrategyPicker>` (props: `value: Strategy`, `onChange`) — используется во всех трёх формах создания.

## Database Schema

### Новая колонка

```sql
ALTER TABLE migrations
    ADD COLUMN strategy TEXT NOT NULL DEFAULT 'CDC_STAGE'
        CHECK (strategy IN ('CDC_STAGE', 'CDC_DIRECT', 'BULK_STAGE', 'BULK_DIRECT'));
```

### Backfill (выполняется один раз, при `ensure_schema`)

```sql
UPDATE migrations SET strategy =
    CASE
        WHEN migration_mode = 'BULK_ONLY' AND migration_strategy = 'DIRECT' THEN 'BULK_DIRECT'
        WHEN migration_mode = 'BULK_ONLY'                                    THEN 'BULK_STAGE'
        WHEN migration_strategy = 'DIRECT'                                   THEN 'CDC_DIRECT'
        ELSE 'CDC_STAGE'
    END
WHERE EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'migrations' AND column_name = 'migration_mode'
);
```

### Удаляемые колонки (после backfill)

- `migrations.migration_mode`
- `migrations.migration_strategy`

### Что НЕ трогаем

- `schema_migrations`, `migration_plan_items`, `migration_chunks` — поля стратегии там нет.
- `connector_groups` — стратегия живёт на уровне отдельной миграции, не группы. Миграции в одной группе могут иметь разные `strategy` (например, в одной группе CDC-таблицы и BULK-таблицы).

### Helper (Python — `backend/services/strategy.py`, новый файл)

```python
from enum import StrEnum

class Strategy(StrEnum):
    CDC_STAGE   = "CDC_STAGE"
    CDC_DIRECT  = "CDC_DIRECT"
    BULK_STAGE  = "BULK_STAGE"
    BULK_DIRECT = "BULK_DIRECT"

    @property
    def has_cdc(self) -> bool:     return self.value.startswith("CDC_")
    @property
    def uses_stage(self) -> bool:  return self.value.endswith("_STAGE")
```

Все ветвления в оркестраторе и валидаторах идут через `Strategy.has_cdc` / `Strategy.uses_stage`, не через текстовое сравнение.

## State Machine After Refactor

Единая фазовая машина (бывший `_GROUP_HANDLERS` без Legacy):

```
NEW
 ├─ валидация ключей: PK/UK обязательны только если strategy.has_cdc
 ├─ queue gate: один heavy-слот на нагрузочные фазы
 ├─ если strategy.has_cdc — проверка что group connector RUNNING
 ├─ создать stage table если strategy.uses_stage
 │
 ├─ CDC_*  ──→ TOPIC_CREATING ──→ CHUNKING
 └─ BULK_* ──→                    CHUNKING

CHUNKING ──→ BULK_LOADING ──→ BULK_LOADED
                              │
       ┌──────────────────────┴─ strategy.uses_stage? ──┐
       │ да                                              │ нет (DIRECT)
       ▼                                                 ▼
   STAGE_VALIDATING → STAGE_VALIDATED                INDEXES_ENABLING
     → BASELINE_PUBLISHING → BASELINE_LOADING
     → BASELINE_PUBLISHED → STAGE_DROPPING
     → INDEXES_ENABLING

INDEXES_ENABLING
       │
       ├─ strategy.has_cdc?
       │   да:  CDC_APPLYING → CDC_CATCHING_UP → CDC_CAUGHT_UP → STEADY_STATE [терминал]
       │   нет: DATA_VERIFYING → COMPLETED [терминал]
       │                       ↘ DATA_MISMATCH → retry_verify / force_complete / cancel
       │
       └─ ошибка пересчёта: остаётся в INDEXES_ENABLING с error_code,
                            user-triggered «Повторить» (trigger_indexes_enabling)

[любая фаза] → CANCELLING → CANCELLED
```

### Удалённые фазы

`PREPARING`, `SCN_FIXED`, `CONNECTOR_STARTING`, `CDC_BUFFERING`, `CDC_APPLY_STARTING`.

### Удалённые handler'ы и структуры

- Словарь `_LEGACY_HANDLERS`
- `_handle_new` (Legacy-версия)
- `_handle_preparing`
- `_handle_scn_fixed`
- `_handle_connector_starting`
- `_handle_cdc_buffering`
- `_handle_cdc_apply_starting`
- Ветка `if m.get("group_id"):` в `_dispatch` (становится прямым вызовом единого словаря handler'ов)

### Переименование

- `_handle_new_group` → `_handle_new`
- `_handle_indexes_enabling_group` → `_handle_indexes_enabling`

### Изменения в существующих handler'ах

- `_handle_new`: ветвление `mode == "BULK_ONLY"` → `not strategy.has_cdc`; ветвление `strategy.uses_stage` определяет, создаём ли stage; диспетчер следующей фазы — `TOPIC_CREATING` (CDC) или `CHUNKING` (BULK).
- `_handle_bulk_loaded`: `strategy == "DIRECT"` → `not strategy.uses_stage` → `INDEXES_ENABLING`; иначе → `STAGE_VALIDATING`.
- `_handle_indexes_enabling`: после успеха — `strategy.has_cdc` → `CDC_APPLYING`; иначе → `DATA_VERIFYING`.
- `_HEAVY_PHASES` в оркестраторе: убираем `PREPARING`, `SCN_FIXED`, `CONNECTOR_STARTING`, `CDC_BUFFERING`.

### Что НЕ меняется

- Логика `STAGE_VALIDATING` / `BASELINE_*` / `STAGE_DROPPING` (handler'ы не зависели от Legacy/Group).
- Логика `CDC_CATCHING_UP` / `CDC_CAUGHT_UP` / `STEADY_STATE`.
- `_check_group_connectors`, `_tick_groups`.
- Manual triggers (`trigger_indexes_enabling`, `trigger_enable_triggers`, `trigger_baseline_restart`).
- `DATA_VERIFYING` / `DATA_MISMATCH` поведение и набор пользовательских actions.

## Files to Modify

### Backend

| File | Изменения |
|------|-----------|
| `backend/services/orchestrator.py` | Удалить Legacy handler'ы и `_LEGACY_HANDLERS`; объединить `_dispatch`; переименовать group-handler'ы; диспетчеризация по `Strategy`. Удалить из `_HEAVY_PHASES` 4 Legacy-фазы. |
| `backend/services/strategy.py` (новый) | Enum `Strategy` + properties `has_cdc` / `uses_stage`. |
| `backend/services/schema_migrations.py` | Удалить из `_PHASE_TO_STAGE` и `_ACTIVE_PHASES` ключи `PREPARING`, `SCN_FIXED`, `CONNECTOR_STARTING`, `CDC_BUFFERING`, `CDC_APPLY_STARTING`. |
| `backend/db/state_db.py` | `ensure_schema`: миграция БД (см. ниже). `_VALID_PHASES` / `_ACTIVE_PHASES`: убрать удалённые 5 фаз. |
| `backend/services/debezium.py` | Удалить `create_connector(m, src_cfg)` (per-migration). `get_connector_status` оставляем. |
| `backend/services/oracle_scn.py` | `get_current_scn` удалить (если не используется снаружи Legacy). `check_supplemental_logging` оставить (можно вызывать как warning в новом `_handle_new`). `open_oracle_conn` оставить. |
| `backend/routes/migrations.py` | Принимать `strategy` (обязательное), валидировать enum. `group_id` обязательное → HTTP 400 иначе. CDC + non-RUNNING group → HTTP 409. |
| `backend/routes/planner.py` | Принимать `strategy` per-table в payload `create-migrations`. |
| `backend/routes/connector_groups.py` | В endpoint'е создания миграций в группе — принимать `strategy`. |

### Frontend

| File | Изменения |
|------|-----------|
| `frontend/src/types/migration.ts` | Type `Strategy = "CDC_STAGE" \| "CDC_DIRECT" \| "BULK_STAGE" \| "BULK_DIRECT"`. Helpers `hasCdc`, `usesStage`, `strategyLabel`, `composeStrategy`. |
| `frontend/src/components/StrategyPicker/` (новый) | Унифицированный компонент с радиогрупом и advanced toggle. |
| `frontend/src/components/CreateMigrationModal/{index,helpers,types}.tsx` | Использовать `<StrategyPicker>`; payload `{strategy}` вместо `{migration_mode, migration_strategy}`. |
| `frontend/src/components/DDLCatalog/PlannerWizard/{index,types,steps/TableSelectionStep}.tsx` | То же + per-table override. |
| `frontend/src/components/ConnectorGroupsPanel/{MigrateModal,types,helpers}.tsx` | То же. |
| `frontend/src/components/MigrationDetail/{tabs/OverviewTab,helpers}.tsx` | Чтение `m.strategy`, отображение через `strategyLabel`. |
| `frontend/src/components/MigrationList/MigrationRow.tsx` | Колонка «Стратегия» с бейджем `strategyLabel(m.strategy)`. |

## API Contract

### `POST /api/migrations` (одиночная миграция)

```diff
{
  "source_schema": "...",
  "source_table":  "...",
  "target_schema": "...",
  "target_table":  "...",
  "group_id":      "uuid",         // ОБЯЗАТЕЛЬНОЕ
- "migration_mode":     "CDC",     // удалено
- "migration_strategy": "STAGE",   // удалено
+ "strategy":           "CDC_STAGE",
  "chunk_size":              100000,
  "effective_key_columns_json": "[...]",  // обязательно для CDC если нет PK/UK
  ...
}
```

### `POST /api/planner/create-migrations` (батч из визарда)

Поле `strategy` per-table.

### `POST /api/connector-groups/<id>/migrate`

Поле `strategy` per-table.

### Валидация (синхронная в API)

- `strategy ∈ {CDC_STAGE, CDC_DIRECT, BULK_STAGE, BULK_DIRECT}` → иначе HTTP 400.
- `group_id` обязательное → иначе HTTP 400 «Миграция должна быть привязана к группе коннектора».
- `strategy.has_cdc` + `group.status != 'RUNNING'` → HTTP 409 «Запустите коннектор группы перед созданием CDC-миграции».
- `strategy.has_cdc` + нет PK/UK + пустой `effective_key_columns_json` → HTTP 400.

### Response

- `GET /api/migrations/<id>`, `GET /api/schema-migrations/<id>/objects` отдают поле `strategy` вместо двух старых.
- Лейблы для UI считаются на клиенте.

### Backward compatibility

**Нет.** Старые клиенты сломаются. Допустимо: фронт и бэк деплоятся вместе.

## DB Migration

Транзакция в `ensure_schema` при старте бэка:

```sql
BEGIN;

ALTER TABLE migrations
    ADD COLUMN IF NOT EXISTS strategy TEXT NOT NULL DEFAULT 'CDC_STAGE'
        CHECK (strategy IN ('CDC_STAGE', 'CDC_DIRECT', 'BULK_STAGE', 'BULK_DIRECT'));

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name='migrations' AND column_name='migration_mode') THEN
        UPDATE migrations SET strategy =
            CASE
                WHEN migration_mode = 'BULK_ONLY' AND migration_strategy = 'DIRECT' THEN 'BULK_DIRECT'
                WHEN migration_mode = 'BULK_ONLY'                                    THEN 'BULK_STAGE'
                WHEN migration_strategy = 'DIRECT'                                   THEN 'CDC_DIRECT'
                ELSE 'CDC_STAGE'
            END;
    END IF;
END $$;

ALTER TABLE migrations DROP COLUMN IF EXISTS migration_mode;
ALTER TABLE migrations DROP COLUMN IF EXISTS migration_strategy;

COMMIT;
```

Идемпотентно: повторный запуск — no-op (`IF NOT EXISTS`, `IF EXISTS`).

### Защитная проверка при старте

```sql
SELECT COUNT(*) FROM migrations
 WHERE phase IN ('PREPARING','SCN_FIXED','CONNECTOR_STARTING','CDC_BUFFERING','CDC_APPLY_STARTING');
```

Если > 0 → бэк падает с понятной ошибкой при старте: «Найдены миграции в Legacy-фазах. Завершите или отмените их перед обновлением». Автомиграцию фаз не делаем — слишком тонко.

## Error Handling

- `_handle_new` для `BULK_*`: если PK/UK нет — не падаем, идём дальше (как уже сделано в spec'е 2026-03-30).
- `_handle_new` для `CDC_*`: без PK/UK и без `effective_key_columns_json` → `FAILED` с `error_code=NO_KEY_COLUMNS`.
- Group-connector NOT RUNNING для CDC: ошибка теперь в API (HTTP 409); дублирующая проверка в `_handle_new` сохраняется как defensive guard.
- Маппинг по `strategy` неизвестного значения (порча БД) → `FAILED` с `error_code=UNKNOWN_STRATEGY`.

## Testing

### Юнит-тесты (обязательные)

- `services/strategy.py`: для каждого из 4 значений — `has_cdc` и `uses_stage` дают ожидаемые булы.
- `schema_migrations._aggregate_stage` / `_aggregate_status`: на новом наборе фаз (без 5 удалённых).
- `_PHASE_TO_STAGE` не содержит удалённых ключей (assert на стартапе или тестом).

### Интеграционные сценарии (smoke)

- `CDC_STAGE`: проход до `STEADY_STATE`.
- `BULK_STAGE`: проход до `COMPLETED` через `DATA_VERIFYING`.
- `BULK_DIRECT`: пропуск `STAGE_VALIDATING` / `BASELINE_*` / `STAGE_DROPPING`, прямо в `INDEXES_ENABLING` → `DATA_VERIFYING`.
- `CDC_DIRECT`: пропуск `STAGE_VALIDATING` / `BASELINE_*` / `STAGE_DROPPING`, прямо в `INDEXES_ENABLING` → `CDC_APPLYING`.
- Создание миграции без `group_id` → HTTP 400.
- Создание CDC-миграции в группе со `status != 'RUNNING'` → HTTP 409.

## Rollout

1. Один PR, один деплой. Backward-compatibility не нужна (Legacy-данных в БД нет, фронт+бэк деплоятся вместе).
2. Перед merge: остановить оркестратор и убедиться, что нет in-flight миграций.
3. После деплоя: `ensure_schema` отрабатывает миграцию БД при старте бэка.
4. Smoke: создать по одной миграции каждого из 4 типов через UI, проверить конечную фазу.
