# Migration Planner — Design Spec

**Date:** 2026-03-27
**Scope:** Спек 1 — таблицы и данные (PL/SQL объекты — отдельный спек позже)

## Проблема

Сейчас пользователь для подготовки миграции вручную переключается между вкладками:
1. TargetPrep — сравнить DDL по одной таблице
2. ConnectorGroups — создать группу коннекторов
3. CreateMigration — создать миграцию для каждой таблицы

Нет единого pipeline. Нет возможности увидеть картину по всей схеме и массово подготовить миграции.

## Решение

Новая вкладка **«Планирование»** — wizard из 4 шагов, объединяющий полный цикл подготовки.

---

## Шаг 1: Сравнение схем

### UI
- Два селектора: source-схема, target-схема (переиспользуем существующий `/api/db/{source,target}/schemas`)
- Кнопка «Сравнить»
- Таблица результатов:

| Колонка       | Описание                                       |
|---------------|-------------------------------------------------|
| Checkbox      | Для выбора таблиц (шаг 2)                      |
| Таблица       | Имя таблицы                                     |
| Статус        | Badge: OK / Различается / Нет в target / Ошибка |
| Колонки       | `+3 / -1 / ~2` (missing / extra / type mismatch)|
| Индексы       | `+2 / !1` (missing / disabled)                  |
| Constraints   | `+1 / !0`                                       |
| Триггеры      | `+1`                                             |

- Фильтры: по статусу (все / различаются / нет в target / OK), поиск по имени
- Клик на строку — раскрывается inline-панель с детальным diff (колонки, индексы, constraints, триггеры) — логика и UI из текущего TargetPrep
- Кнопка «Select All» / «Deselect All» с учётом фильтра

### Backend
- **Новый endpoint** `GET /api/planner/compare-schema?src_schema=X&tgt_schema=Y`
- Переиспользует `list_tables()` + `get_full_ddl_info()` + `_diff_summary()` из существующего кода
- Возвращает массив:
```json
[
  {
    "table": "ORDERS",
    "exists_in_target": true,
    "diff": { "ok": false, "total": 3, "cols_missing": 2, ... }
  },
  {
    "table": "USERS",
    "exists_in_target": false,
    "diff": null
  }
]
```
- Для больших схем (100+ таблиц) — потенциально долго. Сначала делаем синхронно. Если будет тормозить — вынесем в фоновую задачу через job_queue.

### Действия из шага 1
- Кнопка «Создать таблицу» для таблиц со статусом «Нет в target»
- Кнопка «Синхронизировать» для таблиц со статусом «Различается» (sync columns + sync objects)
- Используем существующие endpoints: `/api/target-prep/ensure-table`, `/api/target-prep/sync-columns`, `/api/target-prep/sync-objects`
- После действия — пересчитываем diff только для этой таблицы

---

## Шаг 2: Выбор таблиц и настройка

### UI
- Показываем только выбранные на шаге 1 таблицы
- Для каждой таблицы:
  - Статус готовности (OK / требуется синхронизация)
  - Переключатель режима: CDC / BULK_ONLY
- Глобальные настройки (применяются ко всем, можно переопределить per-table):
  - `chunk_size` (default: 50000)
  - `max_parallel_workers` (default: 4)
  - `migration_strategy`: STAGE / DIRECT (default: STAGE)
- Для CDC-режима: выбор существующей группы коннекторов или создание новой
  - Если «создать новую» — имя группы, topic_prefix (создаётся при execute)
  - Все CDC-таблицы плана привязываются к одной группе коннекторов
  - BULK_ONLY-таблицы не требуют группы коннекторов и игнорируют это поле
- Допускается смешанный план: часть таблиц CDC, часть BULK_ONLY

### Валидация
- Таблицы со статусом «Нет в target» нельзя включить пока не создана
- Предупреждение если таблица имеет расхождения (можно проигнорировать)

---

## Шаг 3: Очерёдность

### UI
- Таблица выбранных миграций с drag-and-drop (или числовой ввод приоритета)
- Автоматическое определение FK-зависимостей между выбранными таблицами
  - Если таблица A ссылается FK на таблицу B — предупреждение: «B должна быть мигрирована до A»
  - Автоматическая сортировка по зависимостям (topological sort) с кнопкой «Авто-порядок»
- Группировка по пакетам (batch): таблицы в одном batch запускаются параллельно
  - Пользователь может drag-and-drop таблицы между batch'ами
  - Batch 1 завершается полностью → запускается Batch 2

### Backend
- **Новый endpoint** `GET /api/planner/fk-dependencies?schema=X&tables=A,B,C`
- Запрос к `all_constraints` (type='R') для выбранных таблиц
- Возвращает: `[{ "table": "ORDERS", "depends_on": ["CUSTOMERS", "PRODUCTS"] }]`

---

## Шаг 4: Обзор и запуск

### UI
- Сводка плана:
  - Количество таблиц
  - Режим (CDC / BULK_ONLY) — сколько каких
  - Количество batch'ей
  - Общие настройки
- Список batch'ей с таблицами в каждом
- Кнопка «Создать миграции» (не запускать, а создать в DRAFT)
- После создания — кнопка «Запустить план» (переводит DRAFT → NEW последовательно по batch'ам)

### Backend
- **Новый endpoint** `POST /api/planner/execute`
- Request body:
```json
{
  "src_schema": "PROD",
  "tgt_schema": "STAGE",
  "connector_group_id": 5,
  "create_connector_group": null,
  // connector_group_id — только для таблиц с mode=CDC; BULK_ONLY таблицы его игнорируют
  "defaults": {
    "chunk_size": 50000,
    "max_parallel_workers": 4,
    "migration_strategy": "STAGE",
    "mode": "CDC"
  },
  "batches": [
    {
      "order": 1,
      "tables": [
        { "table": "CUSTOMERS", "mode": "CDC", "overrides": {} },
        { "table": "PRODUCTS", "mode": "BULK_ONLY", "overrides": { "chunk_size": 100000 } }
      ]
    },
    {
      "order": 2,
      "tables": [
        { "table": "ORDERS", "mode": "CDC", "overrides": {} }
      ]
    }
  ]
}
```
- Создаёт миграции в DRAFT, сохраняет план в PostgreSQL
- **Новый endpoint** `POST /api/planner/plans/{plan_id}/start`
  - Запускает batch 1 (DRAFT → NEW)
  - Фоновый процесс следит за завершением batch'а и запускает следующий

---

## Модель данных (PostgreSQL)

### Таблица `migration_plans`
```sql
CREATE TABLE IF NOT EXISTS migration_plans (
    plan_id         SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    src_schema      TEXT NOT NULL,
    tgt_schema      TEXT NOT NULL,
    connector_group_id INTEGER,
    defaults_json   JSONB NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL DEFAULT 'DRAFT',
    created_at      TIMESTAMPTZ DEFAULT now(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ
);
```

### Таблица `migration_plan_items`
```sql
CREATE TABLE IF NOT EXISTS migration_plan_items (
    item_id         SERIAL PRIMARY KEY,
    plan_id         INTEGER NOT NULL REFERENCES migration_plans(plan_id),
    table_name      TEXT NOT NULL,
    mode            TEXT NOT NULL DEFAULT 'CDC',
    batch_order     INTEGER NOT NULL DEFAULT 1,
    sort_order      INTEGER NOT NULL DEFAULT 0,
    overrides_json  JSONB NOT NULL DEFAULT '{}',
    migration_id    TEXT,
    status          TEXT NOT NULL DEFAULT 'PENDING'
);
```

### Статусы плана
- `DRAFT` — создан, миграции ещё не созданы
- `READY` — миграции созданы в DRAFT
- `RUNNING` — выполняется (текущий batch запущен)
- `COMPLETED` — все batch'и завершены
- `FAILED` — есть ошибки
- `CANCELLED` — отменён пользователем

---

## Архитектура фронтенда

### Новые файлы
- `frontend/src/components/MigrationPlanner.tsx` — основной компонент wizard'а
  - Внутренние компоненты (в том же файле или отдельные, по размеру):
    - `SchemaCompareStep` — шаг 1
    - `TableSelectionStep` — шаг 2
    - `OrderingStep` — шаг 3
    - `ReviewStep` — шаг 4

### Новые backend файлы
- `backend/routes/planner.py` — blueprint с endpoints
  - Переиспользует: `oracle_browser.py`, `target_prep.py` (функции `_diff_summary`), `oracle_ddl_sync.py`

### Интеграция
- Новая вкладка в `App.tsx`: `Tab = "planner" | "migrations" | ...`
- Вкладка «Планирование» — первая в списке (основная точка входа)

---

## Что НЕ входит в этот спек

- PL/SQL объекты (функции, процедуры, пакеты, sequences, types, synonyms) — отдельный спек
- Автоматический мониторинг выполнения плана (пока ручной контроль через вкладку «Миграции»)
- Шаблоны планов (сохранение/загрузка)
- Откат плана (rollback)

---

## Ограничения и допущения

- Source и target используют одинаковые имена таблиц (mapping 1:1 по имени)
- Одна source-схема → одна target-схема за один план
- Максимальный размер схемы для синхронного сравнения — ~500 таблиц (если больше — переделаем на async)
- FK-зависимости определяются только между таблицами в рамках одного плана
