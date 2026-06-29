# Параллелизация конвейера миграций: полосы + index-enable как воркер-джоб

Дата: 2026-06-29
Статус: утверждён дизайн, ожидает плана реализации

## Проблема

Сейчас оркестратор использует **один глобальный «слот загрузки»**. Гейт `slot_busy`
в `_handle_new` (`backend/services/orchestrator.py`) проверяет наличие *любой*
миграции в `_HEAVY_PHASES` и, если такая есть, держит все остальные миграции
(и CDC, и не-CDC) в фазе `NEW`.

`_HEAVY_PHASES` включает `INDEXES_ENABLING`, причём эта фаза может быть ещё и
ручным гейтом (`trigger_indexes_enabling` — кнопка «Включить индексы»). Поэтому:

- Следующая CDC-таблица не может начать bulk-загрузку, пока у текущей не
  завершится index enabling (в т.ч. если оно ждёт ручного нажатия).
- Не-CDC таблицы стоят в той же общей очереди и не идут параллельно с CDC.

Index enabling вдобавок исполняется **inline на координаторе** (фоновый поток в
`_handle_indexes_enabling`, Oracle-conn открывается в процессе координатора): не
переживает рестарт координатора и нагружает его при нескольких одновременных
включениях.

## Цели

1. **Не-CDC таблицы копируются параллельно с CDC.** Максимум 1 CDC bulk-load +
   1 не-CDC bulk-load одновременно (ширина каждой полосы = 1).
2. **Следующая таблица стартует, не дожидаясь index enabling предыдущей** — для
   **обеих** полос. `INDEXES_ENABLING` и весь хвост перестают держать полосу.
3. **Index enabling становится отдельным джобом, который клеймит воркер** (как
   bulk/baseline/CDC/DDL), а не inline-работой координатора.

## Не-цели (YAGNI)

- Ширина полос > 1 (настраиваемый N). Закладываем структуру, но реализуем 1+1.
- Жёсткий кап на число одновременных index-enable джобов. При ширине полос 1
  пайл-ап маловероятен; ограничение даёт пул воркеров. Можно добавить позже.
- Параллелизм на уровне connector-групп.
- Изменения UI помимо корректных lane-позиций в очереди.

## Решение

### Модель полос

Две независимые bulk-полосы шириной 1, разделённые по префиксу стратегии:

- **CDC-полоса:** миграции с `strategy LIKE 'CDC_%'`.
- **Не-CDC полоса:** все остальные.

Полосу занимают только «блокирующие» bulk-фазы:

```
BULK_LANE_PHASES = {
    TOPIC_CREATING, CHUNKING,
    BULK_LOADING, BULK_LOADED,
    STAGE_VALIDATING, STAGE_VALIDATED,
    BASELINE_PUBLISHING, BASELINE_LOADING, BASELINE_PUBLISHED,
    STAGE_DROPPING,
}
```

`INDEXES_ENABLING` и весь хвост (`DATA_VERIFYING`, `CDC_APPLYING`,
`CDC_CATCHING_UP`, `CDC_CAUGHT_UP`, `STEADY_STATE`, ...) полосу **не** держат.

Порядок хвоста (для справки):
`... → BASELINE_PUBLISHED → STAGE_DROPPING → INDEXES_ENABLING → DATA_VERIFYING → CDC_APPLYING/COMPLETED`.

## Архитектура

Изменение разбивается на две фазы реализации. Обе входят в эту спеку; план
(writing-plans) располагает их последовательно, чтобы Фаза 1 давала эффект сразу.

### Фаза 1 — Параллельные полосы

Чистое изменение оркестратора, без изменений схемы и воркера.

**1.1. Lane-гейт в `_handle_new`.** Текущий единый запрос `slot_busy`
(`orchestrator.py`, ~строка 1437) заменяется проверкой по *своей* полосе:
NEW-миграция занимает полосу, только если нет другой миграции той же полосы в
`BULK_LANE_PHASES`. Лейн вычисляется по `LEFT(COALESCE(strategy,''),4) = 'CDC_'`.
Перекрёстно полосы не блокируют.

**1.2. `_update_queue_positions` — lane-aware.** Текущая сквозная нумерация
заменяется на `ROW_NUMBER() OVER (PARTITION BY lane ORDER BY state_changed_at)`,
а признак «слот занят» считается per-lane. UI показывает позицию внутри своей
полосы.

**1.3. Наборы фаз.** Вводится `BULK_LANE_PHASES` (см. выше). Старый
`_HEAVY_PHASES` либо переименовывается, либо сводится к `BULK_LANE_PHASES`
(`INDEXES_ENABLING` исключается из блокирующего набора).

### Фаза 2 — Index enabling как воркер-джоб

**2.1. Таблица `index_enable_jobs`** (по образцу `target_trigger_jobs` и
`migration_chunks`):

```sql
CREATE TABLE IF NOT EXISTS index_enable_jobs (
    job_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    migration_id  UUID NOT NULL REFERENCES migrations(migration_id) ON DELETE CASCADE,
    state         VARCHAR(20) NOT NULL DEFAULT 'PENDING',  -- PENDING|CLAIMED|RUNNING|DONE|FAILED
    worker_id     VARCHAR(200),
    result_json   JSONB,
    error_text    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_at    TIMESTAMPTZ,
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ
);
-- не более одного активного джоба на миграцию
CREATE UNIQUE INDEX IF NOT EXISTS idx_iej_active
    ON index_enable_jobs (migration_id)
    WHERE state IN ('PENDING', 'CLAIMED', 'RUNNING');
```

Создаётся идемпотентно в `_init_schema_legacy` (как соседние таблицы); отдельная
Alembic-ревизия не требуется (baseline делегирует legacy-bootstrap).

**2.2. Воркер.** В `workers/`:

- `claim_index_enable_job(conn)` — `SELECT ... FOR UPDATE SKIP LOCKED` одной
  PENDING-строки, перевод в `CLAIMED` с `worker_id`/`claimed_at`.
- `complete_index_enable_job(conn, job_id, result)` и
  `fail_index_enable_job(conn, job_id, error)` — с guard'ом `AND worker_id = %s`
  (консистентно с фиксами C7).
- Новый поток `index_enable_loop(stop_event)` в `main()` рядом с `compare_loop`:
  клеймит джоб, исполняет DDL включения, отмечает `DONE`/`FAILED`.

**2.3. Порт DDL-логики в воркер (основная стоимость Фазы 2).** Функции
`enable_all_disabled_objects`, `set_table_logging`,
`rebuild_unusable_constraint_indexes`, `_constraint_backing_indexes`,
`is_temporary_table` и зависимая интроспекция живут в
`backend/db/oracle_browser.py`. Воркеры — отдельный пакет и backend не
импортируют. Эта логика портируется в новый модуль `workers/oracle_ddl.py`
(или в `workers/common.py`). Поведение должно совпадать с backend-версией, в т.ч.
недавний фикс M4 (пропуск партиционированных индексов).

**2.4. Оркестратор — `_handle_indexes_enabling` мониторит джоб.** Больше не
исполняет DDL inline. Логика как у `_handle_bulk_loading`:

1. при входе в фазу гарантирует наличие активного джоба (создаёт PENDING, если
   нет);
2. на каждом тике читает state джоба:
   - `DONE` → `_safe_transition(INDEXES_ENABLING → DATA_VERIFYING)` (не-CDC) или
     `→ CDC_APPLYING` (CDC), переносит `result_json` в сообщение;
   - `FAILED` → остаётся в `INDEXES_ENABLING` с `error_code=INDEXES_ENABLE_ERROR`
     (как сейчас) для ручного ретрая;
   - иначе ждёт.

`INDEXES_ENABLING` больше не использует `_in_progress`/поток координатора.

**2.5. Ручной ретрай.** `trigger_indexes_enabling` (кнопка «Включить индексы»)
теперь ставит/пере-ставит PENDING-джоб вместо спавна потока. Принимает те же
фазы, что и сейчас (`INDEXES_ENABLING`, а также `FAILED` с
`INDEXES_ENABLE_ERROR`).

**2.6. Stale-reset.** В тике сбрасывать `CLAIMED/RUNNING` index-enable джобы
старше таймаута обратно в `PENDING` (аналогично `reset_stale_chunks`), чтобы
смерть воркера не вешала фазу навсегда.

## Потоки данных

```
NEW (gate: своя полоса свободна?)
  └─► [BULK_LANE_PHASES]  — держит полосу (CDC или не-CDC)
        └─► STAGE_DROPPING
              └─► INDEXES_ENABLING  — ПОЛОСУ НЕ ДЕРЖИТ
                    ├─ orchestrator: ensure index_enable_job(PENDING)
                    ├─ worker: claim → enable DDL → DONE/FAILED
                    └─ orchestrator poll:
                         DONE   → DATA_VERIFYING (не-CDC) / CDC_APPLYING (CDC)
                         FAILED → INDEXES_ENABLE_ERROR (ручной ретрай)
```

Пока таблица A в `INDEXES_ENABLING` (в хвосте), её полоса свободна → следующая
таблица той же полосы стартует bulk. Параллельно другая полоса ведёт свою
таблицу.

## Обработка ошибок и краевые случаи

- **Пустая таблица-источник:** `_create_chunks_and_transition` уже переводит
  сразу в `INDEXES_ENABLING`, минуя bulk. Корректно: уходит в хвост, полосу не
  занимает, джоб создаётся как обычно.
- **Отмена:** `_handle_cancelling` без изменений. Index-enable джоб для
  отменённой миграции удаляется по `ON DELETE CASCADE`; зависший CLAIMED джоб
  гасится stale-reset или guard'ом `worker_id`.
- **Рестарт координатора во время index enabling:** джоб переживает (он в State
  DB и исполняется воркером). При старте оркестратор снова поллит state.
- **Несколько таблиц в `INDEXES_ENABLING` одновременно:** допустимо; параллелизм
  ограничен пулом воркеров. Жёсткий кап — будущая опция.
- **Двойная обработка джоба:** предотвращается `SKIP LOCKED` при claim и
  guard'ом `worker_id` при complete/fail.

## Тестирование

Чистая SQL/логика — стиль `backend/tests/test_planner_queue.py`.

Фаза 1:
- CDC в `BULK_LOADING` **не** блокирует старт не-CDC, и наоборот.
- Две CDC-миграции **не** идут параллельно в bulk-фазах.
- CDC в `INDEXES_ENABLING` **не** блокирует старт следующей CDC.
- `queue_position` считается внутри своей полосы.

Фаза 2:
- `claim_index_enable_job` отдаёт ровно одну PENDING-строку; повторный claim
  чужим `worker_id` не завершает джоб (guard).
- `_handle_indexes_enabling` создаёт джоб при входе и переходит по `DONE`;
  `FAILED` оставляет фазу с `INDEXES_ENABLE_ERROR`.
- Портированная DDL-логика пропускает партиционированные индексы (M4-паритет).

## Затрагиваемые файлы

- `backend/services/orchestrator.py` — наборы фаз, lane-гейт, `_update_queue_positions`,
  `_handle_indexes_enabling`, `trigger_indexes_enabling`.
- `backend/db/state_db.py` — таблица `index_enable_jobs`, claim/complete/fail/stale-хелперы.
- `workers/common.py`, `workers/worker.py` — `index_enable_loop` + claim/complete/fail.
- `workers/oracle_ddl.py` (новый) — портированная DDL-логика включения объектов.
- `backend/tests/` — новые юнит-тесты.

Схема БД: +1 таблица (идемпотентно). UI: только корректные lane-позиции.
