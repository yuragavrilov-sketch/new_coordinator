# TRUNCATE target option — Design Spec

**Date:** 2026-05-13
**Status:** Approved (brainstorming)

## Problem

Текущее поведение по очистке target-таблицы перед загрузкой данных захардкожено и различается по стратегиям:

| Стратегия | Поведение |
|-----------|-----------|
| `CDC_STAGE`, `BULK_STAGE` | `TRUNCATE TABLE <target>` выполняется в `_handle_baseline_publishing` (orchestrator.py) автоматически, юзер не управляет. |
| `CDC_DIRECT`, `BULK_DIRECT` | TRUNCATE никогда не выполняется. INSERT идёт поверх существующих данных. При повторе миграции после фейла или на target с остатками — PK conflict. |

Результат: для DIRECT-стратегий нет встроенного способа гарантировать чистый старт без ручного `TRUNCATE TABLE` в SQL Developer. Это рутинная операция, которую логично вынести в payload миграции.

## Solution

Один boolean-флаг `truncate_target` (default = `TRUE`):

- Для **DIRECT**-стратегий (`CDC_DIRECT`, `BULK_DIRECT`) — управляет, делать ли `TRUNCATE TABLE <target>` в фазе `NEW` ПЕРЕД переходом в `CHUNKING` / `TOPIC_CREATING`.
- Для **STAGE**-стратегий (`CDC_STAGE`, `BULK_STAGE`) — флаг всегда `TRUE`, API отбивает попытку выставить `FALSE` ошибкой HTTP 400. TRUNCATE в STAGE-флоу выполняется как раньше, в `_handle_baseline_publishing`.

UI: чекбокс в `<StrategyPicker>` под "Дополнительно", рядом с radio STAGE/DIRECT. При STAGE — `disabled + checked`, при DIRECT — редактируемый.

## Goals

1. Добавить пер-миграционный флаг `truncate_target` в `migrations` (BOOLEAN, NOT NULL, DEFAULT TRUE).
2. В `_handle_new` оркестратора: для DIRECT-стратегий и `truncate_target=TRUE` выполнить `TRUNCATE TABLE <target>` перед транзитом в следующую фазу.
3. STAGE + `truncate_target=FALSE` запрещено на API-уровне (HTTP 400).
4. UI: чекбокс в advanced-секции `<StrategyPicker>`, заблокирован для STAGE.

## Non-Goals

- Не добавляем MERGE / UPSERT семантику (отдельная фича).
- Не вводим условный TRUNCATE «только если таблица не пуста» — всегда безусловный.
- Не вводим новую фазу `TARGET_TRUNCATING` — TRUNCATE делается inline в существующем `_handle_new`.
- Не меняем `_handle_baseline_publishing` — там TRUNCATE для STAGE остаётся как был.
- Не пишем автотесты — тестовой инфры в репозитории нет, верификация manual smoke.
- Не добавляем CASCADE-опции (если FK блокирует — юзер разруливает).
- Не добавляем «TRUNCATE с partition exchange» оптимизации.

## Database Schema

### Новая колонка

```sql
ALTER TABLE migrations
    ADD COLUMN IF NOT EXISTS truncate_target BOOLEAN NOT NULL DEFAULT TRUE;
```

Добавляется в существующий блок `for col_sql in [...]` внутри `backend/db/state_db.py::ensure_schema()` — там уже скапливаются такие idempotent ALTER'ы.

### Бэкфилл — не требуется

Default = TRUE покрывает все существующие строки. Это меняет историческое поведение DIRECT-миграций (раньше писали поверх существующего), но:

- DIRECT-миграции в терминальной фазе (`COMPLETED`, `FAILED`, `CANCELLED`) больше не активны.
- CDC_DIRECT в `STEADY_STATE` — TRUNCATE не пройдёт через них, потому что выполняется только при первом проходе `_handle_new`, который для них уже отработал.

## API Contract

Затрагиваются три эндпоинта создания миграции:

- `POST /api/migrations` (одиночная миграция)
- `POST /api/planner/execute-plan` (батч из планировщика)
- `POST /api/connector-groups/<group_id>/create-migration` (из формы группы)

### Payload-поле

```jsonc
{
  "strategy":         "BULK_DIRECT",
  "truncate_target":  true,        // optional, default = true
  // ...остальные поля без изменений
}
```

### Серверная валидация (в каждом из трёх роутов)

```python
truncate_target = bool(body.get("truncate_target", True))
if strategy.uses_stage and truncate_target is False:
    return jsonify({
        "error": "STAGE-стратегия требует TRUNCATE target (поведение неизменяемо). "
                 "Используйте DIRECT, если нужно сохранить существующие данные."
    }), 400
```

### Запись в БД

В трёх существующих `INSERT INTO migrations` добавляется колонка `truncate_target` со значением переменной `truncate_target`.

### Response

Поле `truncate_target` возвращается в GET-эндпоинтах (`/api/migrations/<id>`, `/api/schema-migrations/<id>/objects`) автоматически через `row_to_dict`. Изменений в shape ответа не требуется.

### Backward compatibility

Клиенты, не отправляющие `truncate_target`, получают default = TRUE.

**Намеренная смена поведения:** старые клиенты, создающие DIRECT-миграцию без явного `truncate_target=false`, после деплоя начнут получать TRUNCATE. Это упомянуть в release notes.

## Orchestrator — выполнение TRUNCATE

### Где

Внутри функции `_handle_new` (`backend/services/orchestrator.py`), в существующем daemon-потоке `_run`:

- ПОСЛЕ ветки создания stage-таблицы (`if strategy.uses_stage: ...`)
- ПЕРЕД транзитом в `TOPIC_CREATING` / `CHUNKING`
- Выполняется только для DIRECT (`not strategy.uses_stage`) и только если `m.get("truncate_target", True)`

### Псевдокод вставки

```python
            if strategy.uses_stage:
                # ── existing stage creation ──
                ts = m.get("stage_tablespace") or ""
                oracle_stage.create_stage_table(...)
                stage_msg = "Stage table создана"
            else:
                stage_msg = "Прямая загрузка (без stage)"

            # ── NEW: TRUNCATE target для DIRECT ──
            if not strategy.uses_stage and m.get("truncate_target", True):
                tgt_quoted = f'"{m["target_schema"].upper()}"."{m["target_table"].upper()}"'
                conn = oracle_scn.open_oracle_conn(dst_cfg)
                try:
                    with conn.cursor() as cur:
                        cur.execute(f"TRUNCATE TABLE {tgt_quoted}")
                    conn.commit()
                    print(f"[orchestrator] {mid}: truncated {tgt_quoted}")
                    stage_msg += ", target очищен"
                finally:
                    conn.close()

            # ── existing transition ──
            if not strategy.has_cdc:
                _safe_transition(mid, "NEW", "CHUNKING", ...)
                _create_chunks_and_transition(mid, m)
            else:
                _safe_transition(mid, "NEW", "TOPIC_CREATING", ...)
```

### Корректность для CDC_DIRECT

Для `CDC_DIRECT` TRUNCATE происходит ДО `TOPIC_CREATING` → ДО создания первого Kafka-сообщения для этой таблицы внутри группы. Коннектор группы (на уровне Debezium) фиксирует SCN на старте группы; события до этого SCN не апплятся. Так что TRUNCATE не противоречит CDC-флоу.

### Failure modes

Любая ошибка TRUNCATE бросает исключение из `_run` → перехватывается общим `except Exception` блоком `_run` → `_fail(mid, str(exc), "PREPARING_ERROR")`. Конкретные ожидаемые сценарии:

| Ошибка Oracle | Причина | Что делать пользователю |
|---------------|---------|-------------------------|
| ORA-00054 | Target locked (другой сессией) | Снять блокировку, пересоздать миграцию. |
| ORA-01031 | Insufficient privileges | Выдать DELETE/DROP ANY TABLE миграционному пользователю. |
| ORA-02266 | FK с дочерними строками блокирует TRUNCATE | Отключить дочерние FK или использовать STAGE-стратегию. |
| ORA-00942 | Target table не существует | Создать целевую таблицу через TargetPrep. |
| ORA-* прочее | разное | См. `error_text` в детале миграции. |

## Frontend

### Тип `Migration` (`frontend/src/types/migration.ts`)

```diff
 export interface Migration {
   // ...
+  truncate_target: boolean;
 }
```

То же добавляется в `MigrationSummary` если он используется в местах, где это поле полезно (не обязательно — стартуем без).

### `<StrategyPicker>` — расширение props

```diff
 interface Props {
   value: Strategy;
   onChange: (s: Strategy) => void;
+  truncateTarget: boolean;
+  onTruncateChange: (b: boolean) => void;
   cdcDisabledReason?: string;
 }
```

В advanced-секции (`{advancedOpen && (...)}`) ПОД radio STAGE/DIRECT — новый блок:

```tsx
<div style={{ paddingTop: 8, display: "flex", flexDirection: "column", gap: 4 }}>
  <label style={{ fontWeight: 500, fontSize: 13 }}>
    <input
      type="checkbox"
      checked={usesStage(value) ? true : truncateTarget}
      disabled={usesStage(value)}
      onChange={(e) => onTruncateChange(e.target.checked)}
    />
    {" "}Очистить target перед загрузкой (TRUNCATE TABLE)
  </label>
  <div style={{ fontSize: 12, color: t.text.muted }}>
    {usesStage(value)
      ? "Всегда ON для STAGE — таблица очищается перед публикацией baseline."
      : "Если выключено — данные дописываются поверх существующего (возможны PK-конфликты)."}
  </div>
</div>
```

### Подключение в формы

**`CreateMigrationModal/types.ts`:**
```diff
 interface FormData {
   // ...
+  truncate_target: boolean;
 }
```

**`CreateMigrationModal/helpers.ts`** (default):
```diff
 export const INITIAL_FORM_DATA: FormData = {
   // ...
+  truncate_target: true,
 }
```

**`CreateMigrationModal/index.tsx`** — передать в `<StrategyPicker>`:
```diff
 <StrategyPicker
   value={form.strategy}
   onChange={(s) => setF({ strategy: s })}
+  truncateTarget={form.truncate_target}
+  onTruncateChange={(b) => setF({ truncate_target: b })}
 />
```

И в payload submit:
```diff
 const payload = {
   strategy: form.strategy,
+  truncate_target: form.truncate_target,
   // ...
 };
```

То же для:
- `frontend/src/components/DDLCatalog/PlannerWizard/types.ts` + `index.tsx` + `steps/TableSelectionStep.tsx` (включая per-table override).
- `frontend/src/components/ConnectorGroupsPanel/{types.ts,helpers.ts,MigrateModal.tsx}`.

### Defensive guard

Перед POST в каждой форме:
```ts
if (usesStage(form.strategy)) {
  form.truncate_target = true;
}
```
На случай если UI чекбокс и состояние рассинхронились.

### Отображение в детали (опционально)

В `MigrationDetail/tabs/OverviewTab.tsx` добавить:
```tsx
<InfoRow label="TRUNCATE target" value={detail.truncate_target ? "да" : "нет"} />
```
для дебагинга. В список (`MigrationRow`) не добавляем — деталь стратегии.

## Files to Modify

| File | Изменение |
|------|-----------|
| `backend/db/state_db.py` | Добавить ALTER в существующий блок idempotent column-bringup'a. |
| `backend/routes/migrations.py` | В `create_migration`: чтение `truncate_target` из body, валидация (STAGE+false → 400), добавление в INSERT. |
| `backend/routes/planner.py` | В `execute_plan`: per-table `truncate_target` (с глобальным default'ом), валидация, добавление в INSERT. |
| `backend/routes/connector_groups.py` | В `create_migration_from_table`: то же. |
| `backend/services/orchestrator.py` | В `_handle_new._run`: блок TRUNCATE после stage-блока, перед транзитом. |
| `frontend/src/types/migration.ts` | `Migration.truncate_target: boolean`. |
| `frontend/src/components/StrategyPicker/index.tsx` | Props `truncateTarget`/`onTruncateChange`, чекбокс в advanced-секции. |
| `frontend/src/components/CreateMigrationModal/{types,helpers,index}.{ts,tsx}` | Field + default + проброс в `<StrategyPicker>` + payload. |
| `frontend/src/components/DDLCatalog/PlannerWizard/{types,index,steps/TableSelectionStep}.{ts,tsx}` | То же + per-table override. |
| `frontend/src/components/ConnectorGroupsPanel/{types,helpers,MigrateModal}.{ts,tsx}` | То же. |
| `frontend/src/components/MigrationDetail/tabs/OverviewTab.tsx` | `<InfoRow label="TRUNCATE target" ...>` (опционально). |

## Testing

### Manual smoke (после деплоя)

1. `BULK_DIRECT + truncate_target=true` → target очищается, новые данные.
2. `BULK_DIRECT + truncate_target=false` → дописывается в существующее (или фейлится на PK).
3. `CDC_DIRECT + truncate_target=true` → target очищается, потом chunks + CDC apply, STEADY_STATE.
4. `BULK_STAGE + truncate_target=false` → HTTP 400.
5. `CDC_STAGE` (любое значение truncate_target) → проходит как обычно. TRUNCATE случается в `_handle_baseline_publishing`, не в `_handle_new`.
6. Permissions test: отозвать у юзера `DELETE` на target → DIRECT-миграция с TRUNCATE падает с `PREPARING_ERROR`, error_text содержит ORA-01031.

### Curl negative-case

```bash
curl -X POST localhost:5000/api/migrations -H 'Content-Type: application/json' -d '{
  "migration_name": "test",
  "strategy": "BULK_STAGE",
  "truncate_target": false,
  "group_id": "...",
  "source_schema": "S", "source_table": "T",
  "target_schema": "S", "target_table": "T"
}'
# Expected: HTTP 400, error: "STAGE-стратегия требует TRUNCATE target..."
```

## Rollout

1. Один PR, один деплой. Backward-compat не нужна — фронт+бэк деплоятся вместе.
2. `ensure_schema` при старте бэка добавляет колонку (idempotent).
3. Release note: «Поведение DIRECT-миграций изменилось — теперь target по умолчанию TRUNCATE-ится перед загрузкой. Используйте `truncate_target=false` в payload для прежнего append-поведения».
