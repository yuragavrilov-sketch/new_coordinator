# Two Migration Strategies — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Свернуть матрицу 8 вариантов миграции (Legacy/Group × CDC/BULK_ONLY × STAGE/DIRECT) в 2 user-facing стратегии («С CDC» / «Без CDC») с advanced toggle STAGE/DIRECT. Полностью удалить Legacy-машину состояний.

**Architecture:** Group-машина состояний становится единственным путём исполнения. В БД один enum-столбец `strategy` (`CDC_STAGE`/`CDC_DIRECT`/`BULK_STAGE`/`BULK_DIRECT`) заменяет `migration_mode` + `migration_strategy`. На фронте — компонент `<StrategyPicker>` (радиогруп + collapsible advanced toggle) во всех формах создания миграции. Legacy-фазы (`PREPARING`, `SCN_FIXED`, `CONNECTOR_STARTING`, `CDC_BUFFERING`, `CDC_APPLY_STARTING`) и связанный код удаляются полностью.

**Tech Stack:** Python 3 (Flask, psycopg), PostgreSQL (state DB), TypeScript/React (Vite). Тестовой инфраструктуры в репозитории нет — верификация через grep, type-check, SQL и manual smoke.

**Spec:** `docs/superpowers/specs/2026-05-13-two-strategies-design.md`

---

## File Structure

### Создаются
- `backend/services/strategy.py` — enum `Strategy` + properties `has_cdc` / `uses_stage`.
- `frontend/src/components/StrategyPicker/index.tsx` — унифицированный picker (радиогруп + advanced toggle).
- `frontend/src/components/StrategyPicker/strategyLabel.ts` — TS-хелперы (`hasCdc`, `usesStage`, `composeStrategy`, `strategyLabel`).

### Модифицируются
- `backend/db/state_db.py` — миграция БД в `ensure_schema`; обновление `_ACTIVE_PHASES` в `get_active_migrations`.
- `backend/services/orchestrator.py` — удаление Legacy handler'ов, переименование Group handler'ов, диспетчеризация через `Strategy`.
- `backend/services/schema_migrations.py` — удаление Legacy-фаз из `_PHASE_TO_STAGE` и `_ACTIVE_PHASES`.
- `backend/services/debezium.py` — удаление `create_connector(migration, oracle_cfg)`.
- `backend/services/oracle_scn.py` — удаление `get_current_scn`.
- `backend/routes/migrations.py::create_migration` — приём `strategy`, валидация, обязательный `group_id`, синхронная проверка RUNNING-коннектора.
- `backend/routes/planner.py::execute_plan` — `strategy` per-table.
- `backend/routes/connector_groups.py::create_migration_from_table` — `strategy`.
- `frontend/src/types/migration.ts` — удаление Legacy-фаз; экспорт типа `Strategy`.
- `frontend/src/components/CreateMigrationModal/{index.tsx,helpers.ts,types.ts}` — `<StrategyPicker>` + новый payload.
- `frontend/src/components/DDLCatalog/PlannerWizard/{index.tsx,types.ts,steps/TableSelectionStep.tsx}` — то же.
- `frontend/src/components/ConnectorGroupsPanel/{MigrateModal.tsx,types.ts,helpers.ts,index.tsx}` — то же + display.
- `frontend/src/components/MigrationList/MigrationRow.tsx` — `strategyLabel` вместо `migration_mode`.
- `frontend/src/components/MigrationDetail/{tabs/OverviewTab.tsx,helpers.ts}` — `strategyLabel`, `isCdcMode` через новый enum.

### Не трогаем
- `backend/services/connector_groups.py`, `backend/services/job_queue.py`, `backend/services/validator.py`, `backend/services/oracle_baseline.py`, `backend/services/oracle_chunker.py`, `backend/services/oracle_stage.py`, `backend/services/kafka_*.py` — стратегия туда не просачивается.
- `backend/db/oracle_browser.py` — общие Oracle-утилиты.
- `migration_chunks`, `schema_migrations`, `migration_plan_items`, `connector_groups` таблицы.

---

## Task 1: `Strategy` enum в Python

**Files:**
- Create: `backend/services/strategy.py`

- [ ] **Step 1: Создать файл с enum'ом и hairy-проверкой свойств**

`backend/services/strategy.py`:
```python
"""Migration strategy enum — one field replaces (migration_mode, migration_strategy).

CDC_*  → миграция продолжает реплицировать source через CDC после bulk-load.
BULK_* → один разовый перенос данных без CDC, завершается после DATA_VERIFYING.
*_STAGE  → грузим через промежуточную stage-таблицу (валидация + TRUNCATE + baseline).
*_DIRECT → грузим сразу в target (быстрее, без валидации/baseline).
"""

from enum import StrEnum


class Strategy(StrEnum):
    CDC_STAGE   = "CDC_STAGE"
    CDC_DIRECT  = "CDC_DIRECT"
    BULK_STAGE  = "BULK_STAGE"
    BULK_DIRECT = "BULK_DIRECT"

    @property
    def has_cdc(self) -> bool:
        return self.value.startswith("CDC_")

    @property
    def uses_stage(self) -> bool:
        return self.value.endswith("_STAGE")

    @classmethod
    def parse(cls, raw: str | None) -> "Strategy":
        """Strict parser: raises ValueError on unknown/empty value.

        Используется в API-валидации и при чтении из БД (где CHECK
        constraint гарантирует одно из 4 значений, но мы всё равно
        не хотим silent fallback).
        """
        if not raw:
            raise ValueError("strategy is required")
        return cls(raw.strip().upper())
```

- [ ] **Step 2: Verify импорт и таблица свойств**

Run:
```bash
cd /home/coder/project/coordinator/new_coordinator/backend && python3 -c "
from services.strategy import Strategy
for s in Strategy:
    print(f'{s.value:12} has_cdc={s.has_cdc} uses_stage={s.uses_stage}')
# Сверь с ожидаемой таблицей:
# CDC_STAGE    has_cdc=True  uses_stage=True
# CDC_DIRECT   has_cdc=True  uses_stage=False
# BULK_STAGE   has_cdc=False uses_stage=True
# BULK_DIRECT  has_cdc=False uses_stage=False
assert Strategy.parse('cdc_stage') is Strategy.CDC_STAGE
try: Strategy.parse('UNKNOWN'); raise SystemExit('FAIL: unknown accepted')
except ValueError: pass
print('OK')
"
```
Expected: 4 строки + `OK` в конце.

- [ ] **Step 3: Commit**

```bash
git add backend/services/strategy.py
git commit -m "feat(strategy): add Strategy enum (CDC/BULK × STAGE/DIRECT)"
```

---

## Task 2: Миграция БД — добавить `strategy`, бэкфилл

**Files:**
- Modify: `backend/db/state_db.py:241-265` (блок «Column migrations on migrations table»)

- [ ] **Step 1: Вставить добавление колонки и backfill после существующего блока ALTER'ов**

В `ensure_schema()`, сразу после цикла `for col_sql in [...]` (после строки `print(f"[state_db]   column ok: migrations.{col_name}")`), вставить:

```python
            # ── New `strategy` enum column (replaces migration_mode + migration_strategy) ──
            cur.execute("""
                ALTER TABLE migrations
                    ADD COLUMN IF NOT EXISTS strategy TEXT
                        NOT NULL DEFAULT 'CDC_STAGE'
            """)
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.check_constraints
                        WHERE constraint_name = 'migrations_strategy_check'
                    ) THEN
                        ALTER TABLE migrations
                            ADD CONSTRAINT migrations_strategy_check
                            CHECK (strategy IN ('CDC_STAGE','CDC_DIRECT','BULK_STAGE','BULK_DIRECT'));
                    END IF;
                END$$
            """)
            print("[state_db]   column ok: migrations.strategy")

            # Backfill from legacy columns (only if they still exist)
            cur.execute("""
                DO $$
                BEGIN
                    IF EXISTS (SELECT 1 FROM information_schema.columns
                               WHERE table_name='migrations' AND column_name='migration_mode')
                    THEN
                        UPDATE migrations SET strategy =
                            CASE
                                WHEN migration_mode = 'BULK_ONLY' AND migration_strategy = 'DIRECT' THEN 'BULK_DIRECT'
                                WHEN migration_mode = 'BULK_ONLY'                                    THEN 'BULK_STAGE'
                                WHEN migration_strategy = 'DIRECT'                                   THEN 'CDC_DIRECT'
                                ELSE 'CDC_STAGE'
                            END
                        WHERE TRUE;
                    END IF;
                END$$
            """)
            print("[state_db]   strategy backfilled")
```

- [ ] **Step 2: Verify запуск миграции на dev-БД**

Останови бэк, если он запущен. Запусти один раз `python3 backend/app.py` до сообщения `[state_db]` и можешь нажать Ctrl-C. Затем проверь схему:

```bash
psql -h localhost -U coordinator -d coordinator -c "\d migrations" | grep -E "strategy|migration_mode|migration_strategy"
```
Expected: видишь `strategy text not null default 'CDC_STAGE'`, плюс пока ещё `migration_mode` и `migration_strategy` (их сносим в Task 13).

```bash
psql -h localhost -U coordinator -d coordinator -c "
SELECT strategy, migration_mode, migration_strategy, COUNT(*)
FROM migrations
GROUP BY 1,2,3 ORDER BY 1;"
```
Expected: каждая строка `strategy` соответствует ожидаемой комбинации старых полей. Если БД пустая — `(0 rows)`, всё равно ок.

- [ ] **Step 3: Commit**

```bash
git add backend/db/state_db.py
git commit -m "feat(db): add migrations.strategy column with backfill from legacy fields"
```

---

## Task 3: Использовать `Strategy` при создании миграции в API

**Files:**
- Modify: `backend/routes/migrations.py:104-227` (функция `create_migration`)

- [ ] **Step 1: Заменить чтение `migration_mode`/`migration_strategy` на `strategy` + валидация**

Заменить блок `backend/routes/migrations.py:122-152` целиком на:

```python
                # ── Strategy: single enum field replaces mode + strategy ──
                try:
                    strategy = Strategy.parse(body.get("strategy"))
                except ValueError as exc:
                    return jsonify({"error": f"Invalid strategy: {exc}"}), 400

                # group_id is now MANDATORY (no more Legacy per-migration connector)
                group_id = body.get("group_id") or None
                if not group_id:
                    return jsonify({"error": "group_id is required (Legacy per-migration connector is no longer supported)"}), 400

                connector_name = ""
                topic_prefix = ""
                consumer_group = ""

                if strategy.has_cdc:
                    from services.connector_groups import get_group as _get_group, _active_topic_prefix
                    group = _get_group(group_id)
                    if not group:
                        return jsonify({"error": f"Группа {group_id} не найдена"}), 404
                    if group.get("status") != "RUNNING":
                        return jsonify({"error": (
                            f"Коннектор группы не запущен (status={group.get('status')}). "
                            "Запустите коннектор группы перед созданием CDC-миграции."
                        )}), 409
                    connector_name = group["connector_name"]
                    topic_prefix = _active_topic_prefix(group)
                    src_schema = body.get("source_schema", "").upper()
                    src_table = body.get("source_table", "").upper()
                    prefix = group.get("consumer_group_prefix") or group["topic_prefix"]
                    consumer_group = f"{prefix}_{src_schema}_{src_table}"
```

Импорт в шапке файла (если ещё нет):
```python
from services.strategy import Strategy
```

- [ ] **Step 2: Заменить INSERT — поле `strategy` вместо двух старых**

В том же `create_migration`, в INSERT (`backend/routes/migrations.py:154-203`):

Заменить кусок колонок:
```
                        migration_strategy, migration_mode,
```
на
```
                        strategy,
```

Заменить кусок плейсхолдеров (`%s, %s,` на той же строке) на один `%s,`.

Заменить кусок значений:
```python
                    strategy, mode,
```
на
```python
                    strategy.value,
```

(переменной `mode` больше нет; `strategy` — это `Strategy`-enum-объект.)

- [ ] **Step 3: Поправить пост-INSERT логику обновления коннектора**

`backend/routes/migrations.py:213-219` (рефреш коннектор-tables): заменить условие
```python
        if group_id and mode == "CDC":
```
на
```python
        if strategy.has_cdc:
```

- [ ] **Step 4: Verify типы и грубое поведение через curl**

Type-check (нет mypy в репозитории, поэтому просто синтаксис):
```bash
python3 -c "import ast; ast.parse(open('backend/routes/migrations.py').read()); print('syntax ok')"
```

Запусти бэк, проверь корректное отклонение:
```bash
curl -s -X POST http://localhost:5000/api/migrations \
  -H 'Content-Type: application/json' \
  -d '{"migration_name":"test","source_schema":"S","source_table":"T","target_schema":"S2","target_table":"T2"}'
# Expected: {"error":"Invalid strategy: strategy is required"}
```
```bash
curl -s -X POST http://localhost:5000/api/migrations \
  -H 'Content-Type: application/json' \
  -d '{"migration_name":"test","strategy":"CDC_STAGE","source_schema":"S","source_table":"T","target_schema":"S2","target_table":"T2"}'
# Expected: {"error":"group_id is required ..."}
```

- [ ] **Step 5: Commit**

```bash
git add backend/routes/migrations.py
git commit -m "feat(api): /api/migrations принимает strategy, group_id обязателен"
```

---

## Task 4: `strategy` per-table в planner и connector-groups роутах

**Files:**
- Modify: `backend/routes/planner.py:237-380` (функция `execute_plan`)
- Modify: `backend/routes/connector_groups.py:302-460` (функция `create_migration_from_table`)

- [ ] **Step 1: planner.py — переписать чтение `strategy` per-row и INSERT**

В `execute_plan` (`backend/routes/planner.py:237-380`):

Найти строку 296:
```python
                    strategy = overrides.get("migration_strategy", defaults.get("migration_strategy", "STAGE"))
```
Заменить весь блок чтения `mode`/`strategy` per-table на:
```python
                    raw = overrides.get("strategy") or defaults.get("strategy") or "CDC_STAGE"
                    try:
                        strategy = Strategy.parse(raw)
                    except ValueError as exc:
                        return jsonify({"error": f"Invalid strategy for {table}: {exc}"}), 400
```

В INSERT (~строка 309) заменить:
```
                            migration_strategy, migration_mode,
```
на
```
                            strategy,
```
и в values — одно поле `strategy.value` вместо двух старых переменных.

Импорт в шапке:
```python
from services.strategy import Strategy
```

- [ ] **Step 2: connector_groups.py — то же в `create_migration_from_table`**

В `backend/routes/connector_groups.py:302-460`:

Заменить блок 372-380 (чтение mode + strategy):
```python
    migration_mode = body.get("migration_mode", "CDC").upper()
    if migration_mode not in ("CDC", "BULK_ONLY"):
        migration_mode = "CDC"
    migration_strategy = body.get("migration_strategy", "STAGE").upper()
    if migration_strategy not in ("STAGE", "DIRECT"):
        migration_strategy = "STAGE"

    stage_name = f"STG_{src_schema}_{src_table}" if migration_strategy == "STAGE" else ""
    stage_tablespace = body.get("stage_tablespace", "PAYSTAGE") if migration_strategy == "STAGE" else ""
```
на:
```python
    try:
        strategy = Strategy.parse(body.get("strategy"))
    except ValueError as exc:
        return jsonify({"error": f"Invalid strategy: {exc}"}), 400

    stage_name       = f"STG_{src_schema}_{src_table}" if strategy.uses_stage else ""
    stage_tablespace = body.get("stage_tablespace", "PAYSTAGE") if strategy.uses_stage else ""
```

В обоих INSERT-блоках (строки 404 и 433) заменить колонки `migration_strategy, migration_mode,` на `strategy,` и значения `migration_strategy, migration_mode,` на `strategy.value,`.

Условие на 450:
```python
    if migration_mode == "CDC":
```
заменить на:
```python
    if strategy.has_cdc:
```

Импорт:
```python
from services.strategy import Strategy
```

- [ ] **Step 3: Verify**

```bash
python3 -c "import ast
for f in ('backend/routes/planner.py','backend/routes/connector_groups.py'):
    ast.parse(open(f).read())
print('syntax ok')"
```

Проверь что нигде в `routes/` не осталось живых ссылок на старые поля:
```bash
grep -n "migration_mode\|migration_strategy" backend/routes/
# Expected: пусто
```

- [ ] **Step 4: Commit**

```bash
git add backend/routes/planner.py backend/routes/connector_groups.py
git commit -m "feat(api): planner+groups routes используют strategy"
```

---

## Task 5: Удалить Legacy handler'ы и переименовать Group handler'ы

**Files:**
- Modify: `backend/services/orchestrator.py:129-186` (диспетчеризация), `:140-163` (`_LEGACY_HANDLERS`), `:166-186` (`_GROUP_HANDLERS`)
- Modify: `backend/services/orchestrator.py:328-385` (`_handle_new` Legacy)
- Modify: `backend/services/orchestrator.py:388-446` (`_handle_preparing`)
- Modify: `backend/services/orchestrator.py:448-467` (`_handle_scn_fixed`)
- Modify: `backend/services/orchestrator.py:469-492` (`_handle_connector_starting`)
- Modify: `backend/services/orchestrator.py:570-576` (`_handle_cdc_buffering`)
- Modify: `backend/services/orchestrator.py:1130-1147` (`_handle_cdc_apply_starting`)
- Modify: `backend/services/orchestrator.py:1219-1344` (`_handle_new_group`)
- Modify: `backend/services/orchestrator.py:1390-1471` (`_handle_indexes_enabling_group`)

- [ ] **Step 1: Удалить Legacy handler'ы**

Удали целиком функции:
- `_handle_new(mid, m)` (`backend/services/orchestrator.py:328-385`) — старая Legacy-версия. Внимание: будет переименование `_handle_new_group` → `_handle_new` в Step 4 этой же таски; пока удаляешь только Legacy, обе функции временно живы (`_handle_new_group` тоже не трогаем здесь).
- `_handle_preparing` (`:388-446`)
- `_handle_scn_fixed` (`:448-467`)
- `_handle_connector_starting` (`:469-492`)
- `_handle_cdc_buffering` (`:570-576`)
- `_handle_cdc_apply_starting` (`:1130-1147`)

- [ ] **Step 2: Удалить словарь `_LEGACY_HANDLERS`**

`backend/services/orchestrator.py:140-163` — удалить весь блок `_LEGACY_HANDLERS = { ... }`.

- [ ] **Step 3: Упростить `_dispatch` (без ветки по `group_id`)**

`backend/services/orchestrator.py:129-136`:

Заменить:
```python
def _dispatch(migration_id: str, phase: str, m: dict) -> None:
    # Group-based migrations use a simplified phase machine
    if m.get("group_id"):
        handler = _GROUP_HANDLERS.get(phase)
    else:
        handler = _LEGACY_HANDLERS.get(phase)
    if handler:
        handler(migration_id, m)
```
на:
```python
def _dispatch(migration_id: str, phase: str, m: dict) -> None:
    handler = _PHASE_HANDLERS.get(phase)
    if handler:
        handler(migration_id, m)
```

- [ ] **Step 4: Переименовать `_GROUP_HANDLERS` → `_PHASE_HANDLERS` и вписанные ссылки**

В блоке `backend/services/orchestrator.py:166-186`:
- Заменить `_GROUP_HANDLERS = {` → `_PHASE_HANDLERS = {`
- Заменить `_handle_new_group` → `_handle_new` (в lambda-значении словаря и в самой функции `def _handle_new_group(...)` на `:1219`)
- Заменить `_handle_indexes_enabling_group` → `_handle_indexes_enabling` (в lambda и в `def` на `:1390`)

- [ ] **Step 5: Удалить из `_HEAVY_PHASES` Legacy-фазы**

`backend/services/orchestrator.py:44-53`:

Удалить из set'a строки `"PREPARING", "SCN_FIXED",` и `"CONNECTOR_STARTING", "CDC_BUFFERING",`.

Итоговый `_HEAVY_PHASES`:
```python
_HEAVY_PHASES = frozenset({
    "TOPIC_CREATING",
    "CHUNKING",
    "BULK_LOADING", "BULK_LOADED",
    "STAGE_VALIDATING", "STAGE_VALIDATED",
    "BASELINE_PUBLISHING", "BASELINE_LOADING", "BASELINE_PUBLISHED",
    "STAGE_DROPPING",
})
```

- [ ] **Step 6: Verify, что Legacy-ссылок не осталось**

```bash
cd backend && grep -nE "_LEGACY_HANDLERS|_handle_new_group|_handle_indexes_enabling_group|_handle_preparing|_handle_scn_fixed|_handle_connector_starting|_handle_cdc_buffering|_handle_cdc_apply_starting" services/orchestrator.py
# Expected: пусто
python3 -c "import ast; ast.parse(open('services/orchestrator.py').read()); print('syntax ok')"
# Expected: syntax ok
```

- [ ] **Step 7: Commit**

```bash
git add backend/services/orchestrator.py
git commit -m "refactor(orchestrator): drop Legacy handlers, unify dispatcher"
```

---

## Task 6: Диспетчеризация в orchestrator через `Strategy`

**Files:**
- Modify: `backend/services/orchestrator.py` — `_handle_new` (бывший `_handle_new_group`), `_handle_bulk_loaded`, `_handle_indexes_enabling` (бывший `_handle_indexes_enabling_group`)

- [ ] **Step 1: Импорт Strategy**

В шапке `backend/services/orchestrator.py` (там, где импорты services):
```python
from services.strategy import Strategy
```

- [ ] **Step 2: `_handle_new` — читать `strategy` из row, валидировать ключи**

В функции `_handle_new` (бывший `_handle_new_group`, ~строка 1219):

Заменить блок чтения mode и проверки ключей:
```python
    mode = (m.get("migration_mode") or "CDC").upper()
    pk = m.get("source_pk_exists", False)
    uk = m.get("source_uk_exists", False)
    key_cols = json.loads(m.get("effective_key_columns_json") or "[]")

    if mode != "BULK_ONLY" and not pk and not uk and not key_cols:
        _fail(mid,
              "Таблица не имеет PK/UK и ключевые колонки не заданы.",
              "NO_KEY_COLUMNS")
        return
```
на:
```python
    try:
        strategy = Strategy.parse(m.get("strategy"))
    except ValueError:
        _fail(mid, f"Неизвестная стратегия: {m.get('strategy')!r}", "UNKNOWN_STRATEGY")
        return

    pk = m.get("source_pk_exists", False)
    uk = m.get("source_uk_exists", False)
    key_cols = json.loads(m.get("effective_key_columns_json") or "[]")

    if strategy.has_cdc and not pk and not uk and not key_cols:
        _fail(mid,
              "Таблица не имеет PK/UK и ключевые колонки не заданы.",
              "NO_KEY_COLUMNS")
        return
```

Дальше в той же функции — проверка group connector RUNNING (~строка 1275):
```python
    mode = (m.get("migration_mode") or "CDC").upper()
    if mode != "BULK_ONLY":
        group = connector_groups_svc.get_group(m["group_id"])
        ...
```
заменить условие на:
```python
    if strategy.has_cdc:
        group = connector_groups_svc.get_group(m["group_id"])
        ...
```
(внутри блока — без изменений).

Дальше внутри `_run` (~строка 1296-1340):

```python
            strategy_local = (m.get("migration_strategy") or "STAGE").upper()
            ...
            if mode != "BULK_ONLY":
                ...check_supplemental_logging...

            if strategy_local == "STAGE":
                ...oracle_stage.create_stage_table...
```
заменить на:
```python
            if strategy.has_cdc:
                try:
                    has_supp = oracle_scn.check_supplemental_logging(
                        src_cfg, m["source_schema"], m["source_table"]
                    )
                    if not has_supp:
                        print(
                            f"[orchestrator] WARNING: {m['source_schema']}.{m['source_table']} "
                            "does not have ALL COLUMNS supplemental logging."
                        )
                except Exception as exc:
                    print(f"[orchestrator] supplemental logging check failed: {exc}")

            if strategy.uses_stage:
                ts = m.get("stage_tablespace") or ""
                oracle_stage.create_stage_table(
                    src_cfg, dst_cfg,
                    m["source_schema"], m["source_table"],
                    m["target_schema"], m["stage_table_name"],
                    tablespace=ts,
                )
                stage_msg = "Stage table создана"
            else:
                stage_msg = "Прямая загрузка (без stage)"
```

И транзишен:
```python
            if mode == "BULK_ONLY":
                _safe_transition(mid, "NEW", "CHUNKING", ...)
                _unmark_in_prog(mid)
                _create_chunks_and_transition(mid, m)
                return
            else:
                _safe_transition(mid, "NEW", "TOPIC_CREATING", ...)
```
заменить на:
```python
            if not strategy.has_cdc:
                _safe_transition(mid, "NEW", "CHUNKING",
                                 message=f"{stage_msg}, без CDC → нарезка чанков")
                _unmark_in_prog(mid)
                _create_chunks_and_transition(mid, m)
                return
            else:
                _safe_transition(mid, "NEW", "TOPIC_CREATING",
                                 message=f"{stage_msg}, создание топика Kafka")
```

- [ ] **Step 3: `_handle_bulk_loaded` — ветвление по `strategy.uses_stage`**

`backend/services/orchestrator.py:619-626`:

Заменить:
```python
def _handle_bulk_loaded(mid: str, m: dict) -> None:
    strategy = (m.get("migration_strategy") or "STAGE").upper()
    if strategy == "DIRECT":
        _transition(mid, "INDEXES_ENABLING",
                    message="DIRECT стратегия: данные загружены напрямую, включение индексов")
    else:
        _transition(mid, "STAGE_VALIDATING")
```
на:
```python
def _handle_bulk_loaded(mid: str, m: dict) -> None:
    try:
        strategy = Strategy.parse(m.get("strategy"))
    except ValueError:
        _fail(mid, f"Неизвестная стратегия: {m.get('strategy')!r}", "UNKNOWN_STRATEGY")
        return
    if not strategy.uses_stage:
        _transition(mid, "INDEXES_ENABLING",
                    message="DIRECT: данные загружены напрямую, включение индексов")
    else:
        _transition(mid, "STAGE_VALIDATING")
```

- [ ] **Step 4: `_handle_indexes_enabling` — финальная ветка по `strategy.has_cdc`**

В `_handle_indexes_enabling` (бывший `_handle_indexes_enabling_group`, ~строка 1390):

Внутри `_run`, после успешного rebuild'a, текущий код:
```python
            mode = (m.get("migration_mode") or "CDC").upper()
            if mode == "BULK_ONLY":
                try: oracle_browser.enable_triggers(...)
                ...
                _safe_transition(mid, "INDEXES_ENABLING", "DATA_VERIFYING", ...)
            else:
                _safe_transition(mid, "INDEXES_ENABLING", "CDC_APPLYING", ...)
```
заменить на:
```python
            try:
                strategy = Strategy.parse(m.get("strategy"))
            except ValueError:
                _fail(mid, f"Неизвестная стратегия: {m.get('strategy')!r}", "UNKNOWN_STRATEGY")
                return

            if not strategy.has_cdc:
                try:
                    oracle_browser.enable_triggers(
                        oracle_scn.open_oracle_conn(dst_cfg),
                        m["target_schema"], m["target_table"],
                    )
                except Exception as exc:
                    print(f"[orchestrator] {mid}: enable triggers warning: {exc}")

                _safe_transition(
                    mid, "INDEXES_ENABLING", "DATA_VERIFYING",
                    message=(
                        f"Включено: индексов={n_idx}, констрейнтов={n_con}. "
                        "Без CDC — запуск сверки данных"
                    ),
                    extra_fields={"error_code": None, "error_text": None},
                )
            else:
                _safe_transition(
                    mid, "INDEXES_ENABLING", "CDC_APPLYING",
                    message=(
                        f"Включено: индексов={n_idx}, констрейнтов={n_con}. "
                        "Ожидание CDC apply-worker"
                    ),
                    extra_fields={"error_code": None, "error_text": None},
                )
```

- [ ] **Step 5: Verify — нет ссылок на старые поля в orchestrator**

```bash
grep -nE "migration_mode|migration_strategy" backend/services/orchestrator.py
# Expected: пусто
python3 -c "import ast; ast.parse(open('backend/services/orchestrator.py').read()); print('syntax ok')"
```

- [ ] **Step 6: Commit**

```bash
git add backend/services/orchestrator.py
git commit -m "refactor(orchestrator): диспетчеризация через Strategy enum"
```

---

## Task 7: Удалить Legacy-фазы из `state_db._ACTIVE_PHASES`

**Files:**
- Modify: `backend/db/state_db.py:699-720` (функция `get_active_migrations`)

- [ ] **Step 1: Удалить 5 фаз из tuple**

Заменить `backend/db/state_db.py:701-713`:
```python
    _ACTIVE_PHASES = (
        "NEW", "PREPARING", "SCN_FIXED",
        "CONNECTOR_STARTING", "CDC_BUFFERING",
        "TOPIC_CREATING",
        "CHUNKING", "BULK_LOADING", "BULK_LOADED",
        "STAGE_VALIDATING", "STAGE_VALIDATED",
        "BASELINE_PUBLISHING", "BASELINE_LOADING", "BASELINE_PUBLISHED",
        "STAGE_DROPPING", "INDEXES_ENABLING",
        "DATA_VERIFYING", "DATA_MISMATCH",
        "CDC_APPLY_STARTING", "CDC_APPLYING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP",
        "STEADY_STATE",
        "CANCELLING",
    )
```
на:
```python
    _ACTIVE_PHASES = (
        "NEW",
        "TOPIC_CREATING",
        "CHUNKING", "BULK_LOADING", "BULK_LOADED",
        "STAGE_VALIDATING", "STAGE_VALIDATED",
        "BASELINE_PUBLISHING", "BASELINE_LOADING", "BASELINE_PUBLISHED",
        "STAGE_DROPPING", "INDEXES_ENABLING",
        "DATA_VERIFYING", "DATA_MISMATCH",
        "CDC_APPLYING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP",
        "STEADY_STATE",
        "CANCELLING",
    )
```

- [ ] **Step 2: Найти все остальные упоминания удалённых фаз в state_db.py**

```bash
grep -nE "PREPARING|SCN_FIXED|CONNECTOR_STARTING|CDC_BUFFERING|CDC_APPLY_STARTING" backend/db/state_db.py
```
Если выдало строки, не относящиеся к комментариям или историческим DDL'ам — нужно исследовать и удалить. Если только `_VALID_PHASES` или похожий set — обнови его так же.

- [ ] **Step 3: Verify**

```bash
python3 -c "import ast; ast.parse(open('backend/db/state_db.py').read()); print('syntax ok')"
```

- [ ] **Step 4: Commit**

```bash
git add backend/db/state_db.py
git commit -m "refactor(db): убрать Legacy-фазы из _ACTIVE_PHASES"
```

---

## Task 8: Удалить Legacy-фазы из `schema_migrations.py`

**Files:**
- Modify: `backend/services/schema_migrations.py:21-57` (`_PHASE_TO_STAGE`), `:60-70` (`_ACTIVE_PHASES`)

- [ ] **Step 1: Удалить ключи из `_PHASE_TO_STAGE`**

Удалить из словаря `_PHASE_TO_STAGE` строки:
```python
    "PREPARING":           "schema",
    "SCN_FIXED":           "schema",
    "CONNECTOR_STARTING":  "schema",
    "CDC_BUFFERING":       "cdc",
    "CDC_APPLY_STARTING":  "cdc",
```

- [ ] **Step 2: Удалить из `_ACTIVE_PHASES` set'a**

`backend/services/schema_migrations.py:60-70` — удалить строки с `PREPARING`, `SCN_FIXED`, `CONNECTOR_STARTING`, `CDC_BUFFERING`, `CDC_APPLY_STARTING`.

Также в set `cdc_phases` внутри `_aggregate_status` (`:122-123`): убрать `"CDC_BUFFERING"`, `"CDC_APPLY_STARTING"`.

- [ ] **Step 3: Smoke-проверка маппера**

```bash
cd backend && python3 -c "
from services.schema_migrations import _PHASE_TO_STAGE, _ACTIVE_PHASES, _aggregate_status, _aggregate_stage
removed = {'PREPARING','SCN_FIXED','CONNECTOR_STARTING','CDC_BUFFERING','CDC_APPLY_STARTING'}
assert not (removed & set(_PHASE_TO_STAGE.keys())), _PHASE_TO_STAGE.keys() & removed
assert not (removed & _ACTIVE_PHASES), _ACTIVE_PHASES & removed
# Spot-check aggregator (no Legacy phases)
assert _aggregate_stage(['CDC_APPLYING'], False) == 'cdc'
assert _aggregate_stage(['COMPLETED','COMPLETED'], False) == 'cutover'
assert _aggregate_status(['CDC_APPLYING'], False, False) == 'cdc'
assert _aggregate_status(['DATA_VERIFYING'], False, False) == 'validating'
print('OK')
"
```
Expected: `OK`.

- [ ] **Step 4: Commit**

```bash
git add backend/services/schema_migrations.py
git commit -m "refactor(schema_migrations): убрать Legacy-фазы из агрегатора"
```

---

## Task 9: Удалить Legacy-функции из `debezium.py` и `oracle_scn.py`

**Files:**
- Modify: `backend/services/debezium.py:39-155` (`create_connector`)
- Modify: `backend/services/oracle_scn.py` (`get_current_scn`)

- [ ] **Step 1: Проверить, что `create_connector(migration, oracle_cfg)` нигде больше не вызывается**

```bash
grep -rn "debezium.create_connector\|from services.debezium import.*create_connector\b" backend/
# Expected: пусто (после рефакторинга orchestrator'a в Task 5)
```
Если что-то нашлось — приостанови этот шаг, разберись.

- [ ] **Step 2: Удалить функцию `create_connector(migration: dict, oracle_cfg: dict)` из `debezium.py`**

`backend/services/debezium.py:39-155` — удалить целиком эту функцию. **НЕ ТРОГАЙ** `create_group_connector` (`:218+`) — это другая функция, она используется групповым флоу.

- [ ] **Step 3: Проверить и удалить `get_current_scn`**

```bash
grep -rn "oracle_scn.get_current_scn\|from services.oracle_scn import.*get_current_scn" backend/
# Expected: пусто
```
Если пусто — удали `def get_current_scn(...)` из `backend/services/oracle_scn.py`. Если что-то осталось — оставь функцию.

- [ ] **Step 4: Verify**

```bash
python3 -c "
import ast
for f in ('backend/services/debezium.py','backend/services/oracle_scn.py'):
    ast.parse(open(f).read())
print('syntax ok')
"
# Запусти бэк, убедись что не падает на импорте
python3 -c "import sys; sys.path.insert(0,'backend'); from services import orchestrator, debezium, oracle_scn; print('imports ok')"
```

- [ ] **Step 5: Commit**

```bash
git add backend/services/debezium.py backend/services/oracle_scn.py
git commit -m "chore: удалить per-migration create_connector и get_current_scn"
```

---

## Task 10: Обновить TS типы — `Strategy`, удалить Legacy-фазы

**Files:**
- Modify: `frontend/src/types/migration.ts`

- [ ] **Step 1: Удалить Legacy-фазы из `MigrationPhase` и `ORDERED_PHASES`**

`frontend/src/types/migration.ts:3-15` — `MigrationPhase`: удалить `"PREPARING" | "SCN_FIXED" | "CONNECTOR_STARTING" | "CDC_BUFFERING" | "CDC_APPLY_STARTING"`.

`:183-194` — `ORDERED_PHASES`: удалить те же 5 строк.

`:148-152, 165` — `PHASE_COLORS`: удалить записи `PREPARING`, `SCN_FIXED`, `CONNECTOR_STARTING`, `CDC_BUFFERING`, `CDC_APPLY_STARTING`.

- [ ] **Step 2: Заменить `migration_mode`/`migration_strategy` на `strategy` в `Migration`**

`frontend/src/types/migration.ts:70-71`:
```ts
  migration_strategy: string;
  migration_mode: string;
```
на:
```ts
  strategy: Strategy;
```

И в `MigrationSummary` (`:119`):
```ts
  migration_mode: string;
```
заменить на:
```ts
  strategy: Strategy;
```

- [ ] **Step 3: Добавить тип `Strategy` и хелперы**

В начало файла после `import { t } ...`:
```ts
export type Strategy = "CDC_STAGE" | "CDC_DIRECT" | "BULK_STAGE" | "BULK_DIRECT";

export const hasCdc    = (s: Strategy): boolean => s.startsWith("CDC_");
export const usesStage = (s: Strategy): boolean => s.endsWith("_STAGE");

export const strategyLabel = (s: Strategy): string =>
  `${hasCdc(s) ? "С CDC" : "Без CDC"} (${usesStage(s) ? "stage" : "direct"})`;

export const composeStrategy = (cdc: boolean, stage: boolean): Strategy =>
  `${cdc ? "CDC" : "BULK"}_${stage ? "STAGE" : "DIRECT"}` as Strategy;
```

- [ ] **Step 4: Verify type-check**

```bash
cd frontend && npx tsc --noEmit
# Ожидаются ошибки в местах, которые ещё используют migration_mode / migration_strategy
# Эти места — Tasks 11-14. Запомни список файлов из вывода.
```

- [ ] **Step 5: Commit**

```bash
git add frontend/src/types/migration.ts
git commit -m "feat(types): Strategy enum + хелперы, drop Legacy-фазы"
```

---

## Task 11: Создать компонент `<StrategyPicker>`

**Files:**
- Create: `frontend/src/components/StrategyPicker/index.tsx`

- [ ] **Step 1: Создать компонент**

`frontend/src/components/StrategyPicker/index.tsx`:
```tsx
import { useState } from "react";
import { t } from "../../theme";
import { Strategy, hasCdc, usesStage, composeStrategy } from "../../types/migration";

interface Props {
  value: Strategy;
  onChange: (s: Strategy) => void;
  /** Disable «С CDC» если коннектор группы не RUNNING */
  cdcDisabledReason?: string;
}

export function StrategyPicker({ value, onChange, cdcDisabledReason }: Props) {
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const cdc = hasCdc(value);
  const stage = usesStage(value);

  const setCdc = (c: boolean) => onChange(composeStrategy(c, stage));
  const setStage = (s: boolean) => onChange(composeStrategy(cdc, s));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      <div style={{ fontWeight: 500 }}>Стратегия миграции</div>

      <div style={{ display: "flex", gap: 8 }}>
        <button
          type="button"
          onClick={() => !cdcDisabledReason && setCdc(true)}
          disabled={!!cdcDisabledReason}
          title={cdcDisabledReason || ""}
          style={{
            flex: 1, padding: "8px 12px",
            border: `1px solid ${cdc ? t.purple.base : t.border.base}`,
            background: cdc ? t.purple.bg : t.bg.s2,
            color: cdc ? t.purple.fg : t.text.muted,
            cursor: cdcDisabledReason ? "not-allowed" : "pointer",
            opacity: cdcDisabledReason ? 0.5 : 1,
          }}
        >
          С CDC
        </button>
        <button
          type="button"
          onClick={() => setCdc(false)}
          style={{
            flex: 1, padding: "8px 12px",
            border: `1px solid ${!cdc ? t.green.base : t.border.base}`,
            background: !cdc ? t.green.bg : t.bg.s2,
            color: !cdc ? t.green.fg : t.text.muted,
            cursor: "pointer",
          }}
        >
          Без CDC
        </button>
      </div>
      <div style={{ fontSize: 12, color: t.text.muted }}>
        {cdc
          ? "Bulk-загрузка + apply из Kafka, миграция остаётся в STEADY_STATE."
          : "Один разовый перенос данных, завершается после DATA_VERIFYING."}
      </div>

      <button
        type="button"
        onClick={() => setAdvancedOpen(o => !o)}
        style={{
          alignSelf: "flex-start",
          background: "transparent", border: "none", padding: 0,
          color: t.text.muted, cursor: "pointer", fontSize: 12,
        }}
      >
        {advancedOpen ? "▼" : "▶"} Дополнительно
      </button>

      {advancedOpen && (
        <div style={{ paddingLeft: 12, display: "flex", flexDirection: "column", gap: 8 }}>
          <div style={{ fontWeight: 500, fontSize: 13 }}>Способ загрузки</div>
          <div style={{ display: "flex", gap: 8 }}>
            <button
              type="button"
              onClick={() => setStage(true)}
              style={{
                flex: 1, padding: "6px 10px",
                border: `1px solid ${stage ? t.blue.base : t.border.base}`,
                background: stage ? t.bg.s3 : t.bg.s2,
                color: stage ? t.blue.fg : t.text.muted,
                cursor: "pointer", fontSize: 13,
              }}
            >
              STAGE
            </button>
            <button
              type="button"
              onClick={() => setStage(false)}
              style={{
                flex: 1, padding: "6px 10px",
                border: `1px solid ${!stage ? t.green.base : t.border.base}`,
                background: !stage ? t.green.bg : t.bg.s2,
                color: !stage ? t.green.fg : t.text.muted,
                cursor: "pointer", fontSize: 13,
              }}
            >
              DIRECT
            </button>
          </div>
          <div style={{ fontSize: 12, color: t.text.muted }}>
            {stage
              ? "Через промежуточную stage-таблицу (валидация + TRUNCATE + baseline). Надёжнее."
              : "Прямая загрузка в target. Быстрее, но без stage-валидации."}
          </div>
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Verify type-check**

```bash
cd frontend && npx tsc --noEmit src/components/StrategyPicker/index.tsx
# Expected: no errors specifically from this file
```

- [ ] **Step 3: Commit**

```bash
git add frontend/src/components/StrategyPicker/index.tsx
git commit -m "feat(ui): компонент StrategyPicker (радиогруп + advanced toggle)"
```

---

## Task 12: Подключить `<StrategyPicker>` в `CreateMigrationModal`

**Files:**
- Modify: `frontend/src/components/CreateMigrationModal/types.ts`
- Modify: `frontend/src/components/CreateMigrationModal/helpers.ts`
- Modify: `frontend/src/components/CreateMigrationModal/index.tsx`

- [ ] **Step 1: types.ts — заменить два поля на одно**

`frontend/src/components/CreateMigrationModal/types.ts:24-25`:
```ts
  migration_mode:           "CDC" | "BULK_ONLY";
  migration_strategy:       "STAGE" | "DIRECT";
```
заменить на:
```ts
  strategy:                 Strategy;
```
И добавить импорт в шапку файла:
```ts
import { Strategy } from "../../types/migration";
```

- [ ] **Step 2: helpers.ts — поправить default**

`frontend/src/components/CreateMigrationModal/helpers.ts:34-35`:
```ts
  migration_mode:           "CDC",
  migration_strategy:       "STAGE",
```
заменить на:
```ts
  strategy:                 "CDC_STAGE",
```

- [ ] **Step 3: index.tsx — заменить ручные кнопки на `<StrategyPicker>`**

В `frontend/src/components/CreateMigrationModal/index.tsx`:

Импорты:
```tsx
import { StrategyPicker } from "../StrategyPicker";
import { hasCdc, usesStage } from "../../types/migration";
```

Заменить блок 336-405 (две группы кнопок CDC/BULK_ONLY и STAGE/DIRECT) на:
```tsx
<StrategyPicker
  value={form.strategy}
  onChange={(s) => setF({ strategy: s })}
/>
```

Сценарии валидации (`:175-211`):
```tsx
    if (form.migration_mode === "CDC" && !form.group_id) { ... }
    if (form.migration_strategy === "STAGE" && !form.stage_table_name.trim()) ...
```
заменить:
```tsx
    if (hasCdc(form.strategy) && !form.group_id) {
      ... // существующее тело
    }
    if (usesStage(form.strategy) && !form.stage_table_name.trim())
      ... // существующее тело
```

Payload (`:198-211`):
```tsx
      migration_mode:             form.migration_mode,
      migration_strategy:         form.migration_strategy,
```
заменить на одну строку:
```tsx
      strategy:                   form.strategy,
```

Внутри объекта payload — все остальные `form.migration_strategy === "STAGE"` → `usesStage(form.strategy)`, `form.migration_mode === "CDC"` → `hasCdc(form.strategy)`.

Условие `{form.migration_strategy === "STAGE" && (` (`:450`) → `{usesStage(form.strategy) && (`.

- [ ] **Step 4: Verify type-check**

```bash
cd frontend && npx tsc --noEmit
# Файл CreateMigrationModal/* должен пройти. Остальные ошибки — следующие таски.
```

- [ ] **Step 5: Commit**

```bash
git add frontend/src/components/CreateMigrationModal/
git commit -m "feat(create-migration): StrategyPicker + payload strategy"
```

---

## Task 13: Подключить `<StrategyPicker>` в `PlannerWizard`

**Files:**
- Modify: `frontend/src/components/DDLCatalog/PlannerWizard/types.ts`
- Modify: `frontend/src/components/DDLCatalog/PlannerWizard/index.tsx`
- Modify: `frontend/src/components/DDLCatalog/PlannerWizard/steps/TableSelectionStep.tsx`

- [ ] **Step 1: types.ts — заменить `mode` + `strategy` на одно `strategy`**

`frontend/src/components/DDLCatalog/PlannerWizard/types.ts:20`:
```ts
  mode:       "CDC" | "BULK_ONLY";
  ...
  strategy:   "STAGE" | "DIRECT";  // если оба поля
```
заменить оба упоминания на:
```ts
  strategy:   Strategy;
```
В двух местах в файле (Defaults и Per-table). Импорт `Strategy` из `../../../types/migration`.

- [ ] **Step 2: index.tsx — поправить вызов и default**

`frontend/src/components/DDLCatalog/PlannerWizard/index.tsx:177-188`:
```ts
        migration_strategy:   defaults.strategy,
        migration_mode:       defaults.mode,
        ...
            migration_mode:        it.mode,
            migration_strategy:    it.strategy,
```
заменить на:
```ts
        strategy:   defaults.strategy,
        ...
            strategy:    it.strategy,
```
И где-то рядом убедись, что defaults дефолтят на `"CDC_STAGE"` (если не так — поправь конструктор начального состояния).

- [ ] **Step 3: TableSelectionStep.tsx — заменить два select'a на `<StrategyPicker>`**

`frontend/src/components/DDLCatalog/PlannerWizard/steps/TableSelectionStep.tsx`:

Глобальные defaults (~:85-95): заменить два `<select>` (CDC/BULK_ONLY + STAGE/DIRECT) на:
```tsx
<StrategyPicker
  value={defaults.strategy}
  onChange={(s) => onDefaults({ ...defaults, strategy: s })}
/>
```
Импорт: `import { StrategyPicker } from "../../../StrategyPicker";`.

Per-table (~:180-200): заменить два `<select>` на:
```tsx
<StrategyPicker
  value={table.strategy}
  onChange={(s) => onTableSetting(table, { strategy: s })}
/>
```

- [ ] **Step 4: Verify**

```bash
cd frontend && npx tsc --noEmit src/components/DDLCatalog/PlannerWizard/
```

- [ ] **Step 5: Commit**

```bash
git add frontend/src/components/DDLCatalog/PlannerWizard/
git commit -m "feat(planner): StrategyPicker в визарде планирования"
```

---

## Task 14: Подключить `<StrategyPicker>` в `ConnectorGroupsPanel/MigrateModal`

**Files:**
- Modify: `frontend/src/components/ConnectorGroupsPanel/types.ts`
- Modify: `frontend/src/components/ConnectorGroupsPanel/helpers.ts`
- Modify: `frontend/src/components/ConnectorGroupsPanel/MigrateModal.tsx`
- Modify: `frontend/src/components/ConnectorGroupsPanel/index.tsx`

- [ ] **Step 1: types.ts — одно поле**

`frontend/src/components/ConnectorGroupsPanel/types.ts:25-26`:
```ts
  migration_mode:           "CDC" | "BULK_ONLY";
  migration_strategy:       "STAGE" | "DIRECT";
```
→
```ts
  strategy:                 Strategy;
```
Импорт `Strategy` из `../../types/migration`.

- [ ] **Step 2: helpers.ts — default**

`frontend/src/components/ConnectorGroupsPanel/helpers.ts:29-30` → одно поле `strategy: "CDC_STAGE",`.

- [ ] **Step 3: MigrateModal.tsx — заменить кастомные кнопки на `<StrategyPicker>`**

`frontend/src/components/ConnectorGroupsPanel/MigrateModal.tsx`:

Удалить ModeBtn-helper и две группы кнопок (`:55-156` примерно). Вставить:
```tsx
import { StrategyPicker } from "../StrategyPicker";
import { hasCdc, usesStage } from "../../types/migration";

// в JSX:
<StrategyPicker
  value={params.strategy}
  onChange={(s) => setParams({ ...params, strategy: s })}
/>
```

Условие `{params.migration_strategy === "STAGE" && (` (`:155`) → `{usesStage(params.strategy) && (`.

- [ ] **Step 4: index.tsx — список миграций**

`frontend/src/components/ConnectorGroupsPanel/index.tsx:268`:
```tsx
<td style={{ padding: "4px 8px", color: t.text.muted }}>{m.migration_mode}</td>
```
заменить на:
```tsx
<td style={{ padding: "4px 8px", color: t.text.muted }}>
  {(m as { strategy?: string }).strategy ?? "—"}
</td>
```
(MigrationSummary получит поле `strategy` после backend-обновления.)

- [ ] **Step 5: Verify**

```bash
cd frontend && npx tsc --noEmit src/components/ConnectorGroupsPanel/
```

- [ ] **Step 6: Commit**

```bash
git add frontend/src/components/ConnectorGroupsPanel/
git commit -m "feat(groups): StrategyPicker в форме создания миграции из группы"
```

---

## Task 15: Обновить отображение стратегии в списке и в детале

**Files:**
- Modify: `frontend/src/components/MigrationList/MigrationRow.tsx`
- Modify: `frontend/src/components/MigrationDetail/tabs/OverviewTab.tsx`
- Modify: `frontend/src/components/MigrationDetail/helpers.ts`

- [ ] **Step 1: MigrationRow — бейдж со strategyLabel**

`frontend/src/components/MigrationList/MigrationRow.tsx:54`:
```tsx
{m.migration_mode === "BULK_ONLY" && (
  ... // существующая разметка бейджа
)}
```
заменить на:
```tsx
{m.strategy && (
  <span style={{
    marginLeft: 8, padding: "2px 6px", borderRadius: 4,
    fontSize: 11,
    background: hasCdc(m.strategy) ? t.purple.bg : t.green.bg,
    color: hasCdc(m.strategy) ? t.purple.fg : t.green.fg,
    border: `1px solid ${hasCdc(m.strategy) ? t.purple.dim : t.green.dim}`,
  }}>
    {strategyLabel(m.strategy)}
  </span>
)}
```
Импорты: `import { hasCdc, strategyLabel } from "../../types/migration";`

- [ ] **Step 2: OverviewTab — поле "Стратегия"**

`frontend/src/components/MigrationDetail/tabs/OverviewTab.tsx:250`:
```tsx
<InfoRow label="Стратегия" value={detail.migration_strategy} />
```
заменить на:
```tsx
<InfoRow label="Стратегия" value={strategyLabel(detail.strategy)} />
```
Импорт: `import { strategyLabel } from "../../../types/migration";`

- [ ] **Step 3: MigrationDetail/helpers.ts — `isCdcMode` через новый enum**

`frontend/src/components/MigrationDetail/helpers.ts:38-40`:
```ts
export function isCdcMode(detail: { migration_mode?: string }): boolean {
  return (detail.migration_mode ?? "CDC").toUpperCase() !== "BULK_ONLY";
}
```
заменить на:
```ts
import { Strategy, hasCdc } from "../../types/migration";

export function isCdcMode(detail: { strategy?: Strategy }): boolean {
  return detail.strategy ? hasCdc(detail.strategy) : true;
}
```

- [ ] **Step 4: Verify**

```bash
cd frontend && npx tsc --noEmit
# Expected: 0 ошибок по всему фронту
```

- [ ] **Step 5: Commit**

```bash
git add frontend/src/components/MigrationList/ frontend/src/components/MigrationDetail/
git commit -m "feat(ui): strategyLabel в списке и деталях миграции"
```

---

## Task 16: Дроп старых колонок + defensive guard для Legacy-фаз

**Files:**
- Modify: `backend/db/state_db.py` (`ensure_schema`)

- [ ] **Step 1: Дропнуть старые колонки после backfill'a**

В `ensure_schema()`, после блока backfill'a (добавленного в Task 2), вставить:

```python
            # ── Drop legacy columns (backfilled above) ──
            cur.execute("ALTER TABLE migrations DROP COLUMN IF EXISTS migration_mode")
            cur.execute("ALTER TABLE migrations DROP COLUMN IF EXISTS migration_strategy")
            print("[state_db]   dropped legacy columns: migration_mode, migration_strategy")
```

- [ ] **Step 2: Defensive guard — если в БД есть миграции в Legacy-фазах, падаем**

В `ensure_schema()`, после дропа колонок, вставить:

```python
            cur.execute("""
                SELECT migration_id, phase FROM migrations
                WHERE phase IN ('PREPARING','SCN_FIXED','CONNECTOR_STARTING',
                                'CDC_BUFFERING','CDC_APPLY_STARTING')
                LIMIT 5
            """)
            stuck = cur.fetchall()
            if stuck:
                ids = ", ".join(f"{r[0]} ({r[1]})" for r in stuck)
                raise RuntimeError(
                    f"Найдены миграции в Legacy-фазах: {ids}. "
                    "Завершите или отмените их в предыдущей версии перед обновлением."
                )
```

- [ ] **Step 3: Также убрать ALTER'ы, которые добавляли `migration_mode`/`migration_strategy`**

Из цикла `for col_sql in [...]` в `backend/db/state_db.py:242-262`: удалить две строки:
```python
"ALTER TABLE migrations ADD COLUMN IF NOT EXISTS migration_strategy        VARCHAR(32) NOT NULL DEFAULT 'STAGE'",
...
"ALTER TABLE migrations ADD COLUMN IF NOT EXISTS migration_mode            VARCHAR(32) NOT NULL DEFAULT 'CDC'",
```

- [ ] **Step 4: Verify запуск миграции на dev-БД**

Останови бэк. Запусти повторно, проверь:
```bash
psql -h localhost -U coordinator -d coordinator -c "\d migrations" | grep -E "strategy|migration_mode|migration_strategy"
# Expected: только `strategy text not null default 'CDC_STAGE'`; ни migration_mode, ни migration_strategy
```

```bash
psql -h localhost -U coordinator -d coordinator -c "
SELECT strategy, COUNT(*) FROM migrations GROUP BY 1 ORDER BY 1;"
# Expected: 4 строки (или меньше, в зависимости от данных)
```

- [ ] **Step 5: Commit**

```bash
git add backend/db/state_db.py
git commit -m "feat(db): drop legacy columns, защита от Legacy-фаз при старте"
```

---

## Task 17: Manual smoke по всем 4 стратегиям

- [ ] **Step 1: Запустить бэк и фронт, открыть UI**

```bash
# Бэк
cd backend && python3 app.py &
# Фронт
cd frontend && npm run dev &
```

Открой http://localhost:5173 (или порт из vite-конфига).

- [ ] **Step 2: Создать тестовые таблицы (через UI или через TargetPrep)**

Используй любые две тестовые Oracle-таблицы (например, маленькие, ≤1000 строк) в source-схеме. Создай в target-схеме их пустые копии.

- [ ] **Step 3: Smoke 1 — CDC_STAGE (default)**

В CreateMigrationModal:
- Stratagy: «С CDC» (выбрано по умолчанию)
- Advanced: не открывать (STAGE по дефолту)
- Группа: указать любую RUNNING-группу
- Создать.

Через UI или БД проверить, что миграция дошла до `STEADY_STATE`. Если упала — посмотри логи бэка.

- [ ] **Step 4: Smoke 2 — BULK_STAGE**

В модалке:
- Strategy: «Без CDC»
- Advanced свернут (STAGE по дефолту)
- Создать.

Проверить путь: `NEW → CHUNKING → BULK_LOADING → BULK_LOADED → STAGE_VALIDATING → STAGE_VALIDATED → BASELINE_PUBLISHING → BASELINE_LOADING → BASELINE_PUBLISHED → STAGE_DROPPING → INDEXES_ENABLING → DATA_VERIFYING → COMPLETED`.

- [ ] **Step 5: Smoke 3 — CDC_DIRECT**

- Strategy: «С CDC»
- Advanced: DIRECT
- Group: RUNNING.
- Создать.

Ожидаемый путь: `NEW → TOPIC_CREATING → CHUNKING → BULK_LOADING → BULK_LOADED → INDEXES_ENABLING → CDC_APPLYING → CDC_CATCHING_UP → CDC_CAUGHT_UP → STEADY_STATE`.

- [ ] **Step 6: Smoke 4 — BULK_DIRECT**

- Strategy: «Без CDC»
- Advanced: DIRECT
- Создать.

Ожидаемый путь: `NEW → CHUNKING → BULK_LOADING → BULK_LOADED → INDEXES_ENABLING → DATA_VERIFYING → COMPLETED`.

- [ ] **Step 7: Smoke negative — без `group_id`**

```bash
curl -s -X POST http://localhost:5000/api/migrations \
  -H 'Content-Type: application/json' \
  -d '{"migration_name":"x","strategy":"CDC_STAGE","source_schema":"S","source_table":"T","target_schema":"S","target_table":"T"}'
# Expected: HTTP 400, "group_id is required (Legacy ... no longer supported)"
```

- [ ] **Step 8: Smoke negative — CDC при non-RUNNING группе**

Останови коннектор любой группы (через UI «Stop»). Попробуй создать миграцию с этой группой и `strategy=CDC_STAGE`.
Expected: HTTP 409, «Коннектор группы не запущен …».

- [ ] **Step 9: Final commit (если потребовались правки)**

Если все smoke прошли с первого раза — коммит не нужен. Если потребовались хотфиксы — коммитнуть их по теме:
```bash
git commit -am "fix: <конкретное описание>"
```

---

## Self-Review (выполнен автором плана)

**Spec coverage:**
- Goals 1–4 (две стратегии user-facing, удаление Legacy, единое поле в БД, обязательный `group_id`) — Tasks 1–6, 9, 16.
- Non-goals (DATA_VERIFYING поведение, отсутствие CDC HASH-сверки) — не затрагиваются ни одной из таск (✓).
- User-facing модель (UI + маппинг) — Tasks 10–15.
- DB schema (column, backfill, drop, constraint) — Tasks 2, 16.
- Helper `Strategy` — Task 1.
- State machine после рефактора — Tasks 5, 6, 7, 8.
- Удаление debezium.create_connector + oracle_scn.get_current_scn — Task 9.
- API contract (`strategy` обязательно, `group_id` обязательно, HTTP 409 для non-RUNNING CDC) — Tasks 3, 4 + Smoke 7, 8.
- Defensive guard для Legacy-фаз — Task 16, Step 2.
- Rollout (smoke 4 типов) — Task 17.

**Placeholder scan:** не обнаружено TBD/TODO/«handle edge cases» — все шаги содержат конкретный код или команды.

**Type consistency:** `Strategy.parse()` определён в Task 1 и используется в Tasks 3, 4, 6, 9, 17. `strategyLabel`/`hasCdc`/`usesStage`/`composeStrategy` определены в Task 10 (`types/migration.ts`) и переиспользуются в 11, 12, 13, 14, 15. Поле `strategy: Strategy` единообразно от БД (`migrations.strategy`) до фронта (`Migration.strategy`).
