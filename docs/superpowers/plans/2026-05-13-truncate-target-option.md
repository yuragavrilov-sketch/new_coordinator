# TRUNCATE Target Option — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Добавить пер-миграционный boolean-флаг `truncate_target`, который для DIRECT-стратегий управляет `TRUNCATE TABLE <target>` в фазе `NEW` (default = TRUE). Для STAGE-стратегий флаг всегда TRUE, API запрещает FALSE.

**Architecture:** Новая колонка `migrations.truncate_target BOOLEAN NOT NULL DEFAULT TRUE`. В `_handle_new` orchestrator'а — inline TRUNCATE для DIRECT-стратегий между созданием stage и транзитом в `CHUNKING`/`TOPIC_CREATING`. В UI — чекбокс в advanced-секции `<StrategyPicker>`, заблокирован для STAGE.

**Tech Stack:** Python 3 (Flask, psycopg, oracledb), PostgreSQL state DB, TypeScript/React. Тестов нет — верификация через grep, type-check, SQL и manual smoke.

**Spec:** `docs/superpowers/specs/2026-05-13-truncate-target-option-design.md`

---

## File Structure

### Модифицируются
- `backend/db/state_db.py` — добавить ALTER в существующий блок idempotent column-bringup.
- `backend/routes/migrations.py` — `create_migration` принимает `truncate_target`, валидирует STAGE+false → 400, INSERT.
- `backend/routes/planner.py` — `execute_plan` принимает `truncate_target` per-table.
- `backend/routes/connector_groups.py` — `create_migration_from_table` принимает `truncate_target`.
- `backend/services/orchestrator.py` — `_handle_new`: блок TRUNCATE между stage-блоком и транзитом.
- `frontend/src/types/migration.ts` — `Migration.truncate_target: boolean`.
- `frontend/src/components/StrategyPicker/index.tsx` — props + чекбокс в advanced.
- `frontend/src/components/CreateMigrationModal/{types.ts,helpers.ts,index.tsx}` — поле + default + проброс + payload.
- `frontend/src/components/DDLCatalog/PlannerWizard/{types.ts,index.tsx,steps/TableSelectionStep.tsx}` — то же + per-table override.
- `frontend/src/components/ConnectorGroupsPanel/{types.ts,helpers.ts,MigrateModal.tsx}` — то же.
- `frontend/src/components/MigrationDetail/tabs/OverviewTab.tsx` — `InfoRow` (опционально, см. Task 9).

### Не создаются и не удаляются файлы.

---

## Task 1: БД-колонка `truncate_target`

**Files:**
- Modify: `backend/db/state_db.py` — в `ensure_schema()`, в списке `for col_sql in [...]` (около строки 243-262, перед `data_compare_task_id`)

- [ ] **Step 1: Добавить ALTER в idempotent column-bringup**

Открой `backend/db/state_db.py`, найди список `col_sql` (начинается с `"ALTER TABLE migrations ADD COLUMN IF NOT EXISTS total_rows ..."` около строки 243). После последней записи `"ALTER TABLE migrations ADD COLUMN IF NOT EXISTS data_compare_task_id      UUID"` (около строки 261) — но ПЕРЕД закрывающей `]:` — вставь новую строку:

```python
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS truncate_target           BOOLEAN NOT NULL DEFAULT TRUE",
```

Отступ: 16 пробелов (4 уровня). Запятая в конце — критична для синтаксиса списка.

- [ ] **Step 2: Verify**

Run:
```bash
cd /home/coder/project/coordinator/new_coordinator
python3 -c "import ast; ast.parse(open('backend/db/state_db.py').read()); print('syntax ok')"
grep -n "truncate_target" backend/db/state_db.py
```
Expected:
- `syntax ok`
- Одна строка с `ADD COLUMN IF NOT EXISTS truncate_target BOOLEAN NOT NULL DEFAULT TRUE`

- [ ] **Step 3: Commit**

```bash
cd /home/coder/project/coordinator/new_coordinator && git add backend/db/state_db.py && git commit -m "$(cat <<'EOF'
feat(db): add migrations.truncate_target column

BOOLEAN NOT NULL DEFAULT TRUE. Для DIRECT-стратегий управляет TRUNCATE
target в _handle_new; для STAGE флаг игнорируется (всегда TRUE).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: `/api/migrations` принимает `truncate_target`

**Files:**
- Modify: `backend/routes/migrations.py:104-227` (функция `create_migration`)

- [ ] **Step 1: Прочитать `truncate_target` и валидировать STAGE+false**

В `create_migration()` (`backend/routes/migrations.py:104`), сразу ПОСЛЕ блока валидации `strategy` через `Strategy.parse(body.get("strategy"))` и ПЕРЕД проверкой `group_id`, добавить:

```python
                # ── truncate_target: default TRUE; STAGE forces TRUE ──
                truncate_target = bool(body.get("truncate_target", True))
                if strategy.uses_stage and truncate_target is False:
                    return jsonify({
                        "error": "STAGE-стратегия требует TRUNCATE target (поведение неизменяемо). "
                                 "Используйте DIRECT, если нужно сохранить существующие данные."
                    }), 400
```

Найди существующий `try: strategy = Strategy.parse(body.get("strategy"))` — вставка идёт сразу после `except ValueError as exc: return jsonify(...), 400` блока.

- [ ] **Step 2: Добавить колонку в INSERT**

Найди INSERT INTO migrations (около строки 156). В column tuple добавь `truncate_target,` после колонки `strategy,` (около строки 165 после твоих предыдущих правок):

Старая колоночная строка:
```python
                        strategy,
                        group_id,
                        created_at, updated_at
```
Новая:
```python
                        strategy, truncate_target,
                        group_id,
                        created_at, updated_at
```

В placeholder-строке добавь ещё один `%s` рядом со `strategy`. Найди строку, которая выглядит как `%s,` (одиночный для strategy) и сделай `%s, %s,`. Проверь общее число `%s` после правки — оно должно вырасти на 1.

В values tuple (около строки 200) добавь `truncate_target,` после `strategy.value,`:

```python
                    strategy.value, truncate_target,
                    group_id,
                    now, now,
```

- [ ] **Step 3: Verify**

```bash
cd /home/coder/project/coordinator/new_coordinator
python3 -c "import ast; ast.parse(open('backend/routes/migrations.py').read()); print('syntax ok')"
grep -nE "truncate_target" backend/routes/migrations.py
```
Expected: 3 строки — чтение из body, INSERT columns, INSERT values.

Запусти бэк (если есть) и проверь negative case через curl:
```bash
curl -s -X POST http://localhost:5000/api/migrations \
  -H 'Content-Type: application/json' \
  -d '{"migration_name":"x","strategy":"BULK_STAGE","truncate_target":false,"group_id":"00000000-0000-0000-0000-000000000000","source_schema":"S","source_table":"T","target_schema":"S","target_table":"T"}'
# Expected: HTTP 400 с error: "STAGE-стратегия требует TRUNCATE target..."
```
Если бэк не запущен — пропусти живую проверку, syntax-check достаточно.

- [ ] **Step 4: Commit**

```bash
cd /home/coder/project/coordinator/new_coordinator && git add backend/routes/migrations.py && git commit -m "$(cat <<'EOF'
feat(api): /api/migrations принимает truncate_target

Default TRUE. Для STAGE-стратегий значение FALSE → HTTP 400. Колонка
добавлена в INSERT.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: planner и connector-groups роуты принимают `truncate_target`

**Files:**
- Modify: `backend/routes/planner.py` — `execute_plan()` (около строки 237)
- Modify: `backend/routes/connector_groups.py` — `create_migration_from_table()` (около строки 302)

- [ ] **Step 1: `planner.py::execute_plan` — per-row `truncate_target`**

В `execute_plan()` (`backend/routes/planner.py:237`), внутри цикла по таблицам (где уже читается `strategy` per-row), сразу ПОСЛЕ парсинга `strategy` добавь:

```python
                    raw_truncate = overrides.get("truncate_target")
                    if raw_truncate is None:
                        raw_truncate = defaults.get("truncate_target", True)
                    truncate_target = bool(raw_truncate)
                    if strategy.uses_stage and truncate_target is False:
                        return jsonify({
                            "error": f"Invalid truncate_target for {table}: "
                                     "STAGE-стратегия требует TRUNCATE target (поведение неизменяемо)."
                        }), 400
```

Имя переменной таблицы (`table` в коде выше) — проверь и подставь то, что реально используется в цикле (вероятно `table` или `tbl`).

Найди INSERT INTO migrations (около строки 307). В column tuple добавь `truncate_target,` после `strategy,`. В placeholders добавь ещё один `%s` (на той же строке что `strategy`). В values tuple (около строки 334) добавь `truncate_target,` после `strategy.value,`.

- [ ] **Step 2: `connector_groups.py::create_migration_from_table` — единый `truncate_target`**

В `create_migration_from_table()` (`backend/routes/connector_groups.py:302`), сразу ПОСЛЕ парсинга `strategy` (после `Strategy.parse(body.get("strategy"))` и связанных stage_name/stage_tablespace), добавь:

```python
    truncate_target = bool(body.get("truncate_target", True))
    if strategy.uses_stage and truncate_target is False:
        return jsonify({
            "error": "STAGE-стратегия требует TRUNCATE target (поведение неизменяемо). "
                     "Используйте DIRECT, если нужно сохранить существующие данные."
        }), 400
```

Найди INSERT INTO migrations (около строки 393). В column tuple добавь `truncate_target,` после `strategy,`. В placeholders — ещё один `%s`. В values tuple (около строки 432) — `truncate_target,` после `strategy.value,`.

- [ ] **Step 3: Verify**

```bash
cd /home/coder/project/coordinator/new_coordinator
python3 -c "
import ast
for f in ('backend/routes/planner.py','backend/routes/connector_groups.py'):
    ast.parse(open(f).read())
print('syntax ok')
"
grep -nE "truncate_target" backend/routes/planner.py backend/routes/connector_groups.py
```
Expected: по 3 совпадения в каждом файле.

- [ ] **Step 4: Commit**

```bash
cd /home/coder/project/coordinator/new_coordinator && git add backend/routes/planner.py backend/routes/connector_groups.py && git commit -m "$(cat <<'EOF'
feat(api): planner+groups роуты принимают truncate_target

execute_plan: per-table с глобальным default'ом. create_migration_from_table:
поле в body. STAGE+false → HTTP 400 в обоих случаях.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Orchestrator — TRUNCATE target в `_handle_new`

**Files:**
- Modify: `backend/services/orchestrator.py:998-1018` (внутри `_handle_new`'s `_run`)

- [ ] **Step 1: Вставить блок TRUNCATE между stage-блоком и транзитом**

В `backend/services/orchestrator.py`, найди функцию `_handle_new` (около строки 904). Внутри неё — daemon-поток `_run`, в котором сейчас (между строками 998 и 1010):

```python
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

После блока с `stage_msg = "Прямая загрузка (без stage)"` (около строки 1008) и ПЕРЕД `if not strategy.has_cdc:` (около строки 1010) — вставь новый блок:

```python

            # ── TRUNCATE target для DIRECT-стратегий ──
            if not strategy.uses_stage and m.get("truncate_target", True):
                tgt_quoted = f'"{m["target_schema"].upper()}"."{m["target_table"].upper()}"'
                ora_conn = oracle_scn.open_oracle_conn(dst_cfg)
                try:
                    with ora_conn.cursor() as cur:
                        cur.execute(f"TRUNCATE TABLE {tgt_quoted}")
                    ora_conn.commit()
                    print(f"[orchestrator] {mid}: truncated {tgt_quoted}")
                    stage_msg += ", target очищен"
                finally:
                    ora_conn.close()
```

Отступ — 12 пробелов (3 уровня — внутри `_run` внутри `try:`). Имя локальной переменной — `ora_conn`, не `conn`, чтобы не пересечься с возможным `conn` в окружающем замыкании.

- [ ] **Step 2: Verify**

```bash
cd /home/coder/project/coordinator/new_coordinator
python3 -c "import ast; ast.parse(open('backend/services/orchestrator.py').read()); print('syntax ok')"

# Подтвердить, что блок встал между stage-блоком и транзитом:
sed -n '1000,1025p' backend/services/orchestrator.py
```

Ожидаемая структура:
1. `if strategy.uses_stage:` ... `stage_msg = "Stage table создана"`
2. `else: stage_msg = "Прямая загрузка (без stage)"`
3. **Новый блок:** `if not strategy.uses_stage and m.get("truncate_target", True): ...`
4. `if not strategy.has_cdc:` ... транзит

- [ ] **Step 3: Commit**

```bash
cd /home/coder/project/coordinator/new_coordinator && git add backend/services/orchestrator.py && git commit -m "$(cat <<'EOF'
feat(orchestrator): TRUNCATE target в _handle_new для DIRECT-стратегий

Если strategy.uses_stage == False и truncate_target == True, выполняем
TRUNCATE TABLE <target> на target Oracle перед транзитом в CHUNKING/
TOPIC_CREATING. Ошибка TRUNCATE поднимается из _run и попадает в общий
PREPARING_ERROR.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: TypeScript тип `Migration.truncate_target`

**Files:**
- Modify: `frontend/src/types/migration.ts`

- [ ] **Step 1: Добавить поле в `Migration`**

В `frontend/src/types/migration.ts`, в interface `Migration` (около строки 17), добавь поле в конце (после `data_compare_task_id`):

```ts
  truncate_target: boolean;
```

- [ ] **Step 2: Verify**

```bash
cd /home/coder/project/coordinator/new_coordinator/frontend
grep -n "truncate_target" src/types/migration.ts
# Expected: одна строка `truncate_target: boolean;`
```

(TSC ошибки в других файлах в этот момент ожидаются — они закроются в Tasks 6–9.)

- [ ] **Step 3: Commit**

```bash
cd /home/coder/project/coordinator/new_coordinator && git add frontend/src/types/migration.ts && git commit -m "$(cat <<'EOF'
feat(types): Migration.truncate_target: boolean

Поле читается из API в migrations response, дальше используется
формами создания и detail-view.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: `<StrategyPicker>` — props + чекбокс в advanced

**Files:**
- Modify: `frontend/src/components/StrategyPicker/index.tsx`

- [ ] **Step 1: Добавить props `truncateTarget` и `onTruncateChange`**

Открой `frontend/src/components/StrategyPicker/index.tsx`. Найди блок `interface Props {` (около начала файла):

```tsx
interface Props {
  value: Strategy;
  onChange: (s: Strategy) => void;
  /** Disable «С CDC» если коннектор группы не RUNNING */
  cdcDisabledReason?: string;
}
```

Замени на:
```tsx
interface Props {
  value: Strategy;
  onChange: (s: Strategy) => void;
  truncateTarget: boolean;
  onTruncateChange: (b: boolean) => void;
  /** Disable «С CDC» если коннектор группы не RUNNING */
  cdcDisabledReason?: string;
}
```

Затем в сигнатуре компонента (около строки 13):
```tsx
export function StrategyPicker({ value, onChange, cdcDisabledReason }: Props) {
```
заменить на:
```tsx
export function StrategyPicker({ value, onChange, truncateTarget, onTruncateChange, cdcDisabledReason }: Props) {
```

- [ ] **Step 2: Добавить блок чекбокса внутри `{advancedOpen && (...)}`**

Найди в файле блок `{advancedOpen && (` (около середины файла). Внутри `<div style={{ paddingLeft: 12, display: "flex", flexDirection: "column", gap: 8 }}>` уже есть два дочерних блока: «Способ загрузки» (radio STAGE/DIRECT) и подпись с описанием. После последнего `</div>` подписи добавь новый блок:

```tsx
          <div style={{ paddingTop: 8, display: "flex", flexDirection: "column", gap: 4 }}>
            <label style={{ fontWeight: 500, fontSize: 13, cursor: usesStage(value) ? "not-allowed" : "pointer" }}>
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

Импорт `usesStage` уже есть в файле (используется для radio STAGE/DIRECT).

- [ ] **Step 3: Verify**

```bash
cd /home/coder/project/coordinator/new_coordinator/frontend
grep -nE "truncateTarget|onTruncateChange|truncate_target" src/components/StrategyPicker/index.tsx
# Expected: ≥4 совпадения (props interface, deconstruction, JSX checked, JSX onChange)

npx tsc --noEmit 2>&1 | grep -E "StrategyPicker" || echo "no errors in StrategyPicker"
# Expected: "no errors in StrategyPicker"
```

Если ошибки появляются в потребителях `<StrategyPicker>` — это ожидаемо (Tasks 7–8 их пробросят props).

- [ ] **Step 4: Commit**

```bash
cd /home/coder/project/coordinator/new_coordinator && git add frontend/src/components/StrategyPicker/index.tsx && git commit -m "$(cat <<'EOF'
feat(ui): чекбокс TRUNCATE target в StrategyPicker

Новые props truncateTarget/onTruncateChange. Чекбокс в advanced-секции,
disabled+checked для STAGE-стратегий (всегда ON), редактируемый для
DIRECT.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: `CreateMigrationModal` — поле, default, проброс, payload

**Files:**
- Modify: `frontend/src/components/CreateMigrationModal/types.ts`
- Modify: `frontend/src/components/CreateMigrationModal/helpers.ts`
- Modify: `frontend/src/components/CreateMigrationModal/index.tsx`

- [ ] **Step 1: types.ts**

В interface `FormData` добавь поле в конце (после последнего поля):
```ts
  truncate_target: boolean;
```

- [ ] **Step 2: helpers.ts**

В объекте `INITIAL_FORM_DATA` (или как он там называется) добавь:
```ts
  truncate_target: true,
```

- [ ] **Step 3: index.tsx — проброс в `<StrategyPicker>` и payload**

Найди использование `<StrategyPicker>` в JSX. Сейчас он передаёт `value` и `onChange`. Добавь два новых props:

```tsx
<StrategyPicker
  value={form.strategy}
  onChange={(s) => setF({ strategy: s })}
  truncateTarget={form.truncate_target}
  onTruncateChange={(b) => setF({ truncate_target: b })}
/>
```

Найди payload, который POST-ится на `/api/migrations`. После строки `strategy: form.strategy,` добавь:
```tsx
  truncate_target: form.truncate_target,
```

Перед самим POST добавь defensive guard (чтобы UI и payload не рассинхронились):
```tsx
// Defensive: STAGE всегда требует TRUNCATE.
const trunc = usesStage(form.strategy) ? true : form.truncate_target;
```
И в payload используй `trunc` вместо `form.truncate_target`. Импорт `usesStage`:
```tsx
import { hasCdc, usesStage } from "../../types/migration";
```
(`hasCdc` уже импортирован.)

- [ ] **Step 4: Verify**

```bash
cd /home/coder/project/coordinator/new_coordinator/frontend
grep -nE "truncate_target|truncateTarget" src/components/CreateMigrationModal/
# Expected: совпадения в types.ts, helpers.ts, index.tsx (≥5)

npx tsc --noEmit 2>&1 | grep -E "CreateMigrationModal" || echo "no errors in CreateMigrationModal"
```

- [ ] **Step 5: Commit**

```bash
cd /home/coder/project/coordinator/new_coordinator && git add frontend/src/components/CreateMigrationModal/ && git commit -m "$(cat <<'EOF'
feat(create-migration): пробросить truncate_target в StrategyPicker и payload

FormData получает truncate_target: boolean (default true). Передаётся
в <StrategyPicker> и в POST /api/migrations. Defensive guard на STAGE.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: `PlannerWizard` — поле, default, проброс, payload (с per-table override)

**Files:**
- Modify: `frontend/src/components/DDLCatalog/PlannerWizard/types.ts`
- Modify: `frontend/src/components/DDLCatalog/PlannerWizard/index.tsx`
- Modify: `frontend/src/components/DDLCatalog/PlannerWizard/steps/TableSelectionStep.tsx`

- [ ] **Step 1: types.ts — в defaults и per-table interfaces**

Найди два interface'a (`BatchItem` или похожий + `PlanDefaults` или похожий). В обоих добавь:
```ts
  truncate_target: boolean;
```

- [ ] **Step 2: index.tsx — defaults и per-table state**

Найди где инициализируются `defaults` (например `useState({ strategy: "CDC_STAGE" as Strategy, ... })`) — добавь:
```ts
  truncate_target: true,
```

Аналогично — где инициализируются per-table settings (если есть отдельный объект). Если per-table override — это `tableSettings`, добавь поле там тоже.

Найди формирование payload для `POST /api/planner/execute` (или `execute-plan`). В блоке global defaults и в блоке per-table item добавь поле `truncate_target`:

```ts
// Global defaults block:
        strategy:   defaults.strategy,
        truncate_target: defaults.truncate_target,

// Per-table item block:
            strategy:    it.strategy,
            truncate_target: it.truncate_target,
```

- [ ] **Step 3: TableSelectionStep.tsx — проброс в `<StrategyPicker>`**

Найди использование `<StrategyPicker>` в global defaults area:
```tsx
<StrategyPicker
  value={defaults.strategy}
  onChange={(s) => onDefaults({ ...defaults, strategy: s })}
/>
```
Добавь props:
```tsx
<StrategyPicker
  value={defaults.strategy}
  onChange={(s) => onDefaults({ ...defaults, strategy: s })}
  truncateTarget={defaults.truncate_target}
  onTruncateChange={(b) => onDefaults({ ...defaults, truncate_target: b })}
/>
```

Аналогично для per-table `<StrategyPicker>`:
```tsx
<StrategyPicker
  value={table.strategy}
  onChange={(s) => onTableSetting(table, { strategy: s })}
  truncateTarget={table.truncate_target}
  onTruncateChange={(b) => onTableSetting(table, { truncate_target: b })}
/>
```
(Точное имя callback'а — `onTableSetting`, `onUpdate`, что-то такое — посмотри что использует `onChange` рядом.)

- [ ] **Step 4: Verify**

```bash
cd /home/coder/project/coordinator/new_coordinator/frontend
grep -nE "truncate_target|truncateTarget" src/components/DDLCatalog/PlannerWizard/
# Expected: ≥6 совпадений по всем трём файлам

npx tsc --noEmit 2>&1 | grep -E "PlannerWizard" || echo "no errors in PlannerWizard"
```

- [ ] **Step 5: Commit**

```bash
cd /home/coder/project/coordinator/new_coordinator && git add frontend/src/components/DDLCatalog/PlannerWizard/ && git commit -m "$(cat <<'EOF'
feat(planner): truncate_target в визарде с per-table override

defaults и per-table получают boolean (default true). Передаётся в
<StrategyPicker> и в payload POST /api/planner/execute.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: `ConnectorGroupsPanel/MigrateModal` — поле, default, проброс, payload + опциональный display

**Files:**
- Modify: `frontend/src/components/ConnectorGroupsPanel/types.ts`
- Modify: `frontend/src/components/ConnectorGroupsPanel/helpers.ts`
- Modify: `frontend/src/components/ConnectorGroupsPanel/MigrateModal.tsx`
- Modify: `frontend/src/components/MigrationDetail/tabs/OverviewTab.tsx` (опциональный display)

- [ ] **Step 1: types.ts**

В interface migrate params (рядом с `strategy: Strategy`) добавь:
```ts
  truncate_target: boolean;
```

- [ ] **Step 2: helpers.ts**

В дефолтном объекте (`MIGRATE_DEFAULTS` или похожем) добавь:
```ts
  truncate_target: true,
```

- [ ] **Step 3: MigrateModal.tsx — проброс**

Найди `<StrategyPicker>`. Передай новые props:
```tsx
<StrategyPicker
  value={params.strategy}
  onChange={(s) => setParams({ ...params, strategy: s })}
  truncateTarget={params.truncate_target}
  onTruncateChange={(b) => setParams({ ...params, truncate_target: b })}
/>
```

Payload автоматически попадёт через `...params`, если в этой форме принят такой паттерн. Если payload собирается явно по полям — добавь `truncate_target: params.truncate_target` в объект.

- [ ] **Step 4: OverviewTab.tsx — опциональный display InfoRow**

В `frontend/src/components/MigrationDetail/tabs/OverviewTab.tsx` найди существующий `<InfoRow label="Стратегия" ... />` (около строки 250 после рефактора стратегий). После него добавь:

```tsx
<InfoRow label="TRUNCATE target" value={detail.truncate_target ? "да" : "нет"} />
```

Это для дебагинга — помогает увидеть значение поля в детале.

- [ ] **Step 5: Verify**

```bash
cd /home/coder/project/coordinator/new_coordinator/frontend
grep -nE "truncate_target|truncateTarget" src/components/ConnectorGroupsPanel/ src/components/MigrationDetail/tabs/OverviewTab.tsx
# Expected: совпадения по всем 4 файлам

# Финальный type-check всего проекта:
npx tsc --noEmit
# Expected: 0 ошибок
```

Если `tsc` всё ещё показывает ошибки — посмотри файлы из вывода и допили (вероятно, в каком-то месте уже используется `Migration.truncate_target` или передаётся `params` без поля).

- [ ] **Step 6: Commit**

```bash
cd /home/coder/project/coordinator/new_coordinator && git add frontend/src/components/ConnectorGroupsPanel/ frontend/src/components/MigrationDetail/ && git commit -m "$(cat <<'EOF'
feat(ui): truncate_target в MigrateModal группы + InfoRow в детале

Поле в MigrateModal params (default true), проброс в <StrategyPicker>.
В OverviewTab отображается строка "TRUNCATE target".

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Manual smoke

- [ ] **Step 1: Запустить бэк + фронт**

```bash
# Если бэк/фронт уже запущены docker compose'ом, пересобери:
docker compose build coordinator worker
docker compose up -d --force-recreate coordinator worker
```

Открой UI в браузере (порт указан в `docker-compose.yml`).

- [ ] **Step 2: Smoke 1 — `BULK_DIRECT + truncate_target=true` (default)**

В `CreateMigrationModal`:
- Стратегия: «Без CDC» (default)
- Advanced: DIRECT, чекбокс «Очистить target» ON (по умолчанию)
- Группа: любая
- Source/target таблицы: возьми таблицу, где в target есть несколько строк-«мусора»

Создай. Запусти. Ожидай: при переходе из `NEW` в `CHUNKING` в логах оркестратора видишь `[orchestrator] <mid>: truncated "TGT"."TABLE"`. По завершении (фаза `DATA_VERIFYING` → `COMPLETED`) — target содержит ровно столько строк, сколько в source.

- [ ] **Step 3: Smoke 2 — `BULK_DIRECT + truncate_target=false`**

Та же таблица с предзаполненными «лишними» строками. В Advanced сними галочку «Очистить target». Создай и запусти.

Ожидай: либо `BULK_LOADING` пройдёт и target имеет `source_count + предзаполненные` (если PK не пересекаются), либо упадёт с PK conflict в чанке и фаза станет `FAILED` с `error_code=BULK_LOAD_FAILED`.

- [ ] **Step 4: Smoke 3 — `BULK_STAGE + truncate_target=false` (negative)**

Через curl:
```bash
curl -s -X POST http://localhost:5000/api/migrations \
  -H 'Content-Type: application/json' \
  -d '{"migration_name":"t","strategy":"BULK_STAGE","truncate_target":false,"group_id":"<существующая UUID>","source_schema":"S","source_table":"T","target_schema":"S","target_table":"T"}'
```
Expected: HTTP 400, `"STAGE-стратегия требует TRUNCATE target..."`

- [ ] **Step 5: Smoke 4 — `CDC_DIRECT + truncate_target=true`**

Group with RUNNING connector. Стратегия «С CDC» + Advanced=DIRECT + чекбокс ON.

Ожидай: NEW → TOPIC_CREATING (после TRUNCATE!) → CHUNKING → ... → STEADY_STATE. Target очищается перед началом, CDC apply идёт штатно.

- [ ] **Step 6: Smoke 5 — `CDC_STAGE` (любое truncate_target в UI игнорируется)**

В UI чекбокс заблокирован, всегда ON. Создай миграцию — ожидай прохождения как обычно (TRUNCATE случится в `_handle_baseline_publishing`, не в `_handle_new`).

- [ ] **Step 7: Negative — permissions**

Отзови у миграционного пользователя `DELETE` (или `DROP ANY TABLE`) на target. Создай `BULK_DIRECT + truncate_target=true`. Ожидай: миграция в `FAILED` с `error_code=PREPARING_ERROR`, `error_text` содержит `ORA-01031`.

Верни привилегию обратно, чтобы не сломать остальные тесты.

- [ ] **Step 8: Финальный commit (если нужны хотфиксы)**

Если все smoke прошли — коммит не нужен. Если потребовались мелкие правки — закоммить каждую по теме.

---

## Self-Review

**Spec coverage:**
- DB column `truncate_target` — Task 1.
- API: единое поле, default true, STAGE+false → 400, INSERT — Tasks 2 (migrations), 3 (planner + groups).
- Orchestrator `_handle_new` TRUNCATE — Task 4.
- Frontend type `Migration.truncate_target` — Task 5.
- `<StrategyPicker>` props + чекбокс с disabled-для-STAGE логикой — Task 6.
- Подключение в три формы создания — Tasks 7, 8, 9.
- Defensive guard на стороне UI — Task 7 (`CreateMigrationModal`); аналогичный паттерн в 8/9 опционален, поскольку checkbox в StrategyPicker уже сам блокирует ввод для STAGE.
- Display в detail — Task 9 (опциональный InfoRow).
- Manual smoke — Task 10 (5 позитивных сценариев + permissions + STAGE+false negative).

**Placeholder scan:** TBD/TODO/«handle edge cases» отсутствуют. Все шаги содержат конкретный код, команды и ожидаемые выводы.

**Type consistency:**
- `truncate_target: boolean` в TS используется везде одинаково.
- `truncateTarget: boolean` (camelCase) — это prop-имя в `<StrategyPicker>`; в payload и в БД snake_case `truncate_target`. Различие согласовано: prop-имя локальное для компонента, payload и тип `Migration` — snake_case (согласовано с остальной TS-моделью).
- `Strategy.uses_stage` (Python property), `usesStage(strategy)` (TS helper) — оба возвращают boolean на основе суффикса. Используются в API-валидации, orchestrator-блоке TRUNCATE и UI-чекбоксе одинаково.

Все sections спека покрыты задачами. Гэпов нет.
