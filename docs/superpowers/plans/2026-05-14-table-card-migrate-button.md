# Кнопка «Создать миграцию» в карточке таблицы DDL-каталога — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Добавить кнопку «Создать миграцию» в раскрытой карточке таблицы DDL-каталога, открывающую `CreateMigrationModal` с префиллом source/target schema+table.

**Architecture:** `CreateMigrationModal` принимает optional `prefill` prop; `TableDetail` рисует header с кнопкой/чипом; `TablesTab` пробрасывает контекст; `DDLCatalog` держит state модалки и перезагружает таб после создания.

**Tech Stack:** TypeScript, React 18, Vite. Тестов на фронте нет — gate'ы: `tsc --noEmit` + manual smoke.

**Spec:** `docs/superpowers/specs/2026-05-14-table-card-migrate-button-design.md`

**Файлы:**
- Modify: `frontend/src/components/CreateMigrationModal/types.ts` — экспорт `MigrationPrefill`
- Modify: `frontend/src/components/CreateMigrationModal/index.tsx` — prop `prefill`, init form через `useMemo`
- Modify: `frontend/src/components/DDLCatalog/TablesTab.tsx` — новые props, header в `TableDetail`
- Modify: `frontend/src/components/DDLCatalog/DDLCatalog.tsx` — state, рендер модалки, reload

---

## Task 1: Экспортировать тип `MigrationPrefill`

**Files:**
- Modify: `frontend/src/components/CreateMigrationModal/types.ts`

- [ ] **Step 1: Добавить интерфейс `MigrationPrefill`**

В конец файла `frontend/src/components/CreateMigrationModal/types.ts`:

```ts
export interface MigrationPrefill {
  source_schema: string;
  source_table:  string;
  target_schema: string;
  target_table:  string;
}
```

- [ ] **Step 2: Type-check**

```bash
cd frontend && npx tsc --noEmit
```

Expected: PASS (no errors).

- [ ] **Step 3: Commit**

```bash
git add frontend/src/components/CreateMigrationModal/types.ts
git commit -m "feat(types): MigrationPrefill для префилла формы"
```

---

## Task 2: `CreateMigrationModal` принимает `prefill`

**Files:**
- Modify: `frontend/src/components/CreateMigrationModal/index.tsx:13` (interface Props)
- Modify: `frontend/src/components/CreateMigrationModal/index.tsx:15-16` (signature + initial state)

- [ ] **Step 1: Обновить импорт**

В `frontend/src/components/CreateMigrationModal/index.tsx`, строка 7 — добавить `MigrationPrefill` в импорт типов:

```ts
import type { FormData, TableInfo, EnsureResult, MigrationPrefill } from "./types";
```

- [ ] **Step 2: Обновить interface Props**

Заменить строку 13:

```ts
interface Props {
  onClose: () => void;
  onCreated: () => void;
  prefill?: MigrationPrefill;
}
```

- [ ] **Step 3: Сигнатура компонента и initial form**

Заменить строки 15-16:

```ts
export function CreateMigrationModal({ onClose, onCreated, prefill }: Props) {
  const initialForm = useMemo<FormData>(() => ({
    ...INIT,
    source_schema: prefill?.source_schema ?? INIT.source_schema,
    source_table:  prefill?.source_table  ?? INIT.source_table,
    target_schema: prefill?.target_schema ?? INIT.target_schema,
    target_table:  prefill?.target_table  ?? INIT.target_table,
  }), []);  // eslint-disable-line react-hooks/exhaustive-deps -- prefill captured on mount
  const [form,         setFormRaw]    = useState<FormData>(initialForm);
```

`useMemo` уже импортирован в строке 1.

- [ ] **Step 4: Type-check**

```bash
cd frontend && npx tsc --noEmit
```

Expected: PASS.

- [ ] **Step 5: Build (sanity)**

```bash
cd frontend && npx vite build
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/components/CreateMigrationModal/index.tsx
git commit -m "feat(modal): CreateMigrationModal принимает prefill"
```

---

## Task 3: `TableDetail` + `TablesTab` + `DDLCatalog` — кнопка и модалка

Атомарная задача: меняем три файла одним коммитом, потому что новые props в `TablesTab` обязательные — без обновления `DDLCatalog` `tsc` упадёт.

**Files:**
- Modify: `frontend/src/components/DDLCatalog/TablesTab.tsx`
- Modify: `frontend/src/components/DDLCatalog/DDLCatalog.tsx`

- [ ] **Step 1: `TablesTab.tsx` — импорт `MigrationPrefill`**

В начале `frontend/src/components/DDLCatalog/TablesTab.tsx`, после строки 6:

```ts
import type { MigrationPrefill } from "../CreateMigrationModal/types";
```

- [ ] **Step 2: `TablesTab.tsx` — расширить сигнатуру `TableDetail`**

Заменить строку 282:

```ts
function TableDetail({
  obj, snapshotId, srcSchema, tgtSchema, onMigrate,
}: {
  obj: CatalogObject;
  snapshotId: number | null;
  srcSchema: string;
  tgtSchema: string;
  onMigrate: (prefill: MigrationPrefill) => void;
}) {
```

- [ ] **Step 3: `TablesTab.tsx` — header с кнопкой/чипом**

Сразу после `<td colSpan={6} style={{ padding: "8px 16px 12px 32px", background: t.bg.s2 }}>` (строка 317) вставить:

```tsx
      <div style={{
        display: "flex", alignItems: "center", justifyContent: "space-between",
        padding: "0 4px 8px",
      }}>
        <span style={{ fontSize: 12, color: t.text.disabled }}>
          Карточка таблицы
        </span>
        {(() => {
          const blocked = obj.migration_status === "PLANNED" || obj.migration_status === "IN_PROGRESS";
          const canMigrate = !blocked && !!srcSchema && !!tgtSchema;
          if (canMigrate) {
            return (
              <button
                onClick={() => onMigrate({
                  source_schema: srcSchema,
                  source_table:  obj.object_name,
                  target_schema: tgtSchema,
                  target_table:  obj.object_name,
                })}
                style={{
                  background: t.green.bg, border: `1px solid ${t.green.dim}`,
                  borderRadius: t.radius.sm, color: t.green.fg,
                  padding: "3px 12px", fontSize: t.size.xs,
                  cursor: "pointer", fontWeight: 700,
                }}
              >
                Создать миграцию
              </button>
            );
          }
          if (blocked) {
            return (
              <span style={{
                background: t.amber.base + "22", color: t.amber.base,
                padding: "2px 10px", borderRadius: t.radius.sm,
                fontSize: t.size.xs, fontWeight: 600,
              }}>
                Миграция: {obj.migration_status}
              </span>
            );
          }
          return null;
        })()}
      </div>
```

- [ ] **Step 4: `TablesTab.tsx` — расширить props `TablesTab`**

Заменить interface `Props` (строки 19-28):

```ts
interface Props {
  objects: CatalogObject[];
  snapshotId: number | null;
  selected: Set<string>;
  onToggle: (name: string) => void;
  onToggleAll: (names: string[]) => void;
  syncBusy: Set<string>;
  onCompare: (type: string, name: string) => void;
  onSync: (type: string, name: string, action: string) => void;
  srcSchema: string;
  tgtSchema: string;
  onMigrate: (prefill: MigrationPrefill) => void;
}
```

- [ ] **Step 5: `TablesTab.tsx` — деструктуризация и передача в `TableDetail`**

Заменить строку 353:

```ts
export function TablesTab({
  objects, snapshotId, selected, onToggle, onToggleAll,
  syncBusy, onCompare, onSync, srcSchema, tgtSchema, onMigrate,
}: Props) {
```

Найти `<TableDetail obj={obj} snapshotId={snapshotId} />` (строка ~469) и заменить на:

```tsx
                    <TableDetail
                      obj={obj} snapshotId={snapshotId}
                      srcSchema={srcSchema} tgtSchema={tgtSchema}
                      onMigrate={onMigrate}
                    />
```

- [ ] **Step 6: `DDLCatalog.tsx` — импорты**

В начале `frontend/src/components/DDLCatalog/DDLCatalog.tsx`, после импорта `PlannerWizard` (строка 8):

```ts
import { CreateMigrationModal } from "../CreateMigrationModal";
import type { MigrationPrefill } from "../CreateMigrationModal/types";
```

- [ ] **Step 7: `DDLCatalog.tsx` — state для модалки**

Сразу после `const [showWizard, setShowWizard] = useState(false);` (строка 122) добавить:

```ts
  const [migrateModalPrefill, setMigrateModalPrefill] = useState<MigrationPrefill | null>(null);
```

- [ ] **Step 8: `DDLCatalog.tsx` — передать props в `TablesTab`**

Заменить блок `<TablesTab ... />` (строки 288-294):

```tsx
              {activeTab === "tables" && (
                <TablesTab
                  objects={grouped.tables} snapshotId={snapshotId} selected={selected}
                  onToggle={toggleTable} onToggleAll={toggleAllTables}
                  syncBusy={syncBusy} onCompare={doCompare} onSync={doSync}
                  srcSchema={srcSchema} tgtSchema={tgtSchema}
                  onMigrate={setMigrateModalPrefill}
                />
              )}
```

- [ ] **Step 9: `DDLCatalog.tsx` — рендер модалки**

После закрывающего блока для `showWizard` (после строки 326, перед `</>` секции `snapshotId &&`), добавить:

```tsx
          {migrateModalPrefill && (
            <CreateMigrationModal
              prefill={migrateModalPrefill}
              onClose={() => setMigrateModalPrefill(null)}
              onCreated={() => {
                setMigrateModalPrefill(null);
                loadObjectsForTab(activeTab);
              }}
            />
          )}
```

- [ ] **Step 10: Type-check**

```bash
cd frontend && npx tsc --noEmit
```

Expected: PASS.

- [ ] **Step 11: Build**

```bash
cd frontend && npx vite build
```

Expected: PASS.

- [ ] **Step 12: Commit**

```bash
git add frontend/src/components/DDLCatalog/TablesTab.tsx frontend/src/components/DDLCatalog/DDLCatalog.tsx
git commit -m "feat(ddl-catalog): кнопка «Создать миграцию» и модалка из карточки таблицы"
```

---

## Task 4: Manual smoke

**Files:** runtime testing.

- [ ] **Step 1: Запустить фронт и бек**

```bash
# в одном терминале
python /home/coder/project/coordinator/new_coordinator/backend/app.py

# в другом
cd /home/coder/project/coordinator/new_coordinator/frontend && npm run dev
```

- [ ] **Step 2: Smoke — кнопка появляется**

1. Открыть UI → «DDL-миграции».
2. Выбрать source-схему и target-схему, загрузить snapshot.
3. Раскрыть строку любой таблицы без активной миграции (`migration_status` ∉ {`PLANNED`, `IN_PROGRESS`}).
4. В шапке `TableDetail` справа должна быть зелёная кнопка **«Создать миграцию»**.

Expected: кнопка отрисована.

- [ ] **Step 3: Smoke — префилл работает**

1. Кликнуть «Создать миграцию».
2. В открывшейся модалке поля `source_schema`, `source_table`, `target_schema`, `target_table` должны быть заполнены значениями из контекста (src/tgt schema из верхнего селектора DDL-каталога, table = имя раскрытой таблицы).

Expected: 4 поля префиллены.

- [ ] **Step 4: Smoke — создание и refresh**

1. В модалке выбрать strategy (например, BULK_DIRECT), оставить остальные дефолты.
2. Нажать «Создать».
3. Модалка закрывается.
4. В строке этой таблицы должен появиться `MigrationBadge` (PLANNED/IN_PROGRESS).

Expected: миграция создана, бейдж обновился без F5.

- [ ] **Step 5: Smoke — блокировка при активной миграции**

1. Раскрыть строку таблицы, для которой только что создал миграцию.
2. В шапке `TableDetail` вместо кнопки — амбер-чип `Миграция: PLANNED` (или `IN_PROGRESS`).

Expected: кнопки нет, чип есть.

- [ ] **Step 6: Smoke — старый путь не сломан**

1. Sidebar → «Миграции» → «+ Добавить».
2. Открывается `CreateMigrationModal` без префилла (пустая форма).

Expected: всё как было.

- [ ] **Step 7: Финальный commit (если потребовались правки)**

Если smoke выявил баги — исправить, закоммитить:

```bash
git add -p
git commit -m "fix(ddl-catalog): <конкретный баг>"
```

Если всё ок — ничего не коммитить, просто отметить task как completed.
