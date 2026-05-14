# Кнопка «Создать миграцию» в карточке таблицы DDL-каталога

**Дата:** 2026-05-14
**Статус:** draft → implementation

## Контекст

В DDL-каталоге (sidebar → «DDL-миграции») есть вкладка «Таблицы». Каждая строка — таблица из source-схемы; клик по ▶ раскрывает `TableDetail` с колонками, индексами, ограничениями, триггерами и Diff-сводкой.

Сейчас создать миграцию для одной таблицы можно только так:

1. Перейти в sidebar → «Миграции» → «+ Добавить» → заполнить всю форму с нуля.
2. Перейти в sidebar → «Кластеры» → раскрыть группу → «Migrate» в строке таблицы (только для таблиц внутри уже настроенной CDC-группы).
3. Из самого DDL-каталога — поставить чекбокс на одной/нескольких таблицах → «Визард миграции» → batch-планнер.

Все три пути отрывают пользователя от того места, где он *уже изучает таблицу*. Цель — добавить четвёртый, контекстный путь: кнопка прямо в раскрытой карточке таблицы.

## Цель

Из карточки таблицы DDL-каталога одним кликом открывать существующий `CreateMigrationModal` с префиллом из контекста (src/tgt schema + table). Пользователь докручивает strategy, key, chunk_size и сабмитит.

**Out of scope:**
- 1-click создание без модалки (отвергнуто как агрессивная опция в brainstorm).
- Новый мини-визард (отвергнут — переиспользуем существующий модал).
- Изменения API `/api/migrations`.
- Кнопка вне раскрытой карточки (в строке/в ObjectActions).

## Архитектура

Изменения только во фронте, четыре компонента:

| Компонент | Что меняется |
|---|---|
| `CreateMigrationModal` | Принимает optional prop `prefill?: Prefill`. Инициализирует form-state с префилл-значениями. Поля остаются редактируемыми. |
| `TableDetail` | Принимает props `srcSchema`, `tgtSchema`, `migrationStatus`, `onMigrate(prefill)`. Рендерит кнопку «Создать миграцию» в шапке или чип «Уже мигрируется». |
| `TablesTab` | Пробрасывает `srcSchema`/`tgtSchema` в `TableDetail`, поднимает `onMigrate` к родителю. |
| `DDLCatalog` | Держит state `migrateModalPrefill: Prefill \| null`, рендерит модалку, по `onCreated` перезагружает текущий таб. |

Тип `Prefill`:

```ts
interface MigrationPrefill {
  source_schema: string;
  source_table:  string;
  target_schema: string;
  target_table:  string;
}
```

Кнопка показывается, когда:

- `srcSchema && tgtSchema` (защита от состояния «снапшот ещё не загружен»);
- `obj.migration_status ∉ {"PLANNED", "IN_PROGRESS"}` — нет активной миграции для этой таблицы.

Состояния `COMPLETED`/`FAILED`/прочие (включая `NONE`/пустую строку) — кнопка активна (повторная миграция допустима, например после rollback'а target'а). Если активная миграция есть (`PLANNED` или `IN_PROGRESS`) — вместо кнопки в шапке `TableDetail` отрисуется чип «Миграция: <status>».

## Data flow

```
TableDetail.button onClick
  → onMigrate({src_schema, src_table=obj.object_name, tgt_schema, tgt_table=obj.object_name})
  → TablesTab.props.onMigrate
  → DDLCatalog.setMigrateModalPrefill(prefill)
  → <CreateMigrationModal prefill={...} onClose={() => setMigrateModalPrefill(null)} onCreated={handleCreated}/>

handleCreated:
  setMigrateModalPrefill(null)
  loadObjectsForTab(activeTab)  // refresh migration_status badges
```

## Изменения в `CreateMigrationModal`

Сейчас:

```ts
interface Props { onClose: () => void; onCreated: () => void }
```

Станет:

```ts
interface MigrationPrefill {
  source_schema: string;
  source_table:  string;
  target_schema: string;
  target_table:  string;
}

interface Props {
  onClose: () => void;
  onCreated: () => void;
  prefill?: MigrationPrefill;
}
```

Внутри `useState<FormData>(initialForm)` — `initialForm` вычисляется один раз с учётом `prefill`:

```ts
const initialForm: FormData = useMemo(() => ({
  ...DEFAULT_FORM,
  source_schema: prefill?.source_schema ?? "",
  source_table:  prefill?.source_table  ?? "",
  target_schema: prefill?.target_schema ?? "",
  target_table:  prefill?.target_table  ?? "",
}), []);  // intentionally empty deps — prefill captured on mount
```

Поля редактируемы — никаких дополнительных `disabled`. Это сохраняет гибкость и не плодит код-веток.

## Изменения в `TableDetail`

Добавляются props:

```ts
interface TableDetailProps {
  obj: CatalogObject;
  snapshotId: number | null;
  srcSchema: string;
  tgtSchema: string;
  onMigrate: (prefill: MigrationPrefill) => void;
}
```

В начале JSX рендерится header-строка:

- Если `obj.migration_status ∉ {"PLANNED", "IN_PROGRESS"}` и `srcSchema && tgtSchema` — кнопка **«Создать миграцию»** (зелёная primary, как `t.green.bg`).
- Иначе — чип `Миграция: <obj.migration_status>` в нейтральном стиле.

Кнопка onClick:

```ts
onMigrate({
  source_schema: srcSchema,
  source_table:  obj.object_name,
  target_schema: tgtSchema,
  target_table:  obj.object_name,
});
```

## Изменения в `TablesTab`

Добавляются props `srcSchema`, `tgtSchema`, `onMigrate`. Передаются в `TableDetail` ниже в `<TableDetail obj={obj} snapshotId={snapshotId} srcSchema={srcSchema} tgtSchema={tgtSchema} onMigrate={onMigrate}/>`.

## Изменения в `DDLCatalog`

```ts
const [migrateModalPrefill, setMigrateModalPrefill] = useState<MigrationPrefill | null>(null);

// ...

<TablesTab
  // ...existing props
  srcSchema={srcSchema}
  tgtSchema={tgtSchema}
  onMigrate={setMigrateModalPrefill}
/>

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

## Edge cases

- **`srcSchema`/`tgtSchema` пустые** — снапшота нет, таблиц не видно, проблема не возникает. Defensive: кнопка не рендерится без обеих схем.
- **`migration_status` ∈ {PLANNED, IN_PROGRESS}** — кнопки нет, есть чип. `COMPLETED`/`FAILED` трактуем как «можно мигрировать заново» — кнопка остаётся.
- **prefill не передан** — модалка работает как раньше (форма пустая, как при открытии из `MigrationList`).
- **Юзер изменил prefilled поля в форме** — это его право, форма не лочит ничего.
- **Модалку закрыли без submit** — `setMigrateModalPrefill(null)`, никаких сайд-эффектов.

## Тестирование

- `tsc --noEmit` — обязательный gate.
- Manual smoke:
  1. Раскрыть таблицу без миграции → кнопка есть → клик → модалка открыта, src/tgt префилл, остальное по дефолту → submit → бейдж миграции появился в строке.
  2. Раскрыть таблицу с активной миграцией → кнопки нет, есть чип с фазой.
  3. Открыть модалку → закрыть крестиком → состояние чистое.
  4. Открыть из `MigrationList` («+ Добавить») — старый путь работает (без prefill).

## Объём

3-4 файла, ~50-80 строк дельты. Один план на ~5 задач.
