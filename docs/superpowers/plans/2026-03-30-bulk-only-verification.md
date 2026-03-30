# BULK_ONLY Verification & PK-less Mode — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add automatic data verification (COUNT+HASH) as the final step of BULK_ONLY migrations, allow keyless tables for BULK_ONLY mode, and add a DATA_MISMATCH phase for user-driven resolution.

**Architecture:** Reuse existing `data_compare` infrastructure (ROWID chunking, worker-based COUNT+HASH). Add two new phases (`DATA_VERIFYING`, `DATA_MISMATCH`) to the migration state machine. Orchestrator creates a `data_compare_task` programmatically and monitors its completion. Frontend shows verification progress and mismatch action buttons.

**Tech Stack:** Python/Flask backend, PostgreSQL state DB, React/TypeScript frontend

**Spec:** `docs/superpowers/specs/2026-03-30-bulk-only-strategy-design.md`

---

### Task 1: Add `data_compare_task_id` column and update phase sets in state DB

**Files:**
- Modify: `backend/db/state_db.py` (lines 241-261 for ALTER TABLE block, lines 608-619 for _ACTIVE_PHASES)

- [ ] **Step 1: Add the new column to the ALTER TABLE block**

In `backend/db/state_db.py`, find the list of ALTER TABLE statements (around line 242-261). Add the new column at the end of the list:

```python
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS migration_mode            VARCHAR(32) NOT NULL DEFAULT 'CDC'",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS data_compare_task_id      UUID",
            ]:
```

- [ ] **Step 2: Update `_ACTIVE_PHASES` in `get_active_migrations`**

In `backend/db/state_db.py`, find `_ACTIVE_PHASES` inside `get_active_migrations()` (around line 610-619). Add the two new phases:

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

- [ ] **Step 3: Verify backend starts without errors**

Run: `cd backend && python -c "from db.state_db import ensure_schema; print('OK')"`
Expected: `OK` (imports succeed)

- [ ] **Step 4: Commit**

```bash
git add backend/db/state_db.py
git commit -m "feat: add data_compare_task_id column and DATA_VERIFYING/DATA_MISMATCH to active phases"
```

---

### Task 2: Update phase sets in routes and frontend types

**Files:**
- Modify: `backend/routes/migrations.py` (lines 16-28 for _VALID_PHASES, lines 381-389 for _ACTIVE_PHASES)
- Modify: `frontend/src/types/migration.ts` (lines 1-12 for MigrationPhase, lines 141-169 for PHASE_COLORS, lines 177-187 for ORDERED_PHASES)

- [ ] **Step 1: Update `_VALID_PHASES` in routes**

In `backend/routes/migrations.py`, find `_VALID_PHASES` (line 16-28). Add the two new phases:

```python
_VALID_PHASES = {
    "DRAFT", "NEW", "PREPARING", "SCN_FIXED",
    "CONNECTOR_STARTING", "CDC_BUFFERING",
    "TOPIC_CREATING",
    "CHUNKING", "BULK_LOADING", "BULK_LOADED",
    "STAGE_VALIDATING", "STAGE_VALIDATED",
    "BASELINE_PUBLISHING", "BASELINE_LOADING", "BASELINE_PUBLISHED",
    "STAGE_DROPPING", "INDEXES_ENABLING",
    "DATA_VERIFYING", "DATA_MISMATCH",
    "CDC_APPLY_STARTING", "CDC_APPLYING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP",
    "STEADY_STATE", "PAUSED",
    "CANCELLING", "CANCELLED",
    "COMPLETED", "FAILED",
}
```

- [ ] **Step 2: Update `_ACTIVE_PHASES` in routes**

In `backend/routes/migrations.py`, find `_ACTIVE_PHASES` (line 381-389). Add the new phases:

```python
_ACTIVE_PHASES = {
    "NEW", "PREPARING", "SCN_FIXED",
    "CONNECTOR_STARTING", "CDC_BUFFERING",
    "CHUNKING", "BULK_LOADING", "BULK_LOADED",
    "STAGE_VALIDATING", "STAGE_VALIDATED",
    "BASELINE_PUBLISHING", "BASELINE_PUBLISHED",
    "DATA_VERIFYING", "DATA_MISMATCH",
    "CDC_APPLY_STARTING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP",
    "STEADY_STATE",
}
```

- [ ] **Step 3: Update `MigrationPhase` type**

In `frontend/src/types/migration.ts`, find the `MigrationPhase` type (line 1-12). Add the new phases after `INDEXES_ENABLING`:

```typescript
export type MigrationPhase =
  | "DRAFT" | "NEW" | "PREPARING" | "SCN_FIXED"
  | "CONNECTOR_STARTING" | "CDC_BUFFERING"
  | "TOPIC_CREATING"
  | "CHUNKING" | "BULK_LOADING" | "BULK_LOADED"
  | "STAGE_VALIDATING" | "STAGE_VALIDATED"
  | "BASELINE_PUBLISHING" | "BASELINE_LOADING" | "BASELINE_PUBLISHED"
  | "STAGE_DROPPING" | "INDEXES_ENABLING"
  | "DATA_VERIFYING" | "DATA_MISMATCH"
  | "CDC_APPLY_STARTING" | "CDC_APPLYING" | "CDC_CATCHING_UP" | "CDC_CAUGHT_UP"
  | "STEADY_STATE" | "PAUSED"
  | "CANCELLING" | "CANCELLED"
  | "COMPLETED" | "FAILED";
```

- [ ] **Step 4: Add `PHASE_COLORS` entries**

In `frontend/src/types/migration.ts`, find `PHASE_COLORS` (line 141-169). Add entries after `INDEXES_ENABLING`:

```typescript
  INDEXES_ENABLING:    { bg: "#1a2e1a", text: "#86efac", border: "#15803d" },
  DATA_VERIFYING:      { bg: "#083344", text: "#67e8f9", border: "#0891b2" },
  DATA_MISMATCH:       { bg: "#431407", text: "#fdba74", border: "#ea580c" },
  CDC_APPLY_STARTING:  { bg: "#431407", text: "#fdba74", border: "#ea580c" },
```

- [ ] **Step 5: Add to `ORDERED_PHASES`**

In `frontend/src/types/migration.ts`, find `ORDERED_PHASES` (line 177-187). Add after `INDEXES_ENABLING`:

```typescript
export const ORDERED_PHASES: MigrationPhase[] = [
  "DRAFT", "NEW", "PREPARING", "SCN_FIXED",
  "CONNECTOR_STARTING", "CDC_BUFFERING",
  "TOPIC_CREATING",
  "CHUNKING", "BULK_LOADING", "BULK_LOADED",
  "STAGE_VALIDATING", "STAGE_VALIDATED",
  "BASELINE_PUBLISHING", "BASELINE_LOADING", "BASELINE_PUBLISHED",
  "STAGE_DROPPING", "INDEXES_ENABLING",
  "DATA_VERIFYING", "DATA_MISMATCH",
  "CDC_APPLY_STARTING", "CDC_APPLYING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP",
  "STEADY_STATE",
];
```

- [ ] **Step 6: Verify TypeScript compiles**

Run: `cd frontend && npx tsc --noEmit`
Expected: No errors

- [ ] **Step 7: Commit**

```bash
git add backend/routes/migrations.py frontend/src/types/migration.ts
git commit -m "feat: register DATA_VERIFYING and DATA_MISMATCH phases in routes and frontend types"
```

---

### Task 3: Remove PK gate for BULK_ONLY in orchestrator

**Files:**
- Modify: `backend/services/orchestrator.py` (lines 324-339 for `_handle_new`, lines 1214-1229 for `_handle_new_group`)

- [ ] **Step 1: Update `_handle_new` to skip PK check for BULK_ONLY**

In `backend/services/orchestrator.py`, find `_handle_new` (line 324). Replace the PK validation block (lines 330-339):

```python
def _handle_new(mid: str, m: dict) -> None:
    """
    Validate key columns, then transition to PREPARING — but only if the
    loading slot is free.  The gate is here (before SCN fixation) so that
    queued migrations don't accumulate a growing Kafka CDC backlog.
    """
    mode = (m.get("migration_mode") or "CDC").upper()
    pk = m.get("source_pk_exists", False)
    uk = m.get("source_uk_exists", False)
    key_cols = json.loads(m.get("effective_key_columns_json") or "[]")

    if mode != "BULK_ONLY" and not pk and not uk and not key_cols:
        _fail(mid,
              "Таблица не имеет PK/UK и ключевые колонки не заданы. "
              "Укажите ключевые колонки при создании миграции.",
              "NO_KEY_COLUMNS")
        return
```

- [ ] **Step 2: Update `_handle_new_group` to skip PK check for BULK_ONLY**

In `backend/services/orchestrator.py`, find `_handle_new_group` (line 1214). Replace the PK validation block (lines 1221-1229):

```python
def _handle_new_group(mid: str, m: dict) -> None:
    """Group migration: validate keys, queue gate, create stage, → TOPIC_CREATING.

    Unlike legacy NEW:
    - No SCN fixation
    - Connector already managed at group level
    """
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

- [ ] **Step 3: Commit**

```bash
git add backend/services/orchestrator.py
git commit -m "feat: allow BULK_ONLY migrations without PK/UK/user-defined keys"
```

---

### Task 4: Add DATA_VERIFYING and DATA_MISMATCH handlers to orchestrator

**Files:**
- Modify: `backend/services/orchestrator.py`

- [ ] **Step 1: Change BULK_ONLY branch in `_handle_indexes_enabling` (line 916-934)**

Replace the BULK_ONLY block inside the `_run()` inner function:

```python
            mode = (m.get("migration_mode") or "CDC").upper()
            if mode == "BULK_ONLY":
                # No CDC phase — enable triggers immediately, then verify data
                try:
                    oracle_browser.enable_triggers(
                        oracle_scn.open_oracle_conn(dst_cfg),
                        m["target_schema"], m["target_table"],
                    )
                except Exception as exc:
                    print(f"[orchestrator] {mid}: enable triggers warning: {exc}")

                msg = (
                    f"Включено: индексов={n_idx}, констрейнтов={n_con}. "
                    "Режим BULK_ONLY — запуск сверки данных"
                )
                _safe_transition(
                    mid, "INDEXES_ENABLING", "DATA_VERIFYING",
                    message=msg,
                    extra_fields={"error_code": None, "error_text": None},
                )
```

- [ ] **Step 2: Change BULK_ONLY branch in `_handle_indexes_enabling_group` (line 1432-1448)**

Same change for the group handler:

```python
            mode = (m.get("migration_mode") or "CDC").upper()
            if mode == "BULK_ONLY":
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
                        "Режим BULK_ONLY — запуск сверки данных"
                    ),
                    extra_fields={"error_code": None, "error_text": None},
                )
```

- [ ] **Step 3: Add `_handle_data_verifying` handler**

Add this function after `_handle_indexes_enabling_group` in `orchestrator.py`. It follows the daemon thread pattern used by other handlers:

```python
def _handle_data_verifying(mid: str, m: dict) -> None:
    """Create data_compare task on first tick, then monitor its completion."""
    task_id = m.get("data_compare_task_id")

    if not task_id:
        # First tick — create the data_compare task in a daemon thread
        if _in_prog(mid):
            return
        _mark_in_prog(mid)

        def _run():
            try:
                conn = _state["get_conn"]()
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO data_compare_tasks
                                (source_schema, source_table, target_schema, target_table,
                                 compare_mode, chunk_size, status)
                            VALUES (%s, %s, %s, %s, 'full', %s, 'PENDING')
                            RETURNING task_id
                        """, (m["source_schema"], m["source_table"],
                              m["target_schema"], m["target_table"],
                              m.get("chunk_size") or 100_000))
                        new_task_id = str(cur.fetchone()[0])

                        cur.execute(
                            "UPDATE migrations SET data_compare_task_id = %s, updated_at = NOW() "
                            "WHERE migration_id = %s",
                            (new_task_id, mid))
                    conn.commit()
                finally:
                    conn.close()

                # Launch chunking in background (reuse data_compare logic)
                from routes.data_compare import _create_chunks_and_start
                configs = _state["load_configs"]()
                threading.Thread(
                    target=_create_chunks_and_start,
                    args=(new_task_id, configs,
                          m["source_schema"], m["source_table"],
                          m["target_schema"], m["target_table"],
                          m.get("chunk_size") or 100_000),
                    daemon=True,
                    name=f"dv-chunk-{mid[:8]}",
                ).start()

                print(f"[orchestrator] {mid}: data_compare task created: {new_task_id}")

            except Exception as exc:
                if _current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                    _fail(mid, f"Ошибка создания сверки: {exc}", "DATA_VERIFY_ERROR")
            finally:
                _unmark_in_prog(mid)

        threading.Thread(target=_run, daemon=True, name=f"dv-init-{mid[:8]}").start()
        return

    # Subsequent ticks — check data_compare task status
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, counts_match, hash_match, "
                "       source_count, target_count, chunks_done, chunks_total, error_text "
                "FROM data_compare_tasks WHERE task_id = %s",
                (task_id,))
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        _fail(mid, f"data_compare task {task_id} not found", "DATA_VERIFY_ERROR")
        return

    status, counts_match, hash_match, src_count, tgt_count, done, total, err_text = row

    if status == "FAILED":
        _fail(mid, f"Сверка данных завершилась ошибкой: {err_text or 'unknown'}", "DATA_VERIFY_ERROR")
        return

    if status not in ("DONE", "COMPLETED"):
        return  # Still running

    # Verification complete — check results
    if counts_match and hash_match:
        _safe_transition(
            mid, "DATA_VERIFYING", "COMPLETED",
            message=(
                f"Сверка данных пройдена. Source: {src_count}, Target: {tgt_count}. "
                "COUNT и HASH совпадают."
            ),
            extra_fields={"error_code": None, "error_text": None},
        )
    else:
        details = []
        if not counts_match:
            details.append(f"COUNT mismatch: source={src_count}, target={tgt_count}")
        if not hash_match:
            details.append("HASH mismatch")
        _safe_transition(
            mid, "DATA_VERIFYING", "DATA_MISMATCH",
            message=f"Сверка выявила расхождения: {'; '.join(details)}",
            extra_fields={
                "error_code": "DATA_MISMATCH",
                "error_text": f"source_count={src_count}, target_count={tgt_count}, "
                              f"counts_match={counts_match}, hash_match={hash_match}",
            },
        )
```

- [ ] **Step 4: Add `_handle_data_mismatch` handler**

Add right after `_handle_data_verifying`:

```python
def _handle_data_mismatch(mid: str, m: dict) -> None:
    """Idle phase — wait for user action (retry_verify, force_complete, cancel)."""
    pass
```

- [ ] **Step 5: Register handlers in dispatch maps**

Add to `_LEGACY_HANDLERS` dict (after `INDEXES_ENABLING` entry, around line 155):

```python
    "INDEXES_ENABLING":     lambda mid, m: _handle_indexes_enabling(mid, m),
    "DATA_VERIFYING":       lambda mid, m: _handle_data_verifying(mid, m),
    "DATA_MISMATCH":        lambda mid, m: _handle_data_mismatch(mid, m),
    "CDC_APPLY_STARTING":   lambda mid, m: _handle_cdc_apply_starting(mid, m),
```

Add to `_GROUP_HANDLERS` dict (after `INDEXES_ENABLING` entry, around line 176):

```python
    "INDEXES_ENABLING":     lambda mid, m: _handle_indexes_enabling_group(mid, m),
    "DATA_VERIFYING":       lambda mid, m: _handle_data_verifying(mid, m),
    "DATA_MISMATCH":        lambda mid, m: _handle_data_mismatch(mid, m),
    "CDC_APPLYING":         lambda mid, m: _handle_cdc_applying(mid, m),
```

- [ ] **Step 6: Verify import works**

Run: `cd backend && python -c "from services.orchestrator import _LEGACY_HANDLERS; print('handlers:', len(_LEGACY_HANDLERS))"`
Expected: `handlers: 22` (20 existing + 2 new)

- [ ] **Step 7: Commit**

```bash
git add backend/services/orchestrator.py
git commit -m "feat: add DATA_VERIFYING and DATA_MISMATCH orchestrator handlers"
```

---

### Task 5: Add user actions for DATA_MISMATCH phase

**Files:**
- Modify: `backend/routes/migrations.py` (lines 373-379 for _ACTION_TRANSITIONS, lines 392-468 for action handler)

- [ ] **Step 1: Add new actions to `_ACTION_TRANSITIONS`**

In `backend/routes/migrations.py`, find `_ACTION_TRANSITIONS` (line 373-379). Add the new entries:

```python
_ACTION_TRANSITIONS = {
    "run":            ("DRAFT",          "NEW"),
    "pause":          (None,             "PAUSED"),
    "resume":         ("PAUSED",         "BULK_LOADING"),
    "cancel":         (None,             "CANCELLING"),
    "lag_zero":       ("CDC_CATCHING_UP", "CDC_CAUGHT_UP"),
    "retry_verify":   ("DATA_MISMATCH",  "DATA_VERIFYING"),
    "force_complete": ("DATA_MISMATCH",  "COMPLETED"),
}
```

- [ ] **Step 2: Add cleanup logic for `retry_verify` action**

In the action handler (inside `migration_action`, around line 427), add a block to clear the old task_id when retrying. Add before the `with conn.cursor() as cur:` UPDATE block (before line 441):

```python
            if action == "retry_verify":
                # Clear old data_compare task reference so orchestrator creates a new one
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE migrations SET data_compare_task_id = NULL "
                        "WHERE migration_id = %s",
                        (migration_id,))
```

- [ ] **Step 3: Commit**

```bash
git add backend/routes/migrations.py
git commit -m "feat: add retry_verify and force_complete actions for DATA_MISMATCH phase"
```

---

### Task 6: Add DATA_MISMATCH action buttons to frontend

**Files:**
- Modify: `frontend/src/components/MigrationDetail.tsx` (lines 84-104 for phase sets, lines 1438-1450 for action buttons)

- [ ] **Step 1: Add DataMismatchButtons component**

Add this component near the other action button components (after `EnableTriggersButton`, around line 1222):

```typescript
function DataMismatchButtons({ migrationId, onDone }: { migrationId: string; onDone: () => void }) {
  const [busy, setBusy] = useState(false);
  const [errMsg, setErrMsg] = useState<string | null>(null);

  async function doAction(action: string, confirmMsg?: string) {
    if (confirmMsg && !confirm(confirmMsg)) return;
    setBusy(true);
    setErrMsg(null);
    try {
      const r = await fetch(`/api/migrations/${migrationId}/action`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action }),
      });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        setErrMsg(d.error ?? `Ошибка ${r.status}`);
      } else {
        onDone();
      }
    } catch (e) {
      setErrMsg(String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <>
      <button
        onClick={() => doAction("retry_verify")}
        disabled={busy}
        style={{
          background: "#1e3a5f", border: "1px solid #1d4ed8", borderRadius: 5,
          color: "#93c5fd", padding: "4px 12px", fontSize: 11, fontWeight: 600,
          cursor: busy ? "not-allowed" : "pointer", opacity: busy ? 0.5 : 1,
        }}
      >
        Повторить сверку
      </button>
      <button
        onClick={() => doAction("force_complete", "Завершить миграцию без успешной сверки данных?")}
        disabled={busy}
        style={{
          background: "#431407", border: "1px solid #ea580c", borderRadius: 5,
          color: "#fdba74", padding: "4px 12px", fontSize: 11, fontWeight: 600,
          cursor: busy ? "not-allowed" : "pointer", opacity: busy ? 0.5 : 1,
        }}
      >
        Завершить принудительно
      </button>
      {errMsg && (
        <span style={{ fontSize: 10, color: "#fca5a5" }}>{errMsg}</span>
      )}
    </>
  );
}
```

- [ ] **Step 2: Render DataMismatchButtons for DATA_MISMATCH phase**

In the action buttons section (around line 1438-1450), add after the `EnableIndexesButton` block:

```typescript
          {phase === "DATA_MISMATCH" && (
            <DataMismatchButtons migrationId={migrationId} onDone={loadDetail} />
          )}
```

- [ ] **Step 3: Add DATA_VERIFYING to phase sets that show progress info**

In `MigrationDetail.tsx`, add `DATA_VERIFYING` to `BULK_PHASES` (line 84) so that chunk progress is visible:

```typescript
const BULK_PHASES = new Set(["CHUNKING", "BULK_LOADING", "BULK_LOADED", "BASELINE_LOADING", "DATA_VERIFYING"]);
```

- [ ] **Step 4: Verify TypeScript compiles**

Run: `cd frontend && npx tsc --noEmit`
Expected: No errors

- [ ] **Step 5: Commit**

```bash
git add frontend/src/components/MigrationDetail.tsx
git commit -m "feat: add DATA_MISMATCH action buttons and DATA_VERIFYING progress display"
```

---

### Task 7: Add verification info display to MigrationDetail

**Files:**
- Modify: `frontend/src/components/MigrationDetail.tsx`

- [ ] **Step 1: Add verification results card**

Find where phase-specific info cards are rendered (near the error banners or detail sections). Add a card that shows when the migration is in `DATA_VERIFYING` or `DATA_MISMATCH`:

```typescript
{(phase === "DATA_VERIFYING" || phase === "DATA_MISMATCH") && detail?.data_compare_task_id && (
  <DataVerifyCard taskId={detail.data_compare_task_id} phase={phase} />
)}
```

- [ ] **Step 2: Create DataVerifyCard component**

Add near the other card components:

```typescript
function DataVerifyCard({ taskId, phase }: { taskId: string; phase: string }) {
  const [info, setInfo] = useState<any>(null);

  useEffect(() => {
    let alive = true;
    function load() {
      fetch(`/api/data-compare/${taskId}`)
        .then(r => r.ok ? r.json() : null)
        .then(d => { if (alive && d) setInfo(d); });
    }
    load();
    const iv = setInterval(load, 3000);
    return () => { alive = false; clearInterval(iv); };
  }, [taskId]);

  if (!info) return null;

  const progress = info.chunks_total > 0
    ? Math.round((info.chunks_done / info.chunks_total) * 100)
    : 0;

  return (
    <div style={{
      background: "#0a111f", border: "1px solid #1e293b", borderRadius: 6,
      padding: 12, marginTop: 8,
    }}>
      <div style={{ fontSize: 12, fontWeight: 600, color: "#e2e8f0", marginBottom: 8 }}>
        Сверка данных
        {phase === "DATA_VERIFYING" && (
          <span style={{ color: "#67e8f9", fontWeight: 400, marginLeft: 8 }}>
            {info.status === "RUNNING" ? `${progress}% (${info.chunks_done}/${info.chunks_total})` : info.status}
          </span>
        )}
      </div>
      {(info.source_count != null || info.target_count != null) && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 10, fontSize: 11 }}>
          <div>
            <div style={{ color: "#64748b" }}>Source count</div>
            <div style={{ color: "#e2e8f0", fontWeight: 600 }}>
              {info.source_count?.toLocaleString("ru-RU") ?? "—"}
            </div>
          </div>
          <div>
            <div style={{ color: "#64748b" }}>Target count</div>
            <div style={{ color: "#e2e8f0", fontWeight: 600 }}>
              {info.target_count?.toLocaleString("ru-RU") ?? "—"}
            </div>
          </div>
          <div>
            <div style={{ color: "#64748b" }}>Результат</div>
            <div style={{ fontWeight: 600 }}>
              {info.counts_match === null
                ? <span style={{ color: "#475569" }}>Ожидание</span>
                : info.counts_match && info.hash_match
                  ? <span style={{ color: "#86efac" }}>OK</span>
                  : <span style={{ color: "#fca5a5" }}>
                      {!info.counts_match ? "COUNT mismatch" : "HASH mismatch"}
                    </span>
              }
            </div>
          </div>
        </div>
      )}
      {info.error_text && (
        <div style={{ fontSize: 10, color: "#fca5a5", marginTop: 6 }}>{info.error_text}</div>
      )}
    </div>
  );
}
```

- [ ] **Step 3: Verify TypeScript compiles**

Run: `cd frontend && npx tsc --noEmit`
Expected: No errors

- [ ] **Step 4: Commit**

```bash
git add frontend/src/components/MigrationDetail.tsx
git commit -m "feat: add DataVerifyCard showing verification progress and results"
```

---

### Task 8: Expose `data_compare_task_id` in migration API response

**Files:**
- Modify: `backend/routes/migrations.py`

- [ ] **Step 1: Ensure `data_compare_task_id` is included in migration detail response**

Check the GET `/api/migrations/<id>` endpoint. The migration record is fetched from state_db as a dict — if `state_db.get_migration()` returns all columns as a dict, the new `data_compare_task_id` column will be included automatically. Verify by reading `state_db.get_migration()`.

If it uses `SELECT *` or fetches all columns, no change is needed. If it enumerates columns explicitly, add `data_compare_task_id` to the SELECT list.

- [ ] **Step 2: Verify the field appears in API response**

Run (or check manually after starting the backend):
```bash
curl -s http://localhost:5000/api/migrations | python -m json.tool | grep data_compare
```
Expected: `"data_compare_task_id": null` appears in each migration record.

- [ ] **Step 3: Commit (if changes were needed)**

```bash
git add backend/routes/migrations.py backend/db/state_db.py
git commit -m "feat: expose data_compare_task_id in migration API response"
```

---

### Task 9: End-to-end verification

- [ ] **Step 1: Start backend and verify schema migration**

Run: `cd backend && python app.py`
Expected: No schema errors, new `data_compare_task_id` column created.

- [ ] **Step 2: Verify phase sets are consistent**

Check that all three locations have the new phases:
- `state_db.py` `_ACTIVE_PHASES` — DATA_VERIFYING, DATA_MISMATCH
- `migrations.py` `_VALID_PHASES` — DATA_VERIFYING, DATA_MISMATCH
- `migrations.py` `_ACTIVE_PHASES` — DATA_VERIFYING, DATA_MISMATCH

- [ ] **Step 3: Verify frontend compiles**

Run: `cd frontend && npx tsc --noEmit`
Expected: No errors

- [ ] **Step 4: Test BULK_ONLY flow conceptually**

Trace the new flow:
1. Create migration with `migration_mode=BULK_ONLY`, no PK → should NOT fail in `_handle_new`
2. Migration proceeds through PREPARING → SCN_FIXED → CHUNKING → BULK_LOADING → ... → INDEXES_ENABLING
3. INDEXES_ENABLING → DATA_VERIFYING (not COMPLETED)
4. Orchestrator creates data_compare task, workers process chunks
5. On match → COMPLETED
6. On mismatch → DATA_MISMATCH, user sees buttons
7. retry_verify → clears task_id → DATA_VERIFYING (new task)
8. force_complete → COMPLETED

- [ ] **Step 5: Final commit if any fixups needed**

```bash
git add -u
git commit -m "fix: end-to-end verification fixups for BULK_ONLY data verification"
```
