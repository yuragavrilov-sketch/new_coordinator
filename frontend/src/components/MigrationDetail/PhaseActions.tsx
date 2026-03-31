import React, { useEffect, useState } from "react";

// ── EnableIndexesButton ───────────────────────────────────────────────────────

export function EnableIndexesButton({ migrationId, onDone }: { migrationId: string; onDone: () => void }) {
  const [busy,  setBusy]  = useState(false);
  const [errMsg, setErrMsg] = useState<string | null>(null);

  async function handleClick() {
    setBusy(true);
    setErrMsg(null);
    try {
      const r = await fetch(`/api/migrations/${migrationId}/enable-indexes`, { method: "POST" });
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
    <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-start", gap: 4 }}>
      <button
        onClick={handleClick}
        disabled={busy}
        style={{
          background: busy ? "#1e3a5f" : "#1d4ed8",
          color: busy ? "#64748b" : "#e2e8f0",
          border: "1px solid #2563eb",
          borderRadius: 5,
          padding: "5px 14px",
          fontSize: 12,
          fontWeight: 600,
          cursor: busy ? "not-allowed" : "pointer",
        }}
      >
        {busy ? "Запуск..." : "Включить индексы"}
      </button>
      {errMsg && (
        <span style={{ fontSize: 11, color: "#fca5a5" }}>{errMsg}</span>
      )}
    </div>
  );
}

// ── EnableTriggersButton ─────────────────────────────────────────────────────

export function EnableTriggersButton({ migrationId, onDone }: { migrationId: string; onDone: () => void }) {
  const [busy, setBusy]   = useState(false);
  const [errMsg, setErrMsg] = useState<string | null>(null);
  const [done, setDone]   = useState(false);

  async function handleClick() {
    setBusy(true);
    setErrMsg(null);
    try {
      const r = await fetch(`/api/migrations/${migrationId}/enable-triggers`, { method: "POST" });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        setErrMsg(d.error ?? `Ошибка ${r.status}`);
      } else {
        setDone(true);
        onDone();
      }
    } catch (e) {
      setErrMsg(String(e));
    } finally {
      setBusy(false);
    }
  }

  if (done) {
    return <span style={{ fontSize: 11, color: "#22c55e", fontWeight: 600 }}>Триггеры включены</span>;
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-start", gap: 4 }}>
      <button
        onClick={handleClick}
        disabled={busy}
        style={{
          background: busy ? "#1e3a2f" : "#15803d",
          color: busy ? "#64748b" : "#e2e8f0",
          border: "1px solid #166534",
          borderRadius: 5,
          padding: "5px 14px",
          fontSize: 12,
          fontWeight: 600,
          cursor: busy ? "not-allowed" : "pointer",
        }}
      >
        {busy ? "Включение..." : "Включить триггеры"}
      </button>
      {errMsg && (
        <span style={{ fontSize: 11, color: "#fca5a5" }}>{errMsg}</span>
      )}
    </div>
  );
}

// ── RestartBaselineButton ─────────────────────────────────────────────────────

export function RestartBaselineButton({ migrationId, onDone }: { migrationId: string; onDone: () => void }) {
  const [busy, setBusy]     = useState(false);
  const [errMsg, setErrMsg] = useState<string | null>(null);

  async function handleClick() {
    if (!confirm("Перезапустить baseline? Целевая таблица будет очищена (TRUNCATE) и загрузка начнётся заново.")) return;
    setBusy(true);
    setErrMsg(null);
    try {
      const r = await fetch(`/api/migrations/${migrationId}/restart-baseline`, { method: "POST" });
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
    <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-start", gap: 4 }}>
      <button
        onClick={handleClick}
        disabled={busy}
        style={{
          background: busy ? "#3b2000" : "#92400e",
          color: busy ? "#64748b" : "#fef3c7",
          border: "1px solid #d97706",
          borderRadius: 5,
          padding: "5px 14px",
          fontSize: 12,
          fontWeight: 600,
          cursor: busy ? "not-allowed" : "pointer",
        }}
      >
        {busy ? "Перезапуск..." : "Перезапустить baseline"}
      </button>
      {errMsg && (
        <span style={{ fontSize: 11, color: "#fca5a5" }}>{errMsg}</span>
      )}
    </div>
  );
}

// ── DataMismatchButtons ────────────────────────────────────────────────────

export function DataMismatchButtons({ migrationId, onDone }: { migrationId: string; onDone: () => void }) {
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

// ── DataVerifyCard ─────────────────────────────────────────────────────────

export function DataVerifyCard({ taskId, phase }: { taskId: string; phase: string }) {
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
      padding: 12, marginBottom: 14,
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

// ── StopDeleteButtons ─────────────────────────────────────────────────────────

const _ACTIVE = new Set([
  "NEW", "PREPARING", "SCN_FIXED", "CONNECTOR_STARTING", "CDC_BUFFERING",
  "CHUNKING", "BULK_LOADING", "BULK_LOADED",
  "STAGE_VALIDATING", "STAGE_VALIDATED",
  "BASELINE_PUBLISHING", "BASELINE_LOADING", "BASELINE_PUBLISHED",
  "STAGE_DROPPING", "INDEXES_ENABLING",
  "CDC_APPLY_STARTING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP", "STEADY_STATE",
]);
const _DELETABLE = new Set(["DRAFT", "CANCELLING", "CANCELLED", "FAILED"]);

export function StopDeleteButtons({ migrationId, phase, onDone, onDeleted }: {
  migrationId: string; phase: string; onDone: () => void; onDeleted: () => void;
}) {
  const [busy, setBusy] = useState(false);

  const canStop   = _ACTIVE.has(phase);
  const canDelete = _DELETABLE.has(phase);

  if (!canStop && !canDelete) return null;

  async function doStop() {
    if (!confirm("Остановить миграцию? Текущая операция завершится, после чего миграция перейдёт в CANCELLED.")) return;
    setBusy(true);
    try {
      await fetch(`/api/migrations/${migrationId}/action`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "cancel" }),
      });
      onDone();
    } finally { setBusy(false); }
  }

  async function doDelete() {
    if (!confirm("Удалить миграцию? Это действие необратимо.")) return;
    setBusy(true);
    try {
      const r = await fetch(`/api/migrations/${migrationId}`, { method: "DELETE" });
      if (r.ok) { onDeleted(); } else { onDone(); }
    } finally { setBusy(false); }
  }

  const btnStyle = (bg: string, border: string, color: string): React.CSSProperties => ({
    background: busy ? "#1e293b" : bg,
    color: busy ? "#475569" : color,
    border: `1px solid ${border}`,
    borderRadius: 4, padding: "3px 10px",
    fontSize: 11, fontWeight: 600,
    cursor: busy ? "not-allowed" : "pointer",
  });

  return (
    <div style={{ display: "flex", gap: 4 }}>
      {canStop && (
        <button onClick={doStop} disabled={busy} style={btnStyle("#450a0a", "#7f1d1d", "#fca5a5")}>
          Остановить
        </button>
      )}
      {canDelete && (
        <button onClick={doDelete} disabled={busy} style={btnStyle("#450a0a", "#7f1d1d", "#fca5a5")}>
          Удалить
        </button>
      )}
    </div>
  );
}
