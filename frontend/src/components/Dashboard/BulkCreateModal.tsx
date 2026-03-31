import { useState } from "react";

interface Props {
  mode: "individual" | "group";
  schema: string;
  tables: string[];
  onClose: () => void;
  onCreated: () => void;
}

type MigrationMode = "CDC" | "BULK_ONLY";
type MigrationStrategy = "STAGE" | "DIRECT";

const overlay: React.CSSProperties = {
  position: "fixed", inset: 0, background: "rgba(0,0,0,0.6)",
  display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1000,
};
const card: React.CSSProperties = {
  background: "#1e293b", border: "1px solid #334155", borderRadius: 8,
  padding: 24, maxWidth: 500, width: "100%", color: "#f1f5f9",
};
const header: React.CSSProperties = {
  display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20,
};
const title: React.CSSProperties = { fontSize: 18, fontWeight: 600, margin: 0 };
const closeBtn: React.CSSProperties = {
  background: "none", border: "none", color: "#94a3b8", cursor: "pointer", fontSize: 20, lineHeight: 1,
};
const label: React.CSSProperties = { display: "block", marginBottom: 4, fontSize: 13, color: "#94a3b8" };
const input: React.CSSProperties = {
  width: "100%", padding: "6px 10px", background: "#0f172a", border: "1px solid #334155",
  borderRadius: 6, color: "#f1f5f9", fontSize: 14, boxSizing: "border-box",
};
const select: React.CSSProperties = { ...input };
const fieldGroup: React.CSSProperties = { marginBottom: 14 };
const primaryBtn: React.CSSProperties = {
  background: "#3b82f6", border: "none", borderRadius: 6, color: "#fff",
  padding: "8px 18px", cursor: "pointer", fontWeight: 600, fontSize: 14,
};
const secondaryBtn: React.CSSProperties = {
  background: "#334155", border: "none", borderRadius: 6, color: "#f1f5f9",
  padding: "8px 18px", cursor: "pointer", fontSize: 14, marginRight: 8,
};
const tableList: React.CSSProperties = {
  maxHeight: 120, overflowY: "auto", background: "#0f172a",
  border: "1px solid #334155", borderRadius: 6, padding: "6px 10px", marginBottom: 14,
};
const progressText: React.CSSProperties = { color: "#94a3b8", fontSize: 13, marginTop: 12 };

export function BulkCreateModal({ mode, schema, tables, onClose, onCreated }: Props) {
  const [migrationMode, setMigrationMode] = useState<MigrationMode>(mode === "group" ? "CDC" : "BULK_ONLY");
  const [strategy, setStrategy] = useState<MigrationStrategy>("STAGE");
  const [groupName, setGroupName] = useState(`group-${schema.toLowerCase()}`);
  const [topicPrefix, setTopicPrefix] = useState("cdc");
  const [step, setStep] = useState<1 | 2>(1);
  const [progress, setProgress] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function createMigrations(groupId?: number) {
    for (let i = 0; i < tables.length; i++) {
      const table = tables[i];
      setProgress(`Создано ${i}/${tables.length}...`);
      const body: Record<string, unknown> = {
        migration_name: `${schema}.${table}`,
        source_schema: schema.toUpperCase(),
        source_table: table.toUpperCase(),
        target_schema: schema.toLowerCase(),
        target_table: table.toLowerCase(),
        stage_table_name: `STG_${table}`,
        migration_mode: migrationMode,
        migration_strategy: strategy,
      };
      if (groupId !== undefined) body.group_id = groupId;
      const res = await fetch("/api/migrations", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!res.ok) throw new Error(`Failed to create migration for ${table}`);
    }
  }

  async function handleIndividualCreate() {
    setBusy(true);
    setProgress("Создано 0/" + tables.length + "...");
    try {
      await createMigrations();
      setProgress(`Создано ${tables.length}/${tables.length}. Готово!`);
      setTimeout(onCreated, 800);
    } catch (e) {
      setProgress("Ошибка: " + (e instanceof Error ? e.message : String(e)));
      setBusy(false);
    }
  }

  async function handleGroupCreate() {
    setBusy(true);
    setProgress("Создание группы...");
    try {
      const groupRes = await fetch("/api/connector-groups", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ group_name: groupName, connector_name: groupName, topic_prefix: topicPrefix }),
      });
      if (!groupRes.ok) throw new Error("Failed to create connector group");
      const { group_id } = await groupRes.json();
      setProgress(`Создано 0/${tables.length}...`);
      await createMigrations(group_id);
      setProgress(`Создано ${tables.length}/${tables.length}. Готово!`);
      setTimeout(onCreated, 800);
    } catch (e) {
      setProgress("Ошибка: " + (e instanceof Error ? e.message : String(e)));
      setBusy(false);
    }
  }

  return (
    <div style={overlay} onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div style={card}>
        <div style={header}>
          <h2 style={title}>
            {mode === "individual" ? "Создать миграции" : "Создать группу и миграции"}
          </h2>
          <button style={closeBtn} onClick={onClose} disabled={busy}>×</button>
        </div>

        {mode === "individual" && (
          <>
            <div style={fieldGroup}>
              <span style={label}>Таблицы ({tables.length})</span>
              <div style={tableList}>
                {tables.map((t) => (
                  <div key={t} style={{ fontSize: 13, padding: "2px 0", color: "#cbd5e1" }}>{schema}.{t}</div>
                ))}
              </div>
            </div>
            <div style={fieldGroup}>
              <label style={label}>Migration mode</label>
              <select style={select} value={migrationMode} disabled={busy}
                onChange={(e) => setMigrationMode(e.target.value as MigrationMode)}>
                <option value="BULK_ONLY">BULK_ONLY</option>
                <option value="CDC">CDC</option>
              </select>
            </div>
            <div style={fieldGroup}>
              <label style={label}>Strategy</label>
              <select style={select} value={strategy} disabled={busy}
                onChange={(e) => setStrategy(e.target.value as MigrationStrategy)}>
                <option value="STAGE">STAGE</option>
                <option value="DIRECT">DIRECT</option>
              </select>
            </div>
            {progress && <div style={progressText}>{progress}</div>}
            <div style={{ marginTop: 18, display: "flex", justifyContent: "flex-end" }}>
              <button style={secondaryBtn} onClick={onClose} disabled={busy}>Отмена</button>
              <button style={primaryBtn} onClick={handleIndividualCreate} disabled={busy}>Создать</button>
            </div>
          </>
        )}

        {mode === "group" && step === 1 && (
          <>
            <div style={fieldGroup}>
              <label style={label}>Название группы</label>
              <input style={input} value={groupName}
                onChange={(e) => setGroupName(e.target.value)} />
            </div>
            <div style={fieldGroup}>
              <label style={label}>Topic prefix</label>
              <input style={input} value={topicPrefix}
                onChange={(e) => setTopicPrefix(e.target.value)} />
            </div>
            <div style={fieldGroup}>
              <label style={label}>Migration mode</label>
              <select style={select} value={migrationMode}
                onChange={(e) => setMigrationMode(e.target.value as MigrationMode)}>
                <option value="CDC">CDC</option>
                <option value="BULK_ONLY">BULK_ONLY</option>
              </select>
            </div>
            <div style={{ marginTop: 18, display: "flex", justifyContent: "flex-end" }}>
              <button style={secondaryBtn} onClick={onClose}>Отмена</button>
              <button style={primaryBtn} onClick={() => setStep(2)}
                disabled={!groupName.trim()}>Далее</button>
            </div>
          </>
        )}

        {mode === "group" && step === 2 && (
          <>
            <div style={{ marginBottom: 14, fontSize: 13, color: "#94a3b8", lineHeight: 1.6 }}>
              <div><b style={{ color: "#f1f5f9" }}>Группа:</b> {groupName}</div>
              <div><b style={{ color: "#f1f5f9" }}>Topic prefix:</b> {topicPrefix}</div>
              <div><b style={{ color: "#f1f5f9" }}>Mode:</b> {migrationMode}</div>
            </div>
            <div style={fieldGroup}>
              <span style={label}>Таблицы ({tables.length})</span>
              <div style={tableList}>
                {tables.map((t) => (
                  <div key={t} style={{ fontSize: 13, padding: "2px 0", color: "#cbd5e1" }}>{schema}.{t}</div>
                ))}
              </div>
            </div>
            {progress && <div style={progressText}>{progress}</div>}
            <div style={{ marginTop: 18, display: "flex", justifyContent: "flex-end" }}>
              <button style={secondaryBtn} onClick={() => setStep(1)} disabled={busy}>Назад</button>
              <button style={primaryBtn} onClick={handleGroupCreate} disabled={busy}>
                Создать группу и миграции
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
