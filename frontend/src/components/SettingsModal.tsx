import React, { useEffect, useState } from "react";

// ─── Types ───────────────────────────────────────────────────────────────────

interface OracleConfig {
  host: string;
  port: string;
  service_name: string;
  schema: string;
  user: string;
  password: string;
}

interface KafkaConfig {
  bootstrap_servers: string;
}

interface ConnectConfig {
  url: string;
}

interface AllConfigs {
  oracle_source: OracleConfig;
  oracle_target: OracleConfig;
  kafka: KafkaConfig;
  kafka_connect: ConnectConfig;
}

const ORACLE_DEFAULT: OracleConfig = {
  host: "", port: "1521", service_name: "", schema: "", user: "", password: "",
};
const KAFKA_DEFAULT: KafkaConfig   = { bootstrap_servers: "" };
const CONNECT_DEFAULT: ConnectConfig = { url: "" };

type TabKey = "oracle_source" | "oracle_target" | "kafka" | "kafka_connect";

const TABS: { key: TabKey; label: string }[] = [
  { key: "oracle_source", label: "Oracle Source" },
  { key: "oracle_target", label: "Oracle Target" },
  { key: "kafka",         label: "Kafka"         },
  { key: "kafka_connect", label: "Connect"       },
];

// ─── Field ───────────────────────────────────────────────────────────────────

function Field({
  label, value, onChange, type = "text", placeholder, span,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  type?: string;
  placeholder?: string;
  span?: number; // grid column span
}) {
  return (
    <div style={{ marginBottom: 12, gridColumn: span ? `span ${span}` : undefined }}>
      <label style={{
        display: "block", fontSize: 11, color: "#64748b",
        marginBottom: 4, fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.4,
      }}>
        {label}
      </label>
      <input
        type={type}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        autoComplete="off"
        style={{
          width: "100%", background: "#0f172a", border: "1px solid #334155",
          borderRadius: 5, color: "#e2e8f0", padding: "6px 10px", fontSize: 13,
        }}
      />
    </div>
  );
}

// ─── Main modal ──────────────────────────────────────────────────────────────

interface Props {
  onClose: () => void;
}

export function SettingsModal({ onClose }: Props) {
  const [activeTab, setActiveTab] = useState<TabKey>("oracle_source");
  const [configs, setConfigs] = useState<AllConfigs>({
    oracle_source: { ...ORACLE_DEFAULT },
    oracle_target: { ...ORACLE_DEFAULT },
    kafka:         { ...KAFKA_DEFAULT },
    kafka_connect: { ...CONNECT_DEFAULT },
  });
  const [saving,      setSaving]      = useState(false);
  const [saved,       setSaved]       = useState(false);
  const [error,       setError]       = useState<string | null>(null);
  const [testing,     setTesting]     = useState(false);
  const [testResult,  setTestResult]  = useState<{ status: string; message: string } | null>(null);

  useEffect(() => {
    fetch("/api/config")
      .then((r) => r.json())
      .then((data: Record<string, Record<string, string>>) => {
        setConfigs({
          oracle_source: { ...ORACLE_DEFAULT, ...(data.oracle_source ?? {}) },
          oracle_target: { ...ORACLE_DEFAULT, ...(data.oracle_target ?? {}) },
          kafka:         { ...KAFKA_DEFAULT,  ...(data.kafka         ?? {}) },
          kafka_connect: { ...CONNECT_DEFAULT,...(data.kafka_connect ?? {}) },
        });
      })
      .catch(() => setError("Не удалось загрузить конфиги"));
  }, []);

  async function testConnection() {
    setTesting(true);
    setTestResult(null);
    try {
      const r = await fetch(`/api/config/${activeTab}/test`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(configs[activeTab]),
      });
      const d = await r.json();
      setTestResult(d);
    } catch (e: unknown) {
      setTestResult({ status: "down", message: e instanceof Error ? e.message : String(e) });
    } finally {
      setTesting(false);
    }
  }

  async function saveActive() {
    setSaving(true);
    setError(null);
    setTestResult(null);
    const body = configs[activeTab];
    try {
      const r = await fetch(`/api/config/${activeTab}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      setSaved(true);
      setTimeout(() => setSaved(false), 2500);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }

  function setOracle(key: "oracle_source" | "oracle_target", field: keyof OracleConfig, v: string) {
    setConfigs((p) => ({ ...p, [key]: { ...p[key], [field]: v } }));
  }

  const tabBarStyle: React.CSSProperties = {
    display: "flex", gap: 2, marginBottom: 20,
    borderBottom: "1px solid #334155", paddingBottom: 0,
  };

  function tabStyle(active: boolean): React.CSSProperties {
    return {
      padding: "7px 14px", fontSize: 12, fontWeight: 600,
      cursor: "pointer", border: "none", borderRadius: "5px 5px 0 0",
      background: active ? "#0f172a" : "transparent",
      color: active ? "#e2e8f0" : "#64748b",
      borderBottom: active ? "2px solid #3b82f6" : "2px solid transparent",
      marginBottom: -1,
    };
  }

  const gridTwo: React.CSSProperties = {
    display: "grid", gridTemplateColumns: "1fr 90px", gap: "0 12px",
  };

  return (
    <>
      {/* Overlay */}
      <div
        onClick={onClose}
        style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.65)", zIndex: 200 }}
      />
      {/* Panel */}
      <div style={{
        position: "fixed", top: "50%", left: "50%",
        transform: "translate(-50%, -50%)",
        background: "#1e293b", border: "1px solid #334155",
        borderRadius: 10, padding: "24px 28px",
        width: 500, maxWidth: "92vw", maxHeight: "88vh",
        overflowY: "auto", zIndex: 201,
        fontFamily: "'Inter', 'Segoe UI', sans-serif",
      }}>
        {/* Header */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 18 }}>
          <h2 style={{ margin: 0, fontSize: 15, fontWeight: 700, color: "#e2e8f0" }}>
            Connection Settings
          </h2>
          <button onClick={onClose} style={{
            background: "none", border: "none", color: "#64748b",
            cursor: "pointer", fontSize: 18, lineHeight: 1, padding: 2,
          }}>✕</button>
        </div>

        {/* Tab bar */}
        <div style={tabBarStyle}>
          {TABS.map(({ key, label }) => (
            <button key={key} style={tabStyle(activeTab === key)} onClick={() => {
              setActiveTab(key);
              setTestResult(null);
            }}>
              {label}
            </button>
          ))}
        </div>

        {/* Error */}
        {error && (
          <div style={{
            background: "#450a0a", color: "#fca5a5", padding: "8px 12px",
            borderRadius: 6, marginBottom: 14, fontSize: 12,
          }}>
            {error}
          </div>
        )}

        {/* Tab content */}
        {(activeTab === "oracle_source" || activeTab === "oracle_target") && (
          <div>
            <div style={gridTwo}>
              <Field label="Host" value={configs[activeTab].host}
                onChange={(v) => setOracle(activeTab, "host", v)} placeholder="db-host.example.com" />
              <Field label="Port" value={configs[activeTab].port}
                onChange={(v) => setOracle(activeTab, "port", v)} placeholder="1521" />
            </div>
            <div style={gridTwo}>
              <Field label="Service Name" value={configs[activeTab].service_name}
                onChange={(v) => setOracle(activeTab, "service_name", v)} placeholder="ORCL" />
              <Field label="Schema" value={configs[activeTab].schema}
                onChange={(v) => setOracle(activeTab, "schema", v)} placeholder="SCOTT" />
            </div>
            <Field label="User" value={configs[activeTab].user}
              onChange={(v) => setOracle(activeTab, "user", v)} placeholder="scott" />
            <Field label="Password" type="password" value={configs[activeTab].password}
              onChange={(v) => setOracle(activeTab, "password", v)} />
          </div>
        )}

        {activeTab === "kafka" && (
          <div>
            <Field
              label="Bootstrap Servers"
              value={configs.kafka.bootstrap_servers}
              onChange={(v) => setConfigs((p) => ({ ...p, kafka: { bootstrap_servers: v } }))}
              placeholder="broker1:9092,broker2:9092"
            />
          </div>
        )}

        {activeTab === "kafka_connect" && (
          <div>
            <Field
              label="REST API URL"
              value={configs.kafka_connect.url}
              onChange={(v) => setConfigs((p) => ({ ...p, kafka_connect: { url: v } }))}
              placeholder="http://connect:8083"
            />
          </div>
        )}

        {/* Test result */}
        {testResult && (
          <div style={{
            background: testResult.status === "up" ? "#052e16" : "#450a0a",
            color:      testResult.status === "up" ? "#86efac" : "#fca5a5",
            border:     `1px solid ${testResult.status === "up" ? "#16a34a" : "#7f1d1d"}`,
            borderRadius: 6, padding: "8px 12px", marginTop: 8, fontSize: 12,
          }}>
            {testResult.status === "up" ? "✓ " : "✗ "}{testResult.message}
          </div>
        )}

        {/* Save / Test buttons */}
        <div style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 6 }}>
          <button
            onClick={saveActive}
            disabled={saving}
            style={{
              background: saved ? "#052e16" : "#1d4ed8",
              color: saved ? "#86efac" : "#e2e8f0",
              border: "none", borderRadius: 6,
              padding: "8px 22px", fontSize: 13, cursor: saving ? "wait" : "pointer", fontWeight: 600,
            }}
          >
            {saving ? "Saving…" : saved ? "✓ Saved" : "Save"}
          </button>
          {(activeTab === "oracle_source" || activeTab === "oracle_target") && (
            <button
              onClick={testConnection}
              disabled={testing}
              style={{
                background: "#1e293b", border: "1px solid #334155", borderRadius: 6,
                color: "#94a3b8", padding: "8px 16px", fontSize: 13,
                cursor: testing ? "wait" : "pointer", fontWeight: 600,
              }}
            >
              {testing ? "Проверка…" : "Тест подключения"}
            </button>
          )}
          <span style={{ fontSize: 11, color: "#475569", marginLeft: 4 }}>
            {TABS.find((t) => t.key === activeTab)?.label}
          </span>
        </div>

        {/* DB hint */}
        <p style={{ margin: "18px 0 0", fontSize: 10, color: "#334155", lineHeight: 1.6 }}>
          State DB: env <code style={{ color: "#475569" }}>STATE_DB_DSN</code>
        </p>
      </div>
    </>
  );
}
