import type { ServiceStatuses, ServiceAvailability } from "../hooks/useSSE";

interface Props {
  statuses: ServiceStatuses;
}

const LABELS: Record<string, string> = {
  oracle_source: "Oracle Source",
  oracle_target: "Oracle Target",
  kafka:         "Kafka",
  kafka_connect: "Connect",
};

const COLORS: Record<ServiceAvailability, { bg: string; border: string; text: string; dot: string; dim: string }> = {
  up:      { bg: "#052e16", border: "#166534", text: "#86efac", dot: "#22c55e", dim: "#4ade8088" },
  down:    { bg: "#450a0a", border: "#7f1d1d", text: "#fca5a5", dot: "#ef4444", dim: "#f8717188" },
  unknown: { bg: "#1e293b", border: "#334155", text: "#94a3b8", dot: "#475569", dim: "#47556988" },
};

function fmtTime(iso?: string): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  } catch {
    return "—";
  }
}

export function ServiceStatusBar({ statuses }: Props) {
  const services = ["oracle_source", "oracle_target", "kafka", "kafka_connect"] as const;

  return (
    <div style={{ display: "flex", gap: 8, marginBottom: 16, flexWrap: "wrap" }}>
      {services.map((svc) => {
        const { status, message, checked_at } = statuses[svc];
        const c = COLORS[status];
        return (
          <div
            key={svc}
            title={message}
            style={{
              display: "flex",
              flexDirection: "column",
              gap: 2,
              background: c.bg,
              color: c.text,
              border: `1px solid ${c.border}`,
              borderRadius: 6,
              padding: "6px 12px",
              fontSize: 12,
              fontWeight: 600,
              cursor: "default",
              userSelect: "none",
              minWidth: 120,
            }}
          >
            {/* Top row: dot + name + status */}
            <div style={{ display: "flex", alignItems: "center", gap: 7 }}>
              <span style={{
                width: 7, height: 7, borderRadius: "50%", background: c.dot,
                display: "inline-block", flexShrink: 0,
                animation: status === "up" ? "pulse 2s infinite" : undefined,
              }} />
              <span>{LABELS[svc]}</span>
              <span style={{ fontWeight: 400, opacity: 0.8, marginLeft: "auto" }}>
                {status === "up" ? "OK" : status === "down" ? "DOWN" : "—"}
              </span>
            </div>
            {/* Bottom row: last check time */}
            <div style={{ fontSize: 10, color: c.dim, paddingLeft: 14, fontWeight: 400 }}>
              {checked_at ? fmtTime(checked_at) : "не проверялось"}
            </div>
          </div>
        );
      })}
    </div>
  );
}
