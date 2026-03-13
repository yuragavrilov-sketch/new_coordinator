import { useEffect, useRef, useState } from "react";

export type SSEStatus = "connecting" | "connected" | "error" | "closed";

export type ServiceName = "oracle_source" | "oracle_target" | "kafka" | "kafka_connect";
export type ServiceAvailability = "up" | "down" | "unknown";

export interface ServiceStatus {
  status: ServiceAvailability;
  message: string;
  checked_at?: string;
}

export type ServiceStatuses = Record<ServiceName, ServiceStatus>;

// ── SSE event shapes ──────────────────────────────────────────────────────────

export interface MigrationPhaseEvent {
  type: "migration_phase";
  migration_id: string;
  from_phase?: string;
  phase: string;
  ts: string;
}

export interface ChunkProgressEvent {
  type: "chunk_progress";
  migration_id: string;
  chunks_done: number;
  total_chunks: number;
  ts: string;
}

export interface ConnectorStatusEvent {
  type: "connector_status";
  migration_id: string;
  status: string;
  connector_name: string;
  ts: string;
}

export interface KafkaLagEvent {
  type: "kafka_lag";
  migration_id: string;
  total_lag: number;
  updated_at: string | null;
  ts: string;
}

export type SSEEvent =
  | MigrationPhaseEvent
  | ChunkProgressEvent
  | ConnectorStatusEvent
  | KafkaLagEvent;

// ── Defaults ──────────────────────────────────────────────────────────────────

const DEFAULT_STATUSES: ServiceStatuses = {
  oracle_source: { status: "unknown", message: "Not yet checked" },
  oracle_target: { status: "unknown", message: "Not yet checked" },
  kafka:         { status: "unknown", message: "Not yet checked" },
  kafka_connect: { status: "unknown", message: "Not yet checked" },
};

interface UseSSEOptions {
  url: string;
  maxEvents?: number;
}

export function useSSE({ url, maxEvents = 200 }: UseSSEOptions) {
  const [events,          setEvents]          = useState<SSEEvent[]>([]);
  const [status,          setStatus]          = useState<SSEStatus>("connecting");
  const [serviceStatuses, setServiceStatuses] = useState<ServiceStatuses>(DEFAULT_STATUSES);
  const sourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    const es = new EventSource(url);
    sourceRef.current = es;

    es.addEventListener("connected", () => setStatus("connected"));

    es.onmessage = (e: MessageEvent) => {
      try {
        const parsed = JSON.parse(e.data);

        if (parsed.type === "service_status") {
          setServiceStatuses((prev) => ({
            ...prev,
            [parsed.service]: {
              status:     parsed.status,
              message:    parsed.message,
              checked_at: parsed.ts,
            },
          }));
        } else if (
          parsed.type === "migration_phase"  ||
          parsed.type === "chunk_progress"   ||
          parsed.type === "connector_status" ||
          parsed.type === "kafka_lag"
        ) {
          setEvents((prev) => [parsed as SSEEvent, ...prev].slice(0, maxEvents));
        }

        setStatus("connected");
      } catch {
        // ignore malformed
      }
    };

    es.onerror = () => setStatus("error");

    return () => {
      es.close();
      setStatus("closed");
    };
  }, [url, maxEvents]);

  return { events, status, serviceStatuses };
}
