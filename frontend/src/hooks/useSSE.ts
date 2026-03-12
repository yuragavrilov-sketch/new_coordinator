import { useEffect, useRef, useState } from "react";

export interface CdcEvent {
  id: string;
  table: string;
  schema: string;
  operation: "INSERT" | "UPDATE" | "DELETE" | "UNKNOWN";
  data: Record<string, unknown>;
  old_data?: Record<string, unknown> | null;
  ts: string;
}

export type SSEStatus = "connecting" | "connected" | "error" | "closed";

export type ServiceName = "oracle_source" | "oracle_target" | "kafka" | "kafka_connect";
export type ServiceAvailability = "up" | "down" | "unknown";

export interface ServiceStatus {
  status: ServiceAvailability;
  message: string;
  checked_at?: string; // ISO timestamp of last poll
}

export type ServiceStatuses = Record<ServiceName, ServiceStatus>;

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
  const [events, setEvents] = useState<CdcEvent[]>([]);
  const [status, setStatus] = useState<SSEStatus>("connecting");
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
            [parsed.service]: { status: parsed.status, message: parsed.message, checked_at: parsed.ts },
          }));
        } else {
          const event: CdcEvent = parsed;
          setEvents((prev) => [event, ...prev].slice(0, maxEvents));
        }
        setStatus("connected");
      } catch {
        // ignore malformed messages
      }
    };

    es.onerror = () => {
      setStatus("error");
    };

    return () => {
      es.close();
      setStatus("closed");
    };
  }, [url, maxEvents]);

  const clear = () => setEvents([]);

  return { events, status, clear, serviceStatuses };
}
