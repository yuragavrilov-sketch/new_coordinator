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

interface UseSSEOptions {
  url: string;
  maxEvents?: number;
}

export function useSSE({ url, maxEvents = 200 }: UseSSEOptions) {
  const [events, setEvents] = useState<CdcEvent[]>([]);
  const [status, setStatus] = useState<SSEStatus>("connecting");
  const sourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    const es = new EventSource(url);
    sourceRef.current = es;

    es.addEventListener("connected", () => setStatus("connected"));

    es.onmessage = (e: MessageEvent) => {
      try {
        const event: CdcEvent = JSON.parse(e.data);
        setEvents((prev) => [event, ...prev].slice(0, maxEvents));
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

  return { events, status, clear };
}
