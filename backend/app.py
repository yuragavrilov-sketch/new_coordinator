import json
import queue
import threading
import time
from datetime import datetime

from flask import Flask, Response, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=False)

# Global event bus: list of subscriber queues
_subscribers: list[queue.Queue] = []
_lock = threading.Lock()


# ---------------------------------------------------------------------------
# SSE helpers
# ---------------------------------------------------------------------------

def _subscribe() -> queue.Queue:
    q: queue.Queue = queue.Queue(maxsize=100)
    with _lock:
        _subscribers.append(q)
    return q


def _unsubscribe(q: queue.Queue) -> None:
    with _lock:
        try:
            _subscribers.remove(q)
        except ValueError:
            pass


def _broadcast(event: dict) -> None:
    """Push an event to every connected SSE client."""
    payload = f"data: {json.dumps(event)}\n\n"
    with _lock:
        dead = []
        for q in _subscribers:
            try:
                q.put_nowait(payload)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _subscribers.remove(q)


def _sse_stream(q: queue.Queue):
    """Generator that yields SSE messages from the subscriber queue."""
    try:
        # Send a connected heartbeat immediately
        yield "event: connected\ndata: {}\n\n"
        while True:
            try:
                msg = q.get(timeout=20)
                yield msg
            except queue.Empty:
                # Keep-alive comment
                yield ": keepalive\n\n"
    finally:
        _unsubscribe(q)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/api/events")
def sse_events():
    """SSE endpoint — clients connect here to receive CDC events."""
    q = _subscribe()
    return Response(
        _sse_stream(q),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # disable nginx buffering
        },
    )


@app.post("/api/events")
def push_event():
    """
    Manually push a CDC event (useful for testing / webhook ingestion).

    Body (JSON):
        {
            "table":     "users",
            "operation": "INSERT" | "UPDATE" | "DELETE",
            "schema":    "public",
            "data":      { ...row data... },
            "old_data":  { ...previous row data... }   // UPDATE/DELETE only
        }
    """
    body = request.get_json(force=True)
    event = {
        "id":        body.get("id", str(time.time_ns())),
        "table":     body.get("table", "unknown"),
        "schema":    body.get("schema", "public"),
        "operation": body.get("operation", "UNKNOWN").upper(),
        "data":      body.get("data", {}),
        "old_data":  body.get("old_data"),
        "ts":        datetime.utcnow().isoformat() + "Z",
    }
    _broadcast(event)
    return jsonify({"ok": True, "event": event}), 202


@app.get("/api/health")
def health():
    return jsonify({"status": "ok", "subscribers": len(_subscribers)})


# ---------------------------------------------------------------------------
# Demo: background thread that emits fake CDC events every 5 s
# ---------------------------------------------------------------------------

_DEMO_TABLES = ["users", "orders", "products", "inventory"]
_DEMO_OPS = ["INSERT", "UPDATE", "DELETE"]
_demo_counter = 0


def _demo_producer():
    global _demo_counter
    import random

    while True:
        time.sleep(5)
        _demo_counter += 1
        op = random.choice(_DEMO_OPS)
        table = random.choice(_DEMO_TABLES)
        row = {"id": random.randint(1, 9999), "value": f"demo-{_demo_counter}"}
        event = {
            "id":        str(_demo_counter),
            "table":     table,
            "schema":    "public",
            "operation": op,
            "data":      row,
            "old_data":  {"id": row["id"], "value": "old"} if op != "INSERT" else None,
            "ts":        datetime.utcnow().isoformat() + "Z",
        }
        _broadcast(event)


threading.Thread(target=_demo_producer, daemon=True).start()


if __name__ == "__main__":
    # use_reloader=False: reloader kills SSE connections on every file save
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=True, use_reloader=False)
