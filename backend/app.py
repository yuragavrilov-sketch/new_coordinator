import json
import os
import queue
import threading
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, Response, jsonify, request, send_from_directory

# Load .env from the same directory as this file (backend/.env)
load_dotenv(Path(__file__).parent / ".env")

# Serve the React build from ../frontend/dist on the same origin — no CORS needed
STATIC_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend", "dist")

app = Flask(__name__, static_folder=STATIC_DIR, static_url_path="")


# ---------------------------------------------------------------------------
# PostgreSQL state DB
# ---------------------------------------------------------------------------

PG_DSN = os.environ.get(
    "STATE_DB_DSN",
    "postgresql://postgres:postgres@localhost:5432/migration_state",
)

# In-memory fallback when DB is unavailable
_mem_configs: dict = {
    "oracle_source": {},
    "oracle_target": {},
    "kafka": {},
    "kafka_connect": {},
}
_db_available = False


def _get_db_conn():
    import psycopg2
    return psycopg2.connect(PG_DSN)


def _init_db() -> None:
    conn = _get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS service_configs (
                    service_name VARCHAR(50) PRIMARY KEY,
                    config       JSONB NOT NULL DEFAULT '{}',
                    updated_at   TIMESTAMP DEFAULT NOW()
                )
            """)
            for svc in ("oracle_source", "oracle_target", "kafka", "kafka_connect"):
                cur.execute("""
                    INSERT INTO service_configs (service_name, config)
                    VALUES (%s, '{}'::jsonb)
                    ON CONFLICT (service_name) DO NOTHING
                """, (svc,))
        conn.commit()
    finally:
        conn.close()


def _load_configs() -> dict:
    if not _db_available:
        return {k: dict(v) for k, v in _mem_configs.items()}
    try:
        conn = _get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT service_name, config FROM service_configs")
                return {row[0]: (row[1] or {}) for row in cur.fetchall()}
        finally:
            conn.close()
    except Exception as exc:
        print(f"[db] load_configs error: {exc}")
        return {k: dict(v) for k, v in _mem_configs.items()}


def _save_config(service: str, config: dict) -> None:
    _mem_configs[service] = config  # always update in-memory copy
    if not _db_available:
        return
    try:
        conn = _get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO service_configs (service_name, config, updated_at)
                    VALUES (%s, %s::jsonb, NOW())
                    ON CONFLICT (service_name) DO UPDATE
                        SET config = EXCLUDED.config, updated_at = NOW()
                """, (service, json.dumps(config)))
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        print(f"[db] save_config error: {exc}")


# Try to connect to PostgreSQL on startup
try:
    _init_db()
    _db_available = True
    print("[db] PostgreSQL state DB ready")
except Exception as _db_err:
    print(f"[db] PostgreSQL unavailable ({_db_err}) — using in-memory config store")


# ---------------------------------------------------------------------------
# Service availability checkers
# ---------------------------------------------------------------------------

def _check_oracle(cfg: dict) -> tuple[str, str]:
    host = cfg.get("host", "").strip()
    port = cfg.get("port", 1521)
    service_name = cfg.get("service_name", "").strip()
    user = cfg.get("user", "").strip()
    password = cfg.get("password", "")
    if not host or not service_name or not user:
        return "unknown", "Not configured"
    try:
        import oracledb
        dsn = f"{host}:{port}/{service_name}"
        conn = oracledb.connect(user=user, password=password, dsn=dsn)
        conn.close()
        return "up", "Connected"
    except ImportError:
        return "unknown", "oracledb not installed"
    except Exception as exc:
        return "down", str(exc)[:150]


def _check_kafka(cfg: dict) -> tuple[str, str]:
    servers = cfg.get("bootstrap_servers", "").strip()
    if not servers:
        return "unknown", "Not configured"
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            bootstrap_servers=[s.strip() for s in servers.split(",")],
            request_timeout_ms=5000,
            connections_max_idle_ms=8000,
        )
        consumer.topics()  # force connection attempt
        consumer.close()
        return "up", "Connected"
    except ImportError:
        return "unknown", "kafka-python not installed"
    except Exception as exc:
        return "down", str(exc)[:150]


def _check_kafka_connect(cfg: dict) -> tuple[str, str]:
    url = cfg.get("url", "").strip()
    if not url:
        return "unknown", "Not configured"
    try:
        import requests as req
        r = req.get(f"{url.rstrip('/')}/", timeout=5)
        if r.ok:
            return "up", f"HTTP {r.status_code}"
        return "down", f"HTTP {r.status_code}"
    except ImportError:
        return "unknown", "requests not installed"
    except Exception as exc:
        return "down", str(exc)[:150]


# ---------------------------------------------------------------------------
# Service status state
# ---------------------------------------------------------------------------

_service_status: dict[str, dict] = {
    "oracle_source": {"status": "unknown", "message": "Not yet checked"},
    "oracle_target": {"status": "unknown", "message": "Not yet checked"},
    "kafka":         {"status": "unknown", "message": "Not yet checked"},
    "kafka_connect": {"status": "unknown", "message": "Not yet checked"},
}
_status_lock = threading.Lock()


def _run_status_checks() -> None:
    configs = _load_configs()
    checks = [
        ("oracle_source", _check_oracle),
        ("oracle_target", _check_oracle),
        ("kafka",         _check_kafka),
        ("kafka_connect", _check_kafka_connect),
    ]
    for svc, checker in checks:
        try:
            status, message = checker(configs.get(svc, {}))
        except Exception as exc:
            status, message = "down", str(exc)[:150]
        with _status_lock:
            _service_status[svc] = {"status": status, "message": message}
        _broadcast({
            "type":    "service_status",
            "service": svc,
            "status":  status,
            "message": message,
            "ts":      datetime.utcnow().isoformat() + "Z",
        })


def _status_poller() -> None:
    time.sleep(5)  # let Flask start up
    while True:
        try:
            _run_status_checks()
        except Exception as exc:
            print(f"[status_poller] error: {exc}")
        time.sleep(30)


# ---------------------------------------------------------------------------
# SPA catch-all: any non-API path returns index.html
# ---------------------------------------------------------------------------

@app.get("/")
@app.get("/<path:path>")
def spa(path: str = ""):
    target = os.path.join(STATIC_DIR, path)
    if path and os.path.isfile(target):
        return send_from_directory(STATIC_DIR, path)
    return send_from_directory(STATIC_DIR, "index.html")


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
        yield "event: connected\ndata: {}\n\n"
        # Send current service statuses immediately so the client is up to date
        with _status_lock:
            for svc, info in _service_status.items():
                evt = {
                    "type":    "service_status",
                    "service": svc,
                    "status":  info["status"],
                    "message": info["message"],
                    "ts":      datetime.utcnow().isoformat() + "Z",
                }
                yield f"data: {json.dumps(evt)}\n\n"
        while True:
            try:
                msg = q.get(timeout=20)
                yield msg
            except queue.Empty:
                yield ": keepalive\n\n"
    finally:
        _unsubscribe(q)


# ---------------------------------------------------------------------------
# Routes — SSE / CDC
# ---------------------------------------------------------------------------

@app.get("/api/events")
def sse_events():
    """SSE endpoint — clients connect here to receive CDC events and service statuses."""
    q = _subscribe()
    return Response(
        _sse_stream(q),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/events")
def push_event():
    """Manually push a CDC event (useful for testing / webhook ingestion)."""
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
    return jsonify({
        "status":      "ok",
        "subscribers": len(_subscribers),
        "db":          _db_available,
    })


# ---------------------------------------------------------------------------
# Routes — config
# ---------------------------------------------------------------------------

_ALLOWED_SERVICES = {"oracle_source", "oracle_target", "kafka", "kafka_connect"}


@app.get("/api/config")
def get_configs():
    """Return all service configs."""
    return jsonify(_load_configs())


@app.post("/api/config/<service>")
def save_config_route(service: str):
    """Save config for a single service and trigger an immediate status re-check."""
    if service not in _ALLOWED_SERVICES:
        return jsonify({"error": f"Unknown service: {service}"}), 400
    body = request.get_json(force=True)
    if not isinstance(body, dict):
        return jsonify({"error": "Body must be a JSON object"}), 400
    _save_config(service, body)
    # Re-check the affected service in background
    threading.Thread(target=_check_and_broadcast, args=(service,), daemon=True).start()
    return jsonify({"ok": True})


def _check_and_broadcast(service: str) -> None:
    checker = {
        "oracle_source": _check_oracle,
        "oracle_target": _check_oracle,
        "kafka":         _check_kafka,
        "kafka_connect": _check_kafka_connect,
    }[service]
    cfg = _load_configs().get(service, {})
    try:
        status, message = checker(cfg)
    except Exception as exc:
        status, message = "down", str(exc)[:150]
    with _status_lock:
        _service_status[service] = {"status": status, "message": message}
    _broadcast({
        "type":    "service_status",
        "service": service,
        "status":  status,
        "message": message,
        "ts":      datetime.utcnow().isoformat() + "Z",
    })


@app.get("/api/status")
def get_status():
    """Return the latest cached status for all services."""
    with _status_lock:
        return jsonify(dict(_service_status))


threading.Thread(target=_status_poller, daemon=True).start()


if __name__ == "__main__":
    app.run(
        host=os.environ.get("FLASK_HOST", "0.0.0.0"),
        port=int(os.environ.get("FLASK_PORT", "5000")),
        debug=os.environ.get("FLASK_DEBUG", "true").lower() == "true",
        threaded=True,
        use_reloader=False,
    )
