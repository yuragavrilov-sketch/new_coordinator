"""Flask application entry-point — thin composition root.

Structure
---------
backend/
  app.py                  ← this file
  db/
    state_db.py           ← PostgreSQL helpers (config + migrations schema)
    oracle_browser.py     ← Oracle schema/table/column introspection
  routes/
    sse.py                ← SSE event bus
    config.py             ← service config + status API
    migrations.py         ← migrations CRUD + phase transitions
    oracle_db.py          ← Oracle DB browser API (wizard)
  services/
    checkers.py           ← Oracle / Kafka / Kafka-Connect availability probes
    status_poller.py      ← background status-poll thread
"""

import os
import sys
import threading
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, jsonify, send_from_directory

load_dotenv(Path(__file__).parent / ".env")

sys.path.insert(0, os.path.dirname(__file__))   # make local packages importable

# ── Static dir (React build) ──────────────────────────────────────────────────
STATIC_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend", "dist")

app = Flask(__name__, static_folder=STATIC_DIR, static_url_path="")

# ── Database ──────────────────────────────────────────────────────────────────
from db.state_db import init_db, get_conn, row_to_dict, load_configs, save_config

_db_available = {"value": False}   # mutable dict — lets blueprints read live value

try:
    init_db()
    _db_available["value"] = True
    print("[db] PostgreSQL state DB ready")
except Exception as _db_err:
    print(f"[db] PostgreSQL unavailable ({_db_err}) — using in-memory config store")

# ── Service checkers ──────────────────────────────────────────────────────────
from services.checkers import check_oracle, check_kafka, check_kafka_connect

_service_status: dict = {
    "oracle_source": {"status": "unknown", "message": "Not yet checked"},
    "oracle_target": {"status": "unknown", "message": "Not yet checked"},
    "kafka":         {"status": "unknown", "message": "Not yet checked"},
    "kafka_connect": {"status": "unknown", "message": "Not yet checked"},
}
_status_lock = threading.Lock()

_checkers = {
    "oracle_source": check_oracle,
    "oracle_target": check_oracle,
    "kafka":         check_kafka,
    "kafka_connect": check_kafka_connect,
}

# ── Convenience wrappers ──────────────────────────────────────────────────────
def _load_cfg():       return load_configs(_db_available["value"])
def _save_cfg(s, c):   save_config(s, c, _db_available["value"])

# ── SSE blueprint ─────────────────────────────────────────────────────────────
import routes.sse as sse_mod
from routes.sse import bp as sse_bp, broadcast, subscriber_count

sse_mod.init(_service_status, _status_lock)
app.register_blueprint(sse_bp)

# ── Config blueprint ──────────────────────────────────────────────────────────
import routes.config as config_mod
from routes.config import bp as config_bp

config_mod.init(
    db_available_ref=_db_available,
    mem_configs={},
    service_status=_service_status,
    status_lock=_status_lock,
    broadcast_fn=broadcast,
    load_configs_fn=_load_cfg,
    save_config_fn=_save_cfg,
    checkers=_checkers,
)
app.register_blueprint(config_bp)

# ── Migrations blueprint ──────────────────────────────────────────────────────
import routes.migrations as mig_mod
from routes.migrations import bp as mig_bp

mig_mod.init(
    get_conn_fn=get_conn,
    row_to_dict_fn=row_to_dict,
    db_available_ref=_db_available,
    broadcast_fn=broadcast,
)
app.register_blueprint(mig_bp)

# ── Oracle DB browser blueprint ───────────────────────────────────────────────
import routes.oracle_db as oracle_mod
from routes.oracle_db import bp as oracle_bp

oracle_mod.init(load_configs_fn=_load_cfg)
app.register_blueprint(oracle_bp)

# ── Status poller ─────────────────────────────────────────────────────────────
from services.status_poller import start_poller

start_poller(
    load_configs_fn=_load_cfg,
    checkers=_checkers,
    service_status=_service_status,
    status_lock=_status_lock,
    broadcast_fn=broadcast,
)

# ── SPA catch-all ─────────────────────────────────────────────────────────────

@app.get("/")
@app.get("/<path:path>")
def spa(path: str = ""):
    target = os.path.join(STATIC_DIR, path)
    if path and os.path.isfile(target):
        return send_from_directory(STATIC_DIR, path)
    return send_from_directory(STATIC_DIR, "index.html")


@app.get("/api/health")
def health():
    return jsonify({
        "status":      "ok",
        "subscribers": subscriber_count(),
        "db":          _db_available["value"],
    })


# ── Dev server ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(
        host=os.environ.get("FLASK_HOST", "0.0.0.0"),
        port=int(os.environ.get("FLASK_PORT", "5000")),
        debug=os.environ.get("FLASK_DEBUG", "true").lower() == "true",
        threaded=True,
        use_reloader=False,
    )
