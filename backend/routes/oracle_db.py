"""Oracle DB browser routes — used by the migration creation wizard."""

from flask import Blueprint, jsonify, request
from db.oracle_browser import (
    get_oracle_conn, list_schemas, list_tables, get_table_info, get_oracle_version,
)

bp = Blueprint("oracle_db", __name__)

_state: dict = {}


def init(load_configs_fn):
    _state["load_configs"] = load_configs_fn


@bp.get("/api/db/<db>/schemas")
def list_oracle_schemas(db: str):
    if db not in ("source", "target"):
        return jsonify({"error": "Invalid db"}), 400
    try:
        conn = get_oracle_conn(db, _state["load_configs"](), prefer_owner=True)
        try:
            return jsonify(list_schemas(conn))
        finally:
            conn.close()
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503


@bp.get("/api/db/<db>/tables")
def list_oracle_tables(db: str):
    if db not in ("source", "target"):
        return jsonify({"error": "Invalid db"}), 400
    schema = request.args.get("schema", "").strip().upper()
    if not schema:
        return jsonify({"error": "schema required"}), 400
    try:
        conn = get_oracle_conn(db, _state["load_configs"](), prefer_owner=True)
        try:
            return jsonify(list_tables(conn, schema))
        finally:
            conn.close()
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503


@bp.get("/api/db/source/table-info")
def source_table_info():
    schema = request.args.get("schema", "").strip().upper()
    table  = request.args.get("table",  "").strip().upper()
    if not schema or not table:
        return jsonify({"error": "schema and table required"}), 400
    try:
        conn = get_oracle_conn("source", _state["load_configs"]())
        try:
            return jsonify(get_table_info(conn, schema, table))
        finally:
            conn.close()
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503


@bp.get("/api/db/info")
def db_info():
    """Wizard helper: returns {source, target} = { host, service_name, version, ok }
    so the New-Migration form can show prefilled host/version from existing settings.
    Failures are returned as { ok: false, error } per side — endpoint never 5xx's."""
    configs = _state["load_configs"]()
    out: dict = {}
    for side in ("source", "target"):
        cfg = configs.get(f"oracle_{side}", {})
        info = {
            "host":         cfg.get("host", ""),
            "port":         cfg.get("port", 1521),
            "service_name": cfg.get("service_name", ""),
            "configured":   bool(cfg.get("host") and cfg.get("service_name") and cfg.get("user")),
            "version":      "",
            "version_banner": "",
            "ok":           False,
            "error":        None,
        }
        if info["configured"]:
            try:
                conn = get_oracle_conn(side, configs)
                try:
                    v = get_oracle_version(conn)
                    info["version"]        = v.get("short", "")
                    info["version_banner"] = v.get("banner", "")
                    info["ok"]             = True
                finally:
                    conn.close()
            except Exception as exc:
                info["error"] = str(exc)[:200]
        out[side] = info
    return jsonify(out)
