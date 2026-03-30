"""Oracle DB browser routes — used by the migration creation wizard."""

from flask import Blueprint, jsonify, request
from db.oracle_browser import get_oracle_conn, list_schemas, list_tables, get_table_info

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
