"""Target preparation API — DDL comparison and target object management."""

from flask import Blueprint, jsonify, request
from db.oracle_browser import get_oracle_conn, get_full_ddl_info, execute_target_action
from services.oracle_stage    import sync_target_columns
from services.oracle_ddl_sync import sync_target_objects

bp = Blueprint("target_prep", __name__)

_state: dict = {}


def init(load_configs_fn):
    _state["load_configs"] = load_configs_fn


@bp.get("/api/target-prep/ddl")
def compare_ddl():
    src_schema = request.args.get("src_schema", "").strip().upper()
    src_table  = request.args.get("src_table",  "").strip().upper()
    tgt_schema = request.args.get("tgt_schema", "").strip().upper()
    tgt_table  = request.args.get("tgt_table",  "").strip().upper()

    if not all([src_schema, src_table, tgt_schema, tgt_table]):
        return jsonify({"error": "src_schema, src_table, tgt_schema, tgt_table required"}), 400

    configs = _state["load_configs"]()
    try:
        src_conn = get_oracle_conn("source", configs)
        tgt_conn = get_oracle_conn("target", configs)
        try:
            src_info = get_full_ddl_info(src_conn, src_schema, src_table)
            tgt_info = get_full_ddl_info(tgt_conn, tgt_schema, tgt_table)
            return jsonify({"source": src_info, "target": tgt_info})
        finally:
            src_conn.close()
            tgt_conn.close()
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503


@bp.post("/api/target-prep/action")
def target_action():
    data        = request.json or {}
    action      = data.get("action", "").strip()
    tgt_schema  = data.get("tgt_schema", "").strip().upper()
    tgt_table   = data.get("tgt_table",  "").strip().upper()
    object_name = data.get("object_name", "").strip()

    if not all([action, tgt_schema, tgt_table, object_name]):
        return jsonify({"error": "action, tgt_schema, tgt_table, object_name required"}), 400

    valid = {
        "disable_index", "enable_index",
        "disable_trigger", "enable_trigger",
        "disable_constraint", "enable_constraint",
    }
    if action not in valid:
        return jsonify({"error": f"Invalid action: {action}"}), 400

    configs = _state["load_configs"]()
    try:
        conn = get_oracle_conn("target", configs)
        try:
            execute_target_action(conn, action, tgt_schema, tgt_table, object_name)
            return jsonify({"ok": True})
        finally:
            conn.close()
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503


@bp.post("/api/target-prep/sync-columns")
def sync_columns():
    """Add columns present in source but missing in target (ALTER TABLE ADD)."""
    data       = request.json or {}
    src_schema = data.get("src_schema", "").strip().upper()
    src_table  = data.get("src_table",  "").strip().upper()
    tgt_schema = data.get("tgt_schema", "").strip().upper()
    tgt_table  = data.get("tgt_table",  "").strip().upper()

    if not all([src_schema, src_table, tgt_schema, tgt_table]):
        return jsonify({"error": "src_schema, src_table, tgt_schema, tgt_table required"}), 400

    configs = _state["load_configs"]()
    src_cfg = configs.get("oracle_source", {})
    dst_cfg = configs.get("oracle_target", {})
    try:
        result = sync_target_columns(src_cfg, dst_cfg, src_schema, src_table, tgt_schema, tgt_table)
        return jsonify(result)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503


@bp.post("/api/target-prep/sync-objects")
def sync_objects():
    """Create missing indexes, constraints and/or triggers on target from source."""
    data       = request.json or {}
    src_schema = data.get("src_schema", "").strip().upper()
    src_table  = data.get("src_table",  "").strip().upper()
    tgt_schema = data.get("tgt_schema", "").strip().upper()
    tgt_table  = data.get("tgt_table",  "").strip().upper()
    # optional list: ["constraints", "indexes", "triggers"] — defaults to all
    req_types  = data.get("types")

    if not all([src_schema, src_table, tgt_schema, tgt_table]):
        return jsonify({"error": "src_schema, src_table, tgt_schema, tgt_table required"}), 400

    types = set(req_types) if req_types else None

    configs = _state["load_configs"]()
    src_cfg = configs.get("oracle_source", {})
    dst_cfg = configs.get("oracle_target", {})
    try:
        result = sync_target_objects(src_cfg, dst_cfg, src_schema, src_table, tgt_schema, tgt_table, types)
        return jsonify(result)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503
