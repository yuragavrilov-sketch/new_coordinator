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


def _diff_summary(src: dict, tgt: dict) -> dict:
    """Compute diff counts between source and target DDL info dicts."""
    tgt_col     = {c["name"] for c in tgt["columns"]}
    src_col_map = {c["name"]: c for c in src["columns"]}
    tgt_col_map = {c["name"]: c for c in tgt["columns"]}

    cols_missing = sum(1 for c in src["columns"] if c["name"] not in tgt_col)
    cols_extra   = sum(1 for c in tgt["columns"] if c["name"] not in src_col_map)
    cols_type    = sum(
        1 for c in src["columns"]
        if c["name"] in tgt_col_map
        and c["data_type"] != tgt_col_map[c["name"]]["data_type"]
    )

    tgt_idx      = {i["name"] for i in tgt["indexes"]}
    idx_missing  = sum(1 for i in src["indexes"] if i["name"] not in tgt_idx)
    idx_disabled = sum(1 for i in tgt["indexes"] if i["status"] != "VALID")

    tgt_con_keys = {(c["type_code"], ",".join(c["columns"])) for c in tgt["constraints"]}
    con_missing  = sum(
        1 for c in src["constraints"]
        if (c["type_code"], ",".join(c["columns"])) not in tgt_con_keys
    )
    con_disabled = sum(
        1 for c in tgt["constraints"]
        if c["status"] == "DISABLED" and c["type_code"] != "P"
    )

    tgt_trg     = {t["name"] for t in tgt["triggers"]}
    trg_missing = sum(1 for t in src["triggers"] if t["name"] not in tgt_trg)

    total = cols_missing + cols_extra + cols_type + idx_missing + idx_disabled + con_missing + con_disabled + trg_missing
    return {
        "ok":           total == 0,
        "total":        total,
        "cols_missing": cols_missing,
        "cols_extra":   cols_extra,
        "cols_type":    cols_type,
        "idx_missing":  idx_missing,
        "idx_disabled": idx_disabled,
        "con_missing":  con_missing,
        "con_disabled": con_disabled,
        "trg_missing":  trg_missing,
    }


@bp.post("/api/target-prep/compare-summary")
def compare_summary():
    """Return diff summary (counts only) for a source/target table pair."""
    data       = request.json or {}
    src_schema = data.get("src_schema", "").strip().upper()
    src_table  = data.get("src_table",  "").strip().upper()
    tgt_schema = data.get("tgt_schema", "").strip().upper()
    tgt_table  = data.get("tgt_table",  "").strip().upper()

    if not all([src_schema, src_table, tgt_schema, tgt_table]):
        return jsonify({"error": "src_schema, src_table, tgt_schema, tgt_table required"}), 400

    configs = _state["load_configs"]()
    try:
        src_conn = get_oracle_conn("source", configs)
        tgt_conn = get_oracle_conn("target", configs)
        try:
            src_info = get_full_ddl_info(src_conn, src_schema, src_table)
            tgt_info = get_full_ddl_info(tgt_conn, tgt_schema, tgt_table)
            return jsonify(_diff_summary(src_info, tgt_info))
        finally:
            src_conn.close()
            tgt_conn.close()
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
