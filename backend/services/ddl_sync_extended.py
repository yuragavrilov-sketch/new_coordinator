"""
Sync non-table DDL objects from source to target Oracle.
Tables are handled by existing oracle_stage.py and oracle_ddl_sync.py.
"""
from db.oracle_browser import (
    get_oracle_conn, get_source_code, get_view_info,
    get_mview_info, get_sequence_info, get_synonym_info,
)


def _exec_on_target(tgt_conn, sql: str):
    """Execute DDL on target and commit."""
    with tgt_conn.cursor() as cur:
        cur.execute(sql)
    tgt_conn.commit()


def sync_view(src_conn, tgt_conn, schema: str, name: str) -> dict:
    """CREATE OR REPLACE VIEW on target from source definition."""
    info = get_view_info(src_conn, schema, name)
    if not info.get("sql_text"):
        return {"error": f"View {name} has no SQL text on source"}
    ddl = f'CREATE OR REPLACE VIEW "{schema}"."{name}" AS\n{info["sql_text"]}'
    _exec_on_target(tgt_conn, ddl)
    return {"action": "created", "object": name}


def sync_mview(src_conn, tgt_conn, schema: str, name: str) -> dict:
    """CREATE MATERIALIZED VIEW on target. Drops existing first."""
    info = get_mview_info(src_conn, schema, name)
    if not info.get("sql_text"):
        return {"error": f"MView {name} has no SQL text on source"}
    try:
        _exec_on_target(tgt_conn, f'DROP MATERIALIZED VIEW "{schema}"."{name}"')
    except Exception:
        pass
    refresh = info.get("refresh_type", "FORCE/DEMAND")
    method = refresh.split("/")[0] if "/" in refresh else "FORCE"
    ddl = f'CREATE MATERIALIZED VIEW "{schema}"."{name}" REFRESH {method} AS\n{info["sql_text"]}'
    _exec_on_target(tgt_conn, ddl)
    return {"action": "created", "object": name}


def sync_code_object(src_conn, tgt_conn, schema: str, name: str, obj_type: str) -> dict:
    """CREATE OR REPLACE function/procedure/type on target."""
    if obj_type == "PACKAGE":
        spec = get_source_code(src_conn, schema, name, "PACKAGE")
        body = get_source_code(src_conn, schema, name, "PACKAGE BODY")
        if spec:
            _exec_on_target(tgt_conn, f'CREATE OR REPLACE {spec}')
        if body:
            _exec_on_target(tgt_conn, f'CREATE OR REPLACE {body}')
        return {"action": "compiled", "object": name, "spec": bool(spec), "body": bool(body)}
    elif obj_type == "TYPE":
        src = get_source_code(src_conn, schema, name, "TYPE")
        body = get_source_code(src_conn, schema, name, "TYPE BODY")
        if src:
            _exec_on_target(tgt_conn, f'CREATE OR REPLACE {src}')
        if body:
            _exec_on_target(tgt_conn, f'CREATE OR REPLACE {body}')
        return {"action": "compiled", "object": name, "source": bool(src), "body": bool(body)}
    else:
        code = get_source_code(src_conn, schema, name, obj_type)
        if not code:
            return {"error": f"{obj_type} {name} has no source code on source"}
        _exec_on_target(tgt_conn, f'CREATE OR REPLACE {code}')
        return {"action": "compiled", "object": name}


def sync_sequence(src_conn, tgt_conn, schema: str, name: str, action: str = "create") -> dict:
    """Create or alter sequence on target."""
    info = get_sequence_info(src_conn, schema, name)
    if not info:
        return {"error": f"Sequence {name} not found on source"}

    if action == "create":
        ddl = (
            f'CREATE SEQUENCE "{schema}"."{name}"'
            f' MINVALUE {info["min_value"]}'
            f' MAXVALUE {info["max_value"]}'
            f' INCREMENT BY {info["increment_by"]}'
            f' CACHE {info["cache_size"]}'
            f' START WITH {info["last_number"]}'
        )
        _exec_on_target(tgt_conn, ddl)
        return {"action": "created", "object": name}
    else:
        ddl = (
            f'ALTER SEQUENCE "{schema}"."{name}"'
            f' INCREMENT BY {info["increment_by"]}'
            f' MINVALUE {info["min_value"]}'
            f' MAXVALUE {info["max_value"]}'
            f' CACHE {info["cache_size"]}'
        )
        _exec_on_target(tgt_conn, ddl)
        return {"action": "altered", "object": name}


def sync_synonym(src_conn, tgt_conn, schema: str, name: str) -> dict:
    """CREATE OR REPLACE SYNONYM on target."""
    info = get_synonym_info(src_conn, schema, name)
    if not info:
        return {"error": f"Synonym {name} not found on source"}
    target_ref = f'"{info["table_owner"]}"."{info["table_name"]}"'
    if info.get("db_link"):
        target_ref += f'@{info["db_link"]}'
    ddl = f'CREATE OR REPLACE SYNONYM "{schema}"."{name}" FOR {target_ref}'
    _exec_on_target(tgt_conn, ddl)
    return {"action": "created", "object": name}


# ── Dispatcher ───────────────────────────────────────────────────────────────

def sync_to_target(src_conn, tgt_conn, schema: str, name: str,
                   object_type: str, action: str = "create") -> dict:
    """Route sync request to the correct handler."""
    if object_type == "VIEW":
        return sync_view(src_conn, tgt_conn, schema, name)
    elif object_type == "MATERIALIZED VIEW":
        return sync_mview(src_conn, tgt_conn, schema, name)
    elif object_type in ("FUNCTION", "PROCEDURE", "PACKAGE", "TYPE"):
        return sync_code_object(src_conn, tgt_conn, schema, name, object_type)
    elif object_type == "SEQUENCE":
        return sync_sequence(src_conn, tgt_conn, schema, name, action)
    elif object_type == "SYNONYM":
        return sync_synonym(src_conn, tgt_conn, schema, name)
    else:
        return {"error": f"Unsupported object type: {object_type}"}
