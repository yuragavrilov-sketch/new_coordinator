"""Pre-switchover checks and actions API."""

from flask import Blueprint, jsonify, request
from db.oracle_browser import (
    get_oracle_conn,
    disable_referencing_fks, enable_referencing_fks,
    switch_identity_to_default, restore_identity_always,
)

bp = Blueprint("switchover", __name__)

_state: dict = {}


def init(get_conn_fn, load_configs_fn):
    _state["get_conn"] = get_conn_fn
    _state["load_configs"] = load_configs_fn


@bp.get("/api/switchover/status")
def switchover_status():
    """Gather pre-switchover status for a schema on the target.

    Returns per-table status of: disabled FK, disabled triggers,
    identity columns, unusable indexes, disabled constraints.
    """
    schema = request.args.get("schema", "").strip().upper()
    if not schema:
        return jsonify({"error": "schema required"}), 400

    cfgs = _state["load_configs"]()
    try:
        tgt_conn = get_oracle_conn("target", cfgs, prefer_owner=True)
    except Exception as exc:
        return jsonify({"error": f"Target connection failed: {exc}"}), 503

    try:
        result = {"schema": schema, "tables": []}

        with tgt_conn.cursor() as cur:
            # 1. Disabled FK constraints (on any table in schema)
            cur.execute("""
                SELECT table_name, constraint_name, r_owner, r_constraint_name
                FROM   all_constraints
                WHERE  owner = :s
                  AND  constraint_type = 'R'
                  AND  status = 'DISABLED'
                ORDER BY table_name, constraint_name
            """, {"s": schema})
            disabled_fks = [
                {"table": r[0], "constraint": r[1],
                 "ref_owner": r[2], "ref_constraint": r[3]}
                for r in cur.fetchall()
            ]

            # 2. Disabled triggers
            cur.execute("""
                SELECT table_name, trigger_name
                FROM   all_triggers
                WHERE  owner = :s AND status = 'DISABLED'
                ORDER BY table_name, trigger_name
            """, {"s": schema})
            disabled_triggers = [
                {"table": r[0], "trigger": r[1]}
                for r in cur.fetchall()
            ]

            # 3. Identity columns in BY DEFAULT mode
            cur.execute("""
                SELECT table_name, column_name, generation_type
                FROM   all_tab_identity_cols
                WHERE  owner = :s
                ORDER BY table_name, column_name
            """, {"s": schema})
            identity_cols = [
                {"table": r[0], "column": r[1], "generation_type": r[2]}
                for r in cur.fetchall()
            ]

            # 4. Unusable indexes
            cur.execute("""
                SELECT table_name, index_name, index_type
                FROM   all_indexes
                WHERE  owner = :s AND status = 'UNUSABLE'
                ORDER BY table_name, index_name
            """, {"s": schema})
            unusable_indexes = [
                {"table": r[0], "index": r[1], "type": r[2]}
                for r in cur.fetchall()
            ]

            # 5. Disabled constraints (non-FK)
            cur.execute("""
                SELECT table_name, constraint_name, constraint_type
                FROM   all_constraints
                WHERE  owner = :s
                  AND  constraint_type IN ('P', 'U', 'C')
                  AND  status = 'DISABLED'
                ORDER BY table_name, constraint_name
            """, {"s": schema})
            disabled_constraints = [
                {"table": r[0], "constraint": r[1], "type": r[2]}
                for r in cur.fetchall()
            ]

            # 6. Tables with NOLOGGING
            cur.execute("""
                SELECT table_name
                FROM   all_tables
                WHERE  owner = :s AND logging = 'NO'
                ORDER BY table_name
            """, {"s": schema})
            nologging_tables = [r[0] for r in cur.fetchall()]

        return jsonify({
            "schema": schema,
            "disabled_fks": disabled_fks,
            "disabled_triggers": disabled_triggers,
            "identity_cols": identity_cols,
            "unusable_indexes": unusable_indexes,
            "disabled_constraints": disabled_constraints,
            "nologging_tables": nologging_tables,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        tgt_conn.close()


@bp.post("/api/switchover/enable-fks")
def enable_fks():
    """Enable all disabled FK constraints in schema."""
    body = request.get_json(force=True)
    schema = (body.get("schema") or "").strip().upper()
    if not schema:
        return jsonify({"error": "schema required"}), 400

    cfgs = _state["load_configs"]()
    conn = get_oracle_conn("target", cfgs, prefer_owner=True)
    try:
        results = []
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name, constraint_name
                FROM   all_constraints
                WHERE  owner = :s AND constraint_type = 'R' AND status = 'DISABLED'
            """, {"s": schema})
            fks = cur.fetchall()

            for tbl, con in fks:
                try:
                    cur.execute(f'ALTER TABLE "{schema}"."{tbl}" ENABLE CONSTRAINT "{con}"')
                    results.append({"table": tbl, "constraint": con, "status": "ok"})
                except Exception as exc:
                    results.append({"table": tbl, "constraint": con,
                                    "status": "error", "error": str(exc)})
        conn.commit()
        return jsonify(results)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.post("/api/switchover/enable-triggers")
def enable_triggers():
    """Enable all disabled triggers in schema."""
    body = request.get_json(force=True)
    schema = (body.get("schema") or "").strip().upper()
    if not schema:
        return jsonify({"error": "schema required"}), 400

    cfgs = _state["load_configs"]()
    conn = get_oracle_conn("target", cfgs, prefer_owner=True)
    try:
        results = []
        with conn.cursor() as cur:
            cur.execute("""
                SELECT trigger_name
                FROM   all_triggers
                WHERE  owner = :s AND status = 'DISABLED'
            """, {"s": schema})
            triggers = [r[0] for r in cur.fetchall()]

            for trg in triggers:
                try:
                    cur.execute(f'ALTER TRIGGER "{schema}"."{trg}" ENABLE')
                    results.append({"trigger": trg, "status": "ok"})
                except Exception as exc:
                    results.append({"trigger": trg, "status": "error", "error": str(exc)})
        conn.commit()
        return jsonify(results)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.post("/api/switchover/restore-identity")
def restore_identity():
    """Restore IDENTITY columns to GENERATED ALWAYS."""
    body = request.get_json(force=True)
    schema = (body.get("schema") or "").strip().upper()
    if not schema:
        return jsonify({"error": "schema required"}), 400

    cfgs = _state["load_configs"]()
    conn = get_oracle_conn("target", cfgs, prefer_owner=True)
    try:
        results = []
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT table_name
                FROM   all_tab_identity_cols
                WHERE  owner = :s AND generation_type != 'ALWAYS'
            """, {"s": schema})
            tables = [r[0] for r in cur.fetchall()]

        for tbl in tables:
            try:
                cols = restore_identity_always(conn, schema, tbl)
                for c in cols:
                    results.append({"table": tbl, "column": c, "status": "ok"})
            except Exception as exc:
                results.append({"table": tbl, "status": "error", "error": str(exc)})
        return jsonify(results)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.post("/api/switchover/rebuild-indexes")
def rebuild_indexes():
    """Rebuild all UNUSABLE indexes in schema."""
    body = request.get_json(force=True)
    schema = (body.get("schema") or "").strip().upper()
    if not schema:
        return jsonify({"error": "schema required"}), 400

    cfgs = _state["load_configs"]()
    conn = get_oracle_conn("target", cfgs, prefer_owner=True)
    try:
        results = []
        with conn.cursor() as cur:
            cur.execute("""
                SELECT index_name FROM all_indexes
                WHERE  owner = :s AND status = 'UNUSABLE'
            """, {"s": schema})
            indexes = [r[0] for r in cur.fetchall()]

            for idx in indexes:
                try:
                    cur.execute(f'ALTER INDEX "{schema}"."{idx}" REBUILD')
                    results.append({"index": idx, "status": "ok"})
                except Exception as exc:
                    results.append({"index": idx, "status": "error", "error": str(exc)})
        conn.commit()
        return jsonify(results)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.post("/api/switchover/enable-constraints")
def enable_constraints():
    """Enable all disabled PK/UK/CHECK constraints in schema."""
    body = request.get_json(force=True)
    schema = (body.get("schema") or "").strip().upper()
    if not schema:
        return jsonify({"error": "schema required"}), 400

    cfgs = _state["load_configs"]()
    conn = get_oracle_conn("target", cfgs, prefer_owner=True)
    try:
        results = []
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name, constraint_name, constraint_type
                FROM   all_constraints
                WHERE  owner = :s AND constraint_type IN ('P','U','C') AND status = 'DISABLED'
            """, {"s": schema})
            constraints = cur.fetchall()

            for tbl, con, ctype in constraints:
                try:
                    cur.execute(f'ALTER TABLE "{schema}"."{tbl}" ENABLE CONSTRAINT "{con}"')
                    results.append({"table": tbl, "constraint": con, "type": ctype, "status": "ok"})
                except Exception as exc:
                    results.append({"table": tbl, "constraint": con, "type": ctype,
                                    "status": "error", "error": str(exc)})
        conn.commit()
        return jsonify(results)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.post("/api/switchover/set-logging")
def set_logging():
    """Set LOGGING on all NOLOGGING tables in schema."""
    body = request.get_json(force=True)
    schema = (body.get("schema") or "").strip().upper()
    if not schema:
        return jsonify({"error": "schema required"}), 400

    cfgs = _state["load_configs"]()
    conn = get_oracle_conn("target", cfgs, prefer_owner=True)
    try:
        results = []
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM all_tables
                WHERE  owner = :s AND logging = 'NO'
            """, {"s": schema})
            tables = [r[0] for r in cur.fetchall()]

            for tbl in tables:
                try:
                    cur.execute(f'ALTER TABLE "{schema}"."{tbl}" LOGGING')
                    results.append({"table": tbl, "status": "ok"})
                except Exception as exc:
                    results.append({"table": tbl, "status": "error", "error": str(exc)})
        conn.commit()
        return jsonify(results)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()
