"""Data comparison API — compare row counts and data hashes between source and target."""

import threading
import traceback

from flask import Blueprint, jsonify, request
from db.oracle_browser import get_oracle_conn

bp = Blueprint("data_compare", __name__)

_state: dict = {}

# Column types to skip when computing row hashes (LOBs, spatial, etc.)
_SKIP_TYPES = frozenset({
    "BLOB", "CLOB", "NCLOB", "BFILE", "LONG", "LONG RAW",
    "XMLTYPE", "SDO_GEOMETRY", "ANYDATA", "URITYPE",
})


def init(get_conn_fn, load_configs_fn, broadcast_fn):
    _state["get_conn"] = get_conn_fn
    _state["load_configs"] = load_configs_fn
    _state["broadcast"] = broadcast_fn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _col_expr(col_name: str, col_type: str) -> str:
    """Build NVL(TO_CHAR(...), CHR(0)) expression for a single column."""
    q = f'"{col_name}"'
    if col_type == "DATE":
        return f"NVL(TO_CHAR({q}, 'YYYY-MM-DD HH24:MI:SS'), CHR(0))"
    if col_type.startswith("TIMESTAMP"):
        return f"NVL(TO_CHAR({q}, 'YYYY-MM-DD HH24:MI:SS.FF6'), CHR(0))"
    return f"NVL(TO_CHAR({q}), CHR(0))"


def _get_comparable_columns(conn, schema: str, table: str) -> list[dict]:
    """Return columns suitable for hash comparison (name + data_type)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM   all_tab_columns
            WHERE  owner = :s AND table_name = :t
            ORDER BY column_id
        """, {"s": schema, "t": table})
        return [
            {"name": r[0], "data_type": r[1]}
            for r in cur.fetchall()
            if r[1] not in _SKIP_TYPES
        ]


def _build_query(schema: str, table: str, columns: list[dict],
                 mode: str, last_n: int | None, order_column: str | None) -> str:
    """Build the COUNT + HASH query."""
    # Per-row hash: sum of ORA_HASH of each column value
    hash_parts = [f"ORA_HASH({_col_expr(c['name'], c['data_type'])})" for c in columns]
    row_hash = " + ".join(hash_parts) if hash_parts else "0"

    from_clause = f'"{schema}"."{table}"'

    if mode == "last_n" and order_column and last_n:
        n = int(last_n)
        from_clause = (
            f'(SELECT * FROM "{schema}"."{table}" '
            f'ORDER BY "{order_column}" DESC '
            f'FETCH FIRST {n} ROWS ONLY)'
        )

    return f"SELECT COUNT(*) AS cnt, SUM({row_hash}) AS hash_sum FROM {from_clause}"


def _run_comparison(task_id: str, configs: dict,
                    src_schema: str, src_table: str,
                    tgt_schema: str, tgt_table: str,
                    mode: str, last_n: int | None, order_column: str | None):
    """Background thread: run the comparison and write results to PostgreSQL."""
    conn = _state["get_conn"]()
    try:
        # Mark RUNNING
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE data_compare_tasks SET status = 'RUNNING', started_at = NOW() "
                "WHERE task_id = %s", (task_id,)
            )
        conn.commit()

        # Open Oracle connections
        src_conn = get_oracle_conn("source", configs)
        tgt_conn = get_oracle_conn("target", configs)
        try:
            # Get comparable columns (intersection by name)
            src_cols = _get_comparable_columns(src_conn, src_schema, src_table)
            tgt_cols = _get_comparable_columns(tgt_conn, tgt_schema, tgt_table)
            tgt_names = {c["name"] for c in tgt_cols}
            common_cols = [c for c in src_cols if c["name"] in tgt_names]

            # Build and execute queries
            src_sql = _build_query(src_schema, src_table, common_cols, mode, last_n, order_column)
            tgt_sql = _build_query(tgt_schema, tgt_table, common_cols, mode, last_n, order_column)

            with src_conn.cursor() as cur:
                cur.execute(src_sql)
                src_cnt, src_hash = cur.fetchone()

            with tgt_conn.cursor() as cur:
                cur.execute(tgt_sql)
                tgt_cnt, tgt_hash = cur.fetchone()
        finally:
            src_conn.close()
            tgt_conn.close()

        counts_match = src_cnt == tgt_cnt
        hash_match = src_hash == tgt_hash

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE data_compare_tasks
                SET    status = 'DONE',
                       source_count  = %s, target_count  = %s,
                       source_hash   = %s, target_hash   = %s,
                       counts_match  = %s, hash_match    = %s,
                       completed_at  = NOW()
                WHERE  task_id = %s
            """, (src_cnt, tgt_cnt, str(src_hash) if src_hash else None,
                  str(tgt_hash) if tgt_hash else None,
                  counts_match, hash_match, task_id))
        conn.commit()

        _state["broadcast"]("data_compare", {"task_id": task_id, "status": "DONE"})

    except Exception as exc:
        traceback.print_exc()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE data_compare_tasks SET status = 'FAILED', "
                    "error_text = %s, completed_at = NOW() WHERE task_id = %s",
                    (str(exc)[:2000], task_id),
                )
            conn.commit()
        except Exception:
            pass
        _state["broadcast"]("data_compare", {"task_id": task_id, "status": "FAILED"})
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@bp.get("/api/data-compare/tasks")
def list_tasks():
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM data_compare_tasks ORDER BY created_at DESC LIMIT 100"
            )
            from db.state_db import row_to_dict
            return jsonify([row_to_dict(cur, r) for r in cur.fetchall()])
    finally:
        conn.close()


@bp.post("/api/data-compare/run")
def run_compare():
    data = request.json or {}
    src_schema = data.get("source_schema", "").strip().upper()
    src_table = data.get("source_table", "").strip().upper()
    tgt_schema = data.get("target_schema", "").strip().upper()
    tgt_table = data.get("target_table", "").strip().upper()
    mode = data.get("compare_mode", "full")
    last_n = data.get("last_n")
    order_column = data.get("order_column", "").strip().upper() or None

    if not all([src_schema, src_table, tgt_schema, tgt_table]):
        return jsonify({"error": "source_schema, source_table, target_schema, target_table required"}), 400
    if mode not in ("full", "last_n"):
        return jsonify({"error": "compare_mode must be 'full' or 'last_n'"}), 400
    if mode == "last_n":
        if not last_n or not order_column:
            return jsonify({"error": "last_n and order_column required for last_n mode"}), 400
        last_n = int(last_n)

    configs = _state["load_configs"]()

    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO data_compare_tasks
                    (source_schema, source_table, target_schema, target_table,
                     compare_mode, last_n, order_column, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'PENDING')
                RETURNING task_id
            """, (src_schema, src_table, tgt_schema, tgt_table,
                  mode, last_n, order_column))
            task_id = str(cur.fetchone()[0])
        conn.commit()
    finally:
        conn.close()

    # Launch background thread
    threading.Thread(
        target=_run_comparison,
        args=(task_id, configs, src_schema, src_table, tgt_schema, tgt_table,
              mode, last_n, order_column),
        daemon=True,
        name=f"data-compare-{task_id[:8]}",
    ).start()

    return jsonify({"task_id": task_id, "status": "PENDING"}), 201


@bp.delete("/api/data-compare/tasks/<task_id>")
def delete_task(task_id: str):
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM data_compare_tasks WHERE task_id = %s", (task_id,))
        conn.commit()
        return jsonify({"ok": True})
    finally:
        conn.close()
