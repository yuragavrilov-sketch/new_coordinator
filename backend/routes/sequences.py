"""Sequence comparison and advancement routes."""

from flask import Blueprint, jsonify, request
from db.oracle_browser import get_oracle_conn

bp = Blueprint("sequences", __name__)

_state: dict = {}


def init(load_configs_fn):
    _state["load_configs"] = load_configs_fn


@bp.get("/api/sequences/compare")
def compare_sequences():
    """Compare sequence values between source and target for a given schema."""
    schema = request.args.get("schema", "").strip().upper()
    if not schema:
        return jsonify({"error": "schema required"}), 400

    cfgs = _state["load_configs"]()
    results = []

    try:
        src_conn = get_oracle_conn("source", cfgs, prefer_owner=True)
    except Exception as exc:
        return jsonify({"error": f"Source connection failed: {exc}"}), 503

    try:
        tgt_conn = get_oracle_conn("target", cfgs, prefer_owner=True)
    except Exception as exc:
        src_conn.close()
        return jsonify({"error": f"Target connection failed: {exc}"}), 503

    try:
        # Get all sequences from source
        with src_conn.cursor() as cur:
            cur.execute("""
                SELECT sequence_name, last_number, increment_by, cache_size,
                       min_value, max_value
                FROM   all_sequences
                WHERE  sequence_owner = :s
                ORDER BY sequence_name
            """, {"s": schema})
            src_seqs = {
                row[0]: {
                    "last_number": row[1],
                    "increment_by": row[2],
                    "cache_size": row[3],
                    "min_value": row[4],
                    "max_value": row[5],
                }
                for row in cur.fetchall()
            }

        # Get all sequences from target
        tgt_schema = schema  # same schema name on target
        with tgt_conn.cursor() as cur:
            cur.execute("""
                SELECT sequence_name, last_number, increment_by, cache_size,
                       min_value, max_value
                FROM   all_sequences
                WHERE  sequence_owner = :s
                ORDER BY sequence_name
            """, {"s": tgt_schema})
            tgt_seqs = {
                row[0]: {
                    "last_number": row[1],
                    "increment_by": row[2],
                    "cache_size": row[3],
                    "min_value": row[4],
                    "max_value": row[5],
                }
                for row in cur.fetchall()
            }

        # Merge
        all_names = sorted(set(src_seqs.keys()) | set(tgt_seqs.keys()))
        for name in all_names:
            src = src_seqs.get(name)
            tgt = tgt_seqs.get(name)
            src_val = int(src["last_number"]) if src else None
            tgt_val = int(tgt["last_number"]) if tgt else None
            delta = (src_val - tgt_val) if src_val is not None and tgt_val is not None else None
            results.append({
                "sequence_name": name,
                "source_value": src_val,
                "target_value": tgt_val,
                "delta": delta,
                "increment_by": int(src["increment_by"]) if src else (int(tgt["increment_by"]) if tgt else 1),
                "cache_size": src["cache_size"] if src else (tgt["cache_size"] if tgt else 0),
                "source_only": src is not None and tgt is None,
                "target_only": src is None and tgt is not None,
            })

        return jsonify(results)

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        src_conn.close()
        tgt_conn.close()


@bp.post("/api/sequences/advance")
def advance_sequences():
    """Advance sequences on target to source_value + delta.

    Body: { "schema": "...", "delta": 1000, "sequences": ["SEQ1", "SEQ2", ...] }
    If sequences is omitted/empty, advance ALL sequences that exist on both sides.
    """
    body = request.get_json(force=True)
    schema = (body.get("schema") or "").strip().upper()
    delta = int(body.get("delta", 0))
    seq_filter = body.get("sequences") or []

    if not schema:
        return jsonify({"error": "schema required"}), 400

    cfgs = _state["load_configs"]()

    try:
        src_conn = get_oracle_conn("source", cfgs, prefer_owner=True)
    except Exception as exc:
        return jsonify({"error": f"Source connection failed: {exc}"}), 503

    try:
        tgt_conn = get_oracle_conn("target", cfgs, prefer_owner=True)
    except Exception as exc:
        src_conn.close()
        return jsonify({"error": f"Target connection failed: {exc}"}), 503

    try:
        # Get source sequence values
        with src_conn.cursor() as cur:
            cur.execute("""
                SELECT sequence_name, last_number, increment_by
                FROM   all_sequences
                WHERE  sequence_owner = :s
            """, {"s": schema})
            src_seqs = {row[0]: {"last_number": int(row[1]), "increment_by": int(row[2])}
                        for row in cur.fetchall()}

        # Get target sequence values
        with tgt_conn.cursor() as cur:
            cur.execute("""
                SELECT sequence_name, last_number, increment_by
                FROM   all_sequences
                WHERE  sequence_owner = :s
            """, {"s": schema})
            tgt_seqs = {row[0]: {"last_number": int(row[1]), "increment_by": int(row[2])}
                        for row in cur.fetchall()}

        # Determine which sequences to advance
        if seq_filter:
            names = [n.upper() for n in seq_filter]
        else:
            names = sorted(set(src_seqs.keys()) & set(tgt_seqs.keys()))

        results = []
        for name in names:
            if name not in src_seqs:
                results.append({"sequence_name": name, "status": "skip",
                                "message": "Not found on source"})
                continue
            if name not in tgt_seqs:
                results.append({"sequence_name": name, "status": "skip",
                                "message": "Not found on target"})
                continue

            src_val = src_seqs[name]["last_number"]
            tgt_val = tgt_seqs[name]["last_number"]
            new_val = src_val + delta
            inc = tgt_seqs[name]["increment_by"]

            if new_val <= tgt_val:
                results.append({
                    "sequence_name": name, "status": "skip",
                    "message": f"Target ({tgt_val}) already >= new value ({new_val})",
                    "source_value": src_val, "target_value": tgt_val,
                    "new_value": new_val,
                })
                continue

            try:
                # Advance by altering INCREMENT BY, doing NEXTVAL, then restoring
                jump = new_val - tgt_val
                with tgt_conn.cursor() as cur:
                    cur.execute(f'ALTER SEQUENCE "{schema}"."{name}" INCREMENT BY {jump}')
                    cur.execute(f'SELECT "{schema}"."{name}".NEXTVAL FROM DUAL')
                    cur.execute(f'ALTER SEQUENCE "{schema}"."{name}" INCREMENT BY {inc}')
                tgt_conn.commit()
                results.append({
                    "sequence_name": name, "status": "ok",
                    "source_value": src_val, "old_target_value": tgt_val,
                    "new_value": new_val,
                })
            except Exception as exc:
                tgt_conn.rollback()
                results.append({
                    "sequence_name": name, "status": "error",
                    "error": str(exc),
                })

        return jsonify(results)

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        src_conn.close()
        tgt_conn.close()
