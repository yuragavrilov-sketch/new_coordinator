"""DDL object synchronisation — create missing indexes, constraints, triggers on target."""

import re

from services.oracle_scn import open_oracle_conn


def sync_target_objects(
    src_cfg: dict,
    dst_cfg: dict,
    src_schema: str,
    src_table: str,
    tgt_schema: str,
    tgt_table: str,
    types: set | None = None,
) -> dict:
    """
    Create objects present on source but missing on target.

    types: set of 'constraints', 'indexes', 'triggers' (None = all three)

    Returns:
      {
        "constraints": {"added": [...], "skipped": [...], "errors": [...]},
        "indexes":     {"added": [...], "skipped": [...], "errors": [...]},
        "triggers":    {"added": [...], "skipped": [...], "errors": [...]},
      }
    """
    if types is None:
        types = {"constraints", "indexes", "triggers"}

    src_conn = open_oracle_conn(src_cfg)
    dst_conn = open_oracle_conn(dst_cfg)
    try:
        result = {
            "constraints": {"added": [], "skipped": [], "errors": []},
            "indexes":     {"added": [], "skipped": [], "errors": []},
            "triggers":    {"added": [], "skipped": [], "errors": []},
        }
        if "constraints" in types:
            _sync_constraints(
                src_conn, dst_conn,
                src_schema, src_table, tgt_schema, tgt_table,
                result["constraints"],
            )
        if "indexes" in types:
            _sync_indexes(
                src_conn, dst_conn,
                src_schema, src_table, tgt_schema, tgt_table,
                result["indexes"],
            )
        if "triggers" in types:
            _sync_triggers(
                src_conn, dst_conn,
                src_schema, src_table, tgt_schema, tgt_table,
                result["triggers"],
            )
        return result
    finally:
        src_conn.close()
        dst_conn.close()


# ── Constraints ────────────────────────────────────────────────────────────────

# Oracle stores NOT NULL as CHECK "COL" IS NOT NULL — skip these
_NOT_NULL_RE = re.compile(r'^\s*"[^"]+"\s+IS\s+NOT\s+NULL\s*$', re.IGNORECASE)


def _sync_constraints(src_conn, dst_conn, src_schema, src_table, tgt_schema, tgt_table, out):
    with src_conn.cursor() as cur:
        cur.execute("""
            SELECT ac.constraint_name, ac.constraint_type,
                   ac.search_condition, ac.r_owner, ac.r_constraint_name,
                   ac.delete_rule,
                   LISTAGG(acc.column_name, ',')
                       WITHIN GROUP (ORDER BY acc.position) AS cols
            FROM   all_constraints  ac
            JOIN   all_cons_columns acc
                   ON  ac.constraint_name = acc.constraint_name
                   AND ac.owner           = acc.owner
            WHERE  ac.owner           = :s
              AND  ac.table_name      = :t
              AND  ac.constraint_type IN ('P','U','R','C')
            GROUP BY ac.constraint_name, ac.constraint_type,
                     ac.search_condition, ac.r_owner, ac.r_constraint_name, ac.delete_rule
        """, {"s": src_schema.upper(), "t": src_table.upper()})
        src_rows = cur.fetchall()

    # Target constraints keyed by (type_code, cols) for structural matching
    with dst_conn.cursor() as cur:
        cur.execute("""
            SELECT ac.constraint_type,
                   LISTAGG(acc.column_name, ',')
                       WITHIN GROUP (ORDER BY acc.position) AS cols
            FROM   all_constraints  ac
            JOIN   all_cons_columns acc
                   ON  ac.constraint_name = acc.constraint_name
                   AND ac.owner           = acc.owner
            WHERE  ac.owner      = :s
              AND  ac.table_name = :t
              AND  ac.constraint_type IN ('P','U','R','C')
            GROUP BY ac.constraint_name, ac.constraint_type
        """, {"s": tgt_schema.upper(), "t": tgt_table.upper()})
        tgt_keys = {(r[0], r[1]) for r in cur.fetchall()}

    s = tgt_schema.upper()
    t = tgt_table.upper()

    # PK/UK first so FK can reference them, CHECK last
    _order = {"P": 0, "U": 1, "R": 2, "C": 3}
    for cname, ctype, search_cond, r_owner, r_cname, delete_rule, cols in sorted(
        src_rows, key=lambda r: _order.get(r[1], 9)
    ):
        key = (ctype, cols)
        if key in tgt_keys:
            out["skipped"].append(cname)
            continue

        try:
            if ctype == "P":
                ddl = (
                    f'ALTER TABLE "{s}"."{t}" ADD CONSTRAINT "{cname}" '
                    f"PRIMARY KEY ({_qcols(cols)})"
                )
            elif ctype == "U":
                ddl = (
                    f'ALTER TABLE "{s}"."{t}" ADD CONSTRAINT "{cname}" '
                    f"UNIQUE ({_qcols(cols)})"
                )
            elif ctype == "C":
                cond = str(search_cond or "").strip()
                if not cond or _NOT_NULL_RE.match(cond):
                    out["skipped"].append(cname)
                    continue
                ddl = (
                    f'ALTER TABLE "{s}"."{t}" ADD CONSTRAINT "{cname}" '
                    f"CHECK ({cond})"
                )
            elif ctype == "R":
                ref_table, ref_cols = _resolve_ref(src_conn, r_owner, r_cname)
                if not ref_table:
                    out["errors"].append({
                        "name": cname,
                        "error": f"Cannot resolve referenced constraint {r_owner}.{r_cname}",
                    })
                    continue
                # Map r_owner → tgt_schema when FK points to the same source schema
                ref_schema = (
                    tgt_schema.upper() if r_owner.upper() == src_schema.upper()
                    else r_owner.upper()
                )
                del_clause = (
                    f" ON DELETE {delete_rule}"
                    if delete_rule and delete_rule not in ("NO ACTION", None)
                    else ""
                )
                ddl = (
                    f'ALTER TABLE "{s}"."{t}" ADD CONSTRAINT "{cname}" '
                    f"FOREIGN KEY ({_qcols(cols)}) "
                    f'REFERENCES "{ref_schema}"."{ref_table}" ({_qcols(ref_cols)})'
                    f"{del_clause}"
                )
            else:
                out["skipped"].append(cname)
                continue

            with dst_conn.cursor() as cur:
                cur.execute(ddl)
            tgt_keys.add(key)
            out["added"].append(cname)
        except Exception as exc:
            out["errors"].append({"name": cname, "error": str(exc)})

    dst_conn.commit()


def _resolve_ref(conn, owner: str, constraint_name: str):
    """Return (table_name, comma_cols) for a PK/UK constraint."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ac.table_name,
                   LISTAGG(acc.column_name, ',') WITHIN GROUP (ORDER BY acc.position)
            FROM   all_constraints  ac
            JOIN   all_cons_columns acc
                   ON  ac.constraint_name = acc.constraint_name
                   AND ac.owner           = acc.owner
            WHERE  ac.owner          = :o
              AND  ac.constraint_name = :c
            GROUP BY ac.table_name
        """, {"o": owner, "c": constraint_name})
        row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)


# ── Indexes ────────────────────────────────────────────────────────────────────

def _sync_indexes(src_conn, dst_conn, src_schema, src_table, tgt_schema, tgt_table, out):
    # Only indexes not backing a constraint (Oracle creates those automatically)
    with src_conn.cursor() as cur:
        cur.execute("""
            SELECT ai.index_name, ai.uniqueness, ai.index_type,
                   LISTAGG(aic.column_name, ',')
                       WITHIN GROUP (ORDER BY aic.column_position) AS cols
            FROM   all_indexes     ai
            JOIN   all_ind_columns aic
                   ON  ai.index_name  = aic.index_name
                   AND ai.owner       = aic.index_owner
            WHERE  ai.owner      = :s
              AND  ai.table_name = :t
              AND  NOT EXISTS (
                       SELECT 1 FROM all_constraints ac
                       WHERE  ac.owner      = :s
                         AND  ac.table_name = :t
                         AND  ac.index_name = ai.index_name
                   )
            GROUP BY ai.index_name, ai.uniqueness, ai.index_type
        """, {"s": src_schema.upper(), "t": src_table.upper()})
        src_rows = cur.fetchall()

    with dst_conn.cursor() as cur:
        cur.execute(
            "SELECT index_name FROM all_indexes WHERE owner = :s AND table_name = :t",
            {"s": tgt_schema.upper(), "t": tgt_table.upper()},
        )
        tgt_names = {r[0] for r in cur.fetchall()}

    s = tgt_schema.upper()
    t = tgt_table.upper()

    for idx_name, uniqueness, idx_type, cols in src_rows:
        if idx_name in tgt_names:
            out["skipped"].append(idx_name)
            continue

        # Skip function-based and BITMAP (require expression / special syntax)
        if not idx_type.startswith("NORMAL"):
            out["skipped"].append(f"{idx_name} (type={idx_type}, skipped)")
            continue

        unique_kw = "UNIQUE " if uniqueness == "UNIQUE" else ""
        rev_kw    = " REVERSE" if idx_type == "NORMAL/REV" else ""
        ddl = (
            f'CREATE {unique_kw}INDEX "{s}"."{idx_name}" '
            f'ON "{s}"."{t}" ({_qcols(cols)}){rev_kw}'
        )
        try:
            with dst_conn.cursor() as cur:
                cur.execute(ddl)
            tgt_names.add(idx_name)
            out["added"].append(idx_name)
        except Exception as exc:
            out["errors"].append({"name": idx_name, "error": str(exc)})

    dst_conn.commit()


# ── Triggers ───────────────────────────────────────────────────────────────────

def _sync_triggers(src_conn, dst_conn, src_schema, src_table, tgt_schema, tgt_table, out):
    with src_conn.cursor() as cur:
        cur.execute("""
            SELECT trigger_name, description, trigger_body
            FROM   all_triggers
            WHERE  owner      = :s
              AND  table_name = :t
        """, {"s": src_schema.upper(), "t": src_table.upper()})
        src_rows = cur.fetchall()

    with dst_conn.cursor() as cur:
        cur.execute(
            "SELECT trigger_name FROM all_triggers WHERE owner = :s AND table_name = :t",
            {"s": tgt_schema.upper(), "t": tgt_table.upper()},
        )
        tgt_names = {r[0] for r in cur.fetchall()}

    # Replace source schema.table reference in description header
    src_ref = f"{src_schema.upper()}.{src_table.upper()}"
    tgt_ref = f"{tgt_schema.upper()}.{tgt_table.upper()}"

    for trg_name, description, body in src_rows:
        if trg_name in tgt_names:
            out["skipped"].append(trg_name)
            continue
        try:
            desc = (description or "").replace(src_ref, tgt_ref)
            ddl  = f"CREATE OR REPLACE TRIGGER {desc}\n{body or ''}"
            with dst_conn.cursor() as cur:
                cur.execute(ddl)
            tgt_names.add(trg_name)
            out["added"].append(trg_name)
        except Exception as exc:
            out["errors"].append({"name": trg_name, "error": str(exc)})

    dst_conn.commit()


# ── Helpers ────────────────────────────────────────────────────────────────────

def _qcols(cols_str: str) -> str:
    """'COL1,COL2' → '"COL1", "COL2"'"""
    return ", ".join(f'"{c.strip()}"' for c in cols_str.split(","))
