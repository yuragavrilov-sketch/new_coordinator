"""Oracle DB browser — schemas, tables, columns/keys for migration wizard."""

_SYSTEM_SCHEMAS = frozenset([
    "SYS", "SYSTEM", "OUTLN", "DIP", "ORACLE_OCM", "DBSNMP", "APPQOSSYS",
    "WMSYS", "EXFSYS", "CTXSYS", "XDB", "ANONYMOUS", "ORDSYS", "ORDPLUGINS",
    "SI_INFORMTN_SCHEMA", "MDSYS", "OLAPSYS", "MDDATA", "XS$NULL",
    "APEX_PUBLIC_USER", "FLOWS_FILES", "DVSYS", "LBACSYS", "OJVMSYS",
    "GSMADMIN_INTERNAL", "GSMUSER", "AUDSYS", "DVF", "DBSFWUSER",
    "REMOTE_SCHEDULER_AGENT", "SYS$UMF", "SYSBACKUP", "SYSDG", "SYSKM", "SYSRAC",
])


def get_oracle_conn(db: str, configs: dict):
    """Open an Oracle connection. db = 'source' or 'target'."""
    cfg = configs.get(f"oracle_{db}", {})
    host         = cfg.get("host", "").strip()
    port         = cfg.get("port", 1521)
    service_name = cfg.get("service_name", "").strip()
    user         = cfg.get("user", "").strip()
    password     = cfg.get("password", "")
    if not host or not service_name or not user:
        raise ValueError(f"Oracle {db} не настроен — проверьте Настройки")
    try:
        import oracledb
    except ImportError:
        raise ImportError("oracledb не установлен (pip install oracledb)")
    return oracledb.connect(user=user, password=password, dsn=f"{host}:{port}/{service_name}")


def list_schemas(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT owner FROM all_tables ORDER BY owner")
        return [r[0] for r in cur.fetchall() if r[0] not in _SYSTEM_SCHEMAS]


def list_tables(conn, schema: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM all_tables WHERE owner = :s ORDER BY table_name",
            {"s": schema},
        )
        return [r[0] for r in cur.fetchall()]


def get_table_info(conn, schema: str, table: str) -> dict:
    with conn.cursor() as cur:
        # Columns
        cur.execute("""
            SELECT column_name, data_type, nullable
            FROM   all_tab_columns
            WHERE  owner = :s AND table_name = :t
            ORDER BY column_id
        """, {"s": schema, "t": table})
        columns = [{"name": r[0], "type": r[1], "nullable": r[2] == "Y"}
                   for r in cur.fetchall()]

        # PK columns
        cur.execute("""
            SELECT acc.column_name
            FROM   all_constraints  ac
            JOIN   all_cons_columns acc
                   ON  ac.constraint_name = acc.constraint_name
                   AND ac.owner           = acc.owner
            WHERE  ac.owner           = :s
              AND  ac.table_name      = :t
              AND  ac.constraint_type = 'P'
            ORDER BY acc.position
        """, {"s": schema, "t": table})
        pk_columns = [r[0] for r in cur.fetchall()]

        # UK constraints
        cur.execute("""
            SELECT ac.constraint_name, acc.column_name
            FROM   all_constraints  ac
            JOIN   all_cons_columns acc
                   ON  ac.constraint_name = acc.constraint_name
                   AND ac.owner           = acc.owner
            WHERE  ac.owner           = :s
              AND  ac.table_name      = :t
              AND  ac.constraint_type = 'U'
            ORDER BY ac.constraint_name, acc.position
        """, {"s": schema, "t": table})
        uk_map: dict = {}
        for cname, col in cur.fetchall():
            uk_map.setdefault(cname, []).append(col)
        uk_constraints = [{"name": k, "columns": v} for k, v in uk_map.items()]

    return {
        "columns":        columns,
        "pk_columns":     pk_columns,
        "uk_constraints": uk_constraints,
    }


# ── Full DDL info (for target-prep tab) ────────────────────────────────────

def get_full_ddl_info(conn, schema: str, table: str) -> dict:
    """Return full DDL snapshot: columns, constraints, indexes, triggers."""
    with conn.cursor() as cur:
        # Full column details
        cur.execute("""
            SELECT column_name, data_type, data_length, data_precision, data_scale,
                   nullable, data_default, column_id
            FROM   all_tab_columns
            WHERE  owner = :s AND table_name = :t
            ORDER BY column_id
        """, {"s": schema, "t": table})
        columns = [
            {
                "name":           r[0],
                "data_type":      r[1],
                "data_length":    r[2],
                "data_precision": r[3],
                "data_scale":     r[4],
                "nullable":       r[5] == "Y",
                "data_default":   str(r[6]).strip() if r[6] is not None else None,
                "column_id":      r[7],
            }
            for r in cur.fetchall()
        ]

        # Constraints (PK / UK / FK / CHECK) — subquery avoids outer-join NULLs
        cur.execute("""
            SELECT ac.constraint_name,
                   ac.constraint_type,
                   ac.status,
                   (SELECT LISTAGG(acc.column_name, ',')
                            WITHIN GROUP (ORDER BY acc.position)
                    FROM   all_cons_columns acc
                    WHERE  acc.constraint_name = ac.constraint_name
                      AND  acc.owner           = ac.owner) AS cols
            FROM   all_constraints ac
            WHERE  ac.owner           = :s
              AND  ac.table_name      = :t
              AND  ac.constraint_type IN ('P','U','R','C')
            ORDER BY ac.constraint_type, ac.constraint_name
        """, {"s": schema, "t": table})
        _ctype = {"P": "PRIMARY KEY", "U": "UNIQUE", "R": "FOREIGN KEY", "C": "CHECK"}
        constraints = [
            {
                "name":      r[0],
                "type":      _ctype.get(r[1], r[1]),
                "type_code": r[1],
                "status":    r[2],          # ENABLED / DISABLED
                "columns":   r[3].split(",") if r[3] else [],
            }
            for r in cur.fetchall()
        ]

        # Indexes
        cur.execute("""
            SELECT ai.index_name, ai.uniqueness, ai.index_type, ai.status,
                   LISTAGG(aic.column_name, ',')
                       WITHIN GROUP (ORDER BY aic.column_position) AS cols
            FROM   all_indexes     ai
            JOIN   all_ind_columns aic
                   ON  ai.index_name  = aic.index_name
                   AND ai.owner       = aic.index_owner
            WHERE  ai.owner      = :s
              AND  ai.table_name = :t
            GROUP BY ai.index_name, ai.uniqueness, ai.index_type, ai.status
            ORDER BY ai.index_name
        """, {"s": schema, "t": table})
        indexes = [
            {
                "name":       r[0],
                "unique":     r[1] == "UNIQUE",
                "index_type": r[2],
                "status":     r[3],         # VALID / UNUSABLE
                "columns":    r[4].split(",") if r[4] else [],
            }
            for r in cur.fetchall()
        ]

        # Triggers
        cur.execute("""
            SELECT trigger_name, trigger_type, triggering_event, status
            FROM   all_triggers
            WHERE  owner = :s AND table_name = :t
            ORDER BY trigger_name
        """, {"s": schema, "t": table})
        triggers = [
            {"name": r[0], "trigger_type": r[1], "event": r[2], "status": r[3]}
            for r in cur.fetchall()
        ]

    return {
        "schema":      schema,
        "table":       table,
        "columns":     columns,
        "constraints": constraints,
        "indexes":     indexes,
        "triggers":    triggers,
    }


def execute_target_action(conn, action: str, schema: str, table: str, object_name: str) -> None:
    """Execute a DDL management action on the target database."""
    _valid = {
        "disable_index", "enable_index",
        "disable_trigger", "enable_trigger",
        "disable_constraint", "enable_constraint",
    }
    if action not in _valid:
        raise ValueError(f"Unknown action: {action}")

    # Build DDL statement — identifiers are double-quoted to preserve case
    s, t, o = schema.upper(), table.upper(), object_name
    if action == "disable_index":
        ddl = f'ALTER INDEX "{s}"."{o}" UNUSABLE'
    elif action == "enable_index":
        ddl = f'ALTER INDEX "{s}"."{o}" REBUILD'
    elif action == "disable_trigger":
        ddl = f'ALTER TRIGGER "{s}"."{o}" DISABLE'
    elif action == "enable_trigger":
        ddl = f'ALTER TRIGGER "{s}"."{o}" ENABLE'
    elif action == "disable_constraint":
        ddl = f'ALTER TABLE "{s}"."{t}" DISABLE CONSTRAINT "{o}"'
    else:  # enable_constraint
        ddl = f'ALTER TABLE "{s}"."{t}" ENABLE CONSTRAINT "{o}"'

    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
