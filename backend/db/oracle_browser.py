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
