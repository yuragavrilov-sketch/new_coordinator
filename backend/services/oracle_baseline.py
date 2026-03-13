"""Baseline publish: TRUNCATE final table + INSERT /*+ APPEND PARALLEL */ FROM stage."""

from services.oracle_scn import open_oracle_conn


def publish_baseline(
    dst_cfg: dict,
    target_schema: str,
    target_table: str,
    stage_table: str,
    parallel_degree: int = 4,
) -> int:
    """
    1. TRUNCATE target_schema.target_table
    2. INSERT /*+ APPEND PARALLEL(<n>) */ INTO target SELECT * FROM stage
    3. COMMIT

    Returns the number of rows inserted.
    Idempotent on restart (TRUNCATE makes it safe to retry).
    """
    conn = open_oracle_conn(dst_cfg)
    try:
        tgt = f'"{target_schema.upper()}"."{target_table.upper()}"'
        stg = f'"{target_schema.upper()}"."{stage_table.upper()}"'

        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {tgt}")

            cur.execute(
                f"INSERT /*+ APPEND PARALLEL({parallel_degree}) */ "
                f"INTO {tgt} SELECT * FROM {stg}"
            )
            rows_inserted = cur.rowcount

        conn.commit()
        return rows_inserted if rows_inserted >= 0 else 0
    finally:
        conn.close()
