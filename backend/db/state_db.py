"""PostgreSQL state-DB connection and schema initialisation."""

import json
import os
from datetime import datetime
import decimal

import re

import psycopg2

PG_DSN = os.environ.get(
    "STATE_DB_DSN",
    "postgresql://postgres:postgres@localhost:5432/migration_state",
)


def _masked_dsn(dsn: str) -> str:
    """Replace password in DSN URL with ***."""
    return re.sub(r"(://[^:]+:)[^@]+(@)", r"\1***\2", dsn)


print(f"[state_db] DSN = {_masked_dsn(PG_DSN)}")

# In-memory fallback
_mem_configs: dict = {
    "oracle_source": {},
    "oracle_target": {},
    "kafka": {},
    "kafka_connect": {},
}


def get_conn():
    try:
        return psycopg2.connect(PG_DSN)
    except Exception as exc:
        print(f"[state_db] connection FAILED ({_masked_dsn(PG_DSN)}): {exc}")
        raise


# ---------------------------------------------------------------------------
# Row serialization helpers
# ---------------------------------------------------------------------------

def _clean_value(v):
    if isinstance(v, datetime):
        return v.isoformat() + "Z"
    if isinstance(v, decimal.Decimal):
        return str(v)
    return v


def clean_row(d: dict) -> dict:
    return {k: _clean_value(v) for k, v in d.items()}


def row_to_dict(cursor, row: tuple) -> dict:
    cols = [desc[0] for desc in cursor.description]
    return clean_row(dict(zip(cols, row)))


# ---------------------------------------------------------------------------
# Schema init
# ---------------------------------------------------------------------------

def init_db() -> None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS service_configs (
                    service_name VARCHAR(50) PRIMARY KEY,
                    config       JSONB NOT NULL DEFAULT '{}',
                    updated_at   TIMESTAMP DEFAULT NOW()
                )
            """)
            for svc in ("oracle_source", "oracle_target", "kafka", "kafka_connect"):
                cur.execute("""
                    INSERT INTO service_configs (service_name, config)
                    VALUES (%s, '{}'::jsonb)
                    ON CONFLICT (service_name) DO NOTHING
                """, (svc,))

            cur.execute("""
                CREATE TABLE IF NOT EXISTS migrations (
                    migration_id              uuid PRIMARY KEY,
                    migration_name            varchar(255) NOT NULL,
                    phase                     varchar(32)  NOT NULL DEFAULT 'DRAFT',
                    state_changed_at          timestamp    NOT NULL DEFAULT now(),

                    source_connection_id      varchar(64)  NOT NULL DEFAULT '',
                    target_connection_id      varchar(64)  NOT NULL DEFAULT '',

                    source_schema             varchar(128) NOT NULL DEFAULT '',
                    source_table              varchar(128) NOT NULL DEFAULT '',
                    target_schema             varchar(128) NOT NULL DEFAULT '',
                    target_table              varchar(128) NOT NULL DEFAULT '',

                    stage_table_name          varchar(128) NOT NULL DEFAULT '',
                    connector_name            varchar(255) NOT NULL DEFAULT '',
                    topic_prefix              varchar(255) NOT NULL DEFAULT '',
                    consumer_group            varchar(255) NOT NULL DEFAULT '',

                    chunk_strategy            varchar(32)  NOT NULL DEFAULT '',
                    chunk_size                bigint       NOT NULL DEFAULT 10000,
                    max_parallel_workers      int          NOT NULL DEFAULT 1,
                    apply_mode                varchar(32)  NOT NULL DEFAULT '',

                    source_pk_exists          boolean      NOT NULL DEFAULT false,
                    source_uk_exists          boolean      NOT NULL DEFAULT false,

                    effective_key_type        varchar(32)  NOT NULL DEFAULT '',
                    effective_key_source      varchar(32)  NOT NULL DEFAULT '',
                    effective_key_columns_json text         NOT NULL DEFAULT '[]',

                    key_uniqueness_validated  boolean      NOT NULL DEFAULT false,
                    key_validation_status     varchar(32)  NULL,
                    key_validation_message    text         NULL,

                    start_scn                 numeric(20,0) NULL,
                    scn_fixed_at              timestamp    NULL,

                    created_by                varchar(128) NULL,
                    description               text         NULL,

                    created_at                timestamp    NOT NULL DEFAULT now(),
                    updated_at                timestamp    NOT NULL DEFAULT now(),

                    locked_by                 varchar(128) NULL,
                    lock_until                timestamp    NULL,

                    error_code                varchar(64)  NULL,
                    error_text                text         NULL,
                    failed_phase              varchar(32)  NULL,
                    retry_count               int          NOT NULL DEFAULT 0
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS migration_state_history (
                    id                bigserial    PRIMARY KEY,
                    migration_id      uuid         NOT NULL REFERENCES migrations(migration_id),

                    from_phase        varchar(32)  NULL,
                    to_phase          varchar(32)  NOT NULL,

                    transition_status varchar(16)  NOT NULL DEFAULT 'SUCCESS',
                    transition_reason varchar(64)  NULL,
                    message           text         NULL,

                    actor_type        varchar(32)  NOT NULL DEFAULT 'SYSTEM',
                    actor_id          varchar(128) NULL,
                    correlation_id    varchar(128) NULL,

                    created_at        timestamp    NOT NULL DEFAULT current_timestamp
                )
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_msh_migration_created
                    ON migration_state_history(migration_id, created_at DESC)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_migrations_phase
                    ON migrations(phase)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_migrations_state_changed
                    ON migrations(state_changed_at DESC)
            """)

            # ── New columns on migrations ─────────────────────────────────
            for col_sql in [
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS total_rows           BIGINT",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS total_chunks         INTEGER",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS chunks_done          INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS chunks_failed        INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS validate_hash_sample BOOLEAN NOT NULL DEFAULT FALSE",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS validation_result    JSONB",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS connector_status     VARCHAR(50)",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS kafka_lag            BIGINT",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS kafka_lag_checked_at TIMESTAMPTZ",
                "ALTER TABLE migrations ADD COLUMN IF NOT EXISTS rows_loaded          BIGINT NOT NULL DEFAULT 0",
                # migration_cdc_state columns (table created below, these are for upgrades)
                "ALTER TABLE migration_cdc_state ADD COLUMN IF NOT EXISTS rows_applied BIGINT NOT NULL DEFAULT 0",
            ]:
                try:
                    cur.execute(col_sql)
                except Exception:
                    pass  # table may not exist yet on first run — created below

            # ── migration_chunks ──────────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS migration_chunks (
                    chunk_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    migration_id    UUID NOT NULL REFERENCES migrations(migration_id) ON DELETE CASCADE,
                    chunk_seq       INTEGER NOT NULL,
                    rowid_start     VARCHAR(20) NOT NULL,
                    rowid_end       VARCHAR(20) NOT NULL,
                    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                    rows_loaded     BIGINT NOT NULL DEFAULT 0,
                    worker_id       VARCHAR(200),
                    claimed_at      TIMESTAMPTZ,
                    started_at      TIMESTAMPTZ,
                    completed_at    TIMESTAMPTZ,
                    error_text      TEXT,
                    retry_count     INTEGER NOT NULL DEFAULT 0,
                    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (migration_id, chunk_seq)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_chunks_migration_status
                    ON migration_chunks (migration_id, status)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_chunks_pending
                    ON migration_chunks (status, created_at)
                    WHERE status = 'PENDING'
            """)

            # ── migration_cdc_state ───────────────────────────────────────
            cur.execute("""
                CREATE TABLE IF NOT EXISTS migration_cdc_state (
                    migration_id      UUID PRIMARY KEY REFERENCES migrations(migration_id) ON DELETE CASCADE,
                    consumer_group    VARCHAR(200) NOT NULL DEFAULT '',
                    topic             VARCHAR(200) NOT NULL DEFAULT '',
                    total_lag         BIGINT NOT NULL DEFAULT 0,
                    lag_by_partition  JSONB,
                    last_event_scn    NUMERIC,
                    last_event_ts     TIMESTAMPTZ,
                    apply_rate_rps    NUMERIC(10,2),
                    rows_applied      BIGINT NOT NULL DEFAULT 0,
                    worker_id         VARCHAR(200),
                    worker_heartbeat  TIMESTAMPTZ,
                    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Migration DB helpers (used by orchestrator and routes)
# ---------------------------------------------------------------------------

def get_active_migrations(conn) -> list[dict]:
    """Return all migrations in active (non-terminal) phases."""
    _ACTIVE_PHASES = (
        "NEW", "PREPARING", "SCN_FIXED",
        "CONNECTOR_STARTING", "CDC_BUFFERING",
        "CHUNKING", "BULK_LOADING", "BULK_LOADED",
        "STAGE_VALIDATING", "STAGE_VALIDATED",
        "BASELINE_PUBLISHING", "BASELINE_PUBLISHED",
        "STAGE_DROPPING", "INDEXES_ENABLING",
        "CDC_APPLY_STARTING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP",
        "STEADY_STATE",
    )
    placeholders = ",".join(["%s"] * len(_ACTIVE_PHASES))
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM migrations WHERE phase IN ({placeholders})",
            _ACTIVE_PHASES,
        )
        return [row_to_dict(cur, r) for r in cur.fetchall()]


def transition_phase(
    conn,
    migration_id: str,
    to_phase: str,
    *,
    actor_type: str = "SYSTEM",
    actor_id: str | None = None,
    message: str | None = None,
    error_code: str | None = None,
    error_text: str | None = None,
    extra_fields: dict | None = None,
) -> str:
    """
    Transition a migration to *to_phase*.
    Returns the previous phase.
    Caller must commit/rollback the connection.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT phase FROM migrations WHERE migration_id = %s FOR UPDATE",
            (migration_id,),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Migration {migration_id} not found")
        from_phase = row[0]

        fields: dict = {
            "phase":            to_phase,
            "state_changed_at": "NOW()",
            "updated_at":       "NOW()",
        }
        if to_phase == "FAILED":
            if error_code:
                fields["error_code"] = error_code
            if error_text:
                fields["error_text"] = error_text
            fields["failed_phase"] = from_phase
        if extra_fields:
            fields.update(extra_fields)

        # Build SET clause — values that are the literal NOW() sentinel go in raw
        set_parts, values = [], []
        for k, v in fields.items():
            if v == "NOW()":
                set_parts.append(f"{k} = NOW()")
            else:
                set_parts.append(f"{k} = %s")
                values.append(v)
        values.append(migration_id)
        cur.execute(
            f"UPDATE migrations SET {', '.join(set_parts)} WHERE migration_id = %s",
            values,
        )
        cur.execute("""
            INSERT INTO migration_state_history
                (migration_id, from_phase, to_phase, message, actor_type, actor_id)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (migration_id, from_phase, to_phase, message, actor_type, actor_id))
    return from_phase


def update_migration_fields(conn, migration_id: str, fields: dict) -> None:
    """
    Update arbitrary columns on a migration row.
    Caller must commit the connection.
    """
    if not fields:
        return
    set_parts = [f"{k} = %s" for k in fields]
    values = list(fields.values()) + [migration_id]
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE migrations SET {', '.join(set_parts)}, updated_at = NOW() "
            f"WHERE migration_id = %s",
            values,
        )


# ---------------------------------------------------------------------------
# Service config CRUD
# ---------------------------------------------------------------------------

def load_configs(db_available: bool) -> dict:
    if not db_available:
        return {k: dict(v) for k, v in _mem_configs.items()}
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT service_name, config FROM service_configs")
                return {row[0]: (row[1] or {}) for row in cur.fetchall()}
        finally:
            conn.close()
    except Exception as exc:
        print(f"[db] load_configs error: {exc}")
        return {k: dict(v) for k, v in _mem_configs.items()}


def save_config(service: str, config: dict, db_available: bool) -> None:
    _mem_configs[service] = config
    if not db_available:
        return
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO service_configs (service_name, config, updated_at)
                    VALUES (%s, %s::jsonb, NOW())
                    ON CONFLICT (service_name) DO UPDATE
                        SET config = EXCLUDED.config, updated_at = NOW()
                """, (service, json.dumps(config)))
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        print(f"[db] save_config error: {exc}")
