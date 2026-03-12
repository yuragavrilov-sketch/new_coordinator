"""PostgreSQL state-DB connection and schema initialisation."""

import json
import os
from datetime import datetime
import decimal

import psycopg2

PG_DSN = os.environ.get(
    "STATE_DB_DSN",
    "postgresql://postgres:postgres@localhost:5432/migration_state",
)

# In-memory fallback
_mem_configs: dict = {
    "oracle_source": {},
    "oracle_target": {},
    "kafka": {},
    "kafka_connect": {},
}


def get_conn():
    return psycopg2.connect(PG_DSN)


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

        conn.commit()
    finally:
        conn.close()


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
