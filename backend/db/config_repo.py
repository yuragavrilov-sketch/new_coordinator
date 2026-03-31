"""Service configuration CRUD."""

import json

from db.pool import get_conn

# In-memory fallback
_mem_configs: dict = {
    "oracle_source": {},
    "oracle_target": {},
    "kafka": {},
    "kafka_connect": {},
}


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
