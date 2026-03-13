"""Shared utilities: API client, Oracle connection, config loading."""

import os
import socket
import time
from typing import Optional

import requests

API_URL   = os.environ.get("API_URL",   "http://localhost:5000")
WORKER_ID = os.environ.get("WORKER_ID", f"{socket.gethostname()}:{os.getpid()}")


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def api_get(path: str, timeout: int = 10) -> dict:
    r = requests.get(f"{API_URL}{path}", timeout=timeout)
    r.raise_for_status()
    return r.json()


def api_post(path: str, body: dict, timeout: int = 10, retries: int = 3) -> Optional[dict]:
    url = f"{API_URL}{path}"
    for attempt in range(retries):
        try:
            r = requests.post(url, json=body, timeout=timeout)
            r.raise_for_status()
            return r.json() if r.text.strip() else {}
        except requests.RequestException as exc:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)
    return None


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

_configs_cache: dict = {}
_configs_ts: float = 0.0
_CONFIGS_TTL = 60.0  # seconds


def get_configs(force: bool = False) -> dict:
    """Load service configs from Flask API with a 60-second cache."""
    global _configs_cache, _configs_ts
    if force or (time.time() - _configs_ts > _CONFIGS_TTL):
        _configs_cache = api_get("/api/config")
        _configs_ts = time.time()
    return _configs_cache


# ---------------------------------------------------------------------------
# Oracle connection
# ---------------------------------------------------------------------------

def open_oracle(connection_id: str, configs: Optional[dict] = None):
    """
    Open an Oracle connection using the given service config key.
    connection_id = 'oracle_source' | 'oracle_target'
    """
    try:
        import oracledb
    except ImportError:
        raise ImportError("oracledb не установлен: pip install oracledb")

    cfg = (configs or get_configs()).get(connection_id, {})
    host         = cfg.get("host", "").strip()
    port         = int(cfg.get("port", 1521))
    service_name = cfg.get("service_name", "").strip()
    user         = cfg.get("user", "").strip()
    password     = cfg.get("password", "")

    if not host or not service_name or not user:
        raise ValueError(
            f"Oracle {connection_id} не настроен. Проверьте Настройки в UI."
        )
    return oracledb.connect(
        user=user,
        password=password,
        dsn=f"{host}:{port}/{service_name}",
    )
