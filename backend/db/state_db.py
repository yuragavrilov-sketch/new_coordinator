"""Backward-compatibility shim — re-exports from new sub-modules.

All new code should import directly from db.pool, db.serialization,
db.schema, db.migrations_repo, or db.config_repo.
"""

# Pool
from db.pool import PG_DSN, get_conn  # noqa: F401

# Serialization
from db.serialization import clean_row, row_to_dict  # noqa: F401

# Schema
from db.schema import init_db  # noqa: F401

# Migration state
from db.migrations_repo import (  # noqa: F401
    get_active_migrations,
    transition_phase,
    update_migration_fields,
)

# Config
from db.config_repo import load_configs, save_config  # noqa: F401
