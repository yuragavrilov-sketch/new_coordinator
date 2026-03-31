from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class AppConfig:
    """Centralized application configuration. All values sourced from environment."""

    # PostgreSQL
    state_db_dsn: str = "postgresql://postgres:postgres@localhost:5432/migration_state"
    pg_pool_min: int = 2
    pg_pool_max: int = 10

    # Flask
    flask_host: str = "0.0.0.0"
    flask_port: int = 5000
    flask_debug: bool = True

    # Orchestrator
    orchestrator_tick_interval: int = 5
    orchestrator_start_delay: int = 3

    # Workers / CDC
    cdc_heartbeat_stale_minutes: int = 2

    # Status poller
    status_poller_interval: int = 30
    status_poller_initial_delay: int = 5

    # Kafka
    kafka_bootstrap_servers: str = "kafka:9092"

    # Debezium
    debezium_lob_enabled: bool = True
    debezium_log_mining_batch_size_max: str = "5000"
    debezium_log_mining_sleep_time_increment_ms: str = "400"
    debezium_log_mining_sleep_time_max_ms: str = "1000"
    debezium_topic_replication_factor: str = "1"
    debezium_topic_partitions: str = "1"
    debezium_topic_cleanup_policy: str = "delete"
    debezium_topic_retention_ms: str = "604800000"
    debezium_topic_compression_type: str = "snappy"
    debezium_lob_fetch_size: str = "0"
    debezium_lob_fetch_buffer_size: str = "0"

    @classmethod
    def from_env(cls) -> AppConfig:
        """Build config from environment variables."""

        def _bool(key: str, default: str) -> bool:
            return os.environ.get(key, default).lower() in ("true", "1", "yes")

        return cls(
            state_db_dsn=os.environ.get("STATE_DB_DSN", cls.state_db_dsn),
            pg_pool_min=int(os.environ.get("PG_POOL_MIN", str(cls.pg_pool_min))),
            pg_pool_max=int(os.environ.get("PG_POOL_MAX", str(cls.pg_pool_max))),
            flask_host=os.environ.get("FLASK_HOST", cls.flask_host),
            flask_port=int(os.environ.get("FLASK_PORT", str(cls.flask_port))),
            flask_debug=_bool("FLASK_DEBUG", str(cls.flask_debug)),
            orchestrator_tick_interval=int(os.environ.get("ORCHESTRATOR_TICK_INTERVAL", str(cls.orchestrator_tick_interval))),
            orchestrator_start_delay=int(os.environ.get("ORCHESTRATOR_START_DELAY", str(cls.orchestrator_start_delay))),
            cdc_heartbeat_stale_minutes=int(os.environ.get("CDC_HEARTBEAT_STALE_MINUTES", str(cls.cdc_heartbeat_stale_minutes))),
            status_poller_interval=int(os.environ.get("STATUS_POLLER_INTERVAL", str(cls.status_poller_interval))),
            status_poller_initial_delay=int(os.environ.get("STATUS_POLLER_INITIAL_DELAY", str(cls.status_poller_initial_delay))),
            kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", cls.kafka_bootstrap_servers),
            debezium_lob_enabled=_bool("DEBEZIUM_LOB_ENABLED", str(cls.debezium_lob_enabled)),
            debezium_log_mining_batch_size_max=os.environ.get("DEBEZIUM_LOG_MINING_BATCH_SIZE_MAX", cls.debezium_log_mining_batch_size_max),
            debezium_log_mining_sleep_time_increment_ms=os.environ.get("DEBEZIUM_LOG_MINING_SLEEP_TIME_INCREMENT_MS", cls.debezium_log_mining_sleep_time_increment_ms),
            debezium_log_mining_sleep_time_max_ms=os.environ.get("DEBEZIUM_LOG_MINING_SLEEP_TIME_MAX_MS", cls.debezium_log_mining_sleep_time_max_ms),
            debezium_topic_replication_factor=os.environ.get("DEBEZIUM_TOPIC_REPLICATION_FACTOR", cls.debezium_topic_replication_factor),
            debezium_topic_partitions=os.environ.get("DEBEZIUM_TOPIC_PARTITIONS", cls.debezium_topic_partitions),
            debezium_topic_cleanup_policy=os.environ.get("DEBEZIUM_TOPIC_CLEANUP_POLICY", cls.debezium_topic_cleanup_policy),
            debezium_topic_retention_ms=os.environ.get("DEBEZIUM_TOPIC_RETENTION_MS", cls.debezium_topic_retention_ms),
            debezium_topic_compression_type=os.environ.get("DEBEZIUM_TOPIC_COMPRESSION_TYPE", cls.debezium_topic_compression_type),
            debezium_lob_fetch_size=os.environ.get("DEBEZIUM_LOB_FETCH_SIZE", cls.debezium_lob_fetch_size),
            debezium_lob_fetch_buffer_size=os.environ.get("DEBEZIUM_LOB_FETCH_BUFFER_SIZE", cls.debezium_lob_fetch_buffer_size),
        )
