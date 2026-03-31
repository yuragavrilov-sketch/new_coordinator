from config import AppConfig


def test_defaults():
    cfg = AppConfig()
    assert cfg.state_db_dsn == "postgresql://postgres:postgres@localhost:5432/migration_state"
    assert cfg.pg_pool_min == 2
    assert cfg.pg_pool_max == 10
    assert cfg.flask_host == "0.0.0.0"
    assert cfg.flask_port == 5000
    assert cfg.flask_debug is True
    assert cfg.orchestrator_tick_interval == 5
    assert cfg.cdc_heartbeat_stale_minutes == 2
    assert cfg.status_poller_interval == 30
    assert cfg.kafka_bootstrap_servers == "kafka:9092"
    assert cfg.debezium_lob_enabled is True
    assert cfg.debezium_topic_replication_factor == "1"
    assert cfg.debezium_topic_partitions == "1"


def test_from_env(monkeypatch):
    monkeypatch.setenv("STATE_DB_DSN", "postgresql://test:test@db:5432/test")
    monkeypatch.setenv("PG_POOL_MIN", "5")
    monkeypatch.setenv("PG_POOL_MAX", "20")
    monkeypatch.setenv("FLASK_PORT", "8080")
    monkeypatch.setenv("FLASK_DEBUG", "false")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092")
    monkeypatch.setenv("DEBEZIUM_LOB_ENABLED", "false")

    cfg = AppConfig.from_env()
    assert cfg.state_db_dsn == "postgresql://test:test@db:5432/test"
    assert cfg.pg_pool_min == 5
    assert cfg.pg_pool_max == 20
    assert cfg.flask_port == 8080
    assert cfg.flask_debug is False
    assert cfg.kafka_bootstrap_servers == "kafka1:9092,kafka2:9092"
    assert cfg.debezium_lob_enabled is False
