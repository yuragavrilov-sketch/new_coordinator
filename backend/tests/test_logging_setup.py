import structlog
from logging_setup import setup_logging


def test_setup_configures_structlog():
    setup_logging()
    log = structlog.get_logger()
    assert log is not None


def test_logger_binds_context():
    setup_logging()
    log = structlog.get_logger().bind(migration_id="abc-123")
    assert log is not None
