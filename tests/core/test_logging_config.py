from src.core.logging import configure_logging, get_logger


def test_configure_logging_allows_logger_creation() -> None:
    configure_logging(
        log_level="INFO",
        log_format="console",
        service_name="dataset-processor-tests",
        environment="test",
    )

    logger = get_logger("tests.logging")

    assert logger is not None
