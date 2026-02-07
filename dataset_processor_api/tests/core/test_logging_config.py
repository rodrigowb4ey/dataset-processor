import logging

from asgi_correlation_id.context import correlation_id

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


class _CaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(self.format(record))


def test_uvicorn_access_log_includes_request_id() -> None:
    configure_logging(
        log_level="INFO",
        log_format="console",
        service_name="dataset-processor-tests",
        environment="test",
    )

    root_logger = logging.getLogger()
    formatter = root_logger.handlers[0].formatter
    assert formatter is not None

    capture_handler = _CaptureHandler()
    capture_handler.setFormatter(formatter)
    root_logger.addHandler(capture_handler)

    token = correlation_id.set("request-id-123")
    try:
        logging.getLogger("uvicorn.access").info("GET / HTTP/1.1 200")
    finally:
        correlation_id.reset(token)
        root_logger.removeHandler(capture_handler)

    assert capture_handler.messages
    assert any("uvicorn.access" in line for line in capture_handler.messages)
    assert any("request_id" in line for line in capture_handler.messages)
    assert any("request-id-123" in line for line in capture_handler.messages)
