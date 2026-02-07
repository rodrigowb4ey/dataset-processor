"""Structured logging configuration and helpers."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, cast

import structlog
from asgi_correlation_id.context import correlation_id

if TYPE_CHECKING:
    from structlog.stdlib import BoundLogger
    from structlog.typing import EventDict, WrappedLogger

REDACTED_VALUE = "***REDACTED***"
SENSITIVE_FIELD_FRAGMENTS = (
    "password",
    "secret",
    "token",
    "authorization",
    "access_key",
    "secret_key",
)

_LOGGING_CONFIGURED = False


def _resolve_log_level(log_level: str) -> int:
    """Resolve a user-provided log level into a stdlib constant."""
    normalized = log_level.strip().upper()
    resolved = getattr(logging, normalized, logging.INFO)
    if not isinstance(resolved, int):
        return logging.INFO
    return resolved


def _contains_sensitive_fragment(field_name: str) -> bool:
    """Return whether a field name should be redacted."""
    lowered = field_name.lower()
    return any(fragment in lowered for fragment in SENSITIVE_FIELD_FRAGMENTS)


def _redact_value(field_name: str, value: object) -> object:
    """Recursively redact sensitive values in dictionaries and lists."""
    if _contains_sensitive_fragment(field_name):
        return REDACTED_VALUE

    if isinstance(value, dict):
        return {str(key): _redact_value(str(key), item_value) for key, item_value in value.items()}

    if isinstance(value, list):
        return [_redact_value(field_name, item) for item in value]

    return value


def _redact_sensitive_fields(
    _logger: WrappedLogger,
    _method_name: str,
    event_dict: EventDict,
) -> EventDict:
    """Structlog processor that redacts sensitive values."""
    for key in tuple(event_dict):
        event_dict[key] = _redact_value(key, event_dict[key])
    return event_dict


def _add_runtime_context(
    service_name: str,
    environment: str,
) -> structlog.types.Processor:
    """Create a processor that appends service metadata to each event."""

    def _processor(
        _logger: WrappedLogger,
        _method_name: str,
        event_dict: EventDict,
    ) -> EventDict:
        event_dict.setdefault("service", service_name)
        event_dict.setdefault("environment", environment)

        request_id = correlation_id.get()
        if request_id:
            event_dict.setdefault("request_id", request_id)

        return event_dict

    return _processor


def _build_renderer(log_format: str) -> structlog.types.Processor:
    """Build the final renderer for structured log events."""
    if log_format.strip().lower() == "json":
        return structlog.processors.JSONRenderer()

    return structlog.dev.ConsoleRenderer(colors=True)


def configure_logging(
    *,
    log_level: str,
    log_format: str,
    service_name: str,
    environment: str,
) -> None:
    """Configure stdlib and structlog for the current process."""
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _add_runtime_context(service_name=service_name, environment=environment),
        _redact_sensitive_fields,
    ]

    json_output = log_format.strip().lower() == "json"
    renderer = _build_renderer(log_format)

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(_resolve_log_level(log_level))

    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access", "celery"):
        logger = logging.getLogger(logger_name)
        logger.handlers.clear()
        logger.propagate = True

    processors: list[structlog.types.Processor] = [
        *shared_processors,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
    ]
    if json_output:
        processors.append(structlog.processors.format_exc_info)
    processors.append(structlog.stdlib.ProcessorFormatter.wrap_for_formatter)

    structlog.configure(
        processors=processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    _LOGGING_CONFIGURED = True


def get_logger(name: str | None = None) -> BoundLogger:
    """Return a configured structured logger."""
    return cast("BoundLogger", structlog.get_logger(name))


def bind_context(**values: object) -> None:
    """Bind context values to the active contextvars scope."""
    structlog.contextvars.bind_contextvars(**values)


def clear_context() -> None:
    """Clear contextvars-bound logging values for the current scope."""
    structlog.contextvars.clear_contextvars()
