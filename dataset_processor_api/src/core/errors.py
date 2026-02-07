"""Application-level exception hierarchy mapped to HTTP responses."""

from typing import Any


class AppError(Exception):
    """Base class for expected domain errors surfaced by the API."""

    status_code = 400
    default_detail: Any = "Bad request."

    def __init__(self, detail: Any | None = None) -> None:
        """Initialize the error with custom or default detail payload."""
        self.detail = self.default_detail if detail is None else detail
        super().__init__(str(self.detail))


class InvalidRequestError(AppError):
    """Error raised when request payload validation fails."""

    status_code = 422
    default_detail = "Invalid request."


class UnsupportedMediaTypeError(AppError):
    """Error raised when uploaded file type is not supported."""

    status_code = 415
    default_detail = "Unsupported content type."


class MissingFilenameError(AppError):
    """Error raised when uploaded file does not include a filename."""

    status_code = 422
    default_detail = "filename is required."


class NotFoundError(AppError):
    """Error raised when a requested resource does not exist."""

    status_code = 404
    default_detail = "Resource not found."


class StorageError(AppError):
    """Error raised when storage operations fail."""

    status_code = 503
    default_detail = "Storage service error."


class DatabaseError(AppError):
    """Error raised when database operations fail."""

    status_code = 503
    default_detail = "Database error."


class QueueError(AppError):
    """Error raised when background job enqueue fails."""

    status_code = 503
    default_detail = "Failed to enqueue task."


class UnexpectedError(AppError):
    """Fallback error for unhandled exceptions."""

    status_code = 500
    default_detail = "An unexpected error occurred. Please try again later."
