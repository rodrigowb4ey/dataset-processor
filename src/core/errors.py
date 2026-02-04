from __future__ import annotations

from typing import Any


class AppError(Exception):
    status_code = 400
    default_detail: Any = "Bad request."

    def __init__(self, detail: Any | None = None) -> None:
        self.detail = self.default_detail if detail is None else detail
        super().__init__(str(self.detail))


class InvalidRequestError(AppError):
    status_code = 422
    default_detail = "Invalid request."


class UnsupportedMediaTypeError(AppError):
    status_code = 415
    default_detail = "Unsupported content type."


class MissingFilenameError(AppError):
    status_code = 422
    default_detail = "filename is required."


class NotFoundError(AppError):
    status_code = 404
    default_detail = "Resource not found."


class StorageError(AppError):
    status_code = 503
    default_detail = "Storage service error."


class DatabaseError(AppError):
    status_code = 503
    default_detail = "Database error."


class QueueError(AppError):
    status_code = 503
    default_detail = "Failed to enqueue task."


class UnexpectedError(AppError):
    status_code = 500
    default_detail = "An unexpected error occurred. Please try again later."
