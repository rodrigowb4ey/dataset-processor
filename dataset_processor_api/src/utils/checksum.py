"""Checksum helpers for uploaded file streams."""

import hashlib

from fastapi import UploadFile


async def compute_sha256_and_size(
    upload_file: UploadFile,
    chunk_size: int = 1024 * 1024,
) -> tuple[str, int]:
    """Compute SHA-256 hash and byte size for an uploaded file."""
    hasher = hashlib.sha256()
    size_bytes = 0
    while True:
        chunk = await upload_file.read(chunk_size)
        if not chunk:
            break
        hasher.update(chunk)
        size_bytes += len(chunk)
    await upload_file.seek(0)
    return hasher.hexdigest(), size_bytes
