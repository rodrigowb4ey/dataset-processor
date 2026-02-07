import csv
import io
import json
from typing import Any


class InvalidDatasetFormatError(ValueError):
    pass


def _parse_csv_rows(text: str) -> list[dict[str, Any]]:
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise InvalidDatasetFormatError("CSV file must include a header row.")
    return [dict(row) for row in reader]


def _parse_json_rows(text: str) -> list[dict[str, Any]]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise InvalidDatasetFormatError("Invalid JSON payload.") from exc

    if not isinstance(payload, list):
        raise InvalidDatasetFormatError("JSON dataset must be a list of objects.")

    rows: list[dict[str, Any]] = []
    for idx, item in enumerate(payload):
        if not isinstance(item, dict):
            raise InvalidDatasetFormatError(f"JSON item at index {idx} is not an object.")
        rows.append(item)
    return rows


def parse_rows(content_type: str, payload: bytes) -> list[dict[str, Any]]:
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise InvalidDatasetFormatError("Dataset is not valid UTF-8.") from exc

    if content_type == "text/csv":
        return _parse_csv_rows(text)
    if content_type == "application/json":
        return _parse_json_rows(text)
    raise InvalidDatasetFormatError(f"Unsupported content type: {content_type}")
