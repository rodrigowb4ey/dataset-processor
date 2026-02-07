"""Anomaly detection utilities for duplicate rows and numeric outliers."""

import json
from collections import defaultdict
from typing import Any


def _to_float(value: Any) -> float:
    """Convert numeric-like values to float and reject non-numeric values."""
    if isinstance(value, bool):
        raise ValueError("bool is not numeric")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return float(value)
    raise ValueError("value is not numeric")


def _quantile(sorted_values: list[float], q: float) -> float:
    """Return an interpolated quantile from sorted values."""
    if not sorted_values:
        raise ValueError("empty sequence")
    if len(sorted_values) == 1:
        return sorted_values[0]
    index = (len(sorted_values) - 1) * q
    lower = int(index)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = index - lower
    return sorted_values[lower] * (1.0 - fraction) + sorted_values[upper] * fraction


def _compute_duplicate_count(rows: list[dict[str, Any]]) -> int:
    """Count duplicate rows based on canonical JSON serialization."""
    seen: set[str] = set()
    duplicates = 0
    for row in rows:
        key = json.dumps(row, sort_keys=True, default=str)
        if key in seen:
            duplicates += 1
        else:
            seen.add(key)
    return duplicates


def compute_anomalies(rows: list[dict[str, Any]], max_examples: int = 5) -> dict[str, Any]:
    """Compute duplicate count and IQR outliers per numeric field."""
    numeric_by_field: dict[str, list[tuple[int, float]]] = defaultdict(list)

    for idx, row in enumerate(rows):
        for field, value in row.items():
            try:
                numeric_by_field[field].append((idx, _to_float(value)))
            except ValueError:
                continue

    outliers: dict[str, dict[str, Any]] = {}
    for field, values_with_idx in sorted(numeric_by_field.items()):
        if len(values_with_idx) < 4:
            continue
        values = sorted(value for _, value in values_with_idx)
        q1 = _quantile(values, 0.25)
        q3 = _quantile(values, 0.75)
        iqr = q3 - q1
        if iqr <= 0:
            continue
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        field_outliers: list[dict[str, Any]] = []
        for row_index, number in values_with_idx:
            if number < lower or number > upper:
                field_outliers.append({"row_index": row_index, "value": number})

        if field_outliers:
            outliers[field] = {
                "count": len(field_outliers),
                "examples": field_outliers[:max_examples],
            }

    return {
        "duplicates_count": _compute_duplicate_count(rows),
        "outliers": outliers,
    }
