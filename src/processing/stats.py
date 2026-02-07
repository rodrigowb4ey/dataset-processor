"""Statistics computation utilities for normalized dataset rows."""

from collections import defaultdict
from typing import Any


def _is_null(value: Any) -> bool:
    """Return whether a value should be treated as null."""
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def _to_float(value: Any) -> float:
    """Convert numeric-like values to float and reject non-numeric values."""
    if isinstance(value, bool):
        raise ValueError("bool is not numeric")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return float(value)
    raise ValueError("value is not numeric")


def compute_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute row count, null counts, and numeric statistics by field."""
    all_fields: set[str] = set()
    null_counts: dict[str, int] = defaultdict(int)
    numeric_values: dict[str, list[float]] = defaultdict(list)
    numeric_failures: dict[str, int] = defaultdict(int)

    for row in rows:
        all_fields.update(row.keys())
        for field, value in row.items():
            if _is_null(value):
                null_counts[field] += 1
                continue
            try:
                numeric_values[field].append(_to_float(value))
            except ValueError:
                numeric_failures[field] += 1

    for field in all_fields:
        null_counts.setdefault(field, 0)

    numeric_stats: dict[str, dict[str, float]] = {}
    for field in sorted(all_fields):
        values = numeric_values.get(field, [])
        if not values or numeric_failures.get(field, 0) > 0:
            continue
        numeric_stats[field] = {
            "min": min(values),
            "max": max(values),
            "mean": sum(values) / len(values),
        }

    return {
        "row_count": len(rows),
        "null_counts": dict(sorted(null_counts.items())),
        "numeric": numeric_stats,
    }
