from typing import Any

from src.processing.stats import compute_stats


def test_compute_stats_with_mixed_fields() -> None:
    rows: list[dict[str, Any]] = [
        {"a": "1", "b": None, "c": "x"},
        {"a": "2", "b": "", "c": "3"},
        {"a": "3", "b": "7", "c": "4.5"},
    ]

    stats = compute_stats(rows)

    assert stats["row_count"] == 3
    assert stats["null_counts"] == {"a": 0, "b": 2, "c": 0}
    assert stats["numeric"] == {
        "a": {"min": 1.0, "max": 3.0, "mean": 2.0},
        "b": {"min": 7.0, "max": 7.0, "mean": 7.0},
    }


def test_compute_stats_empty_rows() -> None:
    stats = compute_stats([])

    assert stats == {"row_count": 0, "null_counts": {}, "numeric": {}}
