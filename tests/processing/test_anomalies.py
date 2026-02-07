from src.processing.anomalies import compute_anomalies


def test_compute_anomalies_detects_duplicates_and_outliers() -> None:
    rows = [
        {"id": 1, "value": 10},
        {"id": 2, "value": 11},
        {"id": 3, "value": 12},
        {"id": 4, "value": 13},
        {"id": 5, "value": 14},
        {"id": 6, "value": 15},
        {"id": 7, "value": 100},
        {"id": 1, "value": 10},
    ]

    anomalies = compute_anomalies(rows)

    assert anomalies["duplicates_count"] == 1
    assert anomalies["outliers"]["value"]["count"] == 1
    assert len(anomalies["outliers"]["value"]["examples"]) == 1


def test_compute_anomalies_with_no_outliers() -> None:
    rows = [
        {"id": 1, "value": 10},
        {"id": 2, "value": 11},
        {"id": 3, "value": 12},
    ]

    anomalies = compute_anomalies(rows)

    assert anomalies == {"duplicates_count": 0, "outliers": {}}
