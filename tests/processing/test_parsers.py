import pytest

from src.processing.parsers import InvalidDatasetFormatError, parse_rows


def test_parse_rows_csv_success() -> None:
    payload = b"id,value\n1,10\n2,20\n"

    rows = parse_rows("text/csv", payload)

    assert rows == [{"id": "1", "value": "10"}, {"id": "2", "value": "20"}]


def test_parse_rows_json_success() -> None:
    payload = b'[{"id": 1, "value": 10}, {"id": 2, "value": 20}]'

    rows = parse_rows("application/json", payload)

    assert rows == [{"id": 1, "value": 10}, {"id": 2, "value": 20}]


def test_parse_rows_invalid_json_raises() -> None:
    with pytest.raises(InvalidDatasetFormatError, match="Invalid JSON payload"):
        parse_rows("application/json", b"not-json")


def test_parse_rows_json_not_list_raises() -> None:
    with pytest.raises(InvalidDatasetFormatError, match="must be a list"):
        parse_rows("application/json", b'{"id": 1}')


def test_parse_rows_json_item_not_object_raises() -> None:
    with pytest.raises(InvalidDatasetFormatError, match="is not an object"):
        parse_rows("application/json", b"[1, 2]")


def test_parse_rows_unsupported_content_type_raises() -> None:
    with pytest.raises(InvalidDatasetFormatError, match="Unsupported content type"):
        parse_rows("text/plain", b"hello")


def test_parse_rows_invalid_utf8_raises() -> None:
    with pytest.raises(InvalidDatasetFormatError, match="valid UTF-8"):
        parse_rows("text/csv", b"\x80\x81\x82")
