"""Run the Singer SDK's built-in target test streams against a live warehouse.

The ``.singer`` files come from ``singer_sdk.testing.target_test_streams``. Unlike
``get_target_test_class``, each case gets its own ephemeral schema and config from
the fixtures in ``conftest.py``, and we assert on what landed in the tables.
"""

from __future__ import annotations

import collections
import json
from importlib.resources import files

import pytest
from singer_sdk.exceptions import (
    InvalidRecord,
    MissingKeyPropertiesError,
    RecordsWithoutSchemaException,
)

from target_databricks import sql

STREAMS_DIR = files("singer_sdk.testing.target_test_streams")
STREAM_FILES = sorted(
    (p for p in STREAMS_DIR.iterdir() if p.name.endswith(".singer")),
    key=lambda p: p.name,
)

# The first three match the SDK's own target test classes. The SDK validates records
# by default, so a record missing a required property fails the run (its own test
# class only passes because its runner config disables that).
EXPECTED_ERRORS: dict[str, type[Exception]] = {
    "invalid_schema": Exception,
    "record_before_schema": RecordsWithoutSchemaException,
    "record_missing_key_property": MissingKeyPropertiesError,
    "record_missing_required_property": InvalidRecord,
}

# Row counts where the file-derived rule (see ``expected_rows``) is not right.
EXPECTED_ROWS_OVERRIDE: dict[str, dict[str, int]] = {}

# Cases the target does not support yet: name -> reason.
KNOWN_FAILURES: dict[str, str] = {}


def stem(path) -> str:
    return path.name.removesuffix(".singer")


def parse_expectations(path) -> tuple[set[str], dict[str, int]]:
    """Derive what should have landed from the file's own messages.

    Returns the streams that announced a schema (each must get a table) and the rows
    expected per stream with records: keyed streams hold one row per distinct key
    (upsert), keyless streams one row per record (insert).
    """
    key_properties: dict[str, list[str]] = {}
    keyed: dict[str, set[str]] = collections.defaultdict(set)
    keyless: collections.Counter[str] = collections.Counter()

    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        message = json.loads(line)
        stream = message.get("stream")
        if message["type"] == "SCHEMA":
            key_properties[stream] = message.get("key_properties") or []
        elif message["type"] == "RECORD":
            if keys := key_properties.get(stream):
                keyed[stream].add(json.dumps([message["record"].get(k) for k in keys]))
            else:
                keyless[stream] += 1

    counts: collections.Counter[str] = collections.Counter(keyless)
    counts.update({stream: len(values) for stream, values in keyed.items()})
    return set(key_properties), dict(counts)


def param(path):
    marks = []
    if reason := KNOWN_FAILURES.get(stem(path)):
        marks.append(pytest.mark.xfail(reason=reason, strict=True))
    return pytest.param(path, id=stem(path), marks=marks)


@pytest.mark.parametrize("path", [param(p) for p in STREAM_FILES])
def test_singer_stream(path, run_singer, schema):
    if expected_error := EXPECTED_ERRORS.get(stem(path)):
        with pytest.raises(expected_error):
            run_singer(path)
        return

    run_singer(path)

    streams, counts = parse_expectations(path)
    counts = EXPECTED_ROWS_OVERRIDE.get(stem(path)) or counts
    missing = [s for s in streams if schema.columns(s) is None]
    assert not missing, f"no table created for {missing}"
    actual = {stream: schema.rows(stream, "count(*)")[0][0] for stream in counts}
    assert actual == counts


def stream_file(name: str):
    return STREAMS_DIR / f"{name}.singer"


def test_duplicate_records_last_write_wins(run_singer, schema):
    run_singer(stream_file("duplicate_records"))

    assert schema.rows("test_duplicate_records", "id, metric", order_by="id") == [
        (1, 100),
        (2, 20),
    ]


def test_multiple_state_messages_final_state(run_singer):
    result = run_singer(stream_file("multiple_state_messages"))

    assert result.state_messages[-1] == {
        "test_multiple_state_messages_a": 5,
        "test_multiple_state_messages_b": 6,
    }


def test_schema_updates_add_columns_with_null_history(run_singer, schema):
    run_singer(stream_file("schema_updates"))

    columns = schema.columns("test_schema_updates")
    assert set(columns) == {"id", "a1", "a2", "a3", "a4", "a5", "a6"}
    # row 1 predates a3 and a6
    assert schema.rows("test_schema_updates", "a3, a6", "id LIMIT 1") == [(None, None)]


def test_attribute_names_are_conformed(run_singer, schema):
    run_singer(stream_file("camelcase"))
    run_singer(stream_file("camelcase_complex_schema"))
    run_singer(stream_file("special_chars_in_attributes"))

    assert set(schema.columns("TestCamelcase")) == {"id", "clientname"}
    complex_columns = set(schema.columns("ForecastingTypeToCategory"))
    assert {"newcamelcasedattribute", "_attribute_startswith_underscore"} <= (
        complex_columns
    )
    assert set(schema.columns("test:SpecialChars!in?attributes")) == {"_id", "d"}


def test_stream_files_were_found():
    """Guard against the SDK moving its fixtures, which would silently skip all."""
    assert len(STREAM_FILES) >= 15
    assert sql.conform_name("test:SpecialChars!in?attributes") == (
        "test_specialchars_in_attributes"
    )
