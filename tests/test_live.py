"""Live tests against a real Databricks SQL warehouse (see ``conftest.py``)."""

from __future__ import annotations

import datetime
import json

import pyarrow as pa
import pytest
from pyarrow import ipc

SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": ["string", "null"]},
        "score": {"type": ["number", "null"]},
        "active": {"type": ["boolean", "null"]},
        "created": {"type": ["string", "null"], "format": "date-time"},
        "tags": {"type": ["array", "null"], "items": {"type": "string"}},
    },
}

ANN = {
    "id": 1,
    "name": "ann",
    "score": 1.5,
    "active": True,
    "created": "2024-01-02T03:04:05+00:00",
    "tags": ["a", "b"],
}
BOB = {
    "id": 2,
    "name": "bob",
    "score": None,
    "active": False,
    "created": None,
    "tags": None,
}
CY = {"id": 3, "name": "cy", "score": 3, "active": None, "created": None, "tags": []}


def messages(records: list[dict], *, version: int | None = None) -> str:
    """Build a Singer stream for ``people``, optionally versioned + activated."""
    lines = [
        {
            "type": "SCHEMA",
            "stream": "people",
            "schema": SCHEMA,
            "key_properties": ["id"],
        },
        *(
            {
                "type": "RECORD",
                "stream": "people",
                "record": r,
                **({} if version is None else {"version": version}),
            }
            for r in records
        ),
    ]
    if version is not None:
        lines.append({
            "type": "ACTIVATE_VERSION",
            "stream": "people",
            "version": version,
        })
    return "\n".join(json.dumps(line) for line in lines) + "\n"


def test_upsert_is_idempotent(run_singer, schema):
    run_singer(messages([ANN, BOB]))
    run_singer(messages([ANN, BOB, CY]))

    rows = schema.rows("people", "id, name, score, active, tags", order_by="id")
    assert [r[0] for r in rows] == [1, 2, 3]
    assert rows[0][1:] == ("ann", 1.5, True, '["a", "b"]')


def test_upsert_updates_existing_rows(run_singer, schema):
    run_singer(messages([ANN]))
    run_singer(messages([{"id": 1, "name": "ann2"}]))

    assert schema.rows("people", "id, name") == [(1, "ann2")]


def test_append_only_keeps_duplicates(run_singer, schema):
    run_singer(messages([ANN, BOB]), load_method="append-only")
    run_singer(messages([ANN, BOB]), load_method="append-only")

    assert schema.rows("people", "count(*)") == [(4,)]


def test_overwrite_replaces_table(run_singer, schema):
    run_singer(messages([ANN, BOB]))
    run_singer(messages([{"id": 9, "name": "only"}]), load_method="overwrite")

    assert schema.rows("people", "id") == [(9,)]


@pytest.mark.parametrize("hard_delete", [False, True], ids=["soft", "hard"])
def test_activate_version_deletes_stale_rows(run_singer, schema, hard_delete):
    overrides = {"add_record_metadata": True, "hard_delete": hard_delete}
    run_singer(
        messages([{"id": 1, "name": "keep"}, {"id": 2, "name": "stale"}], version=1),
        **overrides,
    )
    run_singer(messages([{"id": 1, "name": "keep"}], version=2), **overrides)

    rows = schema.rows("people", "id, _sdc_deleted_at IS NOT NULL", order_by="id")
    assert rows == ([(1, False)] if hard_delete else [(1, False), (2, True)])


def arrow_messages(path, table, *, keys=("id",)) -> str:
    schema = {"type": "object", "properties": SCHEMA["properties"]}
    lines = [
        {
            "type": "SCHEMA",
            "stream": "people",
            "schema": schema,
            "key_properties": keys,
        },
        {
            "type": "BATCH",
            "stream": "people",
            "encoding": {"format": "arrow"},
            "manifest": [path.as_uri()],
        },
    ]
    with path.open("wb") as f:
        writer = ipc.new_file(f, table.schema)
        writer.write_table(table)
        writer.close()
    return "\n".join(json.dumps(line) for line in lines) + "\n"


def test_arrow_batch_is_upserted(run_singer, schema, tmp_path):
    table = pa.table(
        {
            "id": [1, 2, 1],
            "name": ["ann", "bob", "ann2"],
            "score": [1.5, None, 2.5],
            "active": [True, False, None],
            "created": pa.array(
                [datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.UTC)] * 3,
                type=pa.timestamp("ns", tz="UTC"),
            ),
            "tags": [["a", "b"], None, ["c"]],
        },
    )
    path = tmp_path / "people.arrow"
    run_singer(arrow_messages(path, table))
    run_singer(arrow_messages(path, table))  # idempotent with keys

    rows = schema.rows("people", "id, name, score, active, tags", order_by="id")
    assert rows == [(1, "ann2", 2.5, None, '["c"]'), (2, "bob", None, False, None)]
    assert schema.rows("people", "year(created)", order_by="id") == [(2024,), (2024,)]
    assert not path.exists()  # consume-once manifest files are cleaned up


def test_arrow_batch_is_appended_without_keys(run_singer, schema, tmp_path):
    table = pa.table({"id": [1, 1], "name": ["a", "b"]})
    run_singer(arrow_messages(tmp_path / "p.arrow", table, keys=[]))

    assert schema.rows("people", "count(*)") == [(2,)]


def test_staged_record_batches_match_inline_loading(run_singer, schema, monkeypatch):
    """Records loaded through the volume must read back like inlined ones."""
    monkeypatch.setattr("target_databricks.sinks.STAGING_MIN_ROWS", 1)
    run_singer(messages([ANN, BOB]))
    run_singer(messages([ANN, BOB, CY, {"id": 1, "name": "ann2"}]))  # upsert

    rows = schema.rows(
        "people",
        "id, name, score, active, tags, year(created)",
        order_by="id",
    )
    assert rows == [
        (1, "ann2", None, None, None, None),
        (2, "bob", None, False, None, None),
        (3, "cy", 3.0, None, "[]", None),
    ]
