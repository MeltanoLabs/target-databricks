"""Live tests against a real Databricks SQL warehouse (see ``conftest.py``)."""

from __future__ import annotations

import json

import pytest

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
