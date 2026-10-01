"""Live tests against a real Databricks SQL warehouse.

Skipped unless ``TARGET_DATABRICKS_SERVER_HOSTNAME``,
``TARGET_DATABRICKS_HTTP_PATH`` and ``TARGET_DATABRICKS_ACCESS_TOKEN`` are set.
``TARGET_DATABRICKS_CATALOG`` is optional.
"""

from __future__ import annotations

import io
import json
import os
import uuid
from contextlib import redirect_stdout

import pytest

from target_databricks.client import DatabricksClient
from target_databricks.target import TargetDatabricks

REQUIRED = (
    "TARGET_DATABRICKS_SERVER_HOSTNAME",
    "TARGET_DATABRICKS_HTTP_PATH",
    "TARGET_DATABRICKS_ACCESS_TOKEN",
)

pytestmark = pytest.mark.skipif(
    not all(os.environ.get(v) for v in REQUIRED),
    reason=f"requires {', '.join(REQUIRED)}",
)

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


def _messages(records: list[dict]) -> list[str]:
    lines = [
        {
            "type": "SCHEMA",
            "stream": "people",
            "schema": SCHEMA,
            "key_properties": ["id"],
        },
        *({"type": "RECORD", "stream": "people", "record": r} for r in records),
    ]
    return [json.dumps(line) for line in lines]


@pytest.fixture(scope="module")
def config():
    cfg = {
        "server_hostname": os.environ["TARGET_DATABRICKS_SERVER_HOSTNAME"],
        "http_path": os.environ["TARGET_DATABRICKS_HTTP_PATH"],
        "access_token": os.environ["TARGET_DATABRICKS_ACCESS_TOKEN"],
        "default_target_schema": f"meltano_test_{uuid.uuid4().hex[:8]}",
    }
    if catalog := os.environ.get("TARGET_DATABRICKS_CATALOG"):
        cfg["catalog"] = catalog
    yield cfg
    client = DatabricksClient(cfg)
    schema = ".".join(
        f"`{p}`" for p in (cfg.get("catalog"), cfg["default_target_schema"]) if p
    )
    client.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    client.close()


def _run(config, records, *, version=None, **overrides):
    messages = _messages(records)
    if version is not None:
        messages = [
            json.dumps({**json.loads(m), "version": version}) if '"RECORD"' in m else m
            for m in messages
        ]
        messages.append(
            json.dumps(
                {"type": "ACTIVATE_VERSION", "stream": "people", "version": version}
            ),
        )
    target = TargetDatabricks(config={**config, **overrides})
    with redirect_stdout(io.StringIO()):
        target.listen(io.StringIO("\n".join(messages) + "\n"))
    return target.client


def _rows(client, config):
    table = ".".join(
        f"`{p}`"
        for p in (config.get("catalog"), config["default_target_schema"], "people")
        if p
    )
    return client.execute(
        f"SELECT id, name, score, active, tags FROM {table} ORDER BY id"
    )


def test_upsert_is_idempotent(config):
    first = [
        {
            "id": 1,
            "name": "ann",
            "score": 1.5,
            "active": True,
            "created": "2024-01-02T03:04:05+00:00",
            "tags": ["a", "b"],
        },
        {
            "id": 2,
            "name": "bob",
            "score": None,
            "active": False,
            "created": None,
            "tags": None,
        },
    ]
    _run(config, first)
    client = _run(
        config,
        [
            *first,
            {
                "id": 3,
                "name": "cy",
                "score": 3,
                "active": None,
                "created": None,
                "tags": [],
            },
        ],
    )
    rows = _rows(client, config)
    assert [r[0] for r in rows] == [1, 2, 3]
    assert rows[0][1:4] == ("ann", 1.5, True)
    assert rows[0][4] == '["a", "b"]'


def test_upsert_updates_existing_rows(config):
    client = _run(config, [{"id": 1, "name": "ann2"}])
    assert _rows(client, config)[0][1] == "ann2"


def test_overwrite_replaces_table(config):
    client = _run(config, [{"id": 9, "name": "only"}], load_method="overwrite")
    assert [r[0] for r in _rows(client, config)] == [9]


@pytest.mark.parametrize("hard_delete", [False, True])
def test_activate_version_deletes_stale_rows(config, hard_delete):
    suffix = f"_v{int(hard_delete)}"
    cfg = {
        **config,
        "default_target_schema": config["default_target_schema"] + suffix,
    }
    overrides = {"add_record_metadata": True, "hard_delete": hard_delete}
    _run(
        cfg,
        [{"id": 1, "name": "keep"}, {"id": 2, "name": "stale"}],
        version=1,
        **overrides,
    )
    client = _run(cfg, [{"id": 1, "name": "keep"}], version=2, **overrides)

    table = ".".join(
        f"`{p}`"
        for p in (cfg.get("catalog"), cfg["default_target_schema"], "people")
        if p
    )
    rows = client.execute(
        f"SELECT id, _sdc_deleted_at IS NOT NULL FROM {table} ORDER BY id"
    )
    assert [tuple(r) for r in rows] == (
        [(1, False)] if hard_delete else [(1, False), (2, True)]
    )
    schema = table.rsplit(".", 1)[0]
    client.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
