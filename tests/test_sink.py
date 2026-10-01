from __future__ import annotations

from typing import TYPE_CHECKING, cast
from unittest import mock

import pytest

from target_databricks.sinks import DatabricksSink
from target_databricks.target import TargetDatabricks

if TYPE_CHECKING:
    from target_databricks.client import DatabricksClient

SCHEMA = {
    "properties": {
        "id": {"type": "integer"},
        "Name": {"type": ["string", "null"]},
    },
}
CONFIG = {
    "server_hostname": "h",
    "http_path": "/p",
    "access_token": "t",
    "catalog": "main",
    "default_target_schema": "raw",
}


class FakeClient:
    def __init__(self, existing=None):
        self.existing = existing
        self.statements: list[tuple[str, dict | None]] = []
        self.overwritten_tables: set[str] = set()

    def execute(self, statement, params=None):
        self.statements.append((statement, params))
        return []

    def table_columns(self, table):  # noqa: ARG002
        return self.existing


def make_sink(*, config=None, existing=None, keys=("id",), stream="users", **kwargs):
    target = TargetDatabricks(config={**CONFIG, **(config or {})})
    client = kwargs.get("client") or FakeClient(existing)
    target._client = cast("DatabricksClient", client)
    sink = DatabricksSink(target, stream, dict(SCHEMA), list(keys))
    return sink, client


def sqls(client):
    return [s for s, _ in client.statements]


def test_setup_creates_schema_and_table():
    sink, client = make_sink()
    sink.setup()
    assert sqls(client) == [
        "CREATE SCHEMA IF NOT EXISTS `main`.`raw`",
        (
            "CREATE TABLE IF NOT EXISTS `main`.`raw`.`users` "
            "(`id` BIGINT, `name` STRING) USING DELTA"
        ),
    ]


def test_setup_adds_missing_columns_only():
    sink, client = make_sink(existing={"id": "BIGINT"})
    sink.setup()
    assert (
        sqls(client)[-1]
        == "ALTER TABLE `main`.`raw`.`users` ADD COLUMNS (`name` STRING)"
    )


def test_setup_existing_complete_table_is_untouched():
    sink, client = make_sink(existing={"id": "BIGINT", "name": "STRING"})
    sink.setup()
    assert len(client.statements) == 1  # just CREATE SCHEMA


def test_schema_from_stream_name_when_no_default():
    config = {k: v for k, v in CONFIG.items() if k != "default_target_schema"}
    target = TargetDatabricks(config=config)
    target._client = cast("DatabricksClient", FakeClient())
    sink = DatabricksSink(target, "Public-Users", dict(SCHEMA), ["id"])
    assert sink.full_table_name == "`main`.`public`.`users`"
    with pytest.raises(ValueError, match="default_target_schema"):
        DatabricksSink(target, "users", dict(SCHEMA), ["id"])


def test_upsert_merges_and_dedupes_within_batch():
    sink, client = make_sink()
    sink.process_batch(
        {
            "records": [
                {"id": 1, "Name": "a"},
                {"id": 2, "Name": "b"},
                {"id": 1, "Name": "c"},
            ]
        },
    )
    ((statement, params),) = client.statements
    assert statement.startswith("MERGE INTO `main`.`raw`.`users`")
    assert params == {"p0_0": 1, "p0_1": "c", "p1_0": 2, "p1_1": "b"}


def test_append_only_inserts_even_with_keys():
    sink, client = make_sink(config={"load_method": "append-only"})
    sink.process_batch({"records": [{"id": 1, "Name": "a"}, {"id": 1, "Name": "b"}]})
    ((statement, params),) = client.statements
    assert statement.startswith("INSERT INTO")
    assert len(params) == 4


def test_no_keys_inserts():
    sink, client = make_sink(keys=())
    sink.process_batch({"records": [{"id": 1, "Name": "a"}]})
    assert sqls(client)[0].startswith("INSERT INTO")


def test_large_batches_are_chunked(monkeypatch):
    monkeypatch.setattr("target_databricks.sinks.CELLS_PER_STATEMENT", 4)
    sink, client = make_sink()
    sink.process_batch({"records": [{"id": i, "Name": "x"} for i in range(5)]})
    assert len(client.statements) == 3  # 2 rows (4 cells) per statement


def test_empty_batch_is_a_noop():
    sink, client = make_sink()
    sink.process_batch({})
    assert client.statements == []


def test_overwrite_truncates_once_per_run():
    client = FakeClient(existing={"id": "BIGINT", "name": "STRING"})
    for _ in range(2):  # e.g. a mid-stream schema change creates a second sink
        sink, _ = make_sink(config={"load_method": "overwrite"}, client=client)
        sink.setup()
    assert sqls(client).count("TRUNCATE TABLE `main`.`raw`.`users`") == 1


def test_activate_version_soft_and_hard_delete():
    sink, client = make_sink(config={"add_record_metadata": True})
    sink.activate_version(7)
    assert "SET `_sdc_deleted_at` = current_timestamp()" in sqls(client)[-1]
    assert "`_sdc_table_version` < 7" in sqls(client)[-1]

    sink, client = make_sink(config={"add_record_metadata": True, "hard_delete": True})
    sink.activate_version(7)
    assert sqls(client) == [
        "DELETE FROM `main`.`raw`.`users` WHERE `_sdc_table_version` < 7"
    ]


def test_activate_version_skipped_without_record_metadata():
    sink, client = make_sink()
    sink.activate_version(7)
    assert client.statements == []


def test_activate_version_flush_is_timed_and_counted():
    sink, client = make_sink(config={"add_record_metadata": True})
    sink.process_record({"id": 1, "Name": "a"}, sink._get_context({}))
    sink.tally_record_read()
    timer = mock.MagicMock()
    sink._batch_timer = timer

    sink.activate_version(3)

    timer.__enter__.assert_called_once()
    assert sqls(client)[0].startswith("MERGE INTO")
    assert sink.current_size == 0
