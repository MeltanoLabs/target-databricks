from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, cast
from unittest import mock

import pyarrow as pa
import pytest
from pyarrow import ipc
from singer_sdk.helpers._batch import BaseBatchFileEncoding  # ruff: ignore[import-private-name]
from singer_sdk.helpers.capabilities import PluginCapabilities

from target_databricks import arrow, sql
from target_databricks.sinks import STAGING_VOLUME, DatabricksSink
from target_databricks.target import TargetDatabricks

if TYPE_CHECKING:
    from pathlib import Path

    from target_databricks.client import DatabricksClient

ARROW = BaseBatchFileEncoding(format="arrow")


def write_ipc(path: Path, table: pa.Table, *, stream: bool = False) -> str:
    with path.open("wb") as f:
        writer = (ipc.new_stream if stream else ipc.new_file)(f, table.schema)
        writer.write_table(table)
        writer.close()
    return path.as_uri()


def test_resolve_manifest_path():
    assert arrow.resolve_manifest_path("file:///tmp/a.arrow") == "/tmp/a.arrow"  # ruff: ignore[hardcoded-temp-file]
    assert arrow.resolve_manifest_path("/tmp/a.arrow") == "/tmp/a.arrow"  # ruff: ignore[hardcoded-temp-file]


@pytest.mark.parametrize("stream", [False, True], ids=["file", "stream"])
def test_read_arrow_file(tmp_path, stream):
    table = pa.table({"id": [1, 2]})
    path = write_ipc(tmp_path / "b.arrow", table, stream=stream)
    assert arrow.read_arrow_file(arrow.resolve_manifest_path(path)) == table


def test_conform_table_aligns_columns():
    table = pa.table({
        "ID": [1, 2],
        "Meta Data": [{"a": 1}, None],
        "extra": ["x", "y"],
    })
    columns = {"id": sql.BIGINT, "meta_data": sql.STRING, "missing": sql.STRING}
    out = arrow.conform_table(table, columns)
    assert out.column_names == ["id", "meta_data", "missing"]
    assert out.column("meta_data").to_pylist() == ['{"a": 1}', None]
    assert out.column("missing").to_pylist() == [None, None]


def test_dedupe_table_keeps_last_row_in_order():
    table = pa.table({"id": [1, 2, 1, 3], "v": ["a", "b", "c", "d"]})
    out = arrow.dedupe_table(table, ["id"])
    assert out.to_pydict() == {"id": [2, 1, 3], "v": ["b", "c", "d"]}


def test_staged_sql():
    columns = {"id": sql.BIGINT, "name": sql.STRING}
    path = "/Volumes/main/raw/v/f.parquet"
    select = "SELECT CAST(`id` AS BIGINT) AS `id`, CAST(`name` AS STRING) AS `name`"
    assert sql.insert_staged_sql("`t`", columns, path) == (
        f"INSERT INTO `t` (`id`, `name`) {select} FROM parquet.`{path}`"
    )
    merge = sql.merge_staged_sql("`t`", columns, ["id"], path)
    assert merge.startswith(f"MERGE INTO `t` AS t USING ({select} FROM parquet.")
    assert "ON t.`id` = s.`id` WHEN MATCHED THEN UPDATE SET t.`name`" in merge
    assert sql.put_sql("/l/it's.parquet", path) == (
        f"PUT '/l/it\\'s.parquet' INTO '{path}' OVERWRITE"
    )
    assert sql.remove_sql(path) == f"REMOVE '{path}'"


class FakeClient:
    def __init__(self, tmp_path: Path, *, fail_put: bool = False):
        self.staging_dir = str(tmp_path)
        self.fail_put = fail_put
        self.overwritten_tables: set[str] = set()
        self.statements: list[tuple[str, dict | None]] = []

    def current_catalog(self):  # ruff: ignore[no-self-use]
        return "main"

    def execute(self, statement, params=None):
        if self.fail_put and statement.startswith("PUT"):
            msg = "no volume access"
            raise RuntimeError(msg)
        self.statements.append((statement, params))
        return []

    def table_columns(self, table):  # ruff: ignore[unused-method-argument,no-self-use]
        return None


def make_sink(tmp_path, *, config=None, keys=("id",), **client_kwargs):
    target = TargetDatabricks(
        config={
            "server_hostname": "h",
            "http_path": "/p",
            "access_token": "t",
            "catalog": "main",
            "default_target_schema": "raw",
            **(config or {}),
        },
    )
    client = FakeClient(tmp_path, **client_kwargs)
    target._client = cast("DatabricksClient", client)
    schema = {
        "properties": {"id": {"type": "integer"}, "name": {"type": ["string", "null"]}},
    }
    return DatabricksSink(target, "users", schema, list(keys)), client


def batch_file(tmp_path: Path, **columns) -> str:
    return write_ipc(tmp_path / "batch.arrow", pa.table(columns or {"id": [1]}))


def test_target_advertises_batch_capability():
    assert PluginCapabilities.BATCH in TargetDatabricks.capabilities


def test_arrow_batch_is_staged_merged_and_cleaned_up(tmp_path):
    sink, client = make_sink(tmp_path)
    uri = batch_file(tmp_path, id=[1, 2, 1], name=["a", "b", "c"])

    sink.process_batch_files(ARROW, [uri])

    statements = [s for s, _ in client.statements]
    volume = f"`main`.`raw`.`{STAGING_VOLUME}`"
    assert statements[0] == f"CREATE VOLUME IF NOT EXISTS {volume}"
    assert statements[1].startswith("PUT '")
    assert statements[2].startswith("MERGE INTO `main`.`raw`.`users`")
    assert statements[3].startswith("REMOVE '/Volumes/main/raw/meltano_staging/")
    assert len(statements) == 4
    assert not (tmp_path / "batch.arrow").exists()
    assert list(tmp_path.glob("*.parquet")) == []


def test_arrow_batch_without_keys_inserts_and_keeps_file_on_request(tmp_path):
    sink, client = make_sink(tmp_path, keys=(), config={"clean_up_batch_files": False})
    uri = batch_file(tmp_path, id=[1, 1])

    sink.process_batch_files(ARROW, [uri])

    assert any(s.startswith("INSERT INTO") for s, _ in client.statements)
    assert (tmp_path / "batch.arrow").exists()


def test_volume_is_created_once_per_sink(tmp_path):
    sink, client = make_sink(tmp_path)
    for i in range(2):
        path = tmp_path / f"b{i}.arrow"
        sink.process_batch_files(ARROW, [write_ipc(path, pa.table({"id": [i]}))])
    creates = [s for s, _ in client.statements if s.startswith("CREATE VOLUME")]
    assert len(creates) == 1


def test_staging_failure_falls_back_to_inline_inserts(tmp_path):
    sink, client = make_sink(tmp_path, fail_put=True)
    ts = datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC)
    uri = batch_file(tmp_path, id=[1], name=[str(ts)])

    sink.process_batch_files(ARROW, [uri])

    statement, params = client.statements[-1]
    assert statement.startswith("MERGE INTO")
    assert params == {"p0_0": 1, "p0_1": str(ts)}


def test_buffered_records_are_flushed_before_arrow_batch(tmp_path):
    sink, client = make_sink(tmp_path)
    sink.process_record({"id": 1, "name": "a"}, sink._get_context({}))
    sink.tally_record_read()

    sink.process_batch_files(ARROW, [batch_file(tmp_path)])

    kinds = [s.split(" ", 1)[0] for s, _ in client.statements]
    assert kinds[0] == "MERGE"  # the buffered record, then volume/PUT/MERGE/REMOVE
    assert kinds[1:] == ["CREATE", "PUT", "MERGE", "REMOVE"]


def test_non_arrow_encodings_use_the_sdk(tmp_path):
    sink, _ = make_sink(tmp_path)
    jsonl = BaseBatchFileEncoding(format="jsonl")
    with mock.patch(
        "singer_sdk.sinks.BatchSink.process_batch_files",
    ) as sdk_impl:
        sink.process_batch_files(jsonl, ["file:///x.jsonl"])
    sdk_impl.assert_called_once_with(jsonl, ["file:///x.jsonl"])
