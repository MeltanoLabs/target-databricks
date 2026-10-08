"""Helpers for consuming Arrow-encoded Singer ``BATCH`` messages.

Taps and mappers can emit ``BATCH`` messages with ``encoding: {"format": "arrow"}``
whose manifest entries are ``file://`` URIs (or bare paths) of Arrow IPC files.
Nothing here talks to Databricks, so it can be unit tested without a warehouse.
"""

from __future__ import annotations

import json
import typing as t
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import ipc

from target_databricks import sql

if t.TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

ARROW_ENCODING_FORMAT = "arrow"

_ROW_NUMBER = "__row_number"


def resolve_manifest_path(file_uri: str) -> str:
    """Resolve a manifest ``file://`` URI (or bare local path) to a filesystem path."""
    return urlparse(file_uri).path or file_uri


def read_arrow_file(path: str) -> pa.Table:
    """Read an Arrow IPC file (or, failing that, stream) into a single table."""
    try:
        with ipc.open_file(path) as reader:
            return reader.read_all()
    except pa.ArrowInvalid:
        with pa.OSFile(path, "rb") as source, ipc.open_stream(source) as stream_reader:
            return stream_reader.read_all()


_ARROW_TYPES: dict[str, pa.DataType] = {
    sql.STRING: pa.string(),
    sql.BIGINT: pa.int64(),
    sql.DOUBLE: pa.float64(),
    sql.BOOLEAN: pa.bool_(),
    # Dates and timestamps travel as ISO strings and are cast by the load statement,
    # exactly like the inline path does.
    sql.DATE: pa.string(),
    sql.TIMESTAMP: pa.string(),
}


def table_from_rows(
    rows: Sequence[Mapping[str, t.Any]],
    columns: Mapping[str, str],
) -> pa.Table:
    """Build a typed table from conformed records, one column per sink column.

    Values are coerced with :func:`sql.serialize_value`, so records load the same
    values whether they are inlined into a statement or staged as Parquet.
    """
    arrays = [
        pa.array(
            [sql.serialize_value(row.get(name), sql_type) for row in rows],
            type=_ARROW_TYPES[sql_type],
        )
        for name, sql_type in columns.items()
    ]
    return pa.table(arrays, names=list(columns))


def _is_nested(arrow_type: pa.DataType) -> bool:
    if pa.types.is_dictionary(arrow_type):
        return pa.types.is_nested(arrow_type.value_type)
    return pa.types.is_nested(arrow_type)


def _to_json_strings(column: pa.ChunkedArray) -> pa.Array:
    return pa.array(
        [None if v is None else json.dumps(v, default=str) for v in column.to_pylist()],
        type=pa.string(),
    )


def conform_table(table: pa.Table, columns: Mapping[str, str]) -> pa.Table:
    """Align a table with the sink's columns.

    Column names are conformed like record keys, columns the table lacks become
    nulls and columns the sink does not know about are dropped. Nested values
    headed for ``STRING`` columns are stored as JSON, like on the record path.
    """
    source = {sql.conform_name(n): table.column(n) for n in table.column_names}
    arrays: list[pa.Array | pa.ChunkedArray] = []
    for name, sql_type in columns.items():
        column = source.get(name)
        if column is None:
            arrays.append(pa.nulls(table.num_rows, type=pa.string()))
        elif sql_type == sql.STRING and _is_nested(column.type):
            arrays.append(_to_json_strings(column))
        else:
            arrays.append(column)
    return pa.table(arrays, names=list(columns))


def dedupe_table(table: pa.Table, keys: Sequence[str]) -> pa.Table:
    """Keep the last row per key; ``MERGE`` rejects duplicate matches."""
    numbered = table.append_column(_ROW_NUMBER, pa.array(range(table.num_rows)))
    last = numbered.group_by(list(keys)).aggregate([(_ROW_NUMBER, "max")])
    indices = last.column(f"{_ROW_NUMBER}_max")
    return table.take(pc.take(indices, pc.sort_indices(indices)))
