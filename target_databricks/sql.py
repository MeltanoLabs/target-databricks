"""Pure SQL-building helpers for Databricks (Delta Lake / Unity Catalog).

Nothing in this module performs I/O, so it is cheap to unit test.
"""

from __future__ import annotations

import datetime
import json
import math
import re
import typing as t
from decimal import Decimal

if t.TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

Params = dict[str, t.Any]

STRING = "STRING"
BIGINT = "BIGINT"
DOUBLE = "DOUBLE"
BOOLEAN = "BOOLEAN"
DATE = "DATE"
TIMESTAMP = "TIMESTAMP"

_INVALID_CHARS = re.compile(r"[^0-9a-zA-Z_]+")


def conform_name(name: str) -> str:
    """Make a stream/property name safe to use as an unquoted-style identifier."""
    conformed = _INVALID_CHARS.sub("_", name).lower() or "_"
    if conformed[0].isdigit():
        conformed = f"_{conformed}"
    return conformed


def quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def fq_name(*parts: str | None) -> str:
    """Join non-empty identifier parts into a quoted dotted name."""
    return ".".join(quote_ident(part) for part in parts if part)


def column_type(prop: Mapping[str, t.Any]) -> str:
    """Map a JSON Schema property to a Delta SQL type."""
    types: set[str] = set()
    formats: set[str] = set()
    for sub in prop.get("anyOf") or [prop]:
        raw = sub.get("type", [])
        types.update([raw] if isinstance(raw, str) else raw)
        if "format" in sub:
            formats.add(sub["format"])
    types.discard("null")

    if types == {"string"}:
        if formats == {"date-time"}:
            return TIMESTAMP
        if formats == {"date"}:
            return DATE
        return STRING
    if types == {"integer"}:
        return BIGINT
    if types and types <= {"integer", "number"}:
        return DOUBLE
    if types == {"boolean"}:
        return BOOLEAN
    # objects, arrays, mixed and untyped properties are stored as JSON strings
    return STRING


def columns_from_schema(schema: Mapping[str, t.Any]) -> dict[str, str]:
    """Return ``{conformed column name: sql type}`` for a stream schema."""
    return {
        conform_name(name): column_type(prop)
        for name, prop in schema.get("properties", {}).items()
    }


def serialize_value(value: t.Any, sql_type: str) -> t.Any:
    """Coerce a record value into something the connector can inline."""
    if value is None:
        return None
    if sql_type == STRING:
        if isinstance(value, str):
            return value
        if isinstance(value, Decimal | int | float) and not isinstance(value, bool):
            return str(value)
        return json.dumps(value, default=str)
    if sql_type == BOOLEAN:
        return bool(value)
    if sql_type == BIGINT:
        return int(value)
    if sql_type == DOUBLE:
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, datetime.datetime | datetime.date):
        return value.isoformat()
    return value


def create_schema_sql(
    catalog: str | None,
    schema: str,
    *,
    retention_days: int | None = None,
) -> str:
    ddl = f"CREATE SCHEMA IF NOT EXISTS {fq_name(catalog, schema)}"
    if retention_days is not None:
        param = "0 HOURS" if retention_days == 0 else f"{retention_days} DAYS"
        ddl = f"{ddl} RETAIN DROPPED FOR {param}"

    return ddl


def create_table_sql(table: str, columns: Mapping[str, str]) -> str:
    cols = ", ".join(f"{quote_ident(n)} {typ}" for n, typ in columns.items())
    return f"CREATE TABLE IF NOT EXISTS {table} ({cols}) USING DELTA"


def add_columns_sql(table: str, columns: Mapping[str, str]) -> str:
    cols = ", ".join(f"{quote_ident(n)} {typ}" for n, typ in columns.items())
    return f"ALTER TABLE {table} ADD COLUMNS ({cols})"


def truncate_sql(table: str) -> str:
    return f"TRUNCATE TABLE {table}"


def _source_select(
    columns: Mapping[str, str],
    rows: Sequence[Mapping[str, t.Any]],
) -> tuple[str, Params]:
    """Build ``SELECT CAST(..) .. FROM VALUES ..`` with inline-style params."""
    params: Params = {}
    value_rows = []
    for r, row in enumerate(rows):
        placeholders = []
        for c, (name, typ) in enumerate(columns.items()):
            key = f"p{r}_{c}"
            params[key] = serialize_value(row.get(name), typ)
            placeholders.append(f"%({key})s")
        value_rows.append(f"({', '.join(placeholders)})")

    aliases = ", ".join(f"c{i}" for i in range(len(columns)))
    projection = ", ".join(
        f"CAST(c{i} AS {typ}) AS {quote_ident(name)}"
        for i, (name, typ) in enumerate(columns.items())
    )
    select = f"SELECT {projection} FROM VALUES {', '.join(value_rows)} AS v({aliases})"
    return select, params


def insert_sql(
    table: str,
    columns: Mapping[str, str],
    rows: Sequence[Mapping[str, t.Any]],
) -> tuple[str, Params]:
    select, params = _source_select(columns, rows)
    names = ", ".join(quote_ident(n) for n in columns)
    return f"INSERT INTO {table} ({names}) {select}", params


def merge_sql(
    table: str,
    columns: Mapping[str, str],
    keys: Sequence[str],
    rows: Sequence[Mapping[str, t.Any]],
) -> tuple[str, Params]:
    select, params = _source_select(columns, rows)
    on = " AND ".join(f"t.{quote_ident(k)} = s.{quote_ident(k)}" for k in keys)
    updates = ", ".join(
        f"t.{quote_ident(n)} = s.{quote_ident(n)}" for n in columns if n not in keys
    )
    names = ", ".join(quote_ident(n) for n in columns)
    values = ", ".join(f"s.{quote_ident(n)}" for n in columns)
    sql = f"MERGE INTO {table} AS t USING ({select}) AS s ON {on} "
    if updates:
        sql += f"WHEN MATCHED THEN UPDATE SET {updates} "
    sql += f"WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})"
    return sql, params


def soft_delete_sql(table: str, version: int) -> str:
    return (
        f"UPDATE {table} SET `_sdc_deleted_at` = current_timestamp() "
        f"WHERE `_sdc_deleted_at` IS NULL AND `_sdc_table_version` < {int(version)}"
    )


def hard_delete_sql(table: str, version: int) -> str:
    return f"DELETE FROM {table} WHERE `_sdc_table_version` < {int(version)}"
