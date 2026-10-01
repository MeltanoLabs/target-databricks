from __future__ import annotations

import datetime
from decimal import Decimal

import pytest

from target_databricks import sql


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("Users", "users"),
        ("first name", "first_name"),
        ("1st", "_1st"),
        ("a-b.c", "a_b_c"),
        ("---", "_"),
        ("_sdc_deleted_at", "_sdc_deleted_at"),
    ],
)
def test_conform_name(name, expected):
    assert sql.conform_name(name) == expected


def test_quote_and_fq_name():
    assert sql.quote_ident("a`b") == "`a``b`"
    assert sql.fq_name("cat", "sch", "tbl") == "`cat`.`sch`.`tbl`"
    assert sql.fq_name(None, "sch", "tbl") == "`sch`.`tbl`"


@pytest.mark.parametrize(
    ("prop", "expected"),
    [
        ({"type": "string"}, "STRING"),
        ({"type": ["string", "null"]}, "STRING"),
        ({"type": ["string", "null"], "format": "date-time"}, "TIMESTAMP"),
        ({"type": "string", "format": "date"}, "DATE"),
        ({"type": "string", "format": "time"}, "STRING"),
        ({"type": ["integer", "null"]}, "BIGINT"),
        ({"type": "number"}, "DOUBLE"),
        ({"type": ["integer", "number"]}, "DOUBLE"),
        ({"type": "boolean"}, "BOOLEAN"),
        ({"type": "object"}, "STRING"),
        ({"type": ["null", "array"]}, "STRING"),
        ({"type": ["string", "integer"]}, "STRING"),
        ({}, "STRING"),
        (
            {"anyOf": [{"type": "string", "format": "date-time"}, {"type": "null"}]},
            "TIMESTAMP",
        ),
        ({"anyOf": [{"type": "integer"}, {"type": "null"}]}, "BIGINT"),
    ],
)
def test_column_type(prop, expected):
    assert sql.column_type(prop) == expected


def test_columns_from_schema_conforms_names():
    schema = {
        "properties": {"Id": {"type": "integer"}, "Full Name": {"type": "string"}}
    }
    assert sql.columns_from_schema(schema) == {"id": "BIGINT", "full_name": "STRING"}


@pytest.mark.parametrize(
    ("value", "typ", "expected"),
    [
        (None, "STRING", None),
        ("x", "STRING", "x"),
        (1, "STRING", "1"),
        (Decimal("1.5"), "STRING", "1.5"),
        (True, "STRING", "true"),
        ({"a": 1}, "STRING", '{"a": 1}'),
        ([1, 2], "STRING", "[1, 2]"),
        (1, "BOOLEAN", True),
        (Decimal(3), "BIGINT", 3),
        (Decimal("1.5"), "DOUBLE", 1.5),
        (float("nan"), "DOUBLE", None),
        (float("inf"), "DOUBLE", None),
        (
            datetime.datetime(2024, 1, 2, 3, 4, tzinfo=datetime.timezone.utc),
            "TIMESTAMP",
            "2024-01-02T03:04:00+00:00",
        ),
        ("2024-01-02", "DATE", "2024-01-02"),
    ],
)
def test_serialize_value(value, typ, expected):
    assert sql.serialize_value(value, typ) == expected


def test_ddl():
    cols = {"id": "BIGINT", "name": "STRING"}
    assert sql.create_schema_sql("c", "s") == "CREATE SCHEMA IF NOT EXISTS `c`.`s`"
    assert sql.create_table_sql("`s`.`t`", cols) == (
        "CREATE TABLE IF NOT EXISTS `s`.`t` (`id` BIGINT, `name` STRING) USING DELTA"
    )
    assert sql.add_columns_sql("`s`.`t`", {"x": "DATE"}) == (
        "ALTER TABLE `s`.`t` ADD COLUMNS (`x` DATE)"
    )
    assert sql.truncate_sql("`s`.`t`") == "TRUNCATE TABLE `s`.`t`"


def test_insert_sql():
    statement, params = sql.insert_sql(
        "`s`.`t`",
        {"id": "BIGINT", "name": "STRING"},
        [{"id": 1, "name": "a"}, {"id": 2}],
    )
    assert statement == (
        "INSERT INTO `s`.`t` (`id`, `name`) "
        "SELECT CAST(c0 AS BIGINT) AS `id`, CAST(c1 AS STRING) AS `name` "
        "FROM VALUES (%(p0_0)s, %(p0_1)s), (%(p1_0)s, %(p1_1)s) AS v(c0, c1)"
    )
    assert params == {"p0_0": 1, "p0_1": "a", "p1_0": 2, "p1_1": None}


def test_merge_sql():
    statement, params = sql.merge_sql(
        "`s`.`t`",
        {"id": "BIGINT", "name": "STRING"},
        ["id"],
        [{"id": 1, "name": "a"}],
    )
    assert statement == (
        "MERGE INTO `s`.`t` AS t USING ("
        "SELECT CAST(c0 AS BIGINT) AS `id`, CAST(c1 AS STRING) AS `name` "
        "FROM VALUES (%(p0_0)s, %(p0_1)s) AS v(c0, c1)) AS s "
        "ON t.`id` = s.`id` "
        "WHEN MATCHED THEN UPDATE SET t.`name` = s.`name` "
        "WHEN NOT MATCHED THEN INSERT (`id`, `name`) VALUES (s.`id`, s.`name`)"
    )
    assert params == {"p0_0": 1, "p0_1": "a"}


def test_merge_sql_key_only_table_has_no_update_clause():
    statement, _ = sql.merge_sql("`t`", {"id": "BIGINT"}, ["id"], [{"id": 1}])
    assert "WHEN MATCHED" not in statement


def test_version_deletes():
    assert (
        sql.hard_delete_sql("`t`", 5)
        == "DELETE FROM `t` WHERE `_sdc_table_version` < 5"
    )
    assert "SET `_sdc_deleted_at` = current_timestamp()" in sql.soft_delete_sql(
        "`t`", 5
    )


def test_sdc_metadata_columns_keep_their_names():
    """ACTIVATE_VERSION SQL refers to `_sdc_*` columns, so they must not be renamed."""
    schema = {
        "properties": {
            "_sdc_deleted_at": {"type": ["null", "string"], "format": "date-time"},
            "_sdc_table_version": {"type": ["null", "integer"]},
        },
    }
    assert sql.columns_from_schema(schema) == {
        "_sdc_deleted_at": "TIMESTAMP",
        "_sdc_table_version": "BIGINT",
    }
