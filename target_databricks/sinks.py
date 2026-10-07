"""Databricks target sink class."""

from __future__ import annotations

from typing import TYPE_CHECKING, override

from singer_sdk.sinks import BatchSink

from target_databricks import sql

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Record

    from target_databricks.target import TargetDatabricks

CELLS_PER_STATEMENT = 20_000
"""Upper bound on rows x columns inlined into a single statement."""


class DatabricksSink(BatchSink):
    """Loads records into a Delta table using ``INSERT`` or ``MERGE``."""

    def __init__(
        self,
        target: TargetDatabricks,
        stream_name: str,
        schema: dict,
        key_properties: list[str] | tuple[str, ...] | None,
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self.client = target.client

        parts = stream_name.split("-")
        self.table_name = sql.conform_name(parts[-1])
        if default_schema := self.config.get("default_target_schema"):
            self.schema_name = default_schema
        elif len(parts) > 1:
            self.schema_name = sql.conform_name(parts[-2])
        else:
            msg = (
                f"Cannot determine the target schema for stream '{stream_name}': set "
                "'default_target_schema' or use '<schema>-<table>' stream names."
            )
            raise ValueError(msg)

        self.catalog: str | None = self.config.get("catalog")
        self.full_table_name = sql.fq_name(
            self.catalog,
            self.schema_name,
            self.table_name,
        )
        self.columns = sql.columns_from_schema(self.schema)
        self.key_columns = [sql.conform_name(k) for k in self.key_properties]
        self.load_method: str = self.config.get("load_method", "upsert")

    @override
    def setup(self) -> None:
        scp = self.config.get("schema_creation_parameters", {})
        self.client.execute(
            sql.create_schema_sql(
                self.catalog,
                self.schema_name,
                retain_drop_for=scp.get("retain_dropped_for"),
            )
        )
        existing = self.client.table_columns(self.full_table_name)
        if existing is None:
            self.client.execute(
                sql.create_table_sql(self.full_table_name, self.columns)
            )
        else:
            missing = {n: t_ for n, t_ in self.columns.items() if n not in existing}
            if missing:
                self.client.execute(sql.add_columns_sql(self.full_table_name, missing))

        if (
            self.load_method == "overwrite"
            and self.full_table_name not in self.client.overwritten_tables
        ):
            self.client.execute(sql.truncate_sql(self.full_table_name))
            self.client.overwritten_tables.add(self.full_table_name)

    @property
    def use_merge(self) -> bool:
        return bool(self.key_columns) and self.load_method != "append-only"

    @staticmethod
    def _conform_record(record: Record) -> Record:
        return {sql.conform_name(k): v for k, v in record.items()}

    def _dedupe(self, rows: list[dict]) -> list[dict]:
        """Keep the last record per key; ``MERGE`` rejects duplicate matches."""
        latest = {tuple(r.get(k) for k in self.key_columns): r for r in rows}
        self.tally_duplicate_merged(len(rows) - len(latest))
        return list(latest.values())

    @override
    def process_batch(self, context: dict) -> None:
        rows = [self._conform_record(r) for r in context.get("records", [])]
        if not rows:
            return
        if self.use_merge:
            rows = self._dedupe(rows)

        chunk = max(1, CELLS_PER_STATEMENT // len(self.columns))
        for start in range(0, len(rows), chunk):
            batch = rows[start : start + chunk]
            if self.use_merge:
                statement, params = sql.merge_sql(
                    self.full_table_name,
                    self.columns,
                    self.key_columns,
                    batch,
                )
            else:
                statement, params = sql.insert_sql(
                    self.full_table_name,
                    self.columns,
                    batch,
                )
            self.client.execute(statement, params)

    @override
    def activate_version(self, new_version: int) -> None:
        if not self.include_sdc_metadata_properties:
            self.logger.warning(
                "Skipping ACTIVATE_VERSION for '%s': it requires "
                "'add_record_metadata' to be enabled.",
                self.stream_name,
            )
            return

        if self.current_size:  # flush buffered rows before changing versions
            context = self.start_drain()
            with self.batch_processing_timer:
                self.process_batch(context)
            self.mark_drained()

        if self.config.get("hard_delete", False):
            statement = sql.hard_delete_sql(self.full_table_name, new_version)
        else:
            statement = sql.soft_delete_sql(self.full_table_name, new_version)
        self.client.execute(statement)
