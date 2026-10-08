"""Databricks target sink class."""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import TYPE_CHECKING, override

import pyarrow.parquet as pq
from singer_sdk.sinks import BatchSink

from target_databricks import arrow, sql

if TYPE_CHECKING:
    from collections.abc import Sequence

    import pyarrow as pa
    from singer_sdk.helpers._batch import BaseBatchFileEncoding
    from singer_sdk.helpers.types import Record

    from target_databricks.target import TargetDatabricks

CELLS_PER_STATEMENT = 20_000
"""Upper bound on rows x columns inlined into a single statement."""

STAGING_VOLUME = "meltano_staging"
"""Volume (created on demand in the target schema) that batches are staged in."""

STAGING_MIN_ROWS = 2_000
"""Record batches smaller than this are inlined: staging has a fixed cost of seconds."""


class DatabricksSink(BatchSink):
    """Loads records into a Delta table using ``INSERT`` or ``MERGE``.

    Batches are staged as Parquet in a Unity Catalog volume and read by a single
    statement. Databricks analyzes every literal of an inlined ``VALUES`` clause
    (about 1.6 ms per row however the rows are grouped), so that path is kept only for
    small batches and as a fallback when staging is not possible.
    """

    MAX_SIZE_DEFAULT = 100_000
    """Rows per batch; staging costs seconds per batch, so batches are large."""

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
        self._staging_volume_ready = False
        self._staging_disabled = False

    @override
    def setup(self) -> None:
        scp = self.config.get("schema_creation_parameters", {})
        self.client.execute(
            sql.create_schema_sql(
                self.catalog,
                self.schema_name,
                retention_days=scp.get("retention_days"),
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

        if len(rows) >= STAGING_MIN_ROWS and self._load_staged(
            arrow.table_from_rows(rows, self.columns),
        ):
            return
        self._insert_rows(rows)

    def _insert_rows(self, rows: list[dict]) -> None:
        """Load conformed rows with ``INSERT``/``MERGE`` statements that inline them."""
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

    def _flush(self) -> None:
        """Process the buffered records, if any, so later work lands after them."""
        if self.current_size:
            context = self.start_drain()
            with self.batch_processing_timer:
                self.process_batch(context)
            self.mark_drained()

    @override
    def process_batch_files(
        self,
        encoding: BaseBatchFileEncoding,
        files: Sequence[str],
    ) -> None:
        if encoding.format != arrow.ARROW_ENCODING_FORMAT:
            super().process_batch_files(encoding, files)
            return

        self._flush()
        for file_uri in files:
            path = Path(arrow.resolve_manifest_path(file_uri))
            table = arrow.read_arrow_file(str(path))
            self.record_counter_metric.increment(table.num_rows)
            self._load_arrow_table(table)
            # Manifest files are consume-once: nothing else reads them again.
            if self.config.get("clean_up_batch_files", True):
                path.unlink(missing_ok=True)

    def _load_arrow_table(self, table: pa.Table) -> None:
        """Load a table by staging it as Parquet in a volume and reading it in SQL."""
        table = arrow.conform_table(table, self.columns)
        if self.use_merge and table.num_rows:
            deduped = arrow.dedupe_table(table, self.key_columns)
            self.tally_duplicate_merged(table.num_rows - deduped.num_rows)
            table = deduped
        if not table.num_rows:
            return
        if not self._load_staged(table):
            self._insert_rows(table.to_pylist())

    def _load_staged(self, table: pa.Table) -> bool:
        """Stage ``table`` in the volume and load it; ``False`` if it can't be staged.

        Only staging failures return ``False`` (the caller then inlines the rows). A
        failure of the load statement itself propagates.
        """
        if self._staging_disabled:
            return False
        try:
            remote_path = self._stage(table)
        except Exception:
            self._staging_disabled = True
            self.logger.warning(
                "Could not stage batch in volume '%s'; falling back to inline "
                "inserts for the rest of the run.",
                STAGING_VOLUME,
                exc_info=True,
            )
            return False

        try:
            if self.use_merge:
                statement = sql.merge_staged_sql(
                    self.full_table_name,
                    self.columns,
                    self.key_columns,
                    remote_path,
                )
            else:
                statement = sql.insert_staged_sql(
                    self.full_table_name,
                    self.columns,
                    remote_path,
                )
            self.client.execute(statement)
        finally:
            try:
                self.client.execute(sql.remove_sql(remote_path))
            except Exception:
                self.logger.debug("Could not remove %s", remote_path, exc_info=True)
        return True

    def _stage(self, table: pa.Table) -> str:
        """Upload ``table`` as a Parquet file to the staging volume; return its path."""
        if not self._staging_volume_ready:
            self.client.execute(
                sql.create_volume_sql(self.catalog, self.schema_name, STAGING_VOLUME),
            )
            self._staging_volume_ready = True

        filename = f"{uuid.uuid4().hex}.parquet"
        local_path = Path(self.client.staging_dir) / filename
        remote_path = sql.volume_path(
            self.client.current_catalog(),
            self.schema_name,
            STAGING_VOLUME,
            filename,
        )
        # Spark has no nanosecond timestamps.
        pq.write_table(
            table,
            local_path,
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )
        try:
            self.client.execute(sql.put_sql(str(local_path), remote_path))
        finally:
            local_path.unlink(missing_ok=True)
        return remote_path

    @override
    def activate_version(self, new_version: int) -> None:
        if not self.include_sdc_metadata_properties:
            self.logger.warning(
                "Skipping ACTIVATE_VERSION for '%s': it requires "
                "'add_record_metadata' to be enabled.",
                self.stream_name,
            )
            return

        self._flush()  # buffered rows must land before changing versions

        if self.config.get("hard_delete", False):
            statement = sql.hard_delete_sql(self.full_table_name, new_version)
        else:
            statement = sql.soft_delete_sql(self.full_table_name, new_version)
        self.client.execute(statement)
