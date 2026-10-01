"""Test Configuration.

Live fixtures (``warehouse_config``, ``admin_client``, ``schema``, ``target_config``,
``run_singer``) skip the requesting test unless the warehouse credentials are set:

- ``TARGET_DATABRICKS_SERVER_HOSTNAME``
- ``TARGET_DATABRICKS_HTTP_PATH``
- ``TARGET_DATABRICKS_ACCESS_TOKEN``
- ``TARGET_DATABRICKS_CATALOG`` (optional)
"""

from __future__ import annotations

import dataclasses
import io
import logging
import os
import sys
import typing as t
import uuid

import pytest
from singer_sdk.testing.runners import TargetTestRunner

from target_databricks import sql
from target_databricks.client import DatabricksClient
from target_databricks.config import SingerConfig
from target_databricks.target import TargetDatabricks

if t.TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from importlib.abc import Traversable


# The connector logs every Thrift request/response at DEBUG, swamping failures.
logging.getLogger("databricks").setLevel(logging.WARNING)


def pytest_configure(config: pytest.Config):
    if sys.version_info < (3, 11):
        config.addinivalue_line(
            "filterwarnings",
            "once:Python 3.10 reached its end of life on 2026-10:FutureWarning",
        )

    elif sys.version_info < (3, 12):
        config.addinivalue_line(
            "filterwarnings",
            "once:Python 3.11 will reach its end of life on 2027-10:FutureWarning",
        )


# Environment variable per setting. Add new required connection settings here.
ENV_SETTINGS: dict[str, str] = {
    "server_hostname": "TARGET_DATABRICKS_SERVER_HOSTNAME",
    "http_path": "TARGET_DATABRICKS_HTTP_PATH",
    "access_token": "TARGET_DATABRICKS_ACCESS_TOKEN",
}
ENV_CATALOG = "TARGET_DATABRICKS_CATALOG"

# `warehouse_config` casts the mapping above to `SingerConfig`, so keep them in sync.
_unknown_settings = ENV_SETTINGS.keys() - SingerConfig.__annotations__.keys()
_unmapped_required = SingerConfig.__required_keys__ - ENV_SETTINGS.keys()
assert not _unknown_settings, f"ENV_SETTINGS has unknown settings: {_unknown_settings}"
assert not _unmapped_required, f"ENV_SETTINGS lacks required: {_unmapped_required}"


@dataclasses.dataclass(frozen=True)
class Schema:
    """An ephemeral schema owned by one test."""

    name: str
    catalog: str | None
    client: DatabricksClient

    @property
    def qualified(self) -> str:
        return sql.fq_name(self.catalog, self.name)

    def table(self, stream_name: str) -> str:
        """Qualified name of the table the sink creates for ``stream_name``."""
        table = sql.conform_name(stream_name.rsplit("-", 1)[-1])
        return sql.fq_name(self.catalog, self.name, table)

    def rows(self, stream_name: str, columns: str = "*", order_by: str = "") -> list:
        order = f" ORDER BY {order_by}" if order_by else ""
        statement = f"SELECT {columns} FROM {self.table(stream_name)}{order}"
        return [tuple(r) for r in self.client.execute(statement)]

    def columns(self, stream_name: str) -> dict[str, str] | None:
        return self.client.table_columns(self.table(stream_name))


@dataclasses.dataclass
class RunResult:
    """Outcome of one target run."""

    state_messages: list[dict]
    stderr: str


@pytest.fixture(scope="session")
def warehouse_config() -> SingerConfig:
    """Connection settings from the environment; skips when they are missing."""
    missing = [env for env in ENV_SETTINGS.values() if not os.environ.get(env)]
    if missing:
        pytest.skip(f"requires {', '.join(missing)}")

    config = t.cast(
        "SingerConfig",
        {setting: os.environ[env] for setting, env in ENV_SETTINGS.items()},
    )
    if catalog := os.environ.get(ENV_CATALOG):
        config["catalog"] = catalog
    return config


@pytest.fixture(scope="session")
def admin_client(warehouse_config: SingerConfig) -> Iterator[DatabricksClient]:
    """One shared connection for fixture setup, assertions and teardown."""
    client = DatabricksClient(warehouse_config)
    yield client
    client.close()


@pytest.fixture
def schema(
    warehouse_config: SingerConfig,
    admin_client: DatabricksClient,
) -> Iterator[Schema]:
    """A unique, empty schema that is always dropped afterwards."""
    catalog = warehouse_config.get("catalog")
    handle = Schema(f"meltano_test_{uuid.uuid4().hex[:8]}", catalog, admin_client)
    admin_client.execute(sql.create_schema_sql(catalog, handle.name))
    try:
        yield handle
    finally:
        admin_client.execute(f"DROP SCHEMA IF EXISTS {handle.qualified} CASCADE")


@pytest.fixture
def target_config(warehouse_config: SingerConfig, schema: Schema) -> SingerConfig:
    """Target config that loads into this test's ephemeral schema."""
    return {**warehouse_config, "default_target_schema": schema.name}


@pytest.fixture
def run_singer(
    target_config: SingerConfig,
    admin_client: DatabricksClient,
) -> Callable[..., RunResult]:
    """Run the target over Singer messages (a file or a string of JSONL lines).

    Keyword arguments override ``target_config`` for that run. Runs reuse the
    worker's ``admin_client`` connection instead of opening a new one each time.
    """

    class SharedClientTarget(TargetDatabricks):
        @property
        def client(self) -> DatabricksClient:
            return admin_client

    def run(source: Traversable | str, **overrides: t.Any) -> RunResult:
        # Pass text, not a path: TargetTestRunner never closes files it opens.
        text = source if isinstance(source, str) else source.read_text(encoding="utf-8")
        # "Truncated once per run" state lives on the client, which now outlives runs.
        admin_client.overwritten_tables.clear()
        runner = TargetTestRunner(
            SharedClientTarget,
            config={**target_config, **overrides},
            input_io=io.StringIO(text),
        )
        runner.sync_all()
        return RunResult(runner.state_messages, runner.stderr or "")

    return run
