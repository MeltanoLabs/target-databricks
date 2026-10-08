"""Thin wrapper around the ``databricks-sql-connector`` DB-API."""

from __future__ import annotations

import atexit
import shutil
import tempfile
import threading
import typing as t
from typing import NotRequired

from databricks import sql as dbsql
from typing_extensions import TypedDict

if t.TYPE_CHECKING:
    from target_databricks.config import SingerConfig
    from target_databricks.sql import Params

USER_AGENT_ENTRY = "meltano-target-databricks"


class _ConnectKwargs(TypedDict, closed=True):
    server_hostname: str
    http_path: str
    use_inline_params: str
    user_agent_entry: str
    access_token: NotRequired[str]
    credentials_provider: NotRequired[t.Callable[[], t.Any]]
    catalog: NotRequired[str]
    staging_allowed_local_path: NotRequired[str]


def connect_kwargs(
    config: SingerConfig,
    staging_dir: str | None = None,
) -> _ConnectKwargs:
    """Build ``databricks.sql.connect`` keyword arguments from target config."""
    kwargs: _ConnectKwargs = {
        "server_hostname": config["server_hostname"],
        "http_path": config["http_path"],
        # Parameters are rendered client-side so that multi-row statements are not
        # subject to server-side parameter-count limits.
        "use_inline_params": "silent",
        "user_agent_entry": USER_AGENT_ENTRY,
    }
    if catalog := config.get("catalog"):
        kwargs["catalog"] = catalog
    if staging_dir:
        # Only files under this directory may be uploaded with ``PUT``.
        kwargs["staging_allowed_local_path"] = staging_dir

    if config.get("auth_type", "pat") == "oauth_m2m":
        kwargs["credentials_provider"] = lambda: _service_principal_headers(config)
    else:
        kwargs["access_token"] = config["access_token"]
    return kwargs


def _service_principal_headers(config: SingerConfig) -> t.Any:
    """Build the OAuth M2M header factory (resolves auth, so created lazily)."""
    from databricks.sdk.core import Config, oauth_service_principal  # ruff: ignore[import-outside-top-level]

    return oauth_service_principal(
        Config(
            host=f"https://{config['server_hostname']}",
            client_id=config["client_id"],
            client_secret=config["client_secret"],
        ),
    )


class DatabricksClient:
    """Lazily-connected, process-wide Databricks SQL client."""

    def __init__(self, config: SingerConfig) -> None:
        self._config = config
        self._connection: t.Any = None
        self._lock = threading.Lock()
        self._staging_dir: str | None = None
        self._catalog: str | None = None
        self.overwritten_tables: set[str] = set()
        """Tables already truncated by this run (``overwrite`` load method)."""

    @property
    def staging_dir(self) -> str:
        """Local directory that files uploaded to a volume must live in."""
        if self._staging_dir is None:
            self._staging_dir = tempfile.mkdtemp(prefix="target-databricks-")
        return self._staging_dir

    def _get_connection(self) -> t.Any:
        if self._connection is None:
            self._connection = dbsql.connect(
                **connect_kwargs(self._config, self.staging_dir),
            )
            atexit.register(self.close)
        return self._connection

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        if self._staging_dir is not None:
            shutil.rmtree(self._staging_dir, ignore_errors=True)
            self._staging_dir = None

    def current_catalog(self) -> str:
        """Return the catalog in use (the configured one, else the default)."""
        if self._catalog is None:
            self._catalog = self._config.get("catalog") or str(
                self.execute("SELECT current_catalog()")[0][0],
            )
        return self._catalog

    def execute(self, statement: str, params: Params | None = None) -> list[t.Any]:
        """Run a statement and return all rows (empty for non-queries)."""
        with self._lock, self._get_connection().cursor() as cursor:
            cursor.execute(statement, params)
            return cursor.fetchall() if cursor.description else []

    def table_columns(self, table: str) -> dict[str, str] | None:
        """Return ``{lowercase column: type}`` or ``None`` if the table is absent."""
        try:
            rows = self.execute(f"DESCRIBE TABLE {table}")
        except dbsql.exc.ServerOperationError as ex:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(ex):
                return None
            raise
        columns: dict[str, str] = {}
        for name, data_type, *_ in rows:
            if not name or name.startswith("#"):  # partition/metadata section
                break
            columns[name.lower()] = data_type.upper()
        return columns
