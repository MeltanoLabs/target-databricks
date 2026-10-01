"""Thin wrapper around the ``databricks-sql-connector`` DB-API."""

from __future__ import annotations

import atexit
import threading
import typing as t

from databricks import sql as dbsql

if t.TYPE_CHECKING:
    from collections.abc import Mapping

    from target_databricks.sql import Params

USER_AGENT_ENTRY = "meltano-target-databricks"


def connect_kwargs(config: Mapping[str, t.Any]) -> dict[str, t.Any]:
    """Build ``databricks.sql.connect`` keyword arguments from target config."""
    kwargs: dict[str, t.Any] = {
        "server_hostname": config["server_hostname"],
        "http_path": config["http_path"],
        # Parameters are rendered client-side so that multi-row statements are not
        # subject to server-side parameter-count limits.
        "use_inline_params": "silent",
        "user_agent_entry": USER_AGENT_ENTRY,
    }
    if catalog := config.get("catalog"):
        kwargs["catalog"] = catalog

    if config.get("auth_type", "pat") == "oauth_m2m":
        kwargs["credentials_provider"] = lambda: _service_principal_headers(config)
    else:
        kwargs["access_token"] = config["access_token"]
    return kwargs


def _service_principal_headers(config: Mapping[str, t.Any]) -> t.Any:
    """Build the OAuth M2M header factory (resolves auth, so created lazily)."""
    from databricks.sdk.core import Config, oauth_service_principal  # noqa: PLC0415

    return oauth_service_principal(
        Config(
            host=f"https://{config['server_hostname']}",
            client_id=config["client_id"],
            client_secret=config["client_secret"],
        ),
    )


class DatabricksClient:
    """Lazily-connected, process-wide Databricks SQL client."""

    def __init__(self, config: Mapping[str, t.Any]) -> None:
        self._config = config
        self._connection: t.Any = None
        self._lock = threading.Lock()
        self.overwritten_tables: set[str] = set()
        """Tables already truncated by this run (``overwrite`` load method)."""

    def _get_connection(self) -> t.Any:
        if self._connection is None:
            self._connection = dbsql.connect(**connect_kwargs(self._config))
            atexit.register(self.close)
        return self._connection

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

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
