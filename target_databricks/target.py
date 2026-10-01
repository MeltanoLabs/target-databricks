"""Databricks target class."""

from __future__ import annotations

import typing as t

from singer_sdk import Target
from singer_sdk import typing as th
from singer_sdk.exceptions import ConfigValidationError

from target_databricks.client import DatabricksClient
from target_databricks.sinks import DatabricksSink


class TargetDatabricks(Target):
    """Singer target for Databricks (Unity Catalog / Delta Lake)."""

    name = "target-databricks"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "server_hostname",
            th.StringType,
            required=True,
            title="Server Hostname",
            description="Workspace hostname, e.g. `dbc-1234.cloud.databricks.com`.",
        ),
        th.Property(
            "http_path",
            th.StringType,
            required=True,
            title="HTTP Path",
            description="SQL warehouse HTTP path, e.g. `/sql/1.0/warehouses/abc`.",
        ),
        th.Property(
            "auth_type",
            th.StringType,
            default="pat",
            allowed_values=["pat", "oauth_m2m"],
            title="Authentication Type",
            description=(
                "`pat` for a personal access token, `oauth_m2m` for a service "
                "principal (client ID and secret)."
            ),
        ),
        th.Property(
            "access_token",
            th.StringType,
            secret=True,
            title="Access Token",
            description="Personal access token. Required when `auth_type` is `pat`.",
        ),
        th.Property(
            "client_id",
            th.StringType,
            title="Client ID",
            description="Service principal client ID. Required for `oauth_m2m`.",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            secret=True,
            title="Client Secret",
            description="Service principal OAuth secret. Required for `oauth_m2m`.",
        ),
        th.Property(
            "catalog",
            th.StringType,
            title="Catalog",
            description="Unity Catalog name. Defaults to the warehouse default.",
        ),
        th.Property(
            "default_target_schema",
            th.StringType,
            title="Default Target Schema",
            description=(
                "Schema to load into. If unset, it is taken from `<schema>-<table>` "
                "stream names."
            ),
        ),
        th.Property(
            "load_method",
            th.StringType,
            default="upsert",
            allowed_values=["upsert", "append-only", "overwrite"],
            title="Load Method",
            description=(
                "`upsert` merges on the stream key properties, `append-only` always "
                "inserts, `overwrite` truncates each table once per run then loads."
            ),
        ),
        th.Property(
            "hard_delete",
            th.BooleanType,
            default=False,
            title="Hard Delete",
            description=(
                "On `ACTIVATE_VERSION`, delete stale rows instead of marking them "
                "with `_sdc_deleted_at`."
            ),
        ),
    ).to_dict()

    default_sink_class = DatabricksSink

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)
        self._client: DatabricksClient | None = None

    @property
    def client(self) -> DatabricksClient:
        if self._client is None:
            self._client = DatabricksClient(self.config)
        return self._client

    def _validate_config(self, *, raise_errors: bool = True) -> list[str]:
        errors = super()._validate_config(raise_errors=False)
        auth_type = self.config.get("auth_type", "pat")
        required = (
            ("client_id", "client_secret")
            if auth_type == "oauth_m2m"
            else ("access_token",)
        )
        errors.extend(
            f"'{key}' is required when auth_type is '{auth_type}'"
            for key in required
            if not self.config.get(key)
        )
        if errors and raise_errors:
            summary = "Config validation failed"
            raise ConfigValidationError(summary, errors=errors)
        return errors


if __name__ == "__main__":
    TargetDatabricks.cli()
