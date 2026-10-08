"""Databricks target class."""

from __future__ import annotations

from typing import Any, ClassVar, cast, override

from singer_sdk import Target
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.helpers.capabilities import CapabilitiesEnum, PluginCapabilities

from target_databricks.client import DatabricksClient
from target_databricks.config import SingerConfig, get_config_jsonschema
from target_databricks.sinks import DatabricksSink


class TargetDatabricks(Target):
    """Singer target for Databricks (Unity Catalog / Delta Lake)."""

    name = "target-databricks"

    default_sink_class = DatabricksSink

    capabilities: ClassVar[list[CapabilitiesEnum]] = [
        *Target.capabilities,
        PluginCapabilities.BATCH,
    ]

    config_jsonschema = get_config_jsonschema()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._client: DatabricksClient | None = None

    @property
    def client(self) -> DatabricksClient:
        """Lazily-connected, process-wide Databricks SQL client."""
        config = cast("SingerConfig", self.config)
        if self._client is None:
            self._client = DatabricksClient(config)
        return self._client

    @override
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
