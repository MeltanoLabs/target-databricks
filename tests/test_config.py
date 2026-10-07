from typing import TYPE_CHECKING

import pytest
from singer_sdk.exceptions import ConfigValidationError

from target_databricks.target import TargetDatabricks

if TYPE_CHECKING:
    from target_databricks.config import SingerConfig


def test_required_fields():
    config: SingerConfig = {}  # type:ignore[typeddict-item] # ty: ignore[missing-typed-dict-key]
    with pytest.raises(ConfigValidationError) as exc_info:
        _ = TargetDatabricks(config=config)

    assert set(exc_info.value.errors) == {
        "'access_token' is required when auth_type is 'pat'",
        "'server_hostname' is a required property",
        "'http_path' is a required property",
    }


def test_retain_dropped_for_pattern():
    config: SingerConfig = {
        "access_token": "value",
        "server_hostname": "h.cloud.databricks.com",
        "http_path": "/sql/1.0/warehouses/x",
        "schema_creation_parameters": {},
    }

    def _set(value: str) -> None:
        nonlocal config
        config["schema_creation_parameters"]["retain_dropped_for"] = value

    # OK
    _set("1 hour")
    _ = TargetDatabricks(config=config)

    # Invalid unit
    _set("1 month")
    with pytest.raises(ConfigValidationError) as exc_info:
        _ = TargetDatabricks(config=config)

    assert set(exc_info.value.errors) == {
        "'1 month' does not match '^\\\\d+ (hour|hours|day|days|week|weeks)$' in config['schema_creation_parameters']['retain_dropped_for']"  # ruff: ignore[line-too-long]
    }

    # SQL injection
    _set("1 hour; DROP SCHEMA analytics")
    with pytest.raises(ConfigValidationError) as exc_info:
        _ = TargetDatabricks(config=config)

    assert set(exc_info.value.errors) == {
        "'1 hour; DROP SCHEMA analytics' does not match '^\\\\d+ (hour|hours|day|days|week|weeks)$' in config['schema_creation_parameters']['retain_dropped_for']"  # ruff: ignore[line-too-long]
    }
