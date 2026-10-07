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

    def _set(value: int) -> None:
        nonlocal config
        config["schema_creation_parameters"]["retention_days"] = value

    # OK
    _set(10)
    _ = TargetDatabricks(config=config)

    # Disable OK
    _set(0)
    _ = TargetDatabricks(config=config)

    # Outside range
    _set(31)
    with pytest.raises(ConfigValidationError) as exc_info:
        _ = TargetDatabricks(config=config)

    assert set(exc_info.value.errors) == {
        "31 is not valid under any of the given schemas in config['schema_creation_parameters']['retention_days']"  # ruff: ignore[line-too-long]
    }
