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


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(0, id="Null"),
        pytest.param(0, id="0"),
        pytest.param(0, id="10"),
        pytest.param(0, id="7 (boundary)"),
        pytest.param(0, id="30 (boundary)"),
    ],
)
def test_valid_retention_days(value: int | None):
    config: SingerConfig = {
        "access_token": "value",
        "server_hostname": "h.cloud.databricks.com",
        "http_path": "/sql/1.0/warehouses/x",
        "schema_creation_parameters": {"retention_days": value},
    }

    _ = TargetDatabricks(config=config)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(5, id="Below range"),
        pytest.param(31, id="Above range"),
        pytest.param("2 weeks", id="Not an integer"),
    ],
)
def test_invalid_retention_days(value: int | None):
    config: SingerConfig = {
        "access_token": "value",
        "server_hostname": "h.cloud.databricks.com",
        "http_path": "/sql/1.0/warehouses/x",
        "schema_creation_parameters": {"retention_days": value},
    }
    with pytest.raises(ConfigValidationError) as exc_info:
        _ = TargetDatabricks(config=config)

    assert len(exc_info.value.errors) == 1
    assert "retention_days" in exc_info.value.errors[0]
