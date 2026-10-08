from __future__ import annotations

import pytest
from singer_sdk.exceptions import ConfigValidationError

from target_databricks.target import TargetDatabricks

BASE = {"server_hostname": "h", "http_path": "/p"}


def test_pat_requires_token():
    with pytest.raises(ConfigValidationError) as exc_info:
        TargetDatabricks(config=BASE)
    assert any("access_token" in e for e in exc_info.value.errors)


def test_m2m_requires_client_credentials():
    with pytest.raises(ConfigValidationError) as exc_info:
        TargetDatabricks(config={**BASE, "auth_type": "oauth_m2m", "client_id": "x"})
    assert any("client_secret" in e for e in exc_info.value.errors)


def test_valid_configs():
    TargetDatabricks(config={**BASE, "access_token": "t"})
    TargetDatabricks(
        config={
            **BASE,
            "auth_type": "oauth_m2m",
            "client_id": "i",
            "client_secret": "s",
        },
    )
