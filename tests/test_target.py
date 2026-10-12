from __future__ import annotations

from unittest import mock

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


def test_endofpipe_closes_client():
    target = TargetDatabricks(config={**BASE, "access_token": "t"})
    with mock.patch.object(type(target.client), "close") as close:
        target.process_endofpipe()
    close.assert_called_once_with()


def test_endofpipe_without_client_is_a_noop():
    target = TargetDatabricks(config={**BASE, "access_token": "t"})
    target.process_endofpipe()
    assert target._client is None
