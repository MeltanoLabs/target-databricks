from __future__ import annotations

from unittest import mock

from target_databricks.client import DatabricksClient, connect_kwargs

BASE = {
    "server_hostname": "h.cloud.databricks.com",
    "http_path": "/sql/1.0/warehouses/x",
}


def test_pat_kwargs():
    kwargs = connect_kwargs({**BASE, "access_token": "tok", "catalog": "main"})
    assert kwargs["access_token"] == "tok"  # noqa: S105
    assert kwargs["catalog"] == "main"
    assert kwargs["use_inline_params"] == "silent"
    assert "credentials_provider" not in kwargs


def test_catalog_omitted_when_unset():
    assert "catalog" not in connect_kwargs({**BASE, "access_token": "tok"})


def test_oauth_m2m_kwargs():
    kwargs = connect_kwargs(
        {
            **BASE,
            "auth_type": "oauth_m2m",
            "client_id": "id",
            "client_secret": "secret",
        },
    )
    assert "access_token" not in kwargs
    assert callable(kwargs["credentials_provider"])


def test_table_columns_stops_at_metadata_section():
    client = DatabricksClient(BASE)
    rows = [
        ("id", "bigint", None),
        ("Name", "string", None),
        ("", "", ""),
        ("# Partition", "", ""),
    ]
    with mock.patch.object(client, "execute", return_value=rows):
        assert client.table_columns("`t`") == {"id": "BIGINT", "name": "STRING"}
