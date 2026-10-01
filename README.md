# target-databricks

Singer target for [Databricks](https://www.databricks.com/) (Unity Catalog / Delta Lake),
built with the [Meltano Singer SDK](https://sdk.meltano.com).

It talks to a SQL warehouse directly through
[`databricks-sql-connector`](https://github.com/databricks/databricks-sql-python); there is
no SQLAlchemy layer.

## Settings

| Setting                 | Required | Description                                                                 |
| ----------------------- | -------- | --------------------------------------------------------------------------- |
| `server_hostname`       | yes      | Workspace hostname, e.g. `dbc-1234.cloud.databricks.com`                    |
| `http_path`             | yes      | SQL warehouse HTTP path, e.g. `/sql/1.0/warehouses/abc`                     |
| `auth_type`             |          | `pat` (default) or `oauth_m2m`                                              |
| `access_token`          | for `pat`| Personal access token                                                       |
| `client_id`             | for M2M  | Service principal client ID                                                 |
| `client_secret`         | for M2M  | Service principal OAuth secret                                              |
| `catalog`               |          | Unity Catalog name (warehouse default if unset)                             |
| `default_target_schema` |          | Target schema; otherwise taken from `<schema>-<table>` stream names         |
| `load_method`           |          | `upsert` (default), `append-only` or `overwrite`                            |
| `hard_delete`           |          | On `ACTIVATE_VERSION`, delete stale rows instead of setting `_sdc_deleted_at` |

Built-in SDK settings (`add_record_metadata`, `batch_size_rows`, `stream_maps`, ...) also apply.
`ACTIVATE_VERSION` handling needs `add_record_metadata: true`.

## How data is loaded

- Tables are created as Delta tables from the stream's JSON Schema (objects/arrays are stored as
  JSON strings) and new columns are added automatically. Existing column types are not altered.
- With key properties, `upsert` uses a native `MERGE INTO`; without them, or with
  `append-only`, rows are inserted. `overwrite` truncates each table once per run.
- Arrow `BATCH` messages are not supported yet.

## Development

```sh
uv sync
uv run pytest            # unit tests; live tests skip without credentials
uvx --with tox-uv tox -e lint
```

Live tests run when `TARGET_DATABRICKS_SERVER_HOSTNAME`, `TARGET_DATABRICKS_HTTP_PATH` and `TARGET_DATABRICKS_ACCESS_TOKEN`
(and optionally `TARGET_DATABRICKS_CATALOG`) are set. They create and drop a uniquely named schema.
