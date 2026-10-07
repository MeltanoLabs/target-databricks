from typing import Any, NotRequired

from singer_sdk import typing as th
from typing_extensions import TypedDict

DEFAULT_AUTH_TYPE = "pat"
DEFAULT_LOAD_METHOD = "upsert"
DEFAULT_HARD_DELETE = False

ALLOWED_AUTH_TYPES = ["pat", "oauth_m2m"]
ALLOWED_LOAD_METHODS = ["upsert", "append-only", "overwrite"]


class SchemaCreationParameters(TypedDict, closed=True):
    retention_days: NotRequired[int | None]


class SingerConfig(TypedDict, closed=False):
    """TypedDict for the target config JSON schema."""

    # Required settings
    server_hostname: str
    http_path: str

    # Settings with a default value
    auth_type: NotRequired[str]
    load_method: NotRequired[str]
    hard_delete: NotRequired[bool]

    # Optional settings
    access_token: NotRequired[str]
    client_id: NotRequired[str]
    client_secret: NotRequired[str]
    catalog: NotRequired[str]
    default_target_schema: NotRequired[str]
    schema_creation_parameters: NotRequired[SchemaCreationParameters]


def get_config_jsonschema() -> dict[str, Any]:
    return th.PropertiesList(
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
            nullable=False,
            default=DEFAULT_AUTH_TYPE,
            allowed_values=ALLOWED_AUTH_TYPES,
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
            nullable=False,
            default=DEFAULT_LOAD_METHOD,
            allowed_values=ALLOWED_LOAD_METHODS,
            title="Load Method",
            description=(
                "`upsert` merges on the stream key properties, `append-only` always "
                "inserts, `overwrite` truncates each table once per run then loads."
            ),
        ),
        th.Property(
            "hard_delete",
            th.BooleanType,
            default=DEFAULT_HARD_DELETE,
            title="Hard Delete",
            description=(
                "On `ACTIVATE_VERSION`, delete stale rows instead of marking them "
                "with `_sdc_deleted_at`."
            ),
        ),
        th.Property(
            name="schema_creation_parameters",
            wrapped=th.ObjectType(
                th.Property(
                    "retention_days",
                    th.OneOf(
                        th.Constant(0),
                        th.IntegerType(minimum=7, maximum=30),
                    ),
                    title="Schema Retention Days",
                    description=(
                        "Optionally sets the recovery period for dropped managed "
                        "tables in the schema, the period during which dropped tables "
                        "can be recovered using the UNDROP TABLE command. If not "
                        "specified, the schema inherits the recovery period from its "
                        "parent catalog (default 7 days). Set the value to 0 to "
                        "disable recovery, or between 7-30 days, inclusive."
                    ),
                ),
            ),
            title="Schema Creation Parameters",
            description=(
                "Parameters to use for `CREATE SCHEMA` when the target schema does not "
                "already exist in the catalog"
            ),
        ),
    ).to_dict()
