"""Postgres API configuration."""

import warnings
from typing import Annotated, Any, Dict, List, Optional, Sequence, Type
from urllib.parse import quote_plus as quote

import boto3
from pydantic import BaseModel, BeforeValidator, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from stac_fastapi.types.config import ApiSettings
from typing_extensions import Self

from stac_fastapi.pgstac.types.base_item_cache import (
    BaseItemCache,
    DefaultBaseItemCache,
)

DEFAULT_INVALID_ID_CHARS = [
    ":",
    "/",
    "?",
    "#",
    "[",
    "]",
    "@",
    "!",
    "$",
    "&",
    "'",
    "(",
    ")",
    "*",
    "+",
    ",",
    ";",
    "=",
]


class ServerSettings(BaseModel):
    """Server runtime parameters.

    Attributes:
        search_path: Postgres search path. Defaults to "pgstac,public".
        application_name: PgSTAC Application name. Defaults to 'pgstac'.
    """

    search_path: str = "pgstac,public"
    application_name: str = "pgstac"

    model_config = SettingsConfigDict(extra="allow")


class PostgresSettings(BaseSettings):
    """Postgres connection settings.

    Attributes:
        pguser: postgres username.
        pgpassword: postgres password.
        pghost: hostname for the connection (required, acts as fallback).
        pgport: database port.
        pgdatabase: database name.
        pghost_reader: hostname for read replica connection (optional, falls back to pghost).
        pghost_writer: hostname for writer/primary connection (optional, falls back to pghost).
        postgres_user_writer: separate username for writer if different from reader.
        iam_auth_enabled: enable AWS RDS IAM authentication.
        aws_region: AWS region to use for generating IAM token.

    Examples:
        Single database mode (no read/write split):
            settings = PostgresSettings(
                pguser="user",
                pgpassword="pass",
                pghost="database.example.com",
                pgport=5432,
                pgdatabase="pgstac"
            )

        Read/write split mode:
            settings = PostgresSettings(
                pguser="user",
                pgpassword="pass",
                pghost="database.example.com",  # fallback
                pghost_reader="read-replica.example.com",
                pghost_writer="primary.example.com",
                pgport=5432,
                pgdatabase="pgstac"
            )

    """

    postgres_user: Annotated[
        Optional[str],
        Field(
            deprecated="`postgres_user` is deprecated, please use `pguser`", default=None
        ),
    ]
    postgres_pass: Annotated[
        Optional[str],
        Field(
            deprecated="`postgres_pass` is deprecated, please use `pgpassword`",
            default=None,
        ),
    ]
    postgres_port: Annotated[
        Optional[int],
        Field(
            deprecated="`postgres_port` is deprecated, please use `pgport`", default=None
        ),
    ]
    postgres_dbname: Annotated[
        Optional[str],
        Field(
            deprecated="`postgres_dbname` is deprecated, please use `pgdatabase`",
            default=None,
        ),
    ]

    pguser: str
    pgpassword: str
    pghost: str
    pgport: int
    pgdatabase: str

    # Read/write split configuration
    pghost_reader: Optional[str] = None
    pghost_writer: Optional[str] = None
    postgres_user_writer: Optional[str] = None

    # IAM authentication
    iam_auth_enabled: bool = False
    aws_region: Optional[str] = None

    db_min_conn_size: int = 1
    db_max_conn_size: int = 10
    db_max_queries: int = 50000
    db_max_inactive_conn_lifetime: float = 300

    server_settings: ServerSettings = ServerSettings()

    model_config = {"env_file": ".env", "extra": "ignore"}

    @model_validator(mode="before")
    @classmethod
    def _pg_settings_compat(cls, data: Any) -> Any:
        if isinstance(data, dict):
            compat = {
                "postgres_user": "pguser",
                "postgres_pass": "pgpassword",
                "postgres_port": "pgport",
                "postgres_dbname": "pgdatabase",
            }
            for old_key, new_key in compat.items():
                if val := data.get(old_key, None):
                    warnings.warn(
                        f"`{old_key}` is deprecated, please use `{new_key}`",
                        DeprecationWarning,
                        stacklevel=1,
                    )
                    data[new_key] = val

        return data

    def get_rds_reader_token(self) -> str:
        """Generate an RDS IAM token for authentication for reader instance."""
        rds_client = boto3.client("rds")
        # Use pghost_reader if set, otherwise fall back to pghost
        reader_host = self.pghost_reader or self.pghost
        reader_token = rds_client.generate_db_auth_token(
            DBHostname=reader_host,
            Port=self.pgport,
            DBUsername=self.pguser,
            Region=self.aws_region or rds_client.meta.region_name,
        )
        return reader_token

    def get_rds_writer_token(self) -> str:
        """Generate an RDS IAM token for authentication for writer instance."""
        rds_client = boto3.client("rds")
        # Use pghost_writer if set, otherwise fall back to pghost
        writer_host = self.pghost_writer or self.pghost
        writer_user = self.postgres_user_writer or self.pguser
        writer_token = rds_client.generate_db_auth_token(
            DBHostname=writer_host,
            Port=self.pgport,
            DBUsername=writer_user,
            Region=self.aws_region or rds_client.meta.region_name,
        )
        return writer_token

    @property
    def reader_pool_kwargs(self) -> Dict[str, Any]:
        """
        Build the default connection parameters for the reader pool.

        If IAM auth is enabled, use a dynamic password callable (bound to get_rds_token).
        Otherwise, use a static password if provided.
        """
        reader_kwargs: Dict[str, Any] = {}
        if self.iam_auth_enabled:
            reader_kwargs["password"] = self.get_rds_reader_token
            reader_kwargs["ssl"] = "require"
        elif self.pgpassword:
            reader_kwargs["password"] = self.pgpassword
        return reader_kwargs

    @property
    def writer_pool_kwargs(self) -> Dict[str, Any]:
        """
        Build the default connection parameters for the writer pool.

        If IAM auth is enabled, use a dynamic password callable (bound to get_rds_token).
        Otherwise, use a static password if provided.
        """
        writer_kwargs: Dict[str, Any] = {}
        if self.iam_auth_enabled:
            writer_kwargs["password"] = self.get_rds_writer_token
            writer_kwargs["ssl"] = "require"
        elif self.pgpassword:
            writer_kwargs["password"] = self.pgpassword
        return writer_kwargs

    @property
    def connection_string(self):
        """Create psql connection string."""
        return f"postgresql://{self.pguser}:{quote(self.pgpassword)}@{self.pghost}:{self.pgport}/{self.pgdatabase}"

    @property
    def reader_connection_string(self):
        """Create reader psql connection string (uses reader host if set, otherwise pghost)."""
        reader_host = self.pghost_reader or self.pghost
        return f"postgresql://{self.pguser}:{quote(self.pgpassword)}@{reader_host}:{self.pgport}/{self.pgdatabase}"

    @property
    def writer_connection_string(self):
        """Create writer psql connection string (uses writer host if set, otherwise pghost)."""
        writer_host = self.pghost_writer or self.pghost
        writer_user = self.postgres_user_writer or self.pguser
        return f"postgresql://{writer_user}:{quote(self.pgpassword)}@{writer_host}:{self.pgport}/{self.pgdatabase}"

    @property
    def testing_connection_string(self):
        """Create testing psql connection string."""
        return f"postgresql://{self.pguser}:{quote(self.pgpassword)}@{self.pghost}:{self.pgport}/pgstactestdb"


def str_to_list(value: Any) -> Any:
    if isinstance(value, str):
        return [v.strip() for v in value.split(",")]
    return value


class Settings(ApiSettings):
    """API settings.

    Attributes:
        prefix_path: An optional path prefix for the underyling FastAPI router.
        use_api_hydrate: perform hydration of stac items within stac-fastapi.
        invalid_id_chars: list of characters that are not allowed in item or collection ids.

    """

    prefix_path: str = ""
    use_api_hydrate: bool = False
    """
    When USE_API_HYDRATE=TRUE, PgSTAC database will receive `NO_HYDRATE=TRUE`

    | use_api_hydrate | nohydrate | Hydration |
    |             --- |       --- |       --- |
    |           False |     False |    PgSTAC |
    |            True |      True |       API |

    ref: https://stac-utils.github.io/pgstac/pgstac/#runtime-configurations
    """
    exclude_hydrate_markers: bool = True
    """
    In some case, PgSTAC can return `DO_NOT_MERGE_MARKER` markers (`𒍟※`).
    If `EXCLUDE_HYDRATE_MARKERS=TRUE` and `USE_API_HYDRATE=TRUE`, stac-fastapi-pgstac
    will exclude those values from the responses.
    """

    invalid_id_chars: List[str] = DEFAULT_INVALID_ID_CHARS
    base_item_cache: Type[BaseItemCache] = DefaultBaseItemCache

    validate_extensions: bool = False
    """
    Validate `stac_extensions` schemas against submitted data when creating or updated STAC objects.

    Implies that the `Transactions` extension is enabled.
    """

    cors_origins: Annotated[Sequence[str], BeforeValidator(str_to_list)] = ("*",)
    cors_origin_regex: Optional[str] = None
    cors_methods: Annotated[Sequence[str], BeforeValidator(str_to_list)] = (
        "GET",
        "POST",
        "OPTIONS",
    )
    cors_credentials: bool = False
    cors_headers: Annotated[Sequence[str], BeforeValidator(str_to_list)] = (
        "Content-Type",
    )

    testing: bool = False

    @model_validator(mode="after")
    def check_origins(self) -> Self:
        if self.cors_origin_regex and "*" in self.cors_origins:
            raise ValueError(
                "Conflicting options found in API settings: `cors_origin_regex` and `*` in `cors_origins`"
            )

        return self
