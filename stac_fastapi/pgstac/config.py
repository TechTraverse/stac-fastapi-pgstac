"""Postgres API configuration."""

from typing import List, Optional, Type
from urllib.parse import quote_plus as quote

from pydantic import BaseModel, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from stac_fastapi.types.config import ApiSettings

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
    """Server runtime parameters."""

    search_path: str = "pgstac,public"
    application_name: str = "pgstac"

    model_config = SettingsConfigDict(extra="allow")


class PostgresSettings(BaseSettings):
    """Postgres-specific API settings.

    Attributes:
        postgres_user: postgres username.
        postgres_pass: postgres password.
        postgres_host_reader: hostname for the reader connection.
        postgres_host_writer: hostname for the writer connection.
        postgres_port: database port.
        postgres_dbname: database name.
        iam_auth_enabled: enable AWS RDS IAM authentication.
        aws_region: AWS region to use for generating IAM token.
        use_api_hydrate: perform hydration of stac items within stac-fastapi.
        invalid_id_chars: list of characters that are not allowed in item or collection ids.
    """

    postgres_user: Optional[str] = None
    postgres_user_writer: Optional[str] = None
    postgres_pass: Optional[str] = None
    postgres_host_reader: Optional[str] = None
    postgres_host_writer: Optional[str] = None
    postgres_port: Optional[int] = 0
    postgres_dbname: Optional[str] = None

    iam_auth_enabled: bool = False
    aws_region: Optional[str] = None

    db_min_conn_size: int = 10
    db_max_conn_size: int = 10
    db_max_queries: int = 50000
    db_max_inactive_conn_lifetime: float = 300

    server_settings: ServerSettings = ServerSettings()

    model_config = {"env_file": ".env", "extra": "ignore"}

    @property
    def reader_connection_string(self):
        """Create reader psql connection string."""
        if self.postgres_pass is None:
            reader_url = f"postgresql://{self.postgres_user}:{self.postgres_pass}@{self.postgres_host_reader}:{self.postgres_port}/{self.postgres_dbname}"
        else:
            reader_url = f"postgresql://{self.postgres_user}:{quote(self.postgres_pass)}@{self.postgres_host_reader}:{self.postgres_port}/{self.postgres_dbname}"
        return reader_url

    @property
    def writer_connection_string(self):
        """Create writer psql connection string."""
        if self.postgres_pass is None:
            writer_url = f"postgresql://{self.postgres_user_writer}:{self.postgres_pass}@{self.postgres_host_writer}:{self.postgres_port}/{self.postgres_dbname}"
        else:
            writer_url = f"postgresql://{self.postgres_user_writer}:{quote(self.postgres_pass)}@{self.postgres_host_writer}:{self.postgres_port}/{self.postgres_dbname}"
        return writer_url

    @property
    def testing_connection_string(self):
        """Create testing psql connection string."""
        if self.postgres_pass is None:
            test_url = f"postgresql://{self.postgres_user}:{self.postgres_pass}@{self.postgres_host_writer}:{self.postgres_port}/pgstactestdb"
        else:
            test_url = f"postgresql://{self.postgres_user}:{quote(self.postgres_pass)}@{self.postgres_host_writer}:{self.postgres_port}/pgstactestdb"
        return test_url


class Settings(ApiSettings):
    use_api_hydrate: bool = False
    invalid_id_chars: List[str] = DEFAULT_INVALID_ID_CHARS
    base_item_cache: Type[BaseItemCache] = DefaultBaseItemCache

    cors_origins: str = "*"
    cors_methods: str = "GET,POST,OPTIONS"

    testing: bool = False

    @field_validator("cors_origins")
    def parse_cors_origin(cls, v):
        """Parse CORS origins."""
        return [origin.strip() for origin in v.split(",")]

    @field_validator("cors_methods")
    def parse_cors_methods(cls, v):
        """Parse CORS methods."""
        return [method.strip() for method in v.split(",")]
