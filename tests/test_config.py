"""test config."""

import warnings

import pytest
from pydantic import ValidationError
from pytest import MonkeyPatch

from stac_fastapi.pgstac.config import PostgresSettings, Settings


async def test_pg_settings_with_env(monkeypatch):
    """Test PostgresSettings with PG* environment variables"""
    monkeypatch.setenv("PGUSER", "username")
    monkeypatch.setenv("PGPASSWORD", "password")
    monkeypatch.setenv("PGHOST", "0.0.0.0")
    monkeypatch.setenv("PGPORT", "1111")
    monkeypatch.setenv("PGDATABASE", "pgstac")
    assert PostgresSettings(_env_file=None)


async def test_pg_settings_with_env_postgres(monkeypatch):
    """Test PostgresSettings with POSTGRES_* environment variables"""
    monkeypatch.setenv("POSTGRES_USER", "username")
    monkeypatch.setenv("POSTGRES_PASS", "password")
    monkeypatch.setenv("POSTGRES_HOST_READER", "0.0.0.0")
    monkeypatch.setenv("POSTGRES_HOST_WRITER", "0.0.0.0")
    monkeypatch.setenv("POSTGRES_PORT", "1111")
    monkeypatch.setenv("POSTGRES_DBNAME", "pgstac")
    with pytest.warns(DeprecationWarning) as record:
        assert PostgresSettings(_env_file=None)
    assert len(record) == 6


async def test_pg_settings_attributes(monkeypatch):
    """Test PostgresSettings with attributes"""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        settings = PostgresSettings(
            pguser="user",
            pgpassword="password",
            pghost="0.0.0.0",
            pgport=1111,
            pgdatabase="pgstac",
            _env_file=None,
        )
        assert settings.pghost == "0.0.0.0"

    # Compat, should work with old style postgres_ attributes
    # Should raise warnings on set attribute
    with pytest.warns(DeprecationWarning) as record:
        settings = PostgresSettings(
            postgres_user="user",
            postgres_pass="password",
            postgres_host_reader="0.0.0.0",
            postgres_port=1111,
            postgres_dbname="pgstac",
            _env_file=None,
        )
        assert settings.pghost == "0.0.0.0"
        assert len(record) == 5

    # Should raise warning when accessing deprecated attributes
    with pytest.warns(DeprecationWarning):
        assert settings.postgres_host_reader == "0.0.0.0"

    with pytest.raises(ValidationError):
        with pytest.warns(DeprecationWarning) as record:
            PostgresSettings(
                postgres_user="user",
                postgres_pass="password",
                postgres_host_reader="0.0.0.0",
                postgres_host_writer="1.1.1.1",
                postgres_port=1111,
                postgres_dbname="pgstac",
                _env_file=None,
            )


@pytest.mark.parametrize(
    "cors_origins",
    [
        "http://stac-fastapi-pgstac.test,http://stac-fastapi.test",
        '["http://stac-fastapi-pgstac.test","http://stac-fastapi.test"]',
    ],
)
def test_cors_origins(monkeypatch: MonkeyPatch, cors_origins: str) -> None:
    monkeypatch.setenv(
        "CORS_ORIGINS",
        cors_origins,
    )
    settings = Settings()
    assert settings.cors_origins == [
        "http://stac-fastapi-pgstac.test",
        "http://stac-fastapi.test",
    ]


@pytest.mark.parametrize(
    "cors_methods",
    [
        "GET,POST",
        '["GET","POST"]',
    ],
)
def test_cors_methods(monkeypatch: MonkeyPatch, cors_methods: str) -> None:
    monkeypatch.setenv(
        "CORS_METHODS",
        cors_methods,
    )
    settings = Settings()
    assert settings.cors_methods == [
        "GET",
        "POST",
    ]


@pytest.mark.parametrize(
    "cors_headers",
    [
        "Content-Type,X-Foo",
        '["Content-Type","X-Foo"]',
    ],
)
def test_cors_headers(monkeypatch: MonkeyPatch, cors_headers: str) -> None:
    monkeypatch.setenv(
        "CORS_HEADERS",
        cors_headers,
    )
    settings = Settings()
    assert settings.cors_headers == [
        "Content-Type",
        "X-Foo",
    ]


def test_postgres_settings_iam_auth_with_region():
    """Test PostgresSettings with IAM auth enabled and region specified."""
    settings = PostgresSettings(
        pguser="user",
        pghost="db.example.com",
        pgport=5432,
        pgdatabase="pgstac",
        use_iam_auth=True,
        aws_region="us-east-1",
        _env_file=None,
    )
    assert settings.use_iam_auth is True
    assert settings.aws_region == "us-east-1"
    assert settings.pgpassword is None


def test_postgres_settings_iam_auth_without_region():
    """Test PostgresSettings with IAM auth enabled but no region (uses boto3 default)."""
    settings = PostgresSettings(
        pguser="user",
        pghost="db.example.com",
        pgport=5432,
        pgdatabase="pgstac",
        use_iam_auth=True,
        aws_region=None,
        _env_file=None,
    )
    assert settings.use_iam_auth is True
    assert settings.aws_region is None


def test_postgres_settings_iam_auth_requires_password_or_iam():
    """Test that either password or IAM auth must be configured."""
    # Should fail without password and without IAM auth
    with pytest.raises(ValidationError):
        PostgresSettings(
            pguser="user",
            pghost="db.example.com",
            pgport=5432,
            pgdatabase="pgstac",
            pgpassword=None,
            use_iam_auth=False,
            _env_file=None,
        )


def test_postgres_settings_connection_string_with_iam_auth():
    """Test that connection_string raises error when IAM auth is enabled."""
    settings = PostgresSettings(
        pguser="user",
        pghost="db.example.com",
        pgport=5432,
        pgdatabase="pgstac",
        use_iam_auth=True,
        aws_region="us-east-1",
        _env_file=None,
    )
    with pytest.raises(ValueError, match="Cannot use connection_string when IAM"):
        _ = settings.connection_string


def test_postgres_settings_iam_auth_with_password_still_works():
    """Test that IAM auth can be enabled even if password is set (password is ignored)."""
    settings = PostgresSettings(
        pguser="user",
        pgpassword="password",
        pghost="db.example.com",
        pgport=5432,
        pgdatabase="pgstac",
        use_iam_auth=True,
        aws_region="us-east-1",
        _env_file=None,
    )
    assert settings.use_iam_auth is True
    assert settings.aws_region == "us-east-1"
    # Password can still be set but won't be used
    assert settings.pgpassword == "password"
