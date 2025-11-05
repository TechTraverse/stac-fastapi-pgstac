"""test config."""

import warnings

import pytest
from pydantic import ValidationError

from stac_fastapi.pgstac.config import PostgresSettings


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
    monkeypatch.setenv("POSTGRES_PORT", "1111")
    monkeypatch.setenv("POSTGRES_DBNAME", "pgstac")
    monkeypatch.setenv("PGHOST", "0.0.0.0")
    with pytest.warns(DeprecationWarning) as record:
        assert PostgresSettings(_env_file=None)
    assert len(record) == 4


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
            pghost="0.0.0.0",
            postgres_port=1111,
            postgres_dbname="pgstac",
            _env_file=None,
        )
        assert settings.pghost == "0.0.0.0"
        assert len(record) == 4


async def test_pg_settings_read_write_split():
    """Test PostgresSettings with read/write split configuration"""
    settings = PostgresSettings(
        pguser="user",
        pgpassword="password",
        pghost="0.0.0.0",
        pghost_reader="read-replica.example.com",
        pghost_writer="primary.example.com",
        pgport=5432,
        pgdatabase="pgstac",
        _env_file=None,
    )
    assert settings.pghost == "0.0.0.0"
    assert settings.pghost_reader == "read-replica.example.com"
    assert settings.pghost_writer == "primary.example.com"
    assert "read-replica.example.com" in settings.reader_connection_string
    assert "primary.example.com" in settings.writer_connection_string


async def test_iam_pool_kwargs_enabled():
    """Test pool kwargs return callable password when IAM enabled"""
    settings = PostgresSettings(
        pguser="user",
        pgpassword="pass",
        pghost="host",
        pgport=5432,
        pgdatabase="db",
        iam_auth_enabled=True,
        _env_file=None,
    )
    reader_kwargs = settings.reader_pool_kwargs
    writer_kwargs = settings.writer_pool_kwargs

    assert callable(reader_kwargs["password"])
    assert reader_kwargs["ssl"] == "require"
    assert callable(writer_kwargs["password"])
    assert writer_kwargs["ssl"] == "require"


async def test_pool_kwargs_without_iam():
    """Test pool kwargs return string password when IAM disabled"""
    settings = PostgresSettings(
        pguser="user",
        pgpassword="pass",
        pghost="host",
        pgport=5432,
        pgdatabase="db",
        iam_auth_enabled=False,
        _env_file=None,
    )
    reader_kwargs = settings.reader_pool_kwargs
    writer_kwargs = settings.writer_pool_kwargs

    assert isinstance(reader_kwargs["password"], str)
    assert "ssl" not in reader_kwargs
    assert isinstance(writer_kwargs["password"], str)
    assert "ssl" not in writer_kwargs
