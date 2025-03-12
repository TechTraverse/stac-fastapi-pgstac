"""Database connection handling."""

import functools
import json
import os
from contextlib import asynccontextmanager, contextmanager
from typing import (
    AsyncIterator,
    Callable,
    Dict,
    Generator,
    List,
    Literal,
    Optional,
    Union,
)

import attr
import boto3
import orjson
from asyncpg import Connection, exceptions
from buildpg import V, asyncpg, render
from fastapi import FastAPI, Request
from stac_fastapi.types.errors import (
    ConflictError,
    DatabaseError,
    ForeignKeyError,
    NotFoundError,
)


def get_rds_token(
    host: Union[str, None],
    port: Union[int, None],
    user: Union[str, None],
    region: Union[str, None],
) -> str:
    """Get RDS token for IAM auth"""
    print(
        f"Retrieving RDS IAM token with host: {host}, port: {port}, user: {user}, region: {region}"
    )
    rds_client = boto3.client("rds")
    token = rds_client.generate_db_auth_token(
        DBHostname=host,
        Port=port,
        DBUsername=user,
        Region=region or rds_client.meta.region_name,
    )
    return token


async def con_init(conn):
    """Use orjson for json returns."""
    await conn.set_type_codec(
        "json",
        encoder=orjson.dumps,
        decoder=orjson.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "jsonb",
        encoder=orjson.dumps,
        decoder=orjson.loads,
        schema="pg_catalog",
    )


ConnectionGetter = Callable[[Request, Literal["r", "w"]], AsyncIterator[Connection]]


async def connect_to_db(
    app: FastAPI, get_conn: Optional[ConnectionGetter] = None
) -> None:
    """Create connection pools & connection retriever on application."""
    settings = app.state.settings
    if app.state.settings.testing:
        readpool = writepool = settings.testing_connection_string
    else:
        readpool = settings.reader_connection_string
        writepool = settings.writer_connection_string

    db = DB()
    app.state.readpool = await db.create_pool(readpool, settings)
    app.state.writepool = await db.create_pool(writepool, settings)
    app.state.get_connection = get_conn if get_conn else get_connection


async def close_db_connection(app: FastAPI) -> None:
    """Close connection."""
    await app.state.readpool.close()
    await app.state.writepool.close()


@asynccontextmanager
async def get_connection(
    request: Request,
    readwrite: Literal["r", "w"] = "r",
) -> AsyncIterator[Connection]:
    """Retrieve connection from database conection pool."""
    pool = request.app.state.writepool if readwrite == "w" else request.app.state.readpool
    with translate_pgstac_errors():
        async with pool.acquire() as conn:
            yield conn


async def dbfunc(conn: Connection, func: str, arg: Union[str, Dict, List]):
    """Wrap PLPGSQL Functions.

    Keyword arguments:
    pool -- the asyncpg pool to use to connect to the database
    func -- the name of the PostgreSQL function to call
    arg -- the argument to the PostgreSQL function as either a string
    or a dict that will be converted into jsonb
    """
    with translate_pgstac_errors():
        if isinstance(arg, str):
            q, p = render(
                """
                SELECT * FROM :func(:item::text);
                """,
                func=V(func),
                item=arg,
            )
            return await conn.fetchval(q, *p)
        else:
            q, p = render(
                """
                SELECT * FROM :func(:item::text::jsonb);
                """,
                func=V(func),
                item=json.dumps(arg),
            )
            return await conn.fetchval(q, *p)


@contextmanager
def translate_pgstac_errors() -> Generator[None, None, None]:
    """Context manager that translates pgstac errors into FastAPI errors."""
    try:
        yield
    except exceptions.UniqueViolationError as e:
        raise ConflictError from e
    except exceptions.NoDataFoundError as e:
        raise NotFoundError from e
    except exceptions.NotNullViolationError as e:
        raise DatabaseError from e
    except exceptions.ForeignKeyViolationError as e:
        raise ForeignKeyError from e


@attr.s
class DB:
    """DB class that can be used with context manager."""

    connection_string = attr.ib(default=None)
    _pool = attr.ib(default=None)
    _connection = attr.ib(default=None)

    async def create_pool(self, connection_string: str, settings, **kwargs):
        """Create a connection pool."""
        print("connection string")

        if os.environ.get("IAM_AUTH_ENABLED") == "TRUE":
            print("iam auth")
            if "eo_readonly" in connection_string:
                print("read only")
                host = settings.postgres_host_reader
                user = settings.postgres_user
            else:
                print("readwrite")
                host = settings.postgres_host_writer
                user = settings.postgres_user_writer
            kwargs["password"] = functools.partial(
                get_rds_token,
                host,
                settings.postgres_port,
                user,
                settings.aws_region,
            )
            kwargs["ssl"] = "require"

        pool = await asyncpg.create_pool_b(
            connection_string,
            min_size=settings.db_min_conn_size,
            max_size=settings.db_max_conn_size,
            max_queries=settings.db_max_queries,
            max_inactive_connection_lifetime=settings.db_max_inactive_conn_lifetime,
            init=con_init,
            server_settings=settings.server_settings.model_dump(),
            **kwargs,
        )
        return pool
