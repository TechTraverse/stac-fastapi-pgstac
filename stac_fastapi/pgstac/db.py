"""Database connection handling."""

import json
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

from stac_fastapi.pgstac.config import PostgresSettings


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
    app: FastAPI,
    get_conn: Optional[ConnectionGetter] = None,
    postgres_settings: Optional[PostgresSettings] = None,
) -> None:
    """Create connection pools & connection retriever on application."""
    app_settings = app.state.settings

    if not postgres_settings:
        postgres_settings = PostgresSettings()

    if app_settings.testing:
        readpool = writepool = postgres_settings.testing_connection_string
    else:
        readpool = postgres_settings.reader_connection_string
        writepool = postgres_settings.writer_connection_string

    db = DB()
    app.state.readpool = await db.create_pool(readpool, postgres_settings, "read")
    app.state.writepool = await db.create_pool(writepool, postgres_settings, "write")
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

    async def create_pool(self, connection_string: str, settings, mode: str, **kwargs):
        """Create a connection pool."""

        if mode == "read":
            merged_pool_kwargs = {**settings.reader_pool_kwargs, **(kwargs or {})}
        else:
            merged_pool_kwargs = {**settings.writer_pool_kwargs, **(kwargs or {})}

        pool = await asyncpg.create_pool(
            connection_string,
            min_size=settings.db_min_conn_size,
            max_size=settings.db_max_conn_size,
            max_queries=settings.db_max_queries,
            max_inactive_connection_lifetime=settings.db_max_inactive_conn_lifetime,
            init=con_init,
            server_settings=settings.server_settings.model_dump(),
            **merged_pool_kwargs,
        )
        return pool
