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

import orjson
from asyncpg import Connection, Pool, exceptions
from buildpg import V, asyncpg, render
from fastapi import FastAPI, HTTPException, Request
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


async def _create_pool(
    settings: PostgresSettings,
    connection_string: Optional[str] = None,
    pool_kwargs: Optional[Dict] = None,
) -> Pool:
    """Create a connection pool."""
    conn_str = connection_string or settings.connection_string
    kwargs = pool_kwargs or {}
    
    return await asyncpg.create_pool(
        conn_str,
        min_size=settings.db_min_conn_size,
        max_size=settings.db_max_conn_size,
        max_queries=settings.db_max_queries,
        max_inactive_connection_lifetime=settings.db_max_inactive_conn_lifetime,
        init=con_init,
        server_settings=settings.server_settings.model_dump(),
        **kwargs,
    )


async def connect_to_db(
    app: FastAPI,
    get_conn: Optional[ConnectionGetter] = None,
    postgres_settings: Optional[PostgresSettings] = None,
    add_write_connection_pool: bool = False,
    write_postgres_settings: Optional[PostgresSettings] = None,
) -> None:
    """Create connection pools & connection retriever on application."""
    if not postgres_settings:
        postgres_settings = PostgresSettings()

    # Create read pool with reader connection string and pool kwargs
    app.state.readpool = await _create_pool(
        postgres_settings,
        connection_string=postgres_settings.reader_connection_string,
        pool_kwargs=postgres_settings.reader_pool_kwargs,
    )

    if add_write_connection_pool:
        if not write_postgres_settings:
            write_postgres_settings = postgres_settings

        # Create write pool with writer connection string and pool kwargs
        app.state.writepool = await _create_pool(
            write_postgres_settings,
            connection_string=write_postgres_settings.writer_connection_string,
            pool_kwargs=write_postgres_settings.writer_pool_kwargs,
        )

    app.state.get_connection = get_conn if get_conn else get_connection


async def close_db_connection(app: FastAPI) -> None:
    """Close connection."""
    await app.state.readpool.close()
    if pool := getattr(app.state, "writepool", None):
        await pool.close()


@asynccontextmanager
async def get_connection(
    request: Request,
    readwrite: Literal["r", "w"] = "r",
) -> AsyncIterator[Connection]:
    """Retrieve connection from database conection pool."""
    pool = request.app.state.readpool
    if readwrite == "w":
        pool = getattr(request.app.state, "writepool", None)
        if not pool:
            raise HTTPException(
                status_code=500,
                detail="Could not find connection pool for write operations",
            )

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
