
### Application Extension

The default `stac-fastapi-pgstac` application comes will **all** extensions enabled (except transaction). Users can use `ENABLED_EXTENSIONS` environment variable to limit the supported extensions.

Available values for `ENABLED_EXTENSIONS`:

- `query`
- `sort`
- `fields`
- `filter`
- `free_text` (only for collection-search)
- `pagination`
- `collection_search`

Example: `ENABLED_EXTENSIONS="pagination,sort"`

Since `6.0.0`, the transaction extension is not enabled by default. To add the transaction endpoints, users can set `ENABLE_TRANSACTIONS_EXTENSIONS=TRUE/YES/1`.

### Database config

- `PGUSER`: postgres username
- `PGPASSWORD`: postgres password
- `PGHOST`: hostname for the connection
- `PGPORT`: database port
- `PGDATABASE`: database name
- `PGHOST_READER`: Optional hostname for read replica connection
- `PGHOST_WRITER`: Optional hostname for writer/primary database connection
- `POSTGRES_USER_WRITER`: Optional separate username for writer connection
- `IAM_AUTH_ENABLED`: Enable AWS RDS IAM authentication. Defaults to `False`
- `AWS_REGION`: AWS region for IAM token generation (optional, uses boto3 default if not set)
- `DB_MIN_CONN_SIZE`: Number of connection the pool will be initialized with. Defaults to `1`
- `DB_MAX_CONN_SIZE` Max number of connections in the pool. Defaults to `10`
- `DB_MAX_QUERIES`: Number of queries after a connection is closed and replaced with a new connection. Defaults to `50000`
- `DB_MAX_INACTIVE_CONN_LIFETIME`: Number of seconds after which inactive connections in the pool will be closed. Defaults to `300`
- `SEARCH_PATH`: Postgres search path. Defaults to `"pgstac,public"`
- `APPLICATION_NAME`: PgSTAC Application name. Defaults to `"pgstac"`

#### Read/Write Split Configuration

The application supports optional read/write split for improved performance and scalability.

**Single Database Mode (default):**
```bash
export PGHOST=database.example.com
# Both read and write operations use the same host
```

**Read/Write Split Mode:**
```bash
export PGHOST=database.example.com         # Required - acts as fallback
export PGHOST_READER=read-replica.example.com
export PGHOST_WRITER=primary.example.com
# Read operations (search, list) use PGHOST_READER
# Write operations (create, update, delete) use PGHOST_WRITER
```

**Notes:**
- `PGHOST` is always required, even when using read/write split
- If `PGHOST_READER` is not set, reads fall back to `PGHOST`
- If `PGHOST_WRITER` is not set, writes fall back to `PGHOST`
- Use `POSTGRES_USER_WRITER` if your writer requires a different username

##### Deprecated

In version `6.0.0` we've renamed the PG configuration variable to match the official naming convention:

- `POSTGRES_USER` -> `PGUSER`
- `POSTGRES_PASS` -> `PGPASSWORD`
- `POSTGRES_PORT` -> `PGPORT`
- `POSTGRES_DBNAME` -> `PGDATABASE`

### Validation/Serialization

- `ENABLE_RESPONSE_MODELS`: use pydantic models to validate endpoint responses. Defaults to `False`
- `ENABLE_DIRECT_RESPONSE`: by-pass the default FastAPI serialization by wrapping the endpoint responses into `starlette.Response` classes. Defaults to `False`

### Misc

- `STAC_FASTAPI_VERSION` (string) is the version number of your API instance (this is not the STAC version)
- `STAC FASTAPI_TITLE` (string) should be a self-explanatory title for your API
- `STAC FASTAPI_DESCRIPTION` (string) should be a good description for your API. It can contain CommonMark
- `STAC_FASTAPI_LANDING_ID` (string) is a unique identifier for your Landing page
- `ROOT_PATH`: set application root-path (when using proxy)
- `CORS_ORIGINS`: A list of origins that should be permitted to make cross-origin requests. Defaults to `*`
- `CORS_ORIGIN_REGEX`: A regex string to match against origins that should be permitted to make cross-origin requests. eg. 'https://.*\.example\.org'.
- `CORS_METHODS`: A list of HTTP methods that should be allowed for cross-origin requests. Defaults to `"GET,POST,OPTIONS"`
- `CORS_CREDENTIALS`: Set to `true` to enable credentials via CORS requests. Note that you'll need to set `CORS_ORIGINS` to something other than `*`, because credentials are [disallowed](https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CORS/Errors/CORSNotSupportingCredentials) for wildcard CORS origins.
- `CORS_HEADERS`: If `CORS_CREDENTIALS` are true and you're using an `Authorization` header, set this to `Content-Type,Authorization`. Alternatively, you can allow all headers by setting this to `*`.
- `USE_API_HYDRATE`: perform hydration of stac items within stac-fastapi
- `INVALID_ID_CHARS`: list of characters that are not allowed in item or collection ids (used in Transaction endpoints)
- `PREFIX_PATH`: An optional path prefix for the underlying FastAPI router.
