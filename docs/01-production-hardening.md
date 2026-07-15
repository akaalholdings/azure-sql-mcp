# Phase 1: Production Hardening

## Problem

The server currently opens and closes a new SQL connection for every single query. Azure SQL connection setup involves TLS negotiation and (for Entra auth) token packing, adding 200-500ms overhead per tool call. There is no retry logic for transient Azure SQL failures, no tool-level timeout backstop, and no structured logging for production monitoring.

---

## 1A. Connection Pooling

### Files
- **New:** `src/azure_sql_mcp/connection_pool.py`
- **Modify:** `src/azure_sql_mcp/connection.py`
- **Modify:** `src/azure_sql_mcp/config.py`
- **Modify:** `src/azure_sql_mcp/server.py`
- **Modify:** `src/azure_sql_mcp/auth.py`

### Design

Create a `ConnectionPool` class with per-database `asyncio.Queue[mssql_python.Connection]` pools.

```python
class ConnectionPool:
    def __init__(self, config: ServerConfig, authenticator: AzureSqlAuthenticator):
        self._pools: dict[str, asyncio.Queue] = defaultdict(
            lambda: asyncio.Queue(maxsize=config.pool_size)
        )
        self._pool_sizes: dict[str, int] = defaultdict(int)
        self._token_acquired_at: float | None = None

    async def acquire(self, database_name: str) -> mssql_python.Connection:
        # 1. Try idle connection from queue
        # 2. Validate with SELECT 1 (2s timeout)
        # 3. Check token age (<45min for Entra auth)
        # 4. If no valid idle: create new if under capacity
        # 5. If at capacity: await queue.get()

    async def release(self, database_name: str, connection) -> None:
        # Return to queue, or close if queue is full

    async def close_all(self) -> None:
        # Drain all pools on shutdown
```

### Configuration

| Config | Env Var | CLI Arg | Default |
|--------|---------|---------|---------|
| Pool size | `AZURE_SQL_POOL_SIZE` | `--azure-sql-pool-size` | 5 |

### Token Refresh Strategy

Azure AD tokens are valid for ~60-75 minutes. The pool tracks `_token_acquired_at` and proactively refreshes when the token is older than 45 minutes, before handing out connections that would use a stale token.

### Executor Refactor

Change `AzureSqlExecutor._execute_batches_sync` from:
```python
connection = mssql_python.connect(...)
try:
    cursor = connection.cursor()
    cursor.execute(query, params)
    return self._consume_batches(cursor)
finally:
    connection.close()
```

To pool-based acquire/release:
```python
connection = await self.pool.acquire(database_name)
succeeded = False
try:
    result = await asyncio.to_thread(self._execute_with_connection, ...)
    succeeded = True
    return result
finally:
    if succeeded:
        await self.pool.release(database_name, connection)
    else:
        connection.close()  # Don't return bad connections to pool
```

### Shutdown Hook

Add `await self.pool.close_all()` to `AzureSqlMcpApplication.run()` shutdown.

---

## 1B. Retry Logic for Transient Failures

### Files
- **New:** `src/azure_sql_mcp/retry.py`
- **Modify:** `src/azure_sql_mcp/connection.py`
- **Modify:** `src/azure_sql_mcp/config.py`

### Design

Azure SQL frequently returns transient errors that succeed on retry:

```python
TRANSIENT_ERROR_CODES = frozenset({
    40197,  # Service encountered an error processing the request
    40501,  # Service is currently busy
    40613,  # Database not currently available
    49918,  # Cannot process request due to insufficient resources
    49919,  # Cannot process create or update request
    49920,  # Cannot process request due to too many operations
    4221,   # Login failed due to long running transaction
    10928,  # Resource ID limit reached
    10929,  # Resource ID minimum guarantee
    10053,  # Transport-level error
    10054,  # Transport-level error
    233,    # Connection broken
})
```

### Retry Decorator

```python
async def with_retry(
    func: Callable,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 8.0,
    jitter_factor: float = 0.5,
) -> Any:
    for attempt in range(max_retries + 1):
        try:
            return await func()
        except Exception as exc:
            if attempt >= max_retries or not _is_transient(exc):
                raise
            delay = min(base_delay * (2 ** attempt), max_delay)
            delay += random.uniform(0, delay * jitter_factor)
            logger.warning(
                "Transient error, retrying",
                extra={"attempt": attempt + 1, "delay": delay, "error": str(exc)},
            )
            await asyncio.sleep(delay)
```

### Configuration

| Config | Env Var | CLI Arg | Default |
|--------|---------|---------|---------|
| Max retries | `AZURE_SQL_MAX_RETRIES` | `--azure-sql-max-retries` | 3 |

Set to 0 to disable retry.

---

## 1C. Tool-Level Timeout

### Files
- **Modify:** `src/azure_sql_mcp/server.py`
- **Modify:** `src/azure_sql_mcp/config.py`

### Design

The driver `connection.timeout` handles server-side command timeouts, but there is no defense against the entire tool call hanging (pool exhaustion, DNS stall, token refresh hang).

Wrap the callback in `_run_tool()`:

```python
async def _run_tool(self, tool_name, database_name, callback) -> ResponseType:
    resolved_database = self.config.validate_database_name(database_name)
    try:
        payload = await asyncio.wait_for(
            callback(resolved_database),
            timeout=self.config.tool_timeout_seconds,
        )
        return self._format_response(payload)
    except asyncio.TimeoutError:
        return self._format_error(
            "timeout",
            f"Tool '{tool_name}' timed out after {self.config.tool_timeout_seconds}s.",
        )
    except Exception as exc:
        return self._format_error("tool_error", str(exc))
```

### Configuration

| Config | Env Var | CLI Arg | Default |
|--------|---------|---------|---------|
| Tool timeout | `AZURE_SQL_TOOL_TIMEOUT_SECONDS` | `--azure-sql-tool-timeout-seconds` | query_timeout + 15 |

---

## 1D. Structured JSON Logging

### Files
- **New:** `src/azure_sql_mcp/logging_config.py`
- **Modify:** `src/azure_sql_mcp/server.py`
- **Modify:** `src/azure_sql_mcp/config.py`

### Design

Production deployments need JSON-formatted logs for Azure Monitor / Application Insights ingestion.

```python
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Include extra fields from logging calls
        for key in ("tool_name", "database_name", "correlation_id", "duration_ms"):
            if hasattr(record, key):
                log_entry[key] = getattr(record, key)
        return json.dumps(log_entry)
```

### Correlation IDs and Timing

In `_run_tool()`, generate a UUID4 correlation ID and measure duration:

```python
correlation_id = str(uuid.uuid4())
start = time.monotonic()
# ... execute tool ...
duration_ms = (time.monotonic() - start) * 1000
logger.info("Tool completed", extra={
    "tool_name": tool_name,
    "correlation_id": correlation_id,
    "duration_ms": round(duration_ms, 2),
})
```

### Configuration

| Config | Env Var | CLI Arg | Default |
|--------|---------|---------|---------|
| Log format | `AZURE_SQL_LOG_FORMAT` | `--log-format` | `text` |

Values: `text` (current behavior), `json` (structured JSON).

---

## Verification

1. Start server with `uv run azure-sql-mcp --help` and verify new CLI args appear
2. Test connection pooling: run multiple tool calls concurrently, verify pool reuse via `DEBUG` logs
3. Test retry: simulate transient error and verify 3 retry attempts with backoff
4. Test timeout: set `--azure-sql-tool-timeout-seconds 1` and run a slow query
5. Test JSON logging: `--log-format json` and verify structured output
