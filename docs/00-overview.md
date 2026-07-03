# Azure SQL MCP Server - Improvement Plan Overview

## Goal

Transform the Azure SQL MCP server from a functional implementation into a best-in-class MCP server for Azure SQL Database.

## Current State

The server is a Python 3.12+ FastMCP application providing:
- 63 MCP tools in unrestricted mode; 53 tools in restricted mode
- Restricted (read-only) and unrestricted execution modes
- 4 auth modes (Entra default, service principal, interactive, SQL password)
- 3 transports (STDIO, SSE, Streamable HTTP) with bearer auth required for HTTP/SSE and explicit remote-admin opt-in for apply-capable admin behavior
- SQL validation via sqlglot AST walking
- Database allowlist enforcement
- Structured tool output, prompt/resources support, and token-safe artifact resources
- Audited admin write policy with dry-run defaults, read-only raw SQL apply, and generated Query Store force/unforce workflows

## Feature Gap Analysis

| Feature | Status | Action |
|---------|--------|--------|
| Buffer/cache hit rates | Done | Maintain |
| Connection health | Done | Maintain |
| Constraint health (invalid FKs) | Done | Maintain |
| Index health (duplicates, bloat) | Done | Maintain |
| Replication/geo-rep health | Done | Maintain |
| Sequence/identity exhaustion | Done | Maintain |
| Workload-based index recs | Done | Maintain |
| Query-specific index analysis | Done | Maintain |
| Resource-blended top queries | Done | Maintain |
| Connection pooling | Done | Maintain |
| Retry for transient errors | Done | Maintain |
| SQL validation approach | Hardened | Maintain |
| MCP Resources | Done | Includes artifact resources |
| MCP Prompts | Done | Maintain |
| Schema comparison | Done | Maintain |
| HTTP/SSE bearer auth | Done | Maintain |
| Admin write policy/audit | Done | Maintain |
| Query Store plan enforcement | Done | Review/dry-run/apply |
| Azure SQL diagnostic query parity | Done | Maintain DB-safe DMV coverage |

### Existing Strengths

- Multi-database allowlist support
- Azure AD authentication (Entra ID, service principal, interactive)
- Query Store integration
- Resource pressure monitoring (CPU, IO, memory, DTU)
- Capability probing with graceful degradation
- Tool annotations (readOnlyHint, destructiveHint)
- Configurable row limits with truncation metadata

## Implementation Phases

| Phase | Description | Doc | Priority | Estimated Effort |
|-------|------------|-----|----------|-----------------|
| 1 | Production Hardening | [01-production-hardening.md](01-production-hardening.md) | Critical | 3-5 days |
| 2 | Health Check Parity | [02-health-checks.md](02-health-checks.md) | High | 2-3 days |
| 3 | Index & Query Enhancements | [03-index-query-enhancements.md](03-index-query-enhancements.md) | High | 3-4 days |
| 4 | MCP Protocol Completeness | [04-mcp-protocol.md](04-mcp-protocol.md) | Medium-High | 2-3 days |
| 5 | Schema Comparison | [05-schema-comparison.md](05-schema-comparison.md) | High | 4-6 days |
| 6 | Additional Tools | [06-additional-tools.md](06-additional-tools.md) | Medium | 2-3 days |
| 7 | SQL Validation Hardening | [07-sql-validation.md](07-sql-validation.md) | Medium | 1-2 days |
| 8 | Testing & DevOps | [08-testing-devops.md](08-testing-devops.md) | Medium | 4-6 days |

## Recommended Implementation Order

```
Phase 1 (Production Hardening)
    |
    v
Phase 2 (Health Checks)
    |
    v
Phase 3 (Index & Query) -----> Phase 4 (MCP Protocol) [can run in parallel]
    |                               |
    v                               v
Phase 5 (Schema Comparison)
    |
    v
Phase 6 (Additional Tools)
    |
    v
Phase 7 (SQL Validation) -----> Phase 8 (Testing & DevOps) [can run in parallel]
```

## Architecture After All Phases

```
src/azure_sql_mcp/
  __init__.py
  server.py                  # MCP app, tool/resource/prompt registration
  config.py                  # CLI + env var configuration
  transport_auth.py          # HTTP/SSE bearer token verifier
  admin_policy.py            # write policy, hard denylist, JSONL audit
  artifact_store.py          # token-safe artifact resources
  auth.py                    # Azure credential management
  connection.py              # Query executor (uses pool)
  connection_pool.py         # [NEW] Per-database connection pooling
  retry.py                   # [NEW] Transient failure retry logic
  logging_config.py          # [NEW] JSON structured logging
  safe_sql.py                # SQL validation (enhanced)
  artifacts.py               # Response data structures
  introspection.py           # Schema/object discovery (enhanced)
  plans.py                   # Execution plans (enhanced with what-if)
  query_store.py             # Query Store analysis (enhanced)
  plan_enforcement.py        # Query Store review/dry-run/apply workflow
  health.py                  # Health checks (6 new categories)
  diagnostics.py             # Azure SQL DB diagnostic query parity tools
  index_recommendations.py   # Index analysis (enhanced)
  query_index_analysis.py    # [NEW] Query-specific index analysis
  capabilities.py            # Capability probing
  sessions.py                # [NEW] Active sessions/running queries
  resources.py               # [NEW] MCP Resource templates
  prompts.py                 # [NEW] MCP Prompt templates
  schema_snapshot.py         # [NEW] Schema snapshot model
  schema_diff.py             # [NEW] Schema diff engine
  ddl_generator.py           # [NEW] DDL migration script generation
  schema_compare.py          # [NEW] Schema comparison orchestration
```

## New Tool Surface (after all phases)

| Tool | Phase | Description |
|------|-------|-------------|
| `search_objects` | 6 | Find objects across schemas by name pattern |
| `get_dependencies` | 6 | Bidirectional dependency graph for any object |
| `get_table_stats` | 6 | Row counts and sizes without table scans |
| `get_active_sessions` | 6 | Running queries and session status |
| `analyze_query_indexes` | 3 | Index recommendations for specific queries |
| `analyze_workload_indexes` | 3 | Workload-based index recommendations |
| `compare_schemas` | 5 | Diff two database schemas |
| `generate_migration_script` | 5 | T-SQL DDL from schema diff |
| `capture_schema_snapshot` | 5 | Snapshot a database schema as JSON |

## New MCP Resources (Phase 4)

- `azuresql://{database}/schemas`
- `azuresql://{database}/{schema}/tables`
- `azuresql://{database}/{schema}/{table}`
- `azuresql://{database}/{schema}/views`
- `azuresql://{database}/{schema}/procedures`

## New MCP Prompts (Phase 4)

- `analyze-slow-queries` - Guided slow query investigation
- `review-index-health` - Index health review workflow
- `explore-schema` - Schema exploration walkthrough
- `compare-schemas` - Cross-database schema comparison
- `troubleshoot-performance` - Comprehensive performance troubleshooting
