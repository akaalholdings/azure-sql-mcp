#!/usr/bin/env bash
# Disposable Azure SQL Database for live testing of azure-sql-mcp.
#
#   scripts/azure_test_db.sh up     create it (idempotent) and write the env file
#   scripts/azure_test_db.sh ip     allow this machine's current public IP
#   scripts/azure_test_db.sh status show the server, database and firewall rules
#   scripts/azure_test_db.sh down   delete the whole resource group (asks first)
#
# The database uses the Azure SQL free offer (serverless GP Gen5 2 vCores) and
# pauses itself when the monthly free allowance is used, so it costs nothing.
# The server is Entra-only with the signed-in az user as admin: no password
# exists. Query Store captures everything in 1-minute intervals and
# LAST_QUERY_PLAN_STATS is on, so live tests see their runs at once.
#
# Needs: az CLI signed in (az login), curl and uv. It only ever acts on a
# resource group tagged purpose=azure-sql-mcp-testing (the one it creates).
# Settings (optional): AZURE_TEST_DB_RG, AZURE_TEST_DB_LOCATION,
# AZURE_TEST_DB_NAME, AZURE_TEST_DB_ENV_FILE.
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
rg="${AZURE_TEST_DB_RG:-rg-azsqlmcp-test}"
location="${AZURE_TEST_DB_LOCATION:-uksouth}"
database="${AZURE_TEST_DB_NAME:-mcptest}"
env_file="${AZURE_TEST_DB_ENV_FILE:-$HOME/.config/azure-sql-mcp/test.env}"
policy_file="$(dirname "$env_file")/test-policy.json"

tag="azure-sql-mcp-testing"

# macOS ships bash 3.2 (no inherit_errexit): every az capture checks its own
# status, and find_server sets a variable instead of returning through $(...).
rg_exists() {
  local out
  out="$(az group exists -n "$rg")" || exit 1
  [[ "$out" == "true" ]]
}

require_own_rg() {
  # Never act on a resource group this script did not create.
  local purpose
  purpose="$(az group show -n "$rg" --query tags.purpose -o tsv)" || exit 1
  if [[ "$purpose" != "$tag" ]]; then
    echo "Refusing: resource group $rg is not tagged purpose=$tag." >&2
    exit 1
  fi
}

find_server() {
  # Sets server to the test server name, or empty when there is none yet.
  server=""
  rg_exists || return 0
  local names count
  names="$(az sql server list -g "$rg" --query "[?starts_with(name, 'azsqlmcp-test-')].name" -o tsv)" || exit 1
  count="$(printf '%s\n' "$names" | grep -c . || true)"
  if [[ "$count" -gt 1 ]]; then
    echo "Refusing: more than one azsqlmcp-test- server in $rg." >&2
    exit 1
  fi
  server="$names"
}

allow_ip() {
  local server="$1" ip
  ip="$(curl -fsS -m 10 https://api.ipify.org)"
  az sql server firewall-rule create -g "$rg" -s "$server" \
    -n "client-${ip//./-}" --start-ip-address "$ip" --end-ip-address "$ip" -o none
  echo "Allowed client IP $ip on $server."
}

write_env() {
  local server="$1"
  mkdir -p "$(dirname "$env_file")"
  umask 077
  cat >"$env_file" <<EOF
# Written by scripts/azure_test_db.sh. No secrets: Entra auth via az login.
AZURE_SQL_SERVER=$server.database.windows.net
AZURE_SQL_DEFAULT_DATABASE=$database
AZURE_SQL_ALLOWED_DATABASES=$database,master
AZURE_SQL_AUTH_MODE=entra-default
AZURE_SQL_DATABASE_POLICY_FILE=$policy_file
EOF
  # Without a policy the MCP refuses every read. This is a disposable test
  # database, so the sandbox experiments (benchmarks, test indexes, plan and
  # view apply) are allowed too; master is read-only (long resource history).
  cat >"$policy_file" <<EOF
{"version": 1, "databases": {
  "$database": {"environment": "development", "allow_read": true, "allow_benchmark": true,
    "allow_test_indexes": true, "allow_view_apply": true, "allow_plan_apply": true,
    "allow_index_history_write": true, "business_cycle_extension_days": 0,
    "max_benchmark_executions": 500, "max_tuning_candidates": 10,
    "max_tuning_session_executions": 500, "max_tuning_session_minutes": 60},
  "master": {"environment": "development", "allow_read": true}}}
EOF
  echo "Wrote $env_file and $policy_file"
}

configure_database() {
  # Query Store and plan stats for live tests, through the MCP's own executor.
  # python -c, not a stdin heredoc: the OpenSSL re-exec must be able to rerun it.
  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a
  AZURE_SQL_ACCESS_MODE=unrestricted AZURE_SQL_WRITE_POLICY=apply AZURE_SQL_TOOL_GROUPS=all \
    uv run --quiet --project "$repo" python -c "$(cat <<'PY'
import asyncio, logging, sys
logging.disable(logging.CRITICAL)
from azure_sql_mcp.driver_openssl import ensure_driver_openssl
ensure_driver_openssl()
from azure_sql_mcp.config import load_server_config
from azure_sql_mcp.connection import BatchExecutionMode
from azure_sql_mcp.server import AzureSqlMcpApplication

async def main(database: str) -> None:
    app = AzureSqlMcpApplication(load_server_config([]))
    # A paused free-offer database answers 40613 while it resumes; a normal
    # read retries that, the admin batch below does not.
    await app.executor.fetch_all(database, "SELECT 1 AS awake")
    await app.executor.execute_batches(
        database,
        "ALTER DATABASE CURRENT SET QUERY_STORE = ON;"
        "ALTER DATABASE CURRENT SET QUERY_STORE (OPERATION_MODE = READ_WRITE, "
        "QUERY_CAPTURE_MODE = ALL, INTERVAL_LENGTH_MINUTES = 1, "
        "DATA_FLUSH_INTERVAL_SECONDS = 60, MAX_STORAGE_SIZE_MB = 1024);"
        "ALTER DATABASE SCOPED CONFIGURATION SET LAST_QUERY_PLAN_STATS = ON;",
        execution_mode=BatchExecutionMode.ADMIN,
    )
    print("Query Store: ALL, 1-minute intervals; LAST_QUERY_PLAN_STATS on.")

asyncio.run(main(sys.argv[1]))
PY
)" "$database"
}

case "${1:-}" in
  up)
    user_id="$(az ad signed-in-user show --query id -o tsv)"
    user_name="$(az ad signed-in-user show --query userPrincipalName -o tsv)"
    if rg_exists; then
      require_own_rg
    else
      az group create -n "$rg" -l "$location" --tags "purpose=$tag" -o none
    fi
    find_server
    if [[ -z "$server" ]]; then
      server="azsqlmcp-test-$(openssl rand -hex 3)"
      az sql server create -g "$rg" -n "$server" -l "$location" --enable-ad-only-auth \
        --external-admin-principal-type User --external-admin-name "$user_name" \
        --external-admin-sid "$user_id" -o none
      echo "Created server $server."
    fi
    allow_ip "$server"
    if ! az sql db show -g "$rg" -s "$server" -n "$database" -o none 2>/dev/null; then
      az sql db create -g "$rg" -s "$server" -n "$database" -e GeneralPurpose -f Gen5 -c 2 \
        --compute-model Serverless --use-free-limit --free-limit-exhaustion-behavior AutoPause \
        --sample-name AdventureWorksLT --backup-storage-redundancy Local -o none
      echo "Created free-offer database $database."
    fi
    write_env "$server"
    configure_database
    ;;
  ip)
    find_server
    [[ -n "$server" ]] || { echo "No test server in $rg; run: $0 up" >&2; exit 1; }
    allow_ip "$server"
    ;;
  status)
    find_server
    [[ -n "$server" ]] || { echo "No test server in $rg."; exit 0; }
    az sql db show -g "$rg" -s "$server" -n "$database" \
      --query '{server:'"'$server'"',database:name,status:status,sku:currentSku.name,freeLimit:useFreeLimit}' -o table
    az sql server firewall-rule list -g "$rg" -s "$server" --query '[].{rule:name,ip:startIpAddress}' -o table
    ;;
  down)
    rg_exists || { echo "No resource group $rg."; exit 0; }
    require_own_rg
    if [[ "${2:-}" != "--yes" ]]; then
      read -r -p "Delete resource group $rg and everything in it? [y/N] " answer
      [[ "$answer" == "y" || "$answer" == "Y" ]] || { echo "Kept."; exit 0; }
    fi
    az group delete -n "$rg" --yes
    rm -f "$env_file" "$policy_file"
    echo "Deleted $rg."
    ;;
  *)
    sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
