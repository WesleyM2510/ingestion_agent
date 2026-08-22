#!/usr/bin/env bash
#
# Post-deploy hook: register the deployed MCP app as a Unity Catalog HTTP
# connection and an MCP Service under cielo.default.
#
# DABs cannot declare UC connections or MCP Services as bundle resources
# (they are not supported resource types), and the connection must point at
# the app's URL, which only exists AFTER `bundle deploy`. So this runs as an
# `experimental.scripts.postdeploy` hook once the app is up.
#
# Idempotent: creates on first run, updates the host on subsequent runs.
#
# Env (set by the bundle / caller):
#   DATABRICKS_PROFILE      profile to use (default: e2-demo-field-eng)
#   MCP_CATALOG             catalog for the MCP Service   (default: cielo)
#   MCP_SCHEMA              schema  for the MCP Service   (default: default)
#   CONNECTION_NAME         flat UC connection name       (default: salesforce_lakeflow_mcp_conn)
#   MCP_SERVICE_ID          MCP Service id                (default: salesforce_lakeflow_mcp)
#   APP_NAME                deployed app name             (default: mcp-salesforce-lakeflow)
set -euo pipefail

PROFILE="${DATABRICKS_PROFILE:-e2-demo-field-eng}"
MCP_CATALOG="${MCP_CATALOG:-cielo}"
MCP_SCHEMA="${MCP_SCHEMA:-default}"
CONNECTION_NAME="${CONNECTION_NAME:-salesforce_lakeflow_mcp_conn}"
MCP_SERVICE_ID="${MCP_SERVICE_ID:-salesforce_lakeflow_mcp}"
APP_NAME="${APP_NAME:-mcp-salesforce-lakeflow}"

cli() { databricks "$@" --profile "$PROFILE"; }

echo ">> Resolving app URL for '${APP_NAME}'..."
APP_URL="$(cli apps get "$APP_NAME" --output json | python3 -c 'import sys,json;print(json.load(sys.stdin).get("url",""))')"
if [[ -z "$APP_URL" ]]; then
  echo "!! App '${APP_NAME}' has no URL yet. Deploy/start the app first (databricks bundle run), then re-run." >&2
  exit 1
fi
# Strip trailing slash; connection wants host without path.
HOST="${APP_URL%/}"
echo ">> App URL: ${HOST}"

# --- Step 1: UC HTTP connection (per-user OAuth U2M) ----------------------
# For a Databricks App, the gateway authenticates on-behalf-of the calling
# user using Databricks OAuth (scope all-apis). This mirrors the shape used by
# the workspace's other app-hosted MCP connections.
CONN_OPTIONS=$(cat <<JSON
{
  "host": "${HOST}",
  "port": "443",
  "base_path": "/mcp",
  "is_mcp_connection": "true",
  "oauth_scope": "all-apis"
}
JSON
)

if cli connections get "$CONNECTION_NAME" >/dev/null 2>&1; then
  echo ">> Connection '${CONNECTION_NAME}' exists — updating host."
  cli connections update "$CONNECTION_NAME" \
    --json "{\"options\": ${CONN_OPTIONS}}"
else
  echo ">> Creating connection '${CONNECTION_NAME}' (HTTP, MCP, per-user OAuth)."
  cli connections create \
    --json "{\"name\": \"${CONNECTION_NAME}\", \"connection_type\": \"HTTP\", \"options\": ${CONN_OPTIONS}}"
fi

# --- Step 2: MCP Service under cielo.default ------------------------------
# Beta: only REST API / UI can create MCP Services (no CLI verb, no SQL DDL).
# include_tool_selectors restricts exposed tools to our domain tools.
echo ">> Registering MCP Service '${MCP_CATALOG}.${MCP_SCHEMA}.${MCP_SERVICE_ID}'."
cli api post \
  "/api/2.1/unity-catalog/mcp-services?parent=schemas/${MCP_CATALOG}.${MCP_SCHEMA}&mcp_service_id=${MCP_SERVICE_ID}" \
  --json "$(cat <<JSON
{
  "comment": "Salesforce Lakeflow Connect ingestion agent MCP server.",
  "config": {
    "source_connection": { "name": "connections/${CONNECTION_NAME}" },
    "include_tool_selectors": [
      "list_connections",
      "list_source_objects",
      "validate_destination",
      "create_connection",
      "create_ingestion_pipeline",
      "schedule_pipeline",
      "trigger_update",
      "supervisor_plan",
      "supervisor_execute",
      "get_ingestion_status"
    ]
  }
}
JSON
)" || echo "!! MCP Service create returned non-zero (may already exist). Check with: databricks api get /api/2.1/unity-catalog/mcp-services/${MCP_CATALOG}.${MCP_SCHEMA}.${MCP_SERVICE_ID}"

echo ">> Done. MCP Service: ${MCP_CATALOG}.${MCP_SCHEMA}.${MCP_SERVICE_ID}"
echo "   Grant EXECUTE to users, then add it in an agent by that 3-level name."
