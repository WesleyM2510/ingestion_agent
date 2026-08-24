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
# Auth model: OAuth Machine-to-Machine (M2M). The connection authenticates as
# a single service principal via the client_credentials grant. This avoids the
# interactive per-user OAuth login and does NOT require the workspace's OIDC
# server to support Dynamic Client Registration (DCR), which app-hosted MCP
# connections would otherwise attempt and fail on when the OIDC metadata has no
# registration_endpoint. Per-user accountability is preserved at the invocation
# layer: AI Gateway records the real caller in system.access.audit (mcpCall);
# the M2M service principal only appears on the Gateway->app hop. See
# ../audit/ for ready-to-run auditing queries.
#
# Idempotent: creates on first run, updates the host on subsequent runs. If an
# existing connection uses a different credential type (e.g. an older U2M
# connection), it is dropped and recreated, because Unity Catalog rejects an
# in-place credential-type transition.
#
# Env (set by the bundle / caller):
#   DATABRICKS_PROFILE      profile to use (default: e2-demo-field-eng)
#   MCP_CATALOG             catalog for the MCP Service   (default: cielo)
#   MCP_SCHEMA              schema  for the MCP Service   (default: default)
#   CONNECTION_NAME         flat UC connection name       (default: salesforce_lakeflow_mcp_conn)
#   MCP_SERVICE_ID          MCP Service id                (default: salesforce_lakeflow_mcp)
#   APP_NAME                deployed app name             (default: mcp-salesforce-lakeflow)
#   OAUTH_CLIENT_ID         service principal OAuth client id      (REQUIRED)
#   OAUTH_CLIENT_SECRET     service principal OAuth client secret  (REQUIRED)
#   OAUTH_SCOPE             scope for the M2M token       (default: all-apis)
#   WORKSPACE_HOST          workspace URL for the token endpoint
#                           (default: auto-resolved from the profile)
#   TOKEN_ENDPOINT          full token endpoint override
#                           (default: ${WORKSPACE_HOST}/oidc/v1/token)
set -euo pipefail

PROFILE="${DATABRICKS_PROFILE:-e2-demo-field-eng}"
MCP_CATALOG="${MCP_CATALOG:-cielo}"
MCP_SCHEMA="${MCP_SCHEMA:-default}"
CONNECTION_NAME="${CONNECTION_NAME:-salesforce_lakeflow_mcp_conn}"
MCP_SERVICE_ID="${MCP_SERVICE_ID:-salesforce_lakeflow_mcp}"
APP_NAME="${APP_NAME:-mcp-salesforce-lakeflow}"
OAUTH_SCOPE="${OAUTH_SCOPE:-all-apis}"

cli() { databricks "$@" --profile "$PROFILE"; }

# --- M2M service principal credentials ------------------------------------
# Minting a service principal + OAuth secret is a privileged, account-level
# step done once, out of band; the resulting client_id/secret are passed in.
if [[ -z "${OAUTH_CLIENT_ID:-}" || -z "${OAUTH_CLIENT_SECRET:-}" ]]; then
  echo "!! OAUTH_CLIENT_ID and OAUTH_CLIENT_SECRET must be set (service principal M2M credentials)." >&2
  echo "   Create them once with: databricks service-principal-secrets create ... (account admin)." >&2
  echo "   Then grant that SP CAN_USE on the app '${APP_NAME}'." >&2
  exit 1
fi

# --- Resolve workspace host + token endpoint ------------------------------
WORKSPACE_HOST="${WORKSPACE_HOST:-$(cli auth describe --output json 2>/dev/null \
  | python3 -c 'import sys,json;print(json.load(sys.stdin).get("details",{}).get("host","").rstrip("/"))' 2>/dev/null || true)}"
if [[ -z "${WORKSPACE_HOST:-}" ]]; then
  echo "!! Could not resolve WORKSPACE_HOST from profile '${PROFILE}'. Set WORKSPACE_HOST explicitly." >&2
  exit 1
fi
TOKEN_ENDPOINT="${TOKEN_ENDPOINT:-${WORKSPACE_HOST}/oidc/v1/token}"
echo ">> Workspace host: ${WORKSPACE_HOST}"
echo ">> Token endpoint: ${TOKEN_ENDPOINT}"

echo ">> Resolving app URL for '${APP_NAME}'..."
APP_URL="$(cli apps get "$APP_NAME" --output json | python3 -c 'import sys,json;print(json.load(sys.stdin).get("url",""))')"
if [[ -z "$APP_URL" ]]; then
  echo "!! App '${APP_NAME}' has no URL yet. Deploy/start the app first (databricks bundle run), then re-run." >&2
  exit 1
fi
# Strip trailing slash; connection wants host without path.
HOST="${APP_URL%/}"
echo ">> App URL: ${HOST}"

# --- Step 1: UC HTTP connection (OAuth M2M) -------------------------------
# The gateway authenticates to the app as the service principal using the
# client_credentials grant against the workspace OIDC token endpoint. No
# authorization_endpoint (that is a U2M-only field) and no DCR.
CONN_OPTIONS=$(cat <<JSON
{
  "host": "${HOST}",
  "port": "443",
  "base_path": "/mcp",
  "is_mcp_connection": "true",
  "client_id": "${OAUTH_CLIENT_ID}",
  "client_secret": "${OAUTH_CLIENT_SECRET}",
  "oauth_scope": "${OAUTH_SCOPE}",
  "token_endpoint": "${TOKEN_ENDPOINT}"
}
JSON
)

if cli connections get "$CONNECTION_NAME" --output json >/tmp/_conn.json 2>/dev/null; then
  EXISTING_CRED="$(python3 -c 'import sys,json;print(json.load(open("/tmp/_conn.json")).get("credential_type",""))' 2>/dev/null || true)"
  if [[ "$EXISTING_CRED" == "OAUTH_M2M" ]]; then
    echo ">> Connection '${CONNECTION_NAME}' exists (OAUTH_M2M) — updating."
    cli connections update "$CONNECTION_NAME" \
      --json "{\"options\": ${CONN_OPTIONS}}"
  else
    # UC forbids transitioning credential type in place; drop and recreate.
    echo ">> Connection '${CONNECTION_NAME}' exists with credential_type='${EXISTING_CRED}'."
    echo ">> Dropping and recreating as OAUTH_M2M (in-place transition not allowed)."
    cli connections delete "$CONNECTION_NAME"
    cli connections create \
      --json "{\"name\": \"${CONNECTION_NAME}\", \"connection_type\": \"HTTP\", \"options\": ${CONN_OPTIONS}}"
  fi
else
  echo ">> Creating connection '${CONNECTION_NAME}' (HTTP, MCP, OAuth M2M)."
  cli connections create \
    --json "{\"name\": \"${CONNECTION_NAME}\", \"connection_type\": \"HTTP\", \"options\": ${CONN_OPTIONS}}"
fi
rm -f /tmp/_conn.json

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
      "get_ingestion_status"
    ]
  }
}
JSON
)" || echo "!! MCP Service create returned non-zero (may already exist). Check with: databricks api get /api/2.1/unity-catalog/mcp-services/${MCP_CATALOG}.${MCP_SCHEMA}.${MCP_SERVICE_ID}"

echo ">> Done. MCP Service: ${MCP_CATALOG}.${MCP_SCHEMA}.${MCP_SERVICE_ID}"
echo "   Grant EXECUTE to users, then add it in an agent by that 3-level name."
echo "   Audit usage with the queries in ../audit/."
