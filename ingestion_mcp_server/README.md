# Salesforce Lakeflow Connect MCP Server

A domain-specific [MCP](https://modelcontextprotocol.io) server that provisions
**Salesforce Lakeflow Connect** ingestion pipelines conversationally. It is hosted
as a **stateless Databricks App** and attached to **Chat in Genie** as a custom
MCP server. Genie owns the conversation; this server owns validation, approval,
provisioning, and monitoring.

```
User → Chat in Genie → MCP Service → this server → Databricks SDK
     → Lakeflow Connect pipeline (+ optional Lakeflow Job schedule)
     → Salesforce → Unity Catalog Delta tables
```

## Layout

| File | Purpose |
|------|---------|
| `server/app.py` | FastMCP + FastAPI app, `/mcp` endpoint (`stateless_http=True`) |
| `server/main.py` | uvicorn entry point (`salesforce-mcp-server`) |
| `server/tools.py` | The 4 MCP tools — thin wrappers, no business logic |
| `server/lakeflow.py` | Databricks adapter: payload building + SDK calls |
| `server/schemas.py` | Pydantic request/response models (the tool contract) |
| `server/store.py` | In-memory plan-approval + idempotency store |
| `server/config.py` | Catalog / schema / connection allowlists (env-driven) |
| `app.yaml` | Databricks App command + allowlist env vars |

## Tools

| Tool | Mutates? | Notes |
|------|----------|-------|
| `validate_salesforce_ingestion` | No | Checks allowlist + connection state |
| `plan_salesforce_ingestion` | No | Returns a reviewable `plan_id` |
| `create_salesforce_ingestion` | **Yes** | Requires `plan_id`, `confirmation="CREATE"`, `idempotency_key` |
| `get_ingestion_status` | No | Pipeline state + recent updates |

The key design decision: **plan is read-only; create binds to a stored plan
and requires explicit confirmation**. Allowlists are enforced by the server,
not the LLM. No delete / edit / full-refresh tools are exposed yet.

## Prerequisites

An administrator creates and authorizes the Salesforce **Unity Catalog
connection** in Catalog Explorer *before* the conversational flow. The server
only ever receives the connection *name* — never Salesforce secrets.

The App identity needs `USE CONNECTION` on the connection, `USE CATALOG` +
schema/table create privileges on the destination, and pipeline/job create
privileges.

## Local development

```bash
uv sync
uv run salesforce-mcp-server        # serves http://localhost:8000/mcp
uv run pytest                       # runs the no-workspace unit tests
```

Test order: list tools → `plan_salesforce_ingestion` → inspect payload →
`create_salesforce_ingestion` against a sandbox catalog → `get_ingestion_status`.

## Deploy to Databricks Apps (Asset Bundle)

Deployment is driven by `databricks.yml`. The bundle syncs the project and
provisions the `mcp-salesforce-lakeflow` app; `app.yaml` supplies the start
command and the `ALLOWED_*` allowlist env vars.

```bash
databricks auth login --host https://e2-demo-field-eng.cloud.databricks.com --profile e2-demo-field-eng

databricks bundle validate --profile e2-demo-field-eng            # dev target (default)
databricks bundle deploy   --profile e2-demo-field-eng            # sync + create the app
databricks bundle run mcp_salesforce_lakeflow --profile e2-demo-field-eng   # start/deploy it
```

Target `prod` locks down the allowlists; deploy it with `-t prod`. The `mcp-`
name prefix aids discovery. Endpoint after deploy: `https://<app-url>/mcp`.

> Databricks Apps env vars live in `app.yaml`, not the bundle. Edit the
> `ALLOWED_*` values there before deploying to production.

## Register as an MCP Service (cielo.default)

Agents don't connect to the app URL directly — they go through **Unity AI
Gateway** as a governed **MCP Service**. DABs has no resource type for UC
connections or MCP Services, so a `postdeploy` hook
(`scripts/register_mcp_service.sh`) creates both once the app is running:

1. a UC **HTTP connection** `salesforce_lakeflow_mcp_conn` pointing at the app
   (`/mcp`, per-user OAuth, scope `all-apis`);
2. an **MCP Service** `cielo.default.salesforce_lakeflow_mcp` exposing only the
   four domain tools.

```bash
databricks bundle deploy --profile e2-demo-field-eng             # creates the app
databricks bundle run mcp_salesforce_lakeflow --profile e2-demo-field-eng   # start it → mints URL
# postdeploy hook registers connection + MCP Service (re-run standalone anytime):
bash ./scripts/register_mcp_service.sh
```

Then grant `EXECUTE` so others can invoke it (owner-only by default):

```bash
databricks api patch \
  "/api/2.1/unity-catalog/permissions/mcp_service/cielo.default.salesforce_lakeflow_mcp" \
  --json '{"changes":[{"principal":"<group>","add":["EXECUTE"]}]}' \
  --profile e2-demo-field-eng
```

Add it in an agent / Chat by its three-level name
`cielo.default.salesforce_lakeflow_mcp` — not the raw app URL.

## Attach to Chat in Genie

Genie Code settings → MCP Servers → Add Server → Custom MCP server →
`mcp-salesforce-lakeflow` → save → enable the four tools.

Give Genie instructions to: collect connection/objects/destination/schedule →
call `validate_*` then `plan_*` → show the plan → wait for explicit `CREATE` →
call `create_*` → then `get_ingestion_status`. Never request or display
Salesforce credentials.

## Security allowlists

Set these env vars in `app.yaml` (comma-separated; empty = permissive):

- `ALLOWED_CATALOGS` — e.g. `main`
- `ALLOWED_SCHEMAS` — `catalog.schema` entries, e.g. `main.salesforce_raw`
- `ALLOWED_CONNECTIONS` — e.g. `salesforce_prod_oauth`
