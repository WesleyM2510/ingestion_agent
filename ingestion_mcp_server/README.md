# Salesforce Lakeflow Ingestion Agent (MCP Server)

A domain-specific [MCP](https://modelcontextprotocol.io) server for the
**Salesforce** source of Lakeflow Connect. **One MCP maps to a single source**;
this repo is the Salesforce MCP. It is hosted as a **stateless Databricks App**
and registered as a governed **MCP Service** behind the Unity **AI Gateway**.

Routing across sources and human-in-the-loop orchestration are the job of an
**external supervisor** — e.g. an Agent Bricks Multi-Agent Supervisor that lists
this MCP Service among its tools — not of this server. This repo stays focused
on the Salesforce source tools.

```
Supervisor (external, e.g. Agent Bricks MAS) → AI Gateway → this MCP (Salesforce)
     → Databricks SDK → Lakeflow Connect pipeline (+ optional Job schedule)
     → Salesforce → Unity Catalog Streaming Tables
```

## Layout

| File | Purpose |
|------|---------|
| `server/app.py` | FastMCP + FastAPI app, `/mcp` endpoint (`stateless_http=True`) |
| `server/main.py` | uvicorn entry point (`salesforce-mcp-server`) |
| `server/tools.py` | The MCP tools — thin wrappers, no business logic |
| `server/lakeflow.py` | Databricks adapter: payload building + SDK calls |
| `server/schemas.py` | Pydantic request/response models (the tool contract) |
| `server/store.py` | In-memory idempotency store |
| `server/config.py` | Catalog / schema / connection allowlists (env-driven) |
| `app.yaml` | Databricks App command + allowlist env vars |

## Tools

**Read** (safe, no confirmation):

| Tool | Notes |
|------|-------|
| `list_connections` | UC connections, flagged against the allowlist |
| `list_source_objects` | Best-effort Salesforce object discovery for a connection |
| `validate_destination` | Checks allowlist + connection state before any write |

**Write** (each requires `confirmation="CONFIRM"` + `idempotency_key`):

| Tool | Notes |
|------|-------|
| `create_connection` | Create a Salesforce UC connection (OAuth handled by the Gateway) |
| `create_ingestion_pipeline` | Create the Lakeflow Connect pipeline |
| `schedule_pipeline` | Create a Lakeflow Job that refreshes the pipeline on cron |
| `trigger_update` | Start an update (incremental by default) |

Plus `get_ingestion_status` (read-only) for pipeline state + recent updates.

The key design decision: **writes never happen on inferred intent** — every
mutating tool needs an explicit `CONFIRM` and is idempotent via an
`idempotency_key`. Allowlists are enforced by the server, not the caller. The
`CONFIRM` guard is the server's own safety net; higher-level plan/approval flows
belong to the external supervisor.

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

Test order: `list_connections` → `validate_destination` →
`create_ingestion_pipeline` (with `confirmation="CONFIRM"`) against a sandbox
catalog → `schedule_pipeline` / `trigger_update` → `get_ingestion_status`.

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
   domain tools (3 read + 4 write + `get_ingestion_status`).

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
`mcp-salesforce-lakeflow` → save → enable the tools.

Drive it by calling the tools in order: `list_connections` /
`list_source_objects` / `validate_destination` (read), then the write tools
(`create_connection` → `create_ingestion_pipeline` → `schedule_pipeline` →
`trigger_update`) each with `confirmation="CONFIRM"`. An external supervisor
(e.g. an Agent Bricks MAS) is responsible for slot-filling and showing a plan
before confirming; this server only enforces the per-tool `CONFIRM` guard.

Never request or display Salesforce credentials — OAuth is handled by the AI
Gateway.

## Security allowlists

Set these env vars in `app.yaml` (comma-separated; empty = permissive):

- `ALLOWED_CATALOGS` — e.g. `main`
- `ALLOWED_SCHEMAS` — `catalog.schema` entries, e.g. `main.salesforce_raw`
- `ALLOWED_CONNECTIONS` — e.g. `salesforce_prod_oauth`
