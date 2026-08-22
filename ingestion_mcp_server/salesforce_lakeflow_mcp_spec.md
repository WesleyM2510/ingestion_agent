# Conversational Salesforce Lakeflow Connect

## Recommendation

Use a domain-specific MCP server hosted as a stateless Databricks App and expose it to Chat in Genie through a governed MCP Service. Keep Genie responsible for conversation and intent gathering; keep the MCP server responsible for validation, approval, provisioning, and monitoring.

Do not expose a generic Databricks REST tool or arbitrary SQL execution tool.

## Architecture

```text
User
  -> Chat in Genie
  -> Governed MCP Service
  -> Salesforce Lakeflow provisioning tools
  -> Databricks REST API / SDK
  -> Lakeflow Connect pipeline + Lakeflow Job schedule
  -> Salesforce -> Unity Catalog Delta tables
```

The Salesforce OAuth connection should be created and authorized by an administrator in Catalog Explorer before the conversational flow begins. The agent receives only the Unity Catalog connection name, never Salesforce secrets.

References:

* [Salesforce ingestion connector](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/salesforce-concepts)
* [Ingest data from Salesforce](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/salesforce-pipeline)
* [Connect agents to third-party tools with MCP Services](https://docs.databricks.com/aws/en/agents/mcp/mcp-services)
* [Host your own MCP server](https://docs.databricks.com/aws/en/agents/mcp/custom-mcp)

## Conversational flow

1. Ask for the Salesforce connection or environment.
2. Ask which Salesforce objects to ingest.
3. Ask for the destination catalog and schema.
4. Ask for refresh frequency.
5. Validate permissions, connection state, object names, and destination names.
6. Present a complete execution plan.
7. Require explicit confirmation.
8. Create the Lakeflow Connect pipeline.
9. Create an optional Lakeflow Job schedule.
10. Return the pipeline ID, job ID, destination tables, and monitoring links.

The agent should never create resources immediately after inferring intent. It should always show a plan and require confirmation for provisioning.

## MCP tool contract

> **Implementation note.** The original three-verb contract below
> (`validate_salesforce_ingestion` / `plan_salesforce_ingestion` /
> `create_salesforce_ingestion`) has been superseded by a finer-grained,
> single-source surface. One MCP maps to one source (this repo = Salesforce):
>
> - **Read:** `list_connections`, `list_source_objects`, `validate_destination`
> - **Write** (each needs `confirmation="CONFIRM"` + `idempotency_key`):
>   `create_connection`, `create_ingestion_pipeline`, `schedule_pipeline`,
>   `trigger_update`
> - **Supervisor** (routing + human-in-the-loop): `supervisor_plan` (was
>   `plan_*`, read-only) and `supervisor_execute` (was `create_*`, runs the
>   confirmed plan's write steps in order).
>
> The JSON below is retained as the original design reference.

### `validate_salesforce_ingestion`

Read-only validation before planning or creation.

```json
{
  "name": "validate_salesforce_ingestion",
  "description": "Validate a Salesforce Lakeflow Connect request without creating resources.",
  "inputSchema": {
    "type": "object",
    "required": ["connection_name", "destination_catalog", "destination_schema", "objects"],
    "properties": {
      "connection_name": {"type": "string"},
      "destination_catalog": {"type": "string"},
      "destination_schema": {"type": "string"},
      "objects": {
        "type": "array",
        "items": {"type": "string"},
        "minItems": 1,
        "maxItems": 250
      }
    }
  }
}
```

Return:

```json
{
  "valid": true,
  "connection_status": "READY",
  "objects_found": ["Account", "Contact", "Opportunity"],
  "objects_missing": [],
  "permission_errors": [],
  "warnings": []
}
```

### `plan_salesforce_ingestion`

Create a normalized, non-executable plan. This tool should not mutate Databricks.

```json
{
  "name": "plan_salesforce_ingestion",
  "description": "Create a reviewable execution plan for a Salesforce Lakeflow Connect pipeline.",
  "inputSchema": {
    "type": "object",
    "required": [
      "pipeline_name",
      "connection_name",
      "destination_catalog",
      "destination_schema",
      "objects"
    ],
    "properties": {
      "pipeline_name": {"type": "string"},
      "connection_name": {"type": "string"},
      "destination_catalog": {"type": "string"},
      "destination_schema": {"type": "string"},
      "objects": {
        "type": "array",
        "items": {
          "type": "object",
          "required": ["source_table"],
          "properties": {
            "source_schema": {"type": "string", "default": "salesforce"},
            "source_table": {"type": "string"},
            "destination_table": {"type": "string"},
            "scd_type": {
              "type": "string",
              "enum": ["SCD_TYPE_1", "SCD_TYPE_2", "APPEND_ONLY"]
            }
          }
        }
      },
      "schedule": {
        "type": "object",
        "properties": {
          "cron_expression": {"type": "string"},
          "timezone": {"type": "string"}
        }
      }
    }
  }
}
```

Return an approval-bound plan:

```json
{
  "plan_id": "plan_01J...",
  "expires_at": "2026-08-07T20:00:00Z",
  "requires_confirmation": true,
  "pipeline_payload": {},
  "job_payload": {},
  "destination_tables": [
    "main.salesforce_raw.account",
    "main.salesforce_raw.opportunity"
  ],
  "warnings": [
    "Formula fields may require full snapshots."
  ]
}
```

### `create_salesforce_ingestion`

Mutating operation. Accept only a previously generated plan and an explicit confirmation.

```json
{
  "name": "create_salesforce_ingestion",
  "description": "Create an approved Salesforce Lakeflow Connect pipeline and optional schedule.",
  "inputSchema": {
    "type": "object",
    "required": ["plan_id", "confirmation", "idempotency_key"],
    "properties": {
      "plan_id": {"type": "string"},
      "confirmation": {"type": "string", "enum": ["CREATE"]},
      "idempotency_key": {"type": "string"}
    }
  }
}
```

Return:

```json
{
  "status": "CREATED",
  "pipeline_id": "01ef...",
  "job_id": "123456789",
  "pipeline_name": "salesforce_to_uc",
  "tables": [
    "main.salesforce_raw.account",
    "main.salesforce_raw.opportunity"
  ],
  "next_action": "RUN_PIPELINE"
}
```

### `get_ingestion_status`

```json
{
  "name": "get_ingestion_status",
  "description": "Return the current state and recent update status of a Lakeflow Connect pipeline.",
  "inputSchema": {
    "type": "object",
    "required": ["pipeline_id"],
    "properties": {
      "pipeline_id": {"type": "string"},
      "include_recent_updates": {"type": "boolean", "default": true}
    }
  }
}
```

## Lakeflow Connect API payload

Lakeflow Connect pipelines use the Databricks Pipelines API and an `ingestion_definition` block. The following is an illustrative request body for `POST /api/2.0/pipelines`:

```json
{
  "name": "salesforce_to_uc",
  "ingestion_definition": {
    "connection_name": "salesforce_prod_oauth",
    "objects": [
      {
        "table": {
          "source_schema": "salesforce",
          "source_table": "Account",
          "destination_catalog": "main",
          "destination_schema": "salesforce_raw"
        }
      },
      {
        "table": {
          "source_schema": "salesforce",
          "source_table": "Opportunity",
          "destination_catalog": "main",
          "destination_schema": "salesforce_raw",
          "destination_table": "opportunity"
        }
      }
    ]
  }
}
```

For a Salesforce connector, use the Unity Catalog connection name—not a Salesforce URL, token, or connection ID—in `connection_name`.

## Schedule payload

Lakeflow Connect is triggered by a run or by a Lakeflow Job schedule. A schedule can be created with `POST /api/2.1/jobs/create`:

```json
{
  "name": "salesforce_to_uc_daily",
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "America/Sao_Paulo",
    "pause_status": "UNPAUSED"
  },
  "tasks": [
    {
      "task_key": "refresh_salesforce_pipeline",
      "pipeline_task": {
        "pipeline_id": "01ef..."
      }
    }
  ]
}
```

Use the workspace's current Jobs API schema when implementing the adapter, because schedule fields and API versions can evolve.

## Security model

* Create the Salesforce Unity Catalog connection outside the agent flow.
* Grant the calling identity only the required `USE CONNECTION`, catalog, schema, pipeline, and job privileges.
* Prefer user identity passthrough when each user must be accountable for provisioning.
* Use service-principal authorization only when the customer intentionally wants centralized provisioning.
* Enforce catalog and schema allowlists in the MCP server.
* Reject arbitrary API paths, arbitrary SQL, and unapproved Salesforce objects.
* Require confirmation for create, update, refresh, delete, and schedule operations.
* Make `create_salesforce_ingestion` idempotent using a customer-supplied idempotency key.
* Log the requesting identity, normalized plan, approval, API calls, resource IDs, and final status.

## Deployment checklist

* Deploy the MCP server as a stateless Databricks App with an `/mcp` endpoint.
* Register the endpoint as an external MCP Service through Unity AI Gateway.
* Restrict the exposed tools to the six domain tools above.
* Configure a service policy that blocks destructive operations by default.
* Grant users `EXECUTE` on the MCP Service.
* Ensure the agent or app has permissions on the target catalog, schema, connection, pipeline, and job resources.
* Test first with a plan-only flow, then with a sandbox Salesforce org.
* Add evaluation cases for ambiguous objects, invalid schemas, duplicate pipeline names, expired plans, and unauthorized destinations.

## Example conversation

```text
User: Connect Salesforce opportunities to Databricks every night.

Agent: Which authorized Salesforce connection should I use?
       I found salesforce_prod_oauth and salesforce_sandbox_oauth.

User: salesforce_prod_oauth

Agent: I will create:
       Pipeline: salesforce_opportunities
       Source: Opportunity
       Destination: main.salesforce_raw.opportunity
       Schedule: Every day at 02:00 America/Sao_Paulo

       Shall I create this pipeline and schedule? Reply CREATE to continue.

User: CREATE

Agent: Created pipeline 01ef... and job 123456789.
       The first ingestion has not been started. Reply RUN to start it.
```
