"""Agent Bricks Multi-Agent Supervisor (MAS) definition for this MCP.

This is the *external supervisor* the spec and README reference: it puts a
human-in-the-loop, plan->approve->execute conversation in front of the
Salesforce Lakeflow ingestion MCP. Routing and slot-filling live here; the MCP
server itself stays thin and still hard-enforces confirmation on every write.

Two independent human-in-the-loop guarantees (defense in depth):
  1. Supervisor layer (this config's `instructions`): the agent must present a
     plan and get explicit user approval before calling any write tool.
  2. MCP server layer: every write tool (create_connection,
     create_ingestion_pipeline, schedule_pipeline, trigger_update) rejects the
     call unless confirmation == "CONFIRM". A misrouting or jailbroken
     supervisor still cannot provision without that token.

Prerequisites (all must be true before the MAS can actually call tools):
  - The app `mcp-salesforce-lakeflow` is deployed and RUNNING (the MCP endpoint
    must answer, or every tool call fails).
  - UC HTTP connection `salesforce_lakeflow_mcp_conn` exists, is OAUTH_M2M, and
    is ACTIVE (created/maintained by scripts/register_mcp_service.sh).
  - The MCP Service `cielo.default.salesforce_lakeflow_mcp` is registered and
    bound to that connection.
  - The Supervisor's service principal has USE CONNECTION on the connection:
      GRANT USE CONNECTION ON CONNECTION salesforce_lakeflow_mcp_conn
        TO `<supervisor_service_principal>`;
    (The SP only exists after the MAS is created; grant it afterward, then
    the first tool call will succeed.)

How to apply:
  This module only *declares* the config (MAS_CONFIG) and, when the Agent
  Bricks `manage_mas` MCP tool is available in your session, applies it via
  `apply()`. `manage_mas` is NOT a normal Python import — it is an MCP tool
  exposed to an agent/host. So either:
    (a) run this from a host where `manage_mas` is wired in, calling `apply()`
        with a thin adapter (see __main__), or
    (b) copy the fields from MAS_CONFIG into the Databricks UI:
        Agent Bricks -> Supervisor Agent -> add External MCP server ->
        select connection `salesforce_lakeflow_mcp_conn`, then paste the
        description + instructions below.
"""

from __future__ import annotations

# The UC HTTP connection that fronts this MCP (see register_mcp_service.sh).
CONNECTION_NAME = "salesforce_lakeflow_mcp_conn"

# Routing description for the single MCP-backed agent. Agent Bricks uses this to
# decide when to route to the Salesforce ingestion tools, so it is deliberately
# comprehensive and action-oriented.
AGENT_DESCRIPTION = (
    "Provision and manage Salesforce Lakeflow Connect ingestion. Capabilities: "
    "list Unity Catalog connections, discover Salesforce source objects, "
    "validate a destination catalog/schema, create a connection, create an "
    "ingestion pipeline, schedule periodic refreshes, trigger an update, and "
    "check ingestion status. Use for ANY Salesforce ingestion question or "
    "action, and for ALL write/provisioning operations."
)

# Human-in-the-loop routing instructions. This is the supervisor-layer HITL
# gate; it complements (does not replace) the MCP server's confirmation check.
SUPERVISOR_INSTRUCTIONS = """
You supervise Salesforce Lakeflow Connect ingestion provisioning. Route every
Salesforce ingestion request to the `salesforce_ingestion` agent.

HUMAN-IN-THE-LOOP — MANDATORY for any write operation (create_connection,
create_ingestion_pipeline, schedule_pipeline, trigger_update):

  1. PLAN FIRST. Before any write, call the relevant read-only tools
     (list_connections, list_source_objects, validate_destination) and present
     a concrete plan to the user: the connection, the exact source objects, the
     destination catalog.schema.table(s), and the schedule (if any).
  2. ASK FOR APPROVAL. Explicitly ask the user to confirm the plan. Never act
     on inferred intent. If the request is ambiguous or under-specified, ask
     clarifying questions instead of guessing.
  3. EXECUTE ONLY AFTER APPROVAL. Once the user clearly approves, call the write
     tool with confirmation="CONFIRM" and a stable idempotency_key. Do not send
     confirmation="CONFIRM" until the user has approved in this conversation.
  4. REPORT. Return the result plainly (status, pipeline_id, tables,
     next_action). If a tool returns REJECTED or ALREADY_EXISTS, explain what
     happened and what the user can do next.

Read-only tools (list_connections, list_source_objects, validate_destination,
get_ingestion_status) may be called freely to build the plan or answer
questions — they never mutate anything.

Never fabricate object names, catalogs, or pipeline ids; get them from the
read tools. If a write tool is rejected for missing confirmation, that is the
safety system working — surface it, do not try to bypass it.
""".strip()

# The full MAS payload. Field names match the Agent Bricks `manage_mas` tool.
MAS_CONFIG: dict = {
    "action": "create_or_update",
    "name": "Salesforce Ingestion Supervisor",
    "description": (
        "Human-in-the-loop supervisor for Salesforce Lakeflow Connect ingestion "
        "provisioning. Presents a plan and requires explicit approval before any "
        "write."
    ),
    "instructions": SUPERVISOR_INSTRUCTIONS,
    "agents": [
        {
            "name": "salesforce_ingestion",
            "connection_name": CONNECTION_NAME,
            "description": AGENT_DESCRIPTION,
        }
    ],
    "examples": [
        {
            "question": "What Salesforce objects can I ingest from sf_cnn?",
            "guideline": (
                "Read-only. Route to salesforce_ingestion -> list_source_objects. "
                "No confirmation needed."
            ),
        },
        {
            "question": "Ingest Account and Contact from sf_cnn into cielo.default, hourly.",
            "guideline": (
                "Write. Route to salesforce_ingestion. FIRST validate_destination "
                "and present a plan (objects, cielo.default.account/contact, hourly "
                "cron), THEN ask the user to approve, THEN create_ingestion_pipeline "
                "and schedule_pipeline with confirmation=CONFIRM. Do not provision "
                "before approval."
            ),
        },
        {
            "question": "Just go ahead and create whatever you think is best.",
            "guideline": (
                "Ambiguous. Do NOT provision. Ask which objects, destination, and "
                "schedule are wanted, then follow the plan->approve->execute flow."
            ),
        },
    ],
}


def apply(manage_mas) -> dict:
    """Create/update the Supervisor Agent using an Agent Bricks `manage_mas` callable.

    `manage_mas` is the Agent Bricks MCP tool (not importable here). Pass it in
    from a host/session where it is available, e.g. a thin wrapper that forwards
    to the tool. Returns whatever manage_mas returns (tile_id, status, etc.).
    """
    return manage_mas(**MAS_CONFIG)


if __name__ == "__main__":
    # No manage_mas in a plain Python process; just print the config for review
    # or for pasting into the Databricks UI.
    import json

    print(json.dumps(MAS_CONFIG, indent=2))
