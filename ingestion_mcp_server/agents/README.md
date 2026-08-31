# Supervisor Agent (Agent Bricks MAS)

`supervisor_mas.py` declares the external **Agent Bricks Multi-Agent Supervisor**
that puts a human-in-the-loop conversation in front of this MCP. The MCP server
stays thin; the supervisor does routing, slot-filling, and plan→approve→execute.

## Human-in-the-loop: two layers (defense in depth)

1. **Supervisor layer** — the `instructions` in `supervisor_mas.py` require the
   agent to present a plan and get explicit user approval before any write.
2. **MCP server layer** — every write tool (`create_connection`,
   `create_ingestion_pipeline`, `schedule_pipeline`, `trigger_update`) rejects
   the call unless `confirmation="CONFIRM"`. Even a misrouting or jailbroken
   supervisor cannot provision without that token.

## Prerequisites

- App `mcp-salesforce-lakeflow` deployed and **RUNNING** (else every tool call
  fails against a dead endpoint).
- UC connection `salesforce_lakeflow_mcp_conn` — `OAUTH_M2M`, `ACTIVE`.
- MCP Service `cielo.default.salesforce_lakeflow_mcp` registered and bound to it
  (see `../scripts/register_mcp_service.sh`).
- The supervisor's service principal has `USE CONNECTION` on the connection —
  granted **after** the MAS is created (the SP only exists then):
  ```sql
  GRANT USE CONNECTION ON CONNECTION salesforce_lakeflow_mcp_conn
    TO `<supervisor_service_principal>`;
  ```

## Apply it

**Option A — programmatic (Agent Bricks `manage_mas` tool available):**
`manage_mas` is an MCP tool, not a Python import. From a host/session where it
is wired in, forward this module's config:
```python
from agents.supervisor_mas import apply
apply(manage_mas)   # manage_mas provided by the host
```
or call `manage_mas(**MAS_CONFIG)` directly.

**Option B — Databricks UI:**
Print the config and paste the fields:
```bash
python agents/supervisor_mas.py
```
Then in the workspace: **Agent Bricks → Supervisor Agent → add External MCP
server → select `salesforce_lakeflow_mcp_conn`**, and paste the `description`
and `instructions` from the printed JSON.

After it reaches `ONLINE` (2–5 min), grant `USE CONNECTION` (above) and test
routing with the example questions in `MAS_CONFIG["examples"]`.
