# Auditing the Salesforce Lakeflow MCP Service

The MCP connection authenticates to the app as a single **service principal**
(OAuth M2M). That does **not** cost you per-user accountability: Unity AI
Gateway authenticates each MCP *invocation* as the **real caller** and enforces
the `EXECUTE` grant against them, so the caller's identity is recorded in the
audit trail. The M2M service principal only appears on the Gateway→app hop
(`run_as`), while the human/agent that initiated the call is the `user_identity`
/ `run_by`.

## Where the data lives

| Table | Use |
|---|---|
| `system.access.audit` | Authoritative audit trail — who invoked the MCP Service, which action, success/failure, source IP, timestamp. |
| `system.ai_gateway.usage` | Usage / cost roll-ups — requester, invocation id, token counts, request tags. |

Both are Unity Catalog **system tables** (retention ~365 days) and generally
require metastore/account admin to query. `system.ai_gateway` and MCP Services
are **preview** surfaces — column names and `request_params` keys can vary by
event type and may change. **Run `00_inspect_schema.sql` first** to confirm the
exact shapes in your metastore before relying on the other queries.

## Files

- `00_inspect_schema.sql` — sample a few raw rows to confirm columns / param keys.
- `01_mcp_invocations_by_user.sql` — every MCP call, attributed to the real caller.
- `02_tool_usage_summary.sql` — call volume per user, with success/failure counts.
- `03_connection_access.sql` — `getConnection` events on the MCP connection.
- `04_gateway_usage.sql` — usage/token roll-up from `system.ai_gateway.usage`.

## Parameters

Queries are written for:

- MCP Service: `cielo.default.salesforce_lakeflow_mcp`
- Connection: `salesforce_lakeflow_mcp_conn`

Adjust the names and the date window (`INTERVAL 30 DAYS`) to taste.
