-- Every MCP Service invocation, attributed to the REAL caller.
--
-- Even though the connection uses an M2M service principal, AI Gateway
-- authenticates each invocation as the calling user and records them here:
--   user_identity.email        -> the human / agent that called the tool
--   identity_metadata.run_by   -> initiator (human)
--   identity_metadata.run_as   -> effective identity (may be the M2M SP)
--
-- Adjust the MCP service name and date window as needed. Confirm the
-- request_params key that holds the service name via 00_inspect_schema.sql;
-- it is commonly a full-name-style arg like the one filtered below.

SELECT
  event_time,
  user_identity.email                       AS caller,
  identity_metadata.run_by                  AS run_by,
  identity_metadata.run_as                  AS run_as,
  action_name,
  request_params,                            -- includes tool name / service id
  response.status_code                       AS status_code,
  source_ip_address,
  request_id
FROM system.access.audit
WHERE event_date >= current_date() - INTERVAL 30 DAYS
  AND action_name = 'mcpCall'
  AND (
    -- one of these will match depending on how the service id is recorded;
    -- verify the exact key with 00_inspect_schema.sql and keep the right line.
    request_params['mcp_service']  = 'cielo.default.salesforce_lakeflow_mcp'
    OR request_params['full_name'] = 'mcp-services/cielo.default.salesforce_lakeflow_mcp'
    OR request_params['name']      = 'mcp-services/cielo.default.salesforce_lakeflow_mcp'
  )
ORDER BY event_time DESC
LIMIT 500;
