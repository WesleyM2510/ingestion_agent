-- Access to the underlying UC HTTP connection (the Gateway->app hop).
--
-- getConnection events fire when the connection is read/used. With M2M this is
-- where the service principal identity is relevant. Direct connection access
-- outside the MCP Service (i.e. someone holding USE CONNECTION calling the app
-- directly) also surfaces here -- worth watching, since it bypasses the MCP
-- Service's tool selection and policies.

SELECT
  event_time,
  user_identity.email        AS accessed_by,
  identity_metadata.run_as   AS run_as,
  action_name,
  request_params,
  response.status_code       AS status_code,
  source_ip_address
FROM system.access.audit
WHERE event_date >= current_date() - INTERVAL 30 DAYS
  AND service_name = 'unityCatalog'
  AND action_name IN ('getConnection', 'updateConnection', 'deleteConnection',
                      'createConnection')
  AND (
    request_params['name']          = 'salesforce_lakeflow_mcp_conn'
    OR request_params['name_arg']   = 'salesforce_lakeflow_mcp_conn'
    OR request_params['full_name_arg'] = 'salesforce_lakeflow_mcp_conn'
  )
ORDER BY event_time DESC
LIMIT 500;
