-- Call-volume summary per user: total calls, successes, failures, active days.
-- Useful for spotting heavy users, error hot spots, and adoption over time.
--
-- status_code semantics come from response.status_code (200 = success).
-- Confirm the request_params key for the service id with 00_inspect_schema.sql.

SELECT
  user_identity.email                                              AS caller,
  COUNT(*)                                                         AS total_calls,
  SUM(CASE WHEN response.status_code = 200 THEN 1 ELSE 0 END)      AS ok_calls,
  SUM(CASE WHEN response.status_code <> 200 THEN 1 ELSE 0 END)     AS failed_calls,
  COUNT(DISTINCT event_date)                                       AS active_days,
  MIN(event_time)                                                  AS first_seen,
  MAX(event_time)                                                  AS last_seen
FROM system.access.audit
WHERE event_date >= current_date() - INTERVAL 30 DAYS
  AND action_name = 'mcpCall'
  AND (
    request_params['mcp_service']  = 'cielo.default.salesforce_lakeflow_mcp'
    OR request_params['full_name'] = 'mcp-services/cielo.default.salesforce_lakeflow_mcp'
    OR request_params['name']      = 'mcp-services/cielo.default.salesforce_lakeflow_mcp'
  )
GROUP BY user_identity.email
ORDER BY total_calls DESC;
