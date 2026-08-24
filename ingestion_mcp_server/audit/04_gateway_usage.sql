-- Usage / cost roll-up from the AI Gateway usage table.
--
-- system.ai_gateway.usage is the easiest surface for "who used it and how
-- much" -- requester, per-invocation ids, token counts, request tags. Columns
-- are preview and may differ in your metastore; verify with:
--   SELECT * FROM system.ai_gateway.usage LIMIT 20;  (see 00_inspect_schema.sql)
--
-- If your rows expose an endpoint / service identifier column, add a filter for
-- the Salesforce Lakeflow MCP service to scope the results.

SELECT
  event_date,
  requester,
  COUNT(*)                          AS requests,
  COUNT(DISTINCT invocation_id)     AS invocations,
  SUM(total_tokens)                 AS total_tokens
FROM system.ai_gateway.usage
WHERE event_date >= current_date() - INTERVAL 30 DAYS
GROUP BY event_date, requester
ORDER BY event_date DESC, requests DESC;
