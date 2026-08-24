-- Inspect raw audit / usage rows BEFORE relying on the other queries.
-- MCP Services and AI Gateway are preview surfaces; action names and the keys
-- inside request_params can vary by event type and change between releases.
-- Use this to confirm the exact shapes in YOUR metastore.

-- 1) Which MCP / connection-related actions actually show up?
SELECT service_name, action_name, COUNT(*) AS n
FROM system.access.audit
WHERE event_date >= current_date() - INTERVAL 30 DAYS
  AND (
    action_name IN ('mcpCall', 'getConnection', 'createMcpService',
                    'updateMcpService', 'deleteMcpService')
    OR service_name = 'unityCatalog'
  )
GROUP BY service_name, action_name
ORDER BY n DESC;

-- 2) Sample a few full rows so you can see request_params keys and identity fields.
SELECT *
FROM system.access.audit
WHERE event_date >= current_date() - INTERVAL 30 DAYS
  AND action_name = 'mcpCall'
LIMIT 20;

-- 3) Confirm the AI Gateway usage table exists and see its columns.
SELECT *
FROM system.ai_gateway.usage
LIMIT 20;
