-- Query: Lakebase sync event metrics per pipeline
-- Must be run as a query (not a view) because event_log() is a table function
-- Run this to get executor time and billing data for each sync update

WITH pipeline_map AS (
  SELECT pipeline_id, table_size, sync_type
  FROM VALUES
    ('933a10dc-22f0-437f-a998-06c324106f81', '100mb', 'snapshot'),
    ('e9c0d487-23d5-4cd2-b45d-300d9dda5651', '100mb', 'triggered'),
    ('1cf44d0c-bd90-4248-bd26-5aa1453b3559', '500mb', 'snapshot'),
    ('c7741132-8250-4c28-a1be-e031f18ce53a', '500mb', 'triggered'),
    ('57d013b1-432d-416c-bbdc-88709f805221', '10gb',  'snapshot'),
    ('679a49cd-0e57-422b-8093-98253c82cc2b', '10gb',  'triggered'),
    ('2721e1ea-fb78-4abc-8f94-b910dd7dd677', '100gb', 'snapshot'),
    ('cdafb07e-4a3f-4d86-8175-6a36d4f5ff06', '100gb', 'triggered'),
    ('9b62d5bd-9f75-40df-8423-4c61b40f922b', '500gb', 'snapshot'),
    ('7a85d5e5-7cd5-43fa-8984-0d92c3b8bf67', '500gb', 'triggered'),
    ('c9906b97-a1eb-443c-96c4-23cb35aba534', '1tb',   'snapshot'),
    ('11912199-cc2b-467c-83bc-71facff8940f', '1tb',   'triggered')
  AS t(pipeline_id, table_size, sync_type)
),
-- Collect event logs per pipeline (run one at a time due to event_log() limitation)
-- Replace PIPELINE_ID with each pipeline_id from the map above
events AS (
  SELECT
    origin:update_id as update_id,
    MIN(timestamp) as update_start,
    MAX(timestamp) as update_end,
    ROUND((UNIX_TIMESTAMP(MAX(timestamp)) - UNIX_TIMESTAMP(MIN(timestamp))), 0) as duration_seconds,
    MAX(CAST(details:flow_progress:metrics:executor_time_ms AS BIGINT)) as executor_time_ms,
    MAX(CAST(details:flow_progress:metrics:executor_cpu_time_ms AS BIGINT)) as executor_cpu_time_ms,
    MAX(CASE WHEN details:update_progress:state = 'COMPLETED' THEN 1 ELSE 0 END) as completed
  FROM event_log("PIPELINE_ID")
  GROUP BY origin:update_id
),
billing AS (
  SELECT
    m.table_size,
    m.sync_type,
    m.pipeline_id,
    SUM(u.usage_quantity) as total_dbus
  FROM system.billing.usage u
  JOIN pipeline_map m
    ON u.usage_metadata.dlt_pipeline_id = m.pipeline_id
  GROUP BY ALL
)
SELECT
  b.table_size,
  b.sync_type,
  b.pipeline_id,
  b.total_dbus,
  e.update_id,
  e.update_start,
  e.update_end,
  e.duration_seconds,
  e.executor_time_ms,
  e.executor_cpu_time_ms,
  e.completed
FROM billing b
LEFT JOIN events e ON 1=1  -- join with events for the specific pipeline
ORDER BY b.table_size, b.sync_type;


-- ============================================================================
-- QUICK PER-PIPELINE EVENT LOG QUERY
-- Run this for each pipeline to see per-update metrics:
-- ============================================================================

-- SELECT
--   origin:update_id as update_id,
--   MIN(timestamp) as started,
--   MAX(timestamp) as ended,
--   ROUND((UNIX_TIMESTAMP(MAX(timestamp)) - UNIX_TIMESTAMP(MIN(timestamp))), 0) as duration_sec,
--   MAX(CAST(details:flow_progress:metrics:executor_time_ms AS BIGINT)) as executor_ms,
--   MAX(CAST(details:flow_progress:metrics:executor_cpu_time_ms AS BIGINT)) as cpu_ms,
--   MAX(CASE WHEN details:update_progress:state = 'COMPLETED' THEN 1 ELSE 0 END) as completed
-- FROM event_log("<pipeline_id>")
-- GROUP BY origin:update_id
-- ORDER BY started;


-- ============================================================================
-- BILLING SUMMARY (works immediately, no event_log dependency)
-- ============================================================================

SELECT
  m.table_size,
  m.sync_type,
  m.pipeline_id,
  u.sku_name,
  u.usage_date,
  SUM(u.usage_quantity) as dbus
FROM system.billing.usage u
JOIN (
  SELECT pipeline_id, table_size, sync_type
  FROM VALUES
    ('933a10dc-22f0-437f-a998-06c324106f81', '100mb', 'snapshot'),
    ('e9c0d487-23d5-4cd2-b45d-300d9dda5651', '100mb', 'triggered'),
    ('1cf44d0c-bd90-4248-bd26-5aa1453b3559', '500mb', 'snapshot'),
    ('c7741132-8250-4c28-a1be-e031f18ce53a', '500mb', 'triggered'),
    ('57d013b1-432d-416c-bbdc-88709f805221', '10gb',  'snapshot'),
    ('679a49cd-0e57-422b-8093-98253c82cc2b', '10gb',  'triggered'),
    ('2721e1ea-fb78-4abc-8f94-b910dd7dd677', '100gb', 'snapshot'),
    ('cdafb07e-4a3f-4d86-8175-6a36d4f5ff06', '100gb', 'triggered'),
    ('9b62d5bd-9f75-40df-8423-4c61b40f922b', '500gb', 'snapshot'),
    ('7a85d5e5-7cd5-43fa-8984-0d92c3b8bf67', '500gb', 'triggered'),
    ('c9906b97-a1eb-443c-96c4-23cb35aba534', '1tb',   'snapshot'),
    ('11912199-cc2b-467c-83bc-71facff8940f', '1tb',   'triggered')
  AS t(pipeline_id, table_size, sync_type)
) m ON u.usage_metadata.dlt_pipeline_id = m.pipeline_id
GROUP BY ALL
ORDER BY
  CASE m.table_size
    WHEN '100mb' THEN 1 WHEN '500mb' THEN 2 WHEN '10gb' THEN 3
    WHEN '100gb' THEN 4 WHEN '500gb' THEN 5 WHEN '1tb' THEN 6
  END,
  m.sync_type, u.usage_date;
