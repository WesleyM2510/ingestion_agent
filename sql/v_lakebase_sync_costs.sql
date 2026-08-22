-- View: cielo_demo.default.v_lakebase_sync_costs
-- Maps Lakebase sync pipeline billing data by table size and sync type
-- Billing data takes a few hours to appear after sync runs

CREATE OR REPLACE VIEW cielo_demo.default.v_lakebase_sync_costs AS
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
usage_data AS (
  SELECT
    m.table_size,
    m.sync_type,
    u.usage_date,
    u.usage_quantity,
    MIN(u.usage_date) OVER (PARTITION BY m.pipeline_id) as first_sync_date
  FROM system.billing.usage u
  JOIN pipeline_map m
    ON u.usage_metadata.dlt_pipeline_id = m.pipeline_id
)
SELECT
  table_size,
  sync_type,
  SUM(CASE WHEN usage_date = first_sync_date THEN usage_quantity ELSE 0 END) as first_sync_dbus,
  SUM(CASE WHEN usage_date > first_sync_date THEN usage_quantity ELSE 0 END) as subsequent_total_dbus,
  COUNT(DISTINCT CASE WHEN usage_date > first_sync_date THEN usage_date END) as subsequent_sync_days,
  CASE
    WHEN COUNT(DISTINCT CASE WHEN usage_date > first_sync_date THEN usage_date END) > 0
    THEN SUM(CASE WHEN usage_date > first_sync_date THEN usage_quantity ELSE 0 END)
         / COUNT(DISTINCT CASE WHEN usage_date > first_sync_date THEN usage_date END)
    ELSE 0
  END as avg_dbu_per_subsequent_sync,
  SUM(usage_quantity) as total_dbus
FROM usage_data
GROUP BY table_size, sync_type
ORDER BY
  CASE table_size
    WHEN '100mb' THEN 1 WHEN '500mb' THEN 2 WHEN '10gb' THEN 3
    WHEN '100gb' THEN 4 WHEN '500gb' THEN 5 WHEN '1tb' THEN 6
  END,
  sync_type;
