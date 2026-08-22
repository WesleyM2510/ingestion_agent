import json, subprocess, sys

def api_call(method, path, data=None):
    cmd = ["databricks", "api", method, path]
    if data:
        cmd += ["--json", json.dumps(data)]
    import os
    env = os.environ.copy()
    env["DATABRICKS_CONFIG_PROFILE"] = "cielo_demo"
    return json.loads(subprocess.check_output(cmd, env=env).decode())

warehouse_id = "71fb564c0670309e"
parent_path = "/Workspace/Users/wesley.maciel@databricks.com"

dashboard_def = {
    "datasets": [
        {
            "name": "jobs_pipeline_detail",
            "displayName": "Jobs & Pipeline Detail",
            "queryLines": [
                "SELECT ",
                "  CASE ",
                "    WHEN sku_name LIKE '%JOBS%' THEN 'Jobs' ",
                "    WHEN sku_name LIKE '%DLT%' THEN 'Pipelines (DLT)' ",
                "    ELSE 'Other' ",
                "  END as workload_type, ",
                "  usage_date, ",
                "  COALESCE(usage_metadata.job_name, usage_metadata.dlt_pipeline_id, 'Unknown') as resource_name, ",
                "  CASE WHEN product_features.is_serverless THEN 'Serverless' ELSE 'Classic' END as compute_mode, ",
                "  SUM(usage_quantity) as dbus ",
                "FROM system.billing.usage ",
                "WHERE (sku_name LIKE '%JOBS%' OR sku_name LIKE '%DLT%') ",
                "  AND usage_date >= date_sub(current_date(), 90) ",
                "GROUP BY ALL "
            ]
        },
        {
            "name": "jobs_top_spenders",
            "displayName": "Top Job Spenders",
            "queryLines": [
                "SELECT ",
                "  COALESCE(usage_metadata.job_name, usage_metadata.dlt_pipeline_id, 'Unknown') as resource_name, ",
                "  CASE WHEN sku_name LIKE '%JOBS%' THEN 'Jobs' ELSE 'Pipelines (DLT)' END as workload_type, ",
                "  CASE WHEN product_features.is_serverless THEN 'Serverless' ELSE 'Classic' END as compute_mode, ",
                "  SUM(usage_quantity) as total_dbus ",
                "FROM system.billing.usage ",
                "WHERE (sku_name LIKE '%JOBS%' OR sku_name LIKE '%DLT%') ",
                "  AND usage_date >= date_sub(current_date(), 90) ",
                "GROUP BY ALL ",
                "ORDER BY total_dbus DESC ",
                "LIMIT 8 "
            ]
        },
        {
            "name": "jobs_summary",
            "displayName": "Jobs Summary KPIs",
            "queryLines": [
                "SELECT ",
                "  SUM(usage_quantity) as total_dbus, ",
                "  COUNT(DISTINCT COALESCE(usage_metadata.job_name, usage_metadata.dlt_pipeline_id)) as distinct_resources, ",
                "  SUM(CASE WHEN sku_name LIKE '%DLT%' THEN usage_quantity ELSE 0 END) as pipeline_dbus, ",
                "  SUM(CASE WHEN sku_name LIKE '%JOBS%' THEN usage_quantity ELSE 0 END) as jobs_dbus ",
                "FROM system.billing.usage ",
                "WHERE (sku_name LIKE '%JOBS%' OR sku_name LIKE '%DLT%') ",
                "  AND usage_date >= date_sub(current_date(), 90) "
            ]
        },
        {
            "name": "lakebase_detail",
            "displayName": "Lakebase & Storage Detail",
            "queryLines": [
                "SELECT ",
                "  CASE ",
                "    WHEN sku_name LIKE '%DATABASE%' THEN 'Lakebase Compute' ",
                "    WHEN sku_name LIKE '%DATABRICKS_STORAGE%' THEN 'Managed Storage' ",
                "    ELSE 'Other' ",
                "  END as cost_category, ",
                "  usage_date, ",
                "  SUM(usage_quantity) as dbus ",
                "FROM system.billing.usage ",
                "WHERE (sku_name LIKE '%DATABASE%' OR sku_name LIKE '%DATABRICKS_STORAGE%') ",
                "  AND usage_date >= date_sub(current_date(), 90) ",
                "GROUP BY ALL "
            ]
        },
        {
            "name": "lakebase_summary",
            "displayName": "Lakebase Summary KPIs",
            "queryLines": [
                "SELECT ",
                "  SUM(usage_quantity) as total_dbus, ",
                "  SUM(CASE WHEN sku_name LIKE '%DATABASE%' THEN usage_quantity ELSE 0 END) as compute_dbus, ",
                "  SUM(CASE WHEN sku_name LIKE '%DATABRICKS_STORAGE%' THEN usage_quantity ELSE 0 END) as storage_dbus ",
                "FROM system.billing.usage ",
                "WHERE (sku_name LIKE '%DATABASE%' OR sku_name LIKE '%DATABRICKS_STORAGE%') ",
                "  AND usage_date >= date_sub(current_date(), 90) "
            ]
        }
    ],
    "pages": [
        {
            "name": "jobs_pipeline",
            "displayName": "Jobs & Pipeline Expenses",
            "pageType": "PAGE_TYPE_CANVAS",
            "layout": [
                {
                    "widget": {
                        "name": "jobs-title",
                        "multilineTextboxSpec": {
                            "lines": ["## Jobs & Pipeline Expenses"]
                        }
                    },
                    "position": {"x": 0, "y": 0, "width": 6, "height": 1}
                },
                {
                    "widget": {
                        "name": "jobs-subtitle",
                        "multilineTextboxSpec": {
                            "lines": ["DBU consumption from Jobs and DLT Pipelines (last 90 days)"]
                        }
                    },
                    "position": {"x": 0, "y": 1, "width": 6, "height": 1}
                },
                {
                    "widget": {
                        "name": "kpi-total-jobs-dbus",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "jobs_summary",
                                "fields": [{"name": "total_dbus", "expression": "`total_dbus`"}],
                                "disaggregated": True
                            }
                        }],
                        "spec": {
                            "version": 2,
                            "widgetType": "counter",
                            "encodings": {
                                "value": {"fieldName": "total_dbus", "displayName": "Total DBUs"}
                            },
                            "frame": {"showTitle": True, "title": "Total DBUs"}
                        }
                    },
                    "position": {"x": 0, "y": 2, "width": 2, "height": 3}
                },
                {
                    "widget": {
                        "name": "kpi-jobs-dbus",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "jobs_summary",
                                "fields": [{"name": "jobs_dbus", "expression": "`jobs_dbus`"}],
                                "disaggregated": True
                            }
                        }],
                        "spec": {
                            "version": 2,
                            "widgetType": "counter",
                            "encodings": {
                                "value": {"fieldName": "jobs_dbus", "displayName": "Jobs DBUs"}
                            },
                            "frame": {"showTitle": True, "title": "Jobs DBUs"}
                        }
                    },
                    "position": {"x": 2, "y": 2, "width": 2, "height": 3}
                },
                {
                    "widget": {
                        "name": "kpi-pipeline-dbus",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "jobs_summary",
                                "fields": [{"name": "pipeline_dbus", "expression": "`pipeline_dbus`"}],
                                "disaggregated": True
                            }
                        }],
                        "spec": {
                            "version": 2,
                            "widgetType": "counter",
                            "encodings": {
                                "value": {"fieldName": "pipeline_dbus", "displayName": "Pipeline DBUs"}
                            },
                            "frame": {"showTitle": True, "title": "Pipeline (DLT) DBUs"}
                        }
                    },
                    "position": {"x": 4, "y": 2, "width": 2, "height": 3}
                },
                {
                    "widget": {
                        "name": "jobs-daily-trend",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "jobs_pipeline_detail",
                                "fields": [
                                    {"name": "daily(usage_date)", "expression": "DATE_TRUNC(\"DAY\", `usage_date`)"},
                                    {"name": "sum(dbus)", "expression": "SUM(`dbus`)"},
                                    {"name": "workload_type", "expression": "`workload_type`"}
                                ],
                                "disaggregated": False
                            }
                        }],
                        "spec": {
                            "version": 3,
                            "widgetType": "line",
                            "encodings": {
                                "x": {"fieldName": "daily(usage_date)", "scale": {"type": "temporal"}, "displayName": "Date"},
                                "y": {"fieldName": "sum(dbus)", "scale": {"type": "quantitative"}, "displayName": "DBUs"},
                                "color": {"fieldName": "workload_type", "scale": {"type": "categorical"}, "displayName": "Workload Type"}
                            },
                            "frame": {"showTitle": True, "title": "Daily DBU Trend by Workload Type"}
                        }
                    },
                    "position": {"x": 0, "y": 5, "width": 6, "height": 6}
                },
                {
                    "widget": {
                        "name": "top-jobs-bar",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "jobs_top_spenders",
                                "fields": [
                                    {"name": "resource_name", "expression": "`resource_name`"},
                                    {"name": "total_dbus", "expression": "`total_dbus`"},
                                    {"name": "workload_type", "expression": "`workload_type`"}
                                ],
                                "disaggregated": True
                            }
                        }],
                        "spec": {
                            "version": 3,
                            "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "total_dbus", "scale": {"type": "quantitative"}, "displayName": "Total DBUs"},
                                "y": {"fieldName": "resource_name", "scale": {"type": "categorical"}, "displayName": "Resource"},
                                "color": {"fieldName": "workload_type", "scale": {"type": "categorical"}, "displayName": "Type"}
                            },
                            "frame": {"showTitle": True, "title": "Top 8 Most Expensive Jobs/Pipelines"}
                        }
                    },
                    "position": {"x": 0, "y": 11, "width": 6, "height": 6}
                },
                {
                    "widget": {
                        "name": "jobs-compute-mode-header",
                        "multilineTextboxSpec": {
                            "lines": ["### Compute Mode Distribution"]
                        }
                    },
                    "position": {"x": 0, "y": 17, "width": 6, "height": 1}
                },
                {
                    "widget": {
                        "name": "compute-mode-pie",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "jobs_pipeline_detail",
                                "fields": [
                                    {"name": "compute_mode", "expression": "`compute_mode`"},
                                    {"name": "sum(dbus)", "expression": "SUM(`dbus`)"}
                                ],
                                "disaggregated": False
                            }
                        }],
                        "spec": {
                            "version": 3,
                            "widgetType": "pie",
                            "encodings": {
                                "angle": {"fieldName": "sum(dbus)", "scale": {"type": "quantitative"}, "displayName": "DBUs"},
                                "color": {"fieldName": "compute_mode", "scale": {"type": "categorical"}, "displayName": "Compute Mode"}
                            },
                            "frame": {"showTitle": True, "title": "Serverless vs Classic"}
                        }
                    },
                    "position": {"x": 0, "y": 18, "width": 3, "height": 5}
                },
                {
                    "widget": {
                        "name": "workload-type-pie",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "jobs_pipeline_detail",
                                "fields": [
                                    {"name": "workload_type", "expression": "`workload_type`"},
                                    {"name": "sum(dbus)", "expression": "SUM(`dbus`)"}
                                ],
                                "disaggregated": False
                            }
                        }],
                        "spec": {
                            "version": 3,
                            "widgetType": "pie",
                            "encodings": {
                                "angle": {"fieldName": "sum(dbus)", "scale": {"type": "quantitative"}, "displayName": "DBUs"},
                                "color": {"fieldName": "workload_type", "scale": {"type": "categorical"}, "displayName": "Workload Type"}
                            },
                            "frame": {"showTitle": True, "title": "Jobs vs Pipelines"}
                        }
                    },
                    "position": {"x": 3, "y": 18, "width": 3, "height": 5}
                }
            ]
        },
        {
            "name": "lakebase_storage",
            "displayName": "Lakebase & Storage Spending",
            "pageType": "PAGE_TYPE_CANVAS",
            "layout": [
                {
                    "widget": {
                        "name": "lakebase-title",
                        "multilineTextboxSpec": {
                            "lines": ["## Lakebase & Storage Spending"]
                        }
                    },
                    "position": {"x": 0, "y": 0, "width": 6, "height": 1}
                },
                {
                    "widget": {
                        "name": "lakebase-subtitle",
                        "multilineTextboxSpec": {
                            "lines": ["DBU consumption from Lakebase Compute and Managed Storage (last 90 days)"]
                        }
                    },
                    "position": {"x": 0, "y": 1, "width": 6, "height": 1}
                },
                {
                    "widget": {
                        "name": "kpi-total-lakebase-dbus",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "lakebase_summary",
                                "fields": [{"name": "total_dbus", "expression": "`total_dbus`"}],
                                "disaggregated": True
                            }
                        }],
                        "spec": {
                            "version": 2,
                            "widgetType": "counter",
                            "encodings": {
                                "value": {"fieldName": "total_dbus", "displayName": "Total DBUs"}
                            },
                            "frame": {"showTitle": True, "title": "Total Lakebase + Storage DBUs"}
                        }
                    },
                    "position": {"x": 0, "y": 2, "width": 2, "height": 3}
                },
                {
                    "widget": {
                        "name": "kpi-compute-dbus",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "lakebase_summary",
                                "fields": [{"name": "compute_dbus", "expression": "`compute_dbus`"}],
                                "disaggregated": True
                            }
                        }],
                        "spec": {
                            "version": 2,
                            "widgetType": "counter",
                            "encodings": {
                                "value": {"fieldName": "compute_dbus", "displayName": "Compute DBUs"}
                            },
                            "frame": {"showTitle": True, "title": "Lakebase Compute DBUs"}
                        }
                    },
                    "position": {"x": 2, "y": 2, "width": 2, "height": 3}
                },
                {
                    "widget": {
                        "name": "kpi-storage-dbus",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "lakebase_summary",
                                "fields": [{"name": "storage_dbus", "expression": "`storage_dbus`"}],
                                "disaggregated": True
                            }
                        }],
                        "spec": {
                            "version": 2,
                            "widgetType": "counter",
                            "encodings": {
                                "value": {"fieldName": "storage_dbus", "displayName": "Storage DBUs"}
                            },
                            "frame": {"showTitle": True, "title": "Managed Storage DBUs"}
                        }
                    },
                    "position": {"x": 4, "y": 2, "width": 2, "height": 3}
                },
                {
                    "widget": {
                        "name": "lakebase-daily-trend",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "lakebase_detail",
                                "fields": [
                                    {"name": "daily(usage_date)", "expression": "DATE_TRUNC(\"DAY\", `usage_date`)"},
                                    {"name": "sum(dbus)", "expression": "SUM(`dbus`)"},
                                    {"name": "cost_category", "expression": "`cost_category`"}
                                ],
                                "disaggregated": False
                            }
                        }],
                        "spec": {
                            "version": 3,
                            "widgetType": "line",
                            "encodings": {
                                "x": {"fieldName": "daily(usage_date)", "scale": {"type": "temporal"}, "displayName": "Date"},
                                "y": {"fieldName": "sum(dbus)", "scale": {"type": "quantitative"}, "displayName": "DBUs"},
                                "color": {"fieldName": "cost_category", "scale": {"type": "categorical"}, "displayName": "Cost Category"}
                            },
                            "frame": {"showTitle": True, "title": "Daily DBU Trend: Compute vs Storage"}
                        }
                    },
                    "position": {"x": 0, "y": 5, "width": 6, "height": 6}
                },
                {
                    "widget": {
                        "name": "lakebase-weekly-bar",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "lakebase_detail",
                                "fields": [
                                    {"name": "weekly(usage_date)", "expression": "DATE_TRUNC(\"WEEK\", `usage_date`)"},
                                    {"name": "sum(dbus)", "expression": "SUM(`dbus`)"},
                                    {"name": "cost_category", "expression": "`cost_category`"}
                                ],
                                "disaggregated": False
                            }
                        }],
                        "spec": {
                            "version": 3,
                            "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "weekly(usage_date)", "scale": {"type": "temporal"}, "displayName": "Week"},
                                "y": {"fieldName": "sum(dbus)", "scale": {"type": "quantitative"}, "displayName": "DBUs"},
                                "color": {"fieldName": "cost_category", "scale": {"type": "categorical"}, "displayName": "Cost Category"}
                            },
                            "frame": {"showTitle": True, "title": "Weekly Spend: Compute vs Storage"}
                        }
                    },
                    "position": {"x": 0, "y": 11, "width": 6, "height": 6}
                },
                {
                    "widget": {
                        "name": "lakebase-split-header",
                        "multilineTextboxSpec": {
                            "lines": ["### Cost Distribution"]
                        }
                    },
                    "position": {"x": 0, "y": 17, "width": 6, "height": 1}
                },
                {
                    "widget": {
                        "name": "lakebase-pie",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "lakebase_detail",
                                "fields": [
                                    {"name": "cost_category", "expression": "`cost_category`"},
                                    {"name": "sum(dbus)", "expression": "SUM(`dbus`)"}
                                ],
                                "disaggregated": False
                            }
                        }],
                        "spec": {
                            "version": 3,
                            "widgetType": "pie",
                            "encodings": {
                                "angle": {"fieldName": "sum(dbus)", "scale": {"type": "quantitative"}, "displayName": "DBUs"},
                                "color": {"fieldName": "cost_category", "scale": {"type": "categorical"}, "displayName": "Category"}
                            },
                            "frame": {"showTitle": True, "title": "Lakebase Compute vs Managed Storage"}
                        }
                    },
                    "position": {"x": 0, "y": 18, "width": 3, "height": 5}
                },
                {
                    "widget": {
                        "name": "lakebase-table",
                        "queries": [{
                            "name": "main_query",
                            "query": {
                                "datasetName": "lakebase_detail",
                                "fields": [
                                    {"name": "usage_date", "expression": "`usage_date`"},
                                    {"name": "cost_category", "expression": "`cost_category`"},
                                    {"name": "sum(dbus)", "expression": "SUM(`dbus`)"}
                                ],
                                "disaggregated": False
                            }
                        }],
                        "spec": {
                            "version": 2,
                            "widgetType": "table",
                            "encodings": {
                                "columns": [
                                    {"fieldName": "usage_date", "displayName": "Date"},
                                    {"fieldName": "cost_category", "displayName": "Category"},
                                    {"fieldName": "sum(dbus)", "displayName": "DBUs"}
                                ]
                            },
                            "frame": {"showTitle": True, "title": "Daily Lakebase Spend Detail"}
                        }
                    },
                    "position": {"x": 3, "y": 18, "width": 3, "height": 5}
                }
            ]
        },
        {
            "name": "filters",
            "displayName": "Filters",
            "pageType": "PAGE_TYPE_GLOBAL_FILTERS",
            "layout": [
                {
                    "widget": {
                        "name": "filter-date-range",
                        "queries": [{
                            "name": "ds_jobs_date",
                            "query": {
                                "datasetName": "jobs_pipeline_detail",
                                "fields": [{"name": "usage_date", "expression": "`usage_date`"}],
                                "disaggregated": False
                            }
                        }],
                        "spec": {
                            "version": 2,
                            "widgetType": "filter-date-range-picker",
                            "encodings": {
                                "fields": [{
                                    "fieldName": "usage_date",
                                    "displayName": "Date Range",
                                    "queryName": "ds_jobs_date"
                                }]
                            },
                            "frame": {"showTitle": True, "title": "Date Range"}
                        }
                    },
                    "position": {"x": 0, "y": 0, "width": 2, "height": 2}
                },
                {
                    "widget": {
                        "name": "filter-workload-type",
                        "queries": [{
                            "name": "ds_jobs_wl",
                            "query": {
                                "datasetName": "jobs_pipeline_detail",
                                "fields": [{"name": "workload_type", "expression": "`workload_type`"}],
                                "disaggregated": False
                            }
                        }],
                        "spec": {
                            "version": 2,
                            "widgetType": "filter-multi-select",
                            "encodings": {
                                "fields": [{
                                    "fieldName": "workload_type",
                                    "displayName": "Workload Type",
                                    "queryName": "ds_jobs_wl"
                                }]
                            },
                            "frame": {"showTitle": True, "title": "Workload Type"}
                        }
                    },
                    "position": {"x": 2, "y": 0, "width": 2, "height": 2}
                },
                {
                    "widget": {
                        "name": "filter-cost-category",
                        "queries": [{
                            "name": "ds_lakebase_cat",
                            "query": {
                                "datasetName": "lakebase_detail",
                                "fields": [{"name": "cost_category", "expression": "`cost_category`"}],
                                "disaggregated": False
                            }
                        }],
                        "spec": {
                            "version": 2,
                            "widgetType": "filter-multi-select",
                            "encodings": {
                                "fields": [{
                                    "fieldName": "cost_category",
                                    "displayName": "Cost Category",
                                    "queryName": "ds_lakebase_cat"
                                }]
                            },
                            "frame": {"showTitle": True, "title": "Cost Category"}
                        }
                    },
                    "position": {"x": 4, "y": 0, "width": 2, "height": 2}
                }
            ]
        }
    ]
}

# Deploy
payload = {
    "display_name": "Jobs, Pipelines & Lakebase Expenses",
    "parent_path": parent_path,
    "warehouse_id": warehouse_id,
    "serialized_dashboard": json.dumps(dashboard_def)
}

result = api_call("post", "/api/2.0/lakeview/dashboards", payload)
dashboard_id = result.get("dashboard_id", "unknown")
print(f"Dashboard ID: {dashboard_id}")
print(f"URL: https://dbc-b82e6ab0-c52e.cloud.databricks.com/dashboardsv3/{dashboard_id}")

# Publish it
pub_result = api_call("post", f"/api/2.0/lakeview/dashboards/{dashboard_id}/published", {
    "embed_credentials": True,
    "warehouse_id": warehouse_id
})
print(f"Published: {pub_result}")
