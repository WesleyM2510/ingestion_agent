"""Databricks adapter: builds Lakeflow Connect payloads and calls the SDK.

This MCP maps to a single source (Salesforce). All Databricks-facing logic lives
here so the MCP tool functions in ``tools.py`` stay thin. Only ``create_*``,
``schedule_pipeline`` and ``trigger_update`` mutate the workspace.
"""

from __future__ import annotations

from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionType
from databricks.sdk.service.jobs import CronSchedule, PauseStatus, PipelineTask, Task
from databricks.sdk.service.pipelines import (
    IngestionConfig,
    IngestionPipelineDefinition,
    TableSpec,
)

from . import salesforce
from .schemas import (
    CreateIngestionPipelineRequest,
    SalesforceObject,
    Schedule,
)


def _destination_table(obj: SalesforceObject) -> str:
    return obj.destination_table or obj.source_table.lower()


# --- payload construction (pure, unit-tested) ------------------------------


def build_table_specs(request: CreateIngestionPipelineRequest) -> list[TableSpec]:
    """Turn the requested objects into SDK ``TableSpec`` objects."""
    specs: list[TableSpec] = []
    for obj in request.objects:
        specs.append(
            TableSpec(
                source_schema=obj.source_schema,
                source_table=obj.source_table,
                destination_catalog=request.destination_catalog,
                destination_schema=request.destination_schema,
                destination_table=_destination_table(obj),
            )
        )
    return specs


def build_ingestion_definition(
    request: CreateIngestionPipelineRequest,
) -> IngestionPipelineDefinition:
    return IngestionPipelineDefinition(
        connection_name=request.connection_name,
        objects=[IngestionConfig(table=spec) for spec in build_table_specs(request)],
    )


def build_pipeline_payload(request: CreateIngestionPipelineRequest) -> dict:
    """A JSON-serializable preview of the pipeline create request.

    Lets a caller review exactly what will be created before confirming.
    """
    return {
        "name": request.pipeline_name,
        "ingestion_definition": {
            "connection_name": request.connection_name,
            "objects": [
                {
                    "table": {
                        "source_schema": obj.source_schema,
                        "source_table": obj.source_table,
                        "destination_catalog": request.destination_catalog,
                        "destination_schema": request.destination_schema,
                        "destination_table": _destination_table(obj),
                        **(
                            {"scd_type": obj.scd_type.value} if obj.scd_type else {}
                        ),
                    }
                }
                for obj in request.objects
            ],
        },
    }


def build_job_payload(pipeline_name: str, pipeline_id: str, schedule: Schedule) -> dict:
    """JSON-serializable preview of a refresh-schedule job."""
    return {
        "name": f"{pipeline_name}_schedule",
        "schedule": {
            "quartz_cron_expression": schedule.cron_expression,
            "timezone_id": schedule.timezone,
            "pause_status": "UNPAUSED",
        },
        "tasks": [
            {
                "task_key": "refresh_pipeline",
                "pipeline_task": {"pipeline_id": pipeline_id},
            }
        ],
    }


def destination_tables(
    catalog: str, schema: str, objects: list[SalesforceObject]
) -> list[str]:
    return [f"{catalog}.{schema}.{_destination_table(o)}" for o in objects]


# --- READ operations -------------------------------------------------------


def list_connections(
    client: WorkspaceClient, connection_type: str | None = None
) -> list[dict]:
    """List UC connections, optionally filtered by type (case-insensitive)."""
    wanted = connection_type.upper() if connection_type else None
    out: list[dict] = []
    for conn in client.connections.list():
        ctype = str(conn.connection_type.value) if conn.connection_type else None
        if wanted and (ctype or "").upper() != wanted:
            continue
        out.append(
            {
                "name": conn.name,
                "connection_type": ctype,
                "comment": conn.comment,
                "owner": conn.owner,
            }
        )
    return out


def validate_connection(client: WorkspaceClient, connection_name: str) -> str:
    """Return the connection status, or raise if it is not usable."""
    conn = client.connections.get(connection_name)
    return "READY" if conn is not None else "UNKNOWN"


def list_source_objects(
    client: WorkspaceClient,
    connection_name: str,
    source_schema: str | None = None,
    name_contains: str | None = None,
) -> tuple[list[dict], list[str]]:
    """List ingestible Salesforce objects behind a connection.

    A Salesforce Lakeflow Connect connection is *not* a federated foreign
    catalog, so it exposes no objects via UC ``list_schemas``/``list_tables``.
    The authoritative source is the Salesforce org itself, queried through its
    ``describeGlobal`` endpoint (see ``server.salesforce``). Live discovery
    requires separately-configured Salesforce connected-app credentials; when
    those are absent the discovery layer returns a curated list of common
    standard objects plus a warning.

    ``source_schema`` labels the returned objects (Lakeflow expects a source
    schema, defaulting to ``objects``); it does not scope the Salesforce
    query. Returns ``(objects, warnings)`` where each object is
    ``{"source_schema": ..., "source_table": <SObject API name>}``.
    """
    schema_label = source_schema or "objects"
    names, warnings = salesforce.discover_objects(connection_name, name_contains)
    objects = [
        {"source_schema": schema_label, "source_table": name} for name in names
    ]
    return objects, warnings


# --- WRITE operations ------------------------------------------------------


def create_connection(
    client: WorkspaceClient,
    name: str,
    connection_type: str,
    options: dict[str, str],
    comment: str | None,
):
    """Create a Unity Catalog connection.

    Secrets/OAuth are handled by the AI Gateway; only non-secret ``options``
    are passed through here.
    """
    return client.connections.create(
        name=name,
        connection_type=ConnectionType(connection_type.upper()),
        options=options,
        comment=comment,
    )


def find_pipeline_by_name(client: WorkspaceClient, name: str) -> dict | None:
    """Return an existing pipeline with an exact name match, or ``None``.

    Pipeline names are not guaranteed unique in Databricks, so the ``name LIKE``
    server filter is only a prefilter; we match the name exactly and return the
    first hit (lowest id, as ``list_pipelines`` defaults to ``id asc``).
    """
    # Escape LIKE wildcards so a name containing % or _ can't over-match.
    escaped = name.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    for p in client.pipelines.list_pipelines(filter=f"name LIKE '{escaped}'"):
        if p.name == name:
            return {"pipeline_id": p.pipeline_id, "name": p.name}
    return None


def create_pipeline(client: WorkspaceClient, request: CreateIngestionPipelineRequest):
    """Create the Lakeflow Connect ingestion pipeline."""
    return client.pipelines.create(
        name=request.pipeline_name,
        ingestion_definition=build_ingestion_definition(request),
        catalog=request.destination_catalog,
        schema=request.destination_schema,
    )


def create_schedule(
    client: WorkspaceClient,
    pipeline_name: str,
    pipeline_id: str,
    schedule: Schedule,
):
    """Create a Lakeflow Job that refreshes the pipeline on a cron schedule."""
    return client.jobs.create(
        name=f"{pipeline_name}_schedule",
        schedule=CronSchedule(
            quartz_cron_expression=schedule.cron_expression,
            timezone_id=schedule.timezone,
            pause_status=PauseStatus.UNPAUSED,
        ),
        tasks=[
            Task(
                task_key="refresh_pipeline",
                pipeline_task=PipelineTask(pipeline_id=pipeline_id),
            )
        ],
    )


def trigger_update(
    client: WorkspaceClient, pipeline_id: str, full_refresh: bool = False
) -> str | None:
    """Start a pipeline update (incremental by default) and return its id."""
    resp = client.pipelines.start_update(pipeline_id, full_refresh=full_refresh)
    return getattr(resp, "update_id", None)


# --- observability ---------------------------------------------------------


def get_pipeline_status(
    client: WorkspaceClient, pipeline_id: str, include_recent_updates: bool
) -> dict:
    pipeline = client.pipelines.get(pipeline_id)
    result = {
        "pipeline_id": pipeline_id,
        "name": pipeline.name,
        "state": str(pipeline.state) if pipeline.state else None,
        "latest_updates": [],
    }
    if include_recent_updates:
        updates = client.pipelines.list_updates(pipeline_id)
        for u in (updates.updates or [])[:5]:
            result["latest_updates"].append(
                {"update_id": u.update_id, "state": str(u.state) if u.state else None}
            )
    return result


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
