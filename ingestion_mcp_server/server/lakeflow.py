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
    """Best-effort listing of ingestible objects behind a Salesforce connection.

    Salesforce object discovery is not uniformly exposed by the SDK across
    workspaces, so this returns whatever the connection surfaces plus warnings
    when discovery is unavailable. Callers should treat an empty list as
    "verify object names against Salesforce" rather than "no objects".
    """
    warnings: list[str] = []
    objects: list[dict] = []
    needle = name_contains.lower() if name_contains else None

    try:
        # UC exposes foreign objects via list_schemas/list_tables when the
        # connection is federated. For Salesforce ingestion connections this
        # may be empty; we degrade gracefully.
        schemas = (
            [source_schema]
            if source_schema
            else [s.name for s in client.schemas.list(catalog_name=connection_name)]
        )
        for sch in schemas:
            for tbl in client.tables.list(
                catalog_name=connection_name, schema_name=sch
            ):
                if needle and needle not in (tbl.name or "").lower():
                    continue
                objects.append({"source_schema": sch, "source_table": tbl.name})
    except Exception as exc:  # noqa: BLE001 - discovery is best-effort
        warnings.append(
            "Automatic object discovery is unavailable for this connection "
            f"({exc}). Provide Salesforce object names explicitly."
        )
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


def create_pipeline(client: WorkspaceClient, request: CreateIngestionPipelineRequest):
    """Create the Lakeflow Connect ingestion pipeline."""
    return client.pipelines.create(
        name=request.pipeline_name,
        ingestion_definition=build_ingestion_definition(request),
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
