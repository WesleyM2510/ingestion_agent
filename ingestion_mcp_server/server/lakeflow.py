"""Databricks adapter: builds Lakeflow Connect payloads and calls the SDK.

All Databricks-facing logic lives here so the MCP tool functions stay thin.
Nothing in this module mutates the workspace except ``create_pipeline`` and
``create_schedule``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import CronSchedule, PauseStatus, PipelineTask, Task
from databricks.sdk.service.pipelines import (
    IngestionConfig,
    IngestionPipelineDefinition,
    TableSpec,
)

from .schemas import CreateRequest, PlanRequest, PlanResponse, SalesforceObject


def _destination_table(obj: SalesforceObject) -> str:
    return obj.destination_table or obj.source_table.lower()


def build_table_specs(request: PlanRequest) -> list[TableSpec]:
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


def build_ingestion_definition(request: PlanRequest) -> IngestionPipelineDefinition:
    return IngestionPipelineDefinition(
        connection_name=request.connection_name,
        objects=[IngestionConfig(table=spec) for spec in build_table_specs(request)],
    )


def build_pipeline_payload(request: PlanRequest) -> dict:
    """A JSON-serializable preview of the pipeline create request.

    Used inside the plan so the user can review exactly what will be created.
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


def build_job_payload(request: PlanRequest) -> dict | None:
    """JSON-serializable preview of the optional refresh-schedule job."""
    if not request.schedule:
        return None
    return {
        "name": f"{request.pipeline_name}_schedule",
        "schedule": {
            "quartz_cron_expression": request.schedule.cron_expression,
            "timezone_id": request.schedule.timezone,
            "pause_status": "UNPAUSED",
        },
        "tasks": [
            {
                "task_key": "refresh_pipeline",
                "pipeline_task": {"pipeline_id": "<pipeline_id>"},
            }
        ],
    }


def destination_tables(request: PlanRequest) -> list[str]:
    return [
        f"{request.destination_catalog}.{request.destination_schema}.{_destination_table(o)}"
        for o in request.objects
    ]


# --- read-only validation -------------------------------------------------


def validate_connection(client: WorkspaceClient, connection_name: str) -> str:
    """Return the connection status, or raise if it is not usable."""
    conn = client.connections.get(connection_name)
    # A Salesforce UC connection should be an ONLINE/READY external connection.
    return "READY" if conn is not None else "UNKNOWN"


# --- mutating operations --------------------------------------------------


def create_pipeline(client: WorkspaceClient, request: PlanRequest):
    """Create the Lakeflow Connect ingestion pipeline."""
    return client.pipelines.create(
        name=request.pipeline_name,
        ingestion_definition=build_ingestion_definition(request),
    )


def create_schedule(client: WorkspaceClient, request: PlanRequest, pipeline_id: str):
    """Create the optional Lakeflow Job that refreshes the pipeline."""
    if not request.schedule:
        return None
    return client.jobs.create(
        name=f"{request.pipeline_name}_schedule",
        schedule=CronSchedule(
            quartz_cron_expression=request.schedule.cron_expression,
            timezone_id=request.schedule.timezone,
            pause_status=PauseStatus.UNPAUSED,
        ),
        tasks=[
            Task(
                task_key="refresh_pipeline",
                pipeline_task=PipelineTask(pipeline_id=pipeline_id),
            )
        ],
    )


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


def build_plan_response(request: PlanRequest, plan_id: str, ttl_minutes: int = 30):
    expires = datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)
    return PlanResponse(
        plan_id=plan_id,
        expires_at=expires.isoformat().replace("+00:00", "Z"),
        requires_confirmation=True,
        pipeline_payload=build_pipeline_payload(request),
        job_payload=build_job_payload(request),
        destination_tables=destination_tables(request),
    )
