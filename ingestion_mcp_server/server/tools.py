"""MCP tool definitions for the Salesforce Lakeflow source.

These functions are thin wrappers: they validate input, enforce allowlists,
require explicit confirmation on writes, and delegate the actual Databricks work
to ``lakeflow.py``. The supervisor tools delegate to ``supervisor.py``.

Tool surface (this MCP maps to a single source: Salesforce):
  Read  : list_connections, list_source_objects, validate_destination
  Write : create_connection, create_ingestion_pipeline, schedule_pipeline,
          trigger_update
  Supervisor: supervisor_plan, supervisor_execute
  Observability: get_ingestion_status
"""

from __future__ import annotations

import logging

from databricks.sdk import WorkspaceClient

from . import lakeflow, supervisor
from .config import allowlist_errors, connection_allowed
from .schemas import (
    ConnectionInfo,
    CreateConnectionRequest,
    CreateConnectionResponse,
    CreateIngestionPipelineRequest,
    CreateIngestionPipelineResponse,
    ListConnectionsRequest,
    ListConnectionsResponse,
    ListSourceObjectsRequest,
    ListSourceObjectsResponse,
    SchedulePipelineRequest,
    SchedulePipelineResponse,
    SourceObjectInfo,
    StatusRequest,
    StatusResponse,
    SupervisorExecuteRequest,
    SupervisorPlanRequest,
    TriggerUpdateRequest,
    TriggerUpdateResponse,
    ValidateDestinationRequest,
    ValidateDestinationResponse,
)
from .store import plan_store

logger = logging.getLogger("salesforce-lakeflow-mcp")


def _client() -> WorkspaceClient:
    # Uses the App's configured identity (default credential chain).
    return WorkspaceClient()


def register_tools(mcp) -> None:
    # === READ ===============================================================

    @mcp.tool()
    def list_connections(request: ListConnectionsRequest) -> dict:
        """List Unity Catalog connections available to this source. Read-only.

        Marks each connection with whether it passes the server allowlist.
        """
        try:
            raw = lakeflow.list_connections(_client(), request.connection_type)
        except Exception as exc:  # noqa: BLE001
            return {"error": f"Connection listing failed: {exc}"}
        connections = [
            ConnectionInfo(allowed=connection_allowed(c["name"]), **c) for c in raw
        ]
        return ListConnectionsResponse(connections=connections).model_dump()

    @mcp.tool()
    def list_source_objects(request: ListSourceObjectsRequest) -> dict:
        """List ingestible Salesforce objects behind a connection. Read-only.

        Object discovery is best-effort; when unavailable the response carries a
        warning and the caller should supply object names explicitly.
        """
        try:
            objects, warnings = lakeflow.list_source_objects(
                _client(),
                request.connection_name,
                request.source_schema,
                request.name_contains,
            )
        except Exception as exc:  # noqa: BLE001
            return {"error": f"Object listing failed: {exc}"}
        return ListSourceObjectsResponse(
            connection_name=request.connection_name,
            objects=[SourceObjectInfo(**o) for o in objects],
            warnings=warnings,
        ).model_dump()

    @mcp.tool()
    def validate_destination(request: ValidateDestinationRequest) -> dict:
        """Validate connection + destination WITHOUT creating anything. Read-only.

        Checks the destination allowlist and the connection state.
        """
        errors = allowlist_errors(
            request.destination_catalog,
            request.destination_schema,
            request.connection_name,
        )
        connection_status: str | None = None
        try:
            connection_status = lakeflow.validate_connection(
                _client(), request.connection_name
            )
        except Exception as exc:  # noqa: BLE001 - surface as a validation error
            errors.append(f"Connection lookup failed: {exc}")

        return ValidateDestinationResponse(
            valid=not errors,
            connection_status=connection_status,
            objects_found=request.objects,
            objects_missing=[],
            permission_errors=errors,
            warnings=[],
        ).model_dump()

    # === WRITE (each requires confirmation == 'CONFIRM') ====================

    @mcp.tool()
    def create_connection(request: CreateConnectionRequest) -> dict:
        """Create a Unity Catalog connection for this source.

        Requires ``confirmation == 'CONFIRM'``. Secrets/OAuth are handled by the
        AI Gateway — never pass Salesforce credentials in ``options``.
        """
        if request.confirmation != "CONFIRM":
            return CreateConnectionResponse(
                status="REJECTED", error="Explicit confirmation 'CONFIRM' is required."
            ).model_dump()

        cached = plan_store.seen_idempotency_key(request.idempotency_key)
        if cached is not None:
            return cached

        try:
            conn = lakeflow.create_connection(
                _client(),
                request.name,
                request.connection_type,
                request.options,
                request.comment,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("connection creation failed")
            return CreateConnectionResponse(status="FAILED", error=str(exc)).model_dump()

        result = CreateConnectionResponse(
            status="CREATED",
            connection_name=getattr(conn, "name", request.name),
            connection_type=request.connection_type.upper(),
        ).model_dump()
        plan_store.record_idempotency(request.idempotency_key, result)
        logger.info("created connection %s", request.name)
        return result

    @mcp.tool()
    def create_ingestion_pipeline(request: CreateIngestionPipelineRequest) -> dict:
        """Create a Salesforce Lakeflow Connect ingestion pipeline.

        Requires ``confirmation == 'CONFIRM'``. Idempotent via ``idempotency_key``.
        Enforces the destination/connection allowlist.
        """
        if request.confirmation != "CONFIRM":
            return CreateIngestionPipelineResponse(
                status="REJECTED", error="Explicit confirmation 'CONFIRM' is required."
            ).model_dump()

        cached = plan_store.seen_idempotency_key(request.idempotency_key)
        if cached is not None:
            return cached

        errors = allowlist_errors(
            request.destination_catalog,
            request.destination_schema,
            request.connection_name,
        )
        if errors:
            return CreateIngestionPipelineResponse(
                status="REJECTED", error="; ".join(errors)
            ).model_dump()

        client = _client()
        try:
            pipeline = lakeflow.create_pipeline(client, request)
        except Exception as exc:  # noqa: BLE001
            logger.exception("pipeline creation failed")
            return CreateIngestionPipelineResponse(
                status="FAILED", error=str(exc)
            ).model_dump()

        tables = lakeflow.destination_tables(
            request.destination_catalog, request.destination_schema, request.objects
        )
        result = CreateIngestionPipelineResponse(
            status="CREATED",
            pipeline_id=pipeline.pipeline_id,
            pipeline_name=request.pipeline_name,
            tables=tables,
            next_action="SCHEDULE_OR_TRIGGER",
        ).model_dump()
        plan_store.record_idempotency(request.idempotency_key, result)
        logger.info("created pipeline %s", pipeline.pipeline_id)
        return result

    @mcp.tool()
    def schedule_pipeline(request: SchedulePipelineRequest) -> dict:
        """Create a Lakeflow Job that refreshes a pipeline on a cron schedule.

        Requires ``confirmation == 'CONFIRM'``. Idempotent via ``idempotency_key``.
        """
        if request.confirmation != "CONFIRM":
            return SchedulePipelineResponse(
                status="REJECTED", error="Explicit confirmation 'CONFIRM' is required."
            ).model_dump()

        cached = plan_store.seen_idempotency_key(request.idempotency_key)
        if cached is not None:
            return cached

        try:
            job = lakeflow.create_schedule(
                _client(),
                request.pipeline_name,
                request.pipeline_id,
                request.schedule,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("schedule creation failed")
            return SchedulePipelineResponse(status="FAILED", error=str(exc)).model_dump()

        result = SchedulePipelineResponse(
            status="CREATED",
            job_id=str(job.job_id) if job else None,
            pipeline_id=request.pipeline_id,
            cron_expression=request.schedule.cron_expression,
        ).model_dump()
        plan_store.record_idempotency(request.idempotency_key, result)
        logger.info("scheduled pipeline %s (job %s)", request.pipeline_id, result["job_id"])
        return result

    @mcp.tool()
    def trigger_update(request: TriggerUpdateRequest) -> dict:
        """Start a pipeline update (incremental by default).

        Requires ``confirmation == 'CONFIRM'``. Idempotent via ``idempotency_key``.
        """
        if request.confirmation != "CONFIRM":
            return TriggerUpdateResponse(
                status="REJECTED", error="Explicit confirmation 'CONFIRM' is required."
            ).model_dump()

        cached = plan_store.seen_idempotency_key(request.idempotency_key)
        if cached is not None:
            return cached

        try:
            update_id = lakeflow.trigger_update(
                _client(), request.pipeline_id, request.full_refresh
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("trigger update failed")
            return TriggerUpdateResponse(status="FAILED", error=str(exc)).model_dump()

        result = TriggerUpdateResponse(
            status="STARTED",
            pipeline_id=request.pipeline_id,
            update_id=update_id,
            full_refresh=request.full_refresh,
        ).model_dump()
        plan_store.record_idempotency(request.idempotency_key, result)
        logger.info("triggered update %s for pipeline %s", update_id, request.pipeline_id)
        return result

    # === SUPERVISOR (routing + human-in-the-loop) ===========================

    @mcp.tool()
    def supervisor_plan(request: SupervisorPlanRequest) -> dict:
        """Route a gathered goal into a reviewable, ordered plan. Does NOT mutate.

        Returns a ``plan_id`` that must be passed to ``supervisor_execute`` after
        the user explicitly confirms.
        """
        plan = supervisor.build_plan(request.goal)
        plan_id = plan_store.put(request.goal, plan.expires_at)
        plan.plan_id = plan_id
        return plan.model_dump()

    @mcp.tool()
    def supervisor_execute(request: SupervisorExecuteRequest) -> dict:
        """Execute the write steps of a confirmed plan.

        Only acts on a ``plan_id`` produced by ``supervisor_plan`` and requires
        ``confirmation == 'CONFIRM'``. Idempotent via ``idempotency_key``.
        """
        if request.confirmation != "CONFIRM":
            return {
                "plan_id": request.plan_id,
                "status": "REJECTED",
                "error": "Explicit confirmation 'CONFIRM' is required.",
            }

        cached = plan_store.seen_idempotency_key(request.idempotency_key)
        if cached is not None:
            return cached

        goal = plan_store.get(request.plan_id)
        if goal is None:
            return {
                "plan_id": request.plan_id,
                "status": "REJECTED",
                "error": "Unknown or expired plan_id. Re-run supervisor_plan.",
            }

        result = supervisor.execute(_client(), goal)
        result.plan_id = request.plan_id
        payload = result.model_dump()

        if result.status == "COMPLETED":
            plan_store.consume(request.plan_id)
        plan_store.record_idempotency(request.idempotency_key, payload)
        logger.info(
            "supervisor executed plan %s -> %s", request.plan_id, result.status
        )
        return payload

    # === OBSERVABILITY ======================================================

    @mcp.tool()
    def get_ingestion_status(request: StatusRequest) -> dict:
        """Return the current state and recent updates of a Lakeflow pipeline."""
        try:
            data = lakeflow.get_pipeline_status(
                _client(), request.pipeline_id, request.include_recent_updates
            )
        except Exception as exc:  # noqa: BLE001
            return StatusResponse(
                pipeline_id=request.pipeline_id, state=f"ERROR: {exc}"
            ).model_dump()
        return StatusResponse(**data).model_dump()
