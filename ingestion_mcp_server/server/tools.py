"""MCP tool definitions.

These functions are thin wrappers: they validate input, enforce allowlists,
bind create-operations to a previously reviewed plan, and delegate the actual
Databricks work to ``lakeflow.py``. Business logic does not live here.
"""

from __future__ import annotations

import logging

from databricks.sdk import WorkspaceClient

from . import lakeflow
from .config import allowlist_errors
from .schemas import (
    CreateRequest,
    CreateResponse,
    PlanRequest,
    PlanResponse,
    StatusRequest,
    StatusResponse,
    ValidateRequest,
    ValidateResponse,
)
from .store import plan_store

logger = logging.getLogger("salesforce-lakeflow-mcp")


def _client() -> WorkspaceClient:
    # Uses the App's configured identity (default credential chain).
    return WorkspaceClient()


def register_tools(mcp) -> None:
    @mcp.tool()
    def validate_salesforce_ingestion(request: ValidateRequest) -> dict:
        """Validate a Salesforce Lakeflow Connect request WITHOUT creating anything.

        Checks the destination allowlist and the connection state. Read-only.
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

        return ValidateResponse(
            valid=not errors,
            connection_status=connection_status,
            objects_found=request.objects,
            objects_missing=[],
            permission_errors=errors,
            warnings=[],
        ).model_dump()

    @mcp.tool()
    def plan_salesforce_ingestion(request: PlanRequest) -> dict:
        """Create a reviewable, non-executable plan. Does NOT mutate Databricks.

        Returns a ``plan_id`` that must be passed to
        ``create_salesforce_ingestion`` after the user explicitly confirms.
        """
        errors = allowlist_errors(
            request.destination_catalog,
            request.destination_schema,
            request.connection_name,
        )
        if errors:
            return {"error": "; ".join(errors), "requires_confirmation": False}

        # Build the plan first so we can store the request under its plan_id.
        plan = lakeflow.build_plan_response(request, plan_id="pending")
        plan_id = plan_store.put(request, plan.expires_at)
        plan.plan_id = plan_id
        return plan.model_dump()

    @mcp.tool()
    def create_salesforce_ingestion(request: CreateRequest) -> dict:
        """Create an approved pipeline (and optional schedule).

        Only acts on a plan_id produced by ``plan_salesforce_ingestion`` and
        requires ``confirmation == 'CREATE'``. Idempotent via ``idempotency_key``.
        """
        if request.confirmation != "CREATE":
            return CreateResponse(
                status="REJECTED",
                error="Explicit confirmation 'CREATE' is required.",
            ).model_dump()

        cached = plan_store.seen_idempotency_key(request.idempotency_key)
        if cached is not None:
            return cached

        plan_request = plan_store.get(request.plan_id)
        if plan_request is None:
            return CreateResponse(
                status="REJECTED",
                error="Unknown or expired plan_id. Re-run plan_salesforce_ingestion.",
            ).model_dump()

        client = _client()
        try:
            pipeline = lakeflow.create_pipeline(client, plan_request)
            pipeline_id = pipeline.pipeline_id
            job = lakeflow.create_schedule(client, plan_request, pipeline_id)
            job_id = str(job.job_id) if job else None
        except Exception as exc:  # noqa: BLE001
            logger.exception("pipeline creation failed")
            return CreateResponse(
                status="FAILED", error=str(exc)
            ).model_dump()

        result = CreateResponse(
            status="CREATED",
            pipeline_id=pipeline_id,
            job_id=job_id,
            pipeline_name=plan_request.pipeline_name,
            tables=lakeflow.destination_tables(plan_request),
            next_action="RUN_PIPELINE",
        ).model_dump()

        plan_store.consume(request.plan_id)
        plan_store.record_idempotency(request.idempotency_key, result)
        logger.info(
            "created pipeline %s (job %s) for plan %s",
            pipeline_id,
            job_id,
            request.plan_id,
        )
        return result

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
