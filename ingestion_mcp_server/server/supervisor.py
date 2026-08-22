"""Multi-Agent Supervisor.

The supervisor sits above the per-source MCP tools (one MCP maps to a single
source; this repo hosts the Salesforce source). It:

  1. **Routes** a gathered natural-language goal to the right ingestion tools.
  2. **Plans** an ordered, reviewable sequence of steps (reads first, then the
     mutating create/schedule/trigger steps).
  3. **Keeps humans in the loop**: ``build_plan`` never mutates anything, and
     ``execute`` runs the write steps only when the caller re-confirms against a
     stored ``plan_id``.

Planning is pure (no workspace calls) so it is unit-testable. Execution binds to
a stored plan and delegates each write step to ``lakeflow.py``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from databricks.sdk import WorkspaceClient

from . import lakeflow
from .config import allowlist_errors
from .schemas import (
    PlanStep,
    StepResult,
    SupervisorExecuteResponse,
    SupervisorGoal,
    SupervisorPlanResponse,
)

logger = logging.getLogger("salesforce-lakeflow-mcp")

PLAN_TTL_MINUTES = 30


def _pipeline_name(goal: SupervisorGoal) -> str:
    return goal.pipeline_name or f"{goal.destination_schema}_ingestion"


def _object_args(goal: SupervisorGoal) -> list[dict]:
    return [o.model_dump(exclude_none=True) for o in goal.objects]


def build_plan(goal: SupervisorGoal) -> SupervisorPlanResponse:
    """Route a goal into an ordered, non-executing plan. Pure — no SDK calls."""
    name = _pipeline_name(goal)
    tables = lakeflow.destination_tables(
        goal.destination_catalog, goal.destination_schema, goal.objects
    )
    warnings = allowlist_errors(
        goal.destination_catalog, goal.destination_schema, goal.connection_name
    )

    steps: list[PlanStep] = []
    n = 0

    # Read step: always validate the destination before proposing writes.
    n += 1
    steps.append(
        PlanStep(
            step=n,
            tool="validate_destination",
            mutates=False,
            summary=(
                f"Validate connection '{goal.connection_name}' and destination "
                f"{goal.destination_catalog}.{goal.destination_schema}."
            ),
            arguments={
                "connection_name": goal.connection_name,
                "destination_catalog": goal.destination_catalog,
                "destination_schema": goal.destination_schema,
                "objects": [o.source_table for o in goal.objects],
            },
        )
    )

    # Optional write step: create the connection if the user asked us to.
    if goal.create_connection_if_missing:
        n += 1
        steps.append(
            PlanStep(
                step=n,
                tool="create_connection",
                mutates=True,
                summary=f"Create Salesforce UC connection '{goal.connection_name}'.",
                arguments={
                    "name": goal.connection_name,
                    "connection_type": "SALESFORCE",
                },
            )
        )

    # Write step: create the ingestion pipeline.
    n += 1
    steps.append(
        PlanStep(
            step=n,
            tool="create_ingestion_pipeline",
            mutates=True,
            summary=f"Create Lakeflow ingestion pipeline '{name}' → {len(tables)} table(s).",
            arguments={
                "pipeline_name": name,
                "connection_name": goal.connection_name,
                "destination_catalog": goal.destination_catalog,
                "destination_schema": goal.destination_schema,
                "objects": _object_args(goal),
            },
        )
    )

    # Optional write step: schedule periodic refresh.
    if goal.schedule:
        n += 1
        steps.append(
            PlanStep(
                step=n,
                tool="schedule_pipeline",
                mutates=True,
                summary=(
                    f"Schedule refresh Job '{name}_schedule' "
                    f"({goal.schedule.cron_expression} {goal.schedule.timezone})."
                ),
                arguments={
                    "pipeline_name": name,
                    "schedule": goal.schedule.model_dump(),
                },
            )
        )

    # Optional write step: trigger the first run.
    if goal.run_after_create:
        n += 1
        steps.append(
            PlanStep(
                step=n,
                tool="trigger_update",
                mutates=True,
                summary=f"Trigger the first update of '{name}'.",
                arguments={"full_refresh": False},
            )
        )

    expires = datetime.now(timezone.utc) + timedelta(minutes=PLAN_TTL_MINUTES)
    return SupervisorPlanResponse(
        plan_id="pending",
        expires_at=expires.isoformat().replace("+00:00", "Z"),
        requires_confirmation=True,
        steps=steps,
        destination_tables=tables,
        warnings=warnings,
    )


def execute(client: WorkspaceClient, goal: SupervisorGoal) -> SupervisorExecuteResponse:
    """Run the write steps of a confirmed plan, threading created IDs forward.

    Stops at the first failing step and returns partial results so the human can
    see exactly how far provisioning got.
    """
    name = _pipeline_name(goal)
    tables = lakeflow.destination_tables(
        goal.destination_catalog, goal.destination_schema, goal.objects
    )
    results: list[StepResult] = []
    pipeline_id: str | None = None
    job_id: str | None = None
    step_no = 0

    def fail(tool: str, exc: Exception) -> SupervisorExecuteResponse:
        logger.exception("supervisor step '%s' failed", tool)
        results.append(
            StepResult(step=step_no, tool=tool, status="FAILED", error=str(exc))
        )
        return SupervisorExecuteResponse(
            plan_id="",
            status="FAILED",
            steps=results,
            pipeline_id=pipeline_id,
            job_id=job_id,
            tables=tables,
            error=f"Step '{tool}' failed: {exc}",
        )

    # create_connection (optional)
    if goal.create_connection_if_missing:
        step_no += 1
        try:
            conn = lakeflow.create_connection(
                client, goal.connection_name, "SALESFORCE", {}, None
            )
            results.append(
                StepResult(
                    step=step_no,
                    tool="create_connection",
                    status="CREATED",
                    detail={"connection_name": getattr(conn, "name", goal.connection_name)},
                )
            )
        except Exception as exc:  # noqa: BLE001
            return fail("create_connection", exc)

    # create_ingestion_pipeline
    step_no += 1
    try:
        from .schemas import CreateIngestionPipelineRequest

        pipe_req = CreateIngestionPipelineRequest(
            pipeline_name=name,
            connection_name=goal.connection_name,
            destination_catalog=goal.destination_catalog,
            destination_schema=goal.destination_schema,
            objects=goal.objects,
            confirmation="CONFIRM",
            idempotency_key="supervisor",
        )
        pipeline = lakeflow.create_pipeline(client, pipe_req)
        pipeline_id = pipeline.pipeline_id
        results.append(
            StepResult(
                step=step_no,
                tool="create_ingestion_pipeline",
                status="CREATED",
                detail={"pipeline_id": pipeline_id, "tables": tables},
            )
        )
    except Exception as exc:  # noqa: BLE001
        return fail("create_ingestion_pipeline", exc)

    # schedule_pipeline (optional)
    if goal.schedule:
        step_no += 1
        try:
            job = lakeflow.create_schedule(client, name, pipeline_id, goal.schedule)
            job_id = str(job.job_id) if job else None
            results.append(
                StepResult(
                    step=step_no,
                    tool="schedule_pipeline",
                    status="CREATED",
                    detail={"job_id": job_id},
                )
            )
        except Exception as exc:  # noqa: BLE001
            return fail("schedule_pipeline", exc)

    # trigger_update (optional)
    if goal.run_after_create:
        step_no += 1
        try:
            update_id = lakeflow.trigger_update(client, pipeline_id, full_refresh=False)
            results.append(
                StepResult(
                    step=step_no,
                    tool="trigger_update",
                    status="STARTED",
                    detail={"update_id": update_id},
                )
            )
        except Exception as exc:  # noqa: BLE001
            return fail("trigger_update", exc)

    return SupervisorExecuteResponse(
        plan_id="",
        status="COMPLETED",
        steps=results,
        pipeline_id=pipeline_id,
        job_id=job_id,
        tables=tables,
    )
