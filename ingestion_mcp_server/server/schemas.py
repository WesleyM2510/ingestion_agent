"""Pydantic request/response models for the Salesforce Lakeflow MCP tools.

These models define the *tool contract*. Business logic lives in ``lakeflow.py``;
the tool functions in ``tools.py`` stay thin.

This MCP maps to a single source (Salesforce). Tool surface:
  Read  : list_connections, list_source_objects, validate_destination
  Write : create_connection, create_ingestion_pipeline, schedule_pipeline,
          trigger_update

Every write tool requires an explicit ``confirmation`` token and an
``idempotency_key`` so provisioning never happens on inferred intent alone.
Orchestration/routing across sources is the job of an external supervisor
(e.g. an Agent Bricks Multi-Agent Supervisor), not this server.
"""

from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field

# The literal a caller must send to authorize any mutating operation.
CONFIRM_TOKEN = "CONFIRM"


class ScdType(str, Enum):
    SCD_TYPE_1 = "SCD_TYPE_1"
    SCD_TYPE_2 = "SCD_TYPE_2"
    APPEND_ONLY = "APPEND_ONLY"


class SalesforceObject(BaseModel):
    """A single Salesforce object to ingest into Unity Catalog."""

    source_table: str = Field(description="Salesforce object name, e.g. Account")
    source_schema: str = Field(
        default="salesforce",
        description="Source schema within the Salesforce connection.",
    )
    destination_table: str | None = Field(
        default=None,
        description="Override the destination table name. Defaults to the "
        "lower-cased source table.",
    )
    scd_type: ScdType | None = Field(
        default=None, description="Optional SCD handling for the destination table."
    )


class Schedule(BaseModel):
    """A Lakeflow Job schedule for periodic refresh."""

    cron_expression: str = Field(
        description="Quartz cron expression, e.g. '0 0 2 * * ?' for daily 02:00."
    )
    timezone: str = Field(
        default="America/Sao_Paulo", description="Timezone id for the schedule."
    )


# --- READ: list_connections ------------------------------------------------


class ListConnectionsRequest(BaseModel):
    connection_type: str | None = Field(
        default=None,
        description="Optional filter, e.g. 'SALESFORCE'. Case-insensitive.",
    )


class ConnectionInfo(BaseModel):
    name: str
    connection_type: str | None = None
    comment: str | None = None
    owner: str | None = None
    allowed: bool = Field(
        default=True, description="Whether the connection is in the server allowlist."
    )


class ListConnectionsResponse(BaseModel):
    connections: list[ConnectionInfo] = Field(default_factory=list)


# --- READ: list_source_objects ---------------------------------------------


class ListSourceObjectsRequest(BaseModel):
    connection_name: str
    source_schema: str | None = Field(
        default=None, description="Optional source schema to scope the listing."
    )
    name_contains: str | None = Field(
        default=None, description="Optional case-insensitive substring filter."
    )


class SourceObjectInfo(BaseModel):
    source_schema: str
    source_table: str


class ListSourceObjectsResponse(BaseModel):
    connection_name: str
    objects: list[SourceObjectInfo] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


# --- READ: validate_destination --------------------------------------------


class ValidateDestinationRequest(BaseModel):
    connection_name: str
    destination_catalog: str
    destination_schema: str
    objects: list[str] = Field(min_length=1, max_length=250)


class ValidateDestinationResponse(BaseModel):
    valid: bool
    connection_status: str | None = None
    objects_found: list[str] = Field(default_factory=list)
    objects_missing: list[str] = Field(default_factory=list)
    permission_errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


# --- WRITE: create_connection ----------------------------------------------


class CreateConnectionRequest(BaseModel):
    name: str = Field(description="Name for the new UC connection.")
    connection_type: str = Field(
        default="SALESFORCE",
        description="UC connection type. Salesforce OAuth is the default.",
    )
    options: dict[str, str] = Field(
        default_factory=dict,
        description="Non-secret connection options (e.g. host). Secrets/OAuth are "
        "handled by the AI Gateway, never passed here in plaintext.",
    )
    comment: str | None = None
    confirmation: Literal["CONFIRM"] = Field(
        description="Must be 'CONFIRM' to authorize creating the connection."
    )
    idempotency_key: str = Field(
        description="Caller-supplied key so retries do not create duplicates."
    )


class CreateConnectionResponse(BaseModel):
    status: str
    connection_name: str | None = None
    connection_type: str | None = None
    error: str | None = None


# --- WRITE: create_ingestion_pipeline --------------------------------------


class CreateIngestionPipelineRequest(BaseModel):
    pipeline_name: str
    connection_name: str
    destination_catalog: str
    destination_schema: str
    objects: list[SalesforceObject] = Field(min_length=1, max_length=250)
    confirmation: Literal["CONFIRM"] = Field(
        description="Must be 'CONFIRM' to authorize creating the pipeline."
    )
    idempotency_key: str = Field(
        description="Caller-supplied key so retries do not create duplicates."
    )


class CreateIngestionPipelineResponse(BaseModel):
    status: str
    pipeline_id: str | None = None
    pipeline_name: str | None = None
    tables: list[str] = Field(default_factory=list)
    next_action: str | None = None
    error: str | None = None


# --- WRITE: schedule_pipeline ----------------------------------------------


class SchedulePipelineRequest(BaseModel):
    pipeline_id: str
    pipeline_name: str = Field(
        description="Used to name the refresh Job, e.g. '<name>_schedule'."
    )
    schedule: Schedule
    confirmation: Literal["CONFIRM"] = Field(
        description="Must be 'CONFIRM' to authorize creating the schedule."
    )
    idempotency_key: str = Field(
        description="Caller-supplied key so retries do not create duplicates."
    )


class SchedulePipelineResponse(BaseModel):
    status: str
    job_id: str | None = None
    pipeline_id: str | None = None
    cron_expression: str | None = None
    error: str | None = None


# --- WRITE: trigger_update -------------------------------------------------


class TriggerUpdateRequest(BaseModel):
    pipeline_id: str
    full_refresh: bool = Field(
        default=False, description="Reprocess all data instead of an incremental run."
    )
    confirmation: Literal["CONFIRM"] = Field(
        description="Must be 'CONFIRM' to authorize starting an update."
    )
    idempotency_key: str = Field(
        description="Caller-supplied key so retries do not start duplicate runs."
    )


class TriggerUpdateResponse(BaseModel):
    status: str
    pipeline_id: str | None = None
    update_id: str | None = None
    full_refresh: bool | None = None
    error: str | None = None


# --- get_pipeline_status (observability) -----------------------------------


class StatusRequest(BaseModel):
    pipeline_id: str
    include_recent_updates: bool = True


class StatusResponse(BaseModel):
    pipeline_id: str
    name: str | None = None
    state: str | None = None
    latest_updates: list[dict] = Field(default_factory=list)
