"""Pydantic request/response models for the Salesforce Lakeflow MCP tools.

These models define the *tool contract*. Business logic lives in ``lakeflow.py``;
the tool functions in ``tools.py`` stay thin.
"""

from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


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
    """Optional Lakeflow Job schedule for periodic refresh."""

    cron_expression: str = Field(
        description="Quartz cron expression, e.g. '0 0 2 * * ?' for daily 02:00."
    )
    timezone: str = Field(
        default="America/Sao_Paulo", description="Timezone id for the schedule."
    )


# --- validate_salesforce_ingestion ---------------------------------------


class ValidateRequest(BaseModel):
    connection_name: str
    destination_catalog: str
    destination_schema: str
    objects: list[str] = Field(min_length=1, max_length=250)


class ValidateResponse(BaseModel):
    valid: bool
    connection_status: str | None = None
    objects_found: list[str] = Field(default_factory=list)
    objects_missing: list[str] = Field(default_factory=list)
    permission_errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


# --- plan_salesforce_ingestion --------------------------------------------


class PlanRequest(BaseModel):
    pipeline_name: str
    connection_name: str
    destination_catalog: str
    destination_schema: str
    objects: list[SalesforceObject] = Field(min_length=1, max_length=250)
    schedule: Schedule | None = None


class PlanResponse(BaseModel):
    plan_id: str
    expires_at: str
    requires_confirmation: bool = True
    pipeline_payload: dict
    job_payload: dict | None = None
    destination_tables: list[str]
    warnings: list[str] = Field(default_factory=list)


# --- create_salesforce_ingestion ------------------------------------------


class CreateRequest(BaseModel):
    plan_id: str
    confirmation: Literal["CREATE"] = Field(
        description="Must be the literal 'CREATE' to authorize provisioning."
    )
    idempotency_key: str = Field(
        description="Caller-supplied key so retries do not create duplicates."
    )


class CreateResponse(BaseModel):
    status: str
    pipeline_id: str | None = None
    job_id: str | None = None
    pipeline_name: str | None = None
    tables: list[str] = Field(default_factory=list)
    next_action: str | None = None
    error: str | None = None


# --- get_ingestion_status --------------------------------------------------


class StatusRequest(BaseModel):
    pipeline_id: str
    include_recent_updates: bool = True


class StatusResponse(BaseModel):
    pipeline_id: str
    name: str | None = None
    state: str | None = None
    latest_updates: list[dict] = Field(default_factory=list)
