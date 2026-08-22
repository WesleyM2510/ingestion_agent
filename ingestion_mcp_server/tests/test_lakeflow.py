"""Tests for the pure (no-workspace) parts of the server.

These do not require a Databricks connection: they cover payload construction,
the idempotency store, and allowlist enforcement.
"""

from server import lakeflow
from server.schemas import (
    CreateIngestionPipelineRequest,
    SalesforceObject,
    Schedule,
    ScdType,
)
from server.store import IdempotencyStore


def _pipeline_request(**overrides) -> CreateIngestionPipelineRequest:
    base = dict(
        pipeline_name="salesforce_to_uc",
        connection_name="salesforce_prod_oauth",
        destination_catalog="main",
        destination_schema="salesforce_raw",
        objects=[
            SalesforceObject(source_table="Account"),
            SalesforceObject(
                source_table="Opportunity",
                destination_table="opportunity",
                scd_type=ScdType.SCD_TYPE_2,
            ),
        ],
        confirmation="CONFIRM",
        idempotency_key="k-test",
    )
    base.update(overrides)
    return CreateIngestionPipelineRequest(**base)


# --- payload construction --------------------------------------------------


def test_destination_tables_defaults_to_lowercase():
    req = _pipeline_request()
    tables = lakeflow.destination_tables(
        req.destination_catalog, req.destination_schema, req.objects
    )
    assert tables == [
        "main.salesforce_raw.account",
        "main.salesforce_raw.opportunity",
    ]


def test_pipeline_payload_shape():
    req = _pipeline_request()
    payload = lakeflow.build_pipeline_payload(req)
    assert payload["name"] == "salesforce_to_uc"
    objs = payload["ingestion_definition"]["objects"]
    assert objs[0]["table"]["source_table"] == "Account"
    assert objs[0]["table"]["destination_table"] == "account"
    # scd_type only present when specified
    assert "scd_type" not in objs[0]["table"]
    assert objs[1]["table"]["scd_type"] == "SCD_TYPE_2"


def test_job_payload_shape():
    job = lakeflow.build_job_payload(
        "salesforce_to_uc", "01ef", Schedule(cron_expression="0 0 2 * * ?")
    )
    assert job["schedule"]["quartz_cron_expression"] == "0 0 2 * * ?"
    assert job["schedule"]["timezone_id"] == "America/Sao_Paulo"
    assert job["tasks"][0]["pipeline_task"]["pipeline_id"] == "01ef"


# --- idempotency store -----------------------------------------------------


def test_idempotency_cache():
    store = IdempotencyStore()
    assert store.seen_idempotency_key("k1") is None
    store.record_idempotency("k1", {"status": "CREATED"})
    assert store.seen_idempotency_key("k1") == {"status": "CREATED"}
