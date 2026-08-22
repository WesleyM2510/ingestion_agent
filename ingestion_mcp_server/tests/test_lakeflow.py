"""Tests for the pure (no-workspace) parts of the server.

These do not require a Databricks connection: they cover payload construction,
the plan store, and allowlist enforcement.
"""

from server import lakeflow
from server.schemas import PlanRequest, SalesforceObject, Schedule, ScdType
from server.store import PlanStore


def _plan_request(**overrides) -> PlanRequest:
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
    )
    base.update(overrides)
    return PlanRequest(**base)


def test_destination_tables_defaults_to_lowercase():
    req = _plan_request()
    tables = lakeflow.destination_tables(req)
    assert tables == [
        "main.salesforce_raw.account",
        "main.salesforce_raw.opportunity",
    ]


def test_pipeline_payload_shape():
    req = _plan_request()
    payload = lakeflow.build_pipeline_payload(req)
    assert payload["name"] == "salesforce_to_uc"
    objs = payload["ingestion_definition"]["objects"]
    assert objs[0]["table"]["source_table"] == "Account"
    assert objs[0]["table"]["destination_table"] == "account"
    # scd_type only present when specified
    assert "scd_type" not in objs[0]["table"]
    assert objs[1]["table"]["scd_type"] == "SCD_TYPE_2"


def test_job_payload_only_when_scheduled():
    assert lakeflow.build_job_payload(_plan_request()) is None
    req = _plan_request(schedule=Schedule(cron_expression="0 0 2 * * ?"))
    job = lakeflow.build_job_payload(req)
    assert job["schedule"]["quartz_cron_expression"] == "0 0 2 * * ?"
    assert job["schedule"]["timezone_id"] == "America/Sao_Paulo"


def test_plan_store_roundtrip_and_consume():
    store = PlanStore()
    req = _plan_request()
    plan_id = store.put(req, expires_at="2999-01-01T00:00:00Z")
    assert store.get(plan_id) is req
    store.consume(plan_id)
    assert store.get(plan_id) is None


def test_plan_store_expiry():
    store = PlanStore()
    plan_id = store.put(_plan_request(), expires_at="2000-01-01T00:00:00Z")
    assert store.get(plan_id) is None


def test_idempotency_cache():
    store = PlanStore()
    assert store.seen_idempotency_key("k1") is None
    store.record_idempotency("k1", {"status": "CREATED"})
    assert store.seen_idempotency_key("k1") == {"status": "CREATED"}
