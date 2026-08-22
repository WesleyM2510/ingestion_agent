"""Tests for the pure (no-workspace) parts of the server.

These do not require a Databricks connection: they cover payload construction,
the plan store, allowlist enforcement, and supervisor planning.
"""

from server import lakeflow, supervisor
from server.schemas import (
    CreateIngestionPipelineRequest,
    SalesforceObject,
    Schedule,
    ScdType,
    SupervisorGoal,
)
from server.store import PlanStore


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


def _goal(**overrides) -> SupervisorGoal:
    base = dict(
        connection_name="salesforce_prod_oauth",
        destination_catalog="main",
        destination_schema="salesforce_raw",
        objects=[
            SalesforceObject(source_table="Account"),
            SalesforceObject(source_table="Opportunity"),
        ],
    )
    base.update(overrides)
    return SupervisorGoal(**base)


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


# --- supervisor planning (pure) --------------------------------------------


def test_plan_minimal_goal_orders_read_then_writes():
    plan = supervisor.build_plan(_goal())
    tools = [s.tool for s in plan.steps]
    # validate first (read), then create pipeline, then default trigger.
    assert tools == [
        "validate_destination",
        "create_ingestion_pipeline",
        "trigger_update",
    ]
    assert plan.steps[0].mutates is False
    assert all(s.mutates for s in plan.steps[1:])
    assert plan.destination_tables == [
        "main.salesforce_raw.account",
        "main.salesforce_raw.opportunity",
    ]


def test_plan_full_goal_includes_connection_and_schedule():
    plan = supervisor.build_plan(
        _goal(
            create_connection_if_missing=True,
            schedule=Schedule(cron_expression="0 0 2 * * ?"),
            pipeline_name="sf_pipe",
        )
    )
    tools = [s.tool for s in plan.steps]
    assert tools == [
        "validate_destination",
        "create_connection",
        "create_ingestion_pipeline",
        "schedule_pipeline",
        "trigger_update",
    ]


def test_plan_default_pipeline_name_from_schema():
    plan = supervisor.build_plan(_goal())
    create = next(s for s in plan.steps if s.tool == "create_ingestion_pipeline")
    assert create.arguments["pipeline_name"] == "salesforce_raw_ingestion"


def test_plan_no_trigger_when_disabled():
    plan = supervisor.build_plan(_goal(run_after_create=False))
    assert "trigger_update" not in [s.tool for s in plan.steps]


# --- plan store ------------------------------------------------------------


def test_plan_store_roundtrip_and_consume():
    store = PlanStore()
    goal = _goal()
    plan_id = store.put(goal, expires_at="2999-01-01T00:00:00Z")
    assert store.get(plan_id) is goal
    store.consume(plan_id)
    assert store.get(plan_id) is None


def test_plan_store_expiry():
    store = PlanStore()
    plan_id = store.put(_goal(), expires_at="2000-01-01T00:00:00Z")
    assert store.get(plan_id) is None


def test_idempotency_cache():
    store = PlanStore()
    assert store.seen_idempotency_key("k1") is None
    store.record_idempotency("k1", {"status": "CREATED"})
    assert store.seen_idempotency_key("k1") == {"status": "CREATED"}
