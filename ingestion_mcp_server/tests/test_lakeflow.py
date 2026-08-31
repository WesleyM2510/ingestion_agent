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


# --- existing-pipeline lookup ----------------------------------------------


class _FakePipelines:
    def __init__(self, pipelines):
        self._pipelines = pipelines
        self.filters: list[str] = []

    def list_pipelines(self, filter=None):
        self.filters.append(filter)
        return iter(self._pipelines)


def _client_with_pipelines(pipelines):
    from types import SimpleNamespace

    return SimpleNamespace(pipelines=_FakePipelines(pipelines))


def test_find_pipeline_by_name_exact_match():
    from types import SimpleNamespace

    client = _client_with_pipelines(
        [
            SimpleNamespace(name="salesforce_to_uc_v2", pipeline_id="AAA"),
            SimpleNamespace(name="salesforce_to_uc", pipeline_id="BBB"),
        ]
    )
    assert lakeflow.find_pipeline_by_name(client, "salesforce_to_uc") == {
        "pipeline_id": "BBB",
        "name": "salesforce_to_uc",
    }


def test_find_pipeline_by_name_returns_none_when_absent():
    from types import SimpleNamespace

    client = _client_with_pipelines([SimpleNamespace(name="other", pipeline_id="X")])
    assert lakeflow.find_pipeline_by_name(client, "salesforce_to_uc") is None


def test_find_pipeline_by_name_escapes_like_wildcards():
    client = _client_with_pipelines([])
    lakeflow.find_pipeline_by_name(client, "sf_100%_load")
    assert client.pipelines.filters[0] == "name LIKE 'sf\\_100\\%\\_load'"


# --- idempotency store -----------------------------------------------------


def test_idempotency_cache():
    store = IdempotencyStore()
    assert store.seen_idempotency_key("k1") is None
    store.record_idempotency("k1", {"status": "CREATED"})
    assert store.seen_idempotency_key("k1") == {"status": "CREATED"}


# --- pipeline name generation -----------------------------------------------


def test_generate_pipeline_name_single_object():
    objs = [SalesforceObject(source_table="Account")]
    name = lakeflow.generate_pipeline_name("cielo", "default", objs)
    assert name == "sf_account_cielo_default"


def test_generate_pipeline_name_multiple_objects():
    objs = [
        SalesforceObject(source_table="Account"),
        SalesforceObject(source_table="Contact"),
    ]
    name = lakeflow.generate_pipeline_name("cielo", "default", objs)
    assert name == "sf_account_contact_cielo_default"


def test_generate_pipeline_name_mixed_case():
    objs = [SalesforceObject(source_table="Account")]
    name = lakeflow.generate_pipeline_name("Cielo", "Default", objs)
    assert name == "sf_account_cielo_default"


def test_generate_pipeline_name_custom_objects_with_special_chars():
    objs = [SalesforceObject(source_table="Custom__c")]
    name = lakeflow.generate_pipeline_name("cielo", "default", objs)
    assert name == "sf_custom_c_cielo_default"


def test_generate_pipeline_name_with_spaces_and_hyphens():
    objs = [SalesforceObject(source_table="My-Account")]
    name = lakeflow.generate_pipeline_name("cielo", "default", objs)
    assert name == "sf_my_account_cielo_default"


def test_generate_pipeline_name_long_object_list_triggers_hash():
    # Create a long list of objects that exceeds 60 chars for objects part
    objs = [
        SalesforceObject(source_table=f"VeryLongObjectName{i}") for i in range(10)
    ]
    name = lakeflow.generate_pipeline_name("cielo", "default", objs)
    # Should contain hash: sf_<truncated>_<8hexchars>_cielo_default
    assert name.startswith("sf_")
    assert name.endswith("_cielo_default")
    # Extract the hash part (should be 8 hex chars)
    parts = name.split("_")
    # Format is sf_..._HASH_cielo_default, so HASH is -3 from end
    hash_part = parts[-3]
    assert len(hash_part) == 8
    assert all(c in "0123456789abcdef" for c in hash_part)


def test_generate_pipeline_name_long_list_is_deterministic():
    objs = [
        SalesforceObject(source_table=f"VeryLongObjectName{i}") for i in range(10)
    ]
    name1 = lakeflow.generate_pipeline_name("cielo", "default", objs)
    name2 = lakeflow.generate_pipeline_name("cielo", "default", objs)
    assert name1 == name2
