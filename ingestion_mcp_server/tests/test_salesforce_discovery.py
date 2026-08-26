"""Tests for Salesforce source-object discovery (no live org required).

Cover credential resolution from env, live ``describeGlobal`` parsing, the
curated fallback when no credentials are configured, and graceful degradation
when a live call fails.
"""

from unittest import mock

from server import lakeflow, salesforce


def _clear_sf_env(monkeypatch):
    # Ensure no ambient SF_* vars leak into fallback assertions.
    import os

    for key in list(os.environ):
        if key.startswith("SF_"):
            monkeypatch.delenv(key, raising=False)


# --- credential resolution -------------------------------------------------


def test_resolve_credentials_none_without_instance_url(monkeypatch):
    _clear_sf_env(monkeypatch)
    assert salesforce.resolve_credentials("con_salesforce_prd") is None


def test_per_connection_env_wins_over_global(monkeypatch):
    _clear_sf_env(monkeypatch)
    monkeypatch.setenv("SF_INSTANCE_URL", "https://global.my.salesforce.com")
    monkeypatch.setenv(
        "SF_CON_SALESFORCE_PRD_INSTANCE_URL", "https://prd.my.salesforce.com/"
    )
    creds = salesforce.resolve_credentials("con_salesforce_prd")
    assert creds.instance_url == "https://prd.my.salesforce.com"  # trailing / stripped
    assert creds.api_version == salesforce.DEFAULT_API_VERSION


def test_fetch_access_token_prefers_preissued_token():
    creds = salesforce.SalesforceCredentials(
        instance_url="https://x.my.salesforce.com", token="tok"
    )
    assert salesforce.fetch_access_token(creds) == "tok"


def test_fetch_access_token_requires_client_creds_without_token():
    creds = salesforce.SalesforceCredentials(instance_url="https://x.my.salesforce.com")
    try:
        salesforce.fetch_access_token(creds)
    except ValueError as exc:
        assert "CLIENT_ID" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected ValueError")


# --- discovery -------------------------------------------------------------


def test_fallback_returns_common_objects_when_no_creds(monkeypatch):
    _clear_sf_env(monkeypatch)
    names, warnings = salesforce.discover_objects("con_salesforce_prd")
    assert names == salesforce.COMMON_STANDARD_OBJECTS
    assert warnings and "No Salesforce credentials" in warnings[0]


def test_live_discovery_filters_queryable_and_sorts(monkeypatch):
    _clear_sf_env(monkeypatch)
    monkeypatch.setenv("SF_INSTANCE_URL", "https://x.my.salesforce.com")
    monkeypatch.setenv("SF_TOKEN", "tok")
    fake = [
        {"name": "Opportunity", "queryable": True},
        {"name": "Account", "queryable": True},
        {"name": "Invoice__c", "queryable": True},  # custom object surfaced
        {"name": "NotQueryable", "queryable": False},  # dropped
    ]
    with mock.patch.object(salesforce, "describe_global", return_value=fake):
        names, warnings = salesforce.discover_objects("con_salesforce_prd")
    assert names == ["Account", "Invoice__c", "Opportunity"]
    assert warnings == []


def test_name_contains_filter_is_case_insensitive(monkeypatch):
    _clear_sf_env(monkeypatch)
    names, _ = salesforce.discover_objects("con_salesforce_prd", name_contains="OPP")
    assert names == ["Opportunity", "OpportunityLineItem"]


def test_live_failure_degrades_to_fallback(monkeypatch):
    _clear_sf_env(monkeypatch)
    monkeypatch.setenv("SF_INSTANCE_URL", "https://x.my.salesforce.com")
    monkeypatch.setenv("SF_TOKEN", "tok")
    with mock.patch.object(
        salesforce, "describe_global", side_effect=ValueError("boom")
    ):
        names, warnings = salesforce.discover_objects("con_salesforce_prd")
    assert names == salesforce.COMMON_STANDARD_OBJECTS
    assert warnings and "Live Salesforce discovery failed" in warnings[0]


# --- lakeflow adapter wiring -----------------------------------------------


def test_list_source_objects_labels_schema_and_uses_discovery(monkeypatch):
    _clear_sf_env(monkeypatch)
    objects, warnings = lakeflow.list_source_objects(
        client=None, connection_name="con_salesforce_prd", source_schema="sfdc"
    )
    assert objects[0]["source_schema"] == "sfdc"
    assert objects[0]["source_table"] == "Account"
    assert warnings  # fallback warning present
