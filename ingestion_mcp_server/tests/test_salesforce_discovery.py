"""Tests for Salesforce source-object discovery (no live org required).

Cover credential resolution from env, live ``describeGlobal`` parsing, the
curated fallback when no credentials are configured, and graceful degradation
when a live call fails.
"""

import base64
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
    names, warnings = salesforce.discover_objects(None, "con_salesforce_prd")
    assert names == salesforce.COMMON_STANDARD_OBJECTS
    assert warnings and "No working Salesforce credentials found" in warnings[0]


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
        names, warnings = salesforce.discover_objects(None, "con_salesforce_prd")
    assert names == ["Account", "Invoice__c", "Opportunity"]
    assert warnings == []


def test_name_contains_filter_is_case_insensitive(monkeypatch):
    _clear_sf_env(monkeypatch)
    names, _ = salesforce.discover_objects(None, "con_salesforce_prd", name_contains="OPP")
    assert names == ["Opportunity", "OpportunityLineItem"]


def test_live_failure_degrades_to_fallback(monkeypatch):
    _clear_sf_env(monkeypatch)
    monkeypatch.setenv("SF_INSTANCE_URL", "https://x.my.salesforce.com")
    monkeypatch.setenv("SF_TOKEN", "tok")
    with mock.patch.object(
        salesforce, "describe_global", side_effect=ValueError("boom")
    ):
        names, warnings = salesforce.discover_objects(None, "con_salesforce_prd")
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


# --- secret scope resolution ------------------------------------------------


def test_resolve_credentials_from_secret_scope_success(monkeypatch):
    """Secret scope provides creds -> live discovery used."""
    _clear_sf_env(monkeypatch)

    # Mock a fake client with secrets API.
    fake_client = mock.MagicMock()

    # Mock the three secret reads, each returning a base64-encoded value.
    def make_secret_response(plaintext: str):
        resp = mock.MagicMock()
        resp.value = base64.b64encode(plaintext.encode("utf-8")).decode("utf-8")
        return resp

    fake_client.secrets.get_secret.side_effect = [
        make_secret_response("https://test.my.salesforce.com"),  # INSTANCE_URL
        make_secret_response("client_id_123"),  # CLIENT_ID
        make_secret_response("client_secret_456"),  # CLIENT_SECRET
    ]

    creds = salesforce.resolve_credentials_from_secret_scope(fake_client)
    assert creds is not None
    assert creds.instance_url == "https://test.my.salesforce.com"
    assert creds.client_id == "client_id_123"
    assert creds.client_secret == "client_secret_456"
    assert creds.api_version == salesforce.DEFAULT_API_VERSION


def test_resolve_credentials_from_secret_scope_missing_instance_url(monkeypatch):
    """Empty INSTANCE_URL in secret scope -> None."""
    _clear_sf_env(monkeypatch)

    fake_client = mock.MagicMock()

    def make_secret_response(plaintext: str):
        resp = mock.MagicMock()
        resp.value = base64.b64encode(plaintext.encode("utf-8")).decode("utf-8")
        return resp

    fake_client.secrets.get_secret.return_value = make_secret_response("")

    creds = salesforce.resolve_credentials_from_secret_scope(fake_client)
    assert creds is None


def test_resolve_credentials_from_secret_scope_read_error(monkeypatch):
    """Secret scope read fails (missing key, permission denied, etc.) -> None."""
    _clear_sf_env(monkeypatch)

    fake_client = mock.MagicMock()
    fake_client.secrets.get_secret.side_effect = Exception("RESOURCE_DOES_NOT_EXIST")

    creds = salesforce.resolve_credentials_from_secret_scope(fake_client)
    assert creds is None


def test_resolve_credentials_from_secret_scope_client_none(monkeypatch):
    """Client is None -> None (skip secret scope path)."""
    _clear_sf_env(monkeypatch)

    creds = salesforce.resolve_credentials_from_secret_scope(None)
    assert creds is None


def test_secret_scope_provides_creds_live_discovery_used(monkeypatch):
    """Secret scope credentials used for live discovery."""
    _clear_sf_env(monkeypatch)

    fake_client = mock.MagicMock()

    def make_secret_response(plaintext: str):
        resp = mock.MagicMock()
        resp.value = base64.b64encode(plaintext.encode("utf-8")).decode("utf-8")
        return resp

    # Use a proper function for side_effect to handle multiple calls correctly.
    def get_secret_side_effect(scope, key):
        if key == "INSTANCE_URL":
            return make_secret_response("https://test.my.salesforce.com")
        elif key == "CLIENT_ID":
            return make_secret_response("client_id")
        elif key == "CLIENT_SECRET":
            return make_secret_response("client_secret")
        raise Exception(f"Unexpected key: {key}")

    fake_client.secrets.get_secret.side_effect = get_secret_side_effect

    fake_sobjects = [
        {"name": "Account", "queryable": True},
        {"name": "Contact", "queryable": True},
        {"name": "Invoice__c", "queryable": True},
    ]

    # Mock both describe_global and fetch_access_token to avoid HTTP calls.
    with mock.patch.object(salesforce, "fetch_access_token", return_value="mock_token"):
        with mock.patch.object(salesforce, "describe_global", return_value=fake_sobjects):
            names, warnings = salesforce.discover_objects(fake_client, "con_salesforce_prd")

    assert names == ["Account", "Contact", "Invoice__c"]
    assert warnings == []  # live discovery succeeded, no fallback warning


def test_secret_scope_missing_env_fallback_used(monkeypatch):
    """Secret scope missing AND no env -> curated fallback with scope warning."""
    _clear_sf_env(monkeypatch)

    fake_client = mock.MagicMock()
    fake_client.secrets.get_secret.side_effect = Exception("scope not found")

    names, warnings = salesforce.discover_objects(fake_client, "con_salesforce_prd")

    assert names == salesforce.COMMON_STANDARD_OBJECTS
    assert warnings
    assert "SALESFORCE_MCP_SCOPE" in warnings[0]
    assert "create a Databricks secret scope" in warnings[0]


def test_env_credentials_fallback_when_secret_scope_unavailable(monkeypatch):
    """Secret scope missing, but env vars present -> env vars used."""
    _clear_sf_env(monkeypatch)
    monkeypatch.setenv("SF_INSTANCE_URL", "https://env.my.salesforce.com")
    monkeypatch.setenv("SF_TOKEN", "env_token")

    fake_client = mock.MagicMock()
    fake_client.secrets.get_secret.side_effect = Exception("scope not found")

    fake_sobjects = [
        {"name": "Account", "queryable": True},
        {"name": "Opportunity", "queryable": True},
    ]

    with mock.patch.object(salesforce, "describe_global", return_value=fake_sobjects):
        names, warnings = salesforce.discover_objects(fake_client, "con_salesforce_prd")

    assert names == ["Account", "Opportunity"]
    assert warnings == []  # env vars used, no warning


def test_base64_decoding_of_secret_values(monkeypatch):
    """Verify base64 decoding is correct."""
    _clear_sf_env(monkeypatch)

    fake_client = mock.MagicMock()

    # Create responses with specific base64-encoded values.
    instance_url_b64 = base64.b64encode(b"https://acme.my.salesforce.com").decode("utf-8")
    client_id_b64 = base64.b64encode(b"xyz789").decode("utf-8")
    client_secret_b64 = base64.b64encode(b"secret@123!").decode("utf-8")

    def make_response(value: str):
        resp = mock.MagicMock()
        resp.value = value
        return resp

    fake_client.secrets.get_secret.side_effect = [
        make_response(instance_url_b64),
        make_response(client_id_b64),
        make_response(client_secret_b64),
    ]

    creds = salesforce.resolve_credentials_from_secret_scope(fake_client)

    assert creds.instance_url == "https://acme.my.salesforce.com"
    assert creds.client_id == "xyz789"
    assert creds.client_secret == "secret@123!"
