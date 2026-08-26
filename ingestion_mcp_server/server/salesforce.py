"""Salesforce source-object discovery.

Databricks / Lakeflow Connect exposes no API that enumerates the Salesforce
objects (SObjects) behind a connection: the source of truth is the Salesforce
org itself. The Databricks ingestion UI reads them from Salesforce's
``describeGlobal`` endpoint (``GET /services/data/vXX.0/sobjects/``); this module
does the same.

Credentials: the UC connection's OAuth secret is held by the workspace and is
*not* readable by this app, so discovery uses a *separate* Salesforce connected
app configured via environment variables. Per-connection config wins over a
global default; connection names are upper-cased with non-alphanumerics turned
into ``_`` to form the prefix, e.g. connection ``con_salesforce_prd`` reads:

    SF_CON_SALESFORCE_PRD_INSTANCE_URL   (e.g. https://acme.my.salesforce.com)
    SF_CON_SALESFORCE_PRD_CLIENT_ID
    SF_CON_SALESFORCE_PRD_CLIENT_SECRET
    SF_CON_SALESFORCE_PRD_API_VERSION    (optional, default 60.0)
    SF_CON_SALESFORCE_PRD_TOKEN          (optional pre-issued bearer token;
                                          skips the client-credentials exchange)

Falling back to the unprefixed ``SF_INSTANCE_URL`` / ``SF_CLIENT_ID`` / ... when
a per-connection value is absent.

When no credentials are configured the caller degrades to a curated list of
common standard objects (see ``COMMON_STANDARD_OBJECTS``) rather than returning
nothing. Only live discovery can surface custom ``__c`` objects.
"""

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_API_VERSION = "60.0"
_HTTP_TIMEOUT = 30

# Common Salesforce standard objects, used as a fallback suggestion list when no
# Salesforce credentials are configured for live discovery. This is NOT
# exhaustive and never includes org-specific custom (``__c``) objects.
COMMON_STANDARD_OBJECTS = [
    "Account",
    "Contact",
    "Lead",
    "Opportunity",
    "OpportunityLineItem",
    "Case",
    "Campaign",
    "CampaignMember",
    "User",
    "Product2",
    "Pricebook2",
    "PricebookEntry",
    "Order",
    "OrderItem",
    "Contract",
    "Quote",
    "Task",
    "Event",
    "Asset",
    "Solution",
]


class SalesforceCredentials:
    """Resolved Salesforce connected-app credentials for one connection."""

    def __init__(
        self,
        instance_url: str,
        api_version: str = DEFAULT_API_VERSION,
        client_id: str | None = None,
        client_secret: str | None = None,
        token: str | None = None,
    ) -> None:
        self.instance_url = instance_url.rstrip("/")
        self.api_version = api_version
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = token


def _env_prefix(connection_name: str) -> str:
    return "SF_" + re.sub(r"[^A-Za-z0-9]", "_", connection_name).upper() + "_"


def _resolve(connection_name: str, suffix: str) -> str | None:
    """Per-connection env var wins over the unprefixed global default."""
    return os.getenv(_env_prefix(connection_name) + suffix) or os.getenv(
        "SF_" + suffix
    )


def resolve_credentials(connection_name: str) -> SalesforceCredentials | None:
    """Build credentials from env, or ``None`` if no instance URL is configured."""
    instance_url = _resolve(connection_name, "INSTANCE_URL")
    if not instance_url:
        return None
    return SalesforceCredentials(
        instance_url=instance_url,
        api_version=_resolve(connection_name, "API_VERSION") or DEFAULT_API_VERSION,
        client_id=_resolve(connection_name, "CLIENT_ID"),
        client_secret=_resolve(connection_name, "CLIENT_SECRET"),
        token=_resolve(connection_name, "TOKEN"),
    )


def _http_json(request: urllib.request.Request) -> dict:
    with urllib.request.urlopen(request, timeout=_HTTP_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_access_token(creds: SalesforceCredentials) -> str:
    """Return a bearer token: the pre-issued one, or a client-credentials grant."""
    if creds.token:
        return creds.token
    if not (creds.client_id and creds.client_secret):
        raise ValueError(
            "Salesforce credentials incomplete: need either a TOKEN or both "
            "CLIENT_ID and CLIENT_SECRET for the client-credentials grant."
        )
    body = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": creds.client_id,
            "client_secret": creds.client_secret,
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        f"{creds.instance_url}/services/oauth2/token",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    data = _http_json(req)
    token = data.get("access_token")
    if not token:
        raise ValueError(f"Salesforce token response missing access_token: {data}")
    return token


def describe_global(creds: SalesforceCredentials, token: str) -> list[dict]:
    """Return SObject metadata dicts from Salesforce ``describeGlobal``."""
    req = urllib.request.Request(
        f"{creds.instance_url}/services/data/v{creds.api_version}/sobjects/",
        headers={"Authorization": f"Bearer {token}"},
        method="GET",
    )
    data = _http_json(req)
    return data.get("sobjects", [])


def discover_objects(
    connection_name: str, name_contains: str | None = None
) -> tuple[list[str], list[str]]:
    """Discover ingestible Salesforce object API names for a connection.

    Returns ``(object_names, warnings)``. When credentials are configured this
    performs live discovery (queryable objects only, including custom ``__c``);
    otherwise it returns the curated common-standard-object fallback with a
    warning explaining how to enable full discovery.
    """
    warnings: list[str] = []
    needle = name_contains.lower() if name_contains else None

    def _filter(names: list[str]) -> list[str]:
        if not needle:
            return names
        return [n for n in names if needle in n.lower()]

    creds = resolve_credentials(connection_name)
    if creds is None:
        warnings.append(
            "No Salesforce credentials configured for connection "
            f"'{connection_name}'; returning a curated list of common standard "
            "objects. Set SF_<CONNECTION>_INSTANCE_URL / _CLIENT_ID / "
            "_CLIENT_SECRET (or SF_INSTANCE_URL / ...) to enable live discovery "
            "of all objects, including custom (__c) objects."
        )
        return _filter(list(COMMON_STANDARD_OBJECTS)), warnings

    try:
        token = fetch_access_token(creds)
        sobjects = describe_global(creds, token)
    except (urllib.error.URLError, ValueError, KeyError) as exc:
        warnings.append(
            f"Live Salesforce discovery failed ({exc}); returning common "
            "standard objects instead. Verify the connected-app credentials and "
            "instance URL."
        )
        return _filter(list(COMMON_STANDARD_OBJECTS)), warnings

    names = sorted(
        obj["name"]
        for obj in sobjects
        if obj.get("queryable") and obj.get("name")
    )
    if not names:
        warnings.append(
            "Salesforce returned no queryable objects for this connection."
        )
    return _filter(names), warnings
