"""Server configuration and Unity Catalog allowlists.

The security model requires the MCP server (not the LLM) to enforce which
catalogs/schemas and Salesforce objects may be targeted. Configure these via
environment variables in ``app.yaml``.
"""

from __future__ import annotations

import os


def _csv_env(name: str) -> set[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return set()
    return {item.strip() for item in raw.split(",") if item.strip()}


# Comma-separated allowlists. Empty set means "no allowlist configured";
# callers should treat that as deny-by-default in production but the MVP
# allows it so local testing works out of the box.
ALLOWED_CATALOGS: set[str] = _csv_env("ALLOWED_CATALOGS")
ALLOWED_SCHEMAS: set[str] = _csv_env("ALLOWED_SCHEMAS")  # "catalog.schema" entries
ALLOWED_CONNECTIONS: set[str] = _csv_env("ALLOWED_CONNECTIONS")


def catalog_allowed(catalog: str) -> bool:
    return not ALLOWED_CATALOGS or catalog in ALLOWED_CATALOGS


def schema_allowed(catalog: str, schema: str) -> bool:
    return not ALLOWED_SCHEMAS or f"{catalog}.{schema}" in ALLOWED_SCHEMAS


def connection_allowed(connection_name: str) -> bool:
    return not ALLOWED_CONNECTIONS or connection_name in ALLOWED_CONNECTIONS


def allowlist_errors(catalog: str, schema: str, connection: str) -> list[str]:
    errors: list[str] = []
    if not catalog_allowed(catalog):
        errors.append(f"Destination catalog '{catalog}' is not in the allowlist.")
    if not schema_allowed(catalog, schema):
        errors.append(
            f"Destination schema '{catalog}.{schema}' is not in the allowlist."
        )
    if not connection_allowed(connection):
        errors.append(f"Connection '{connection}' is not in the allowlist.")
    return errors
