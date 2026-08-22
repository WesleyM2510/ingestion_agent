"""FastMCP + FastAPI application.

``stateless_http=True`` is required for a custom Databricks App connected to
Chat in Genie. The MCP endpoint is mounted at ``/mcp``.
"""

from fastapi import FastAPI
from fastmcp import FastMCP

from .tools import register_tools

mcp = FastMCP("salesforce-lakeflow-ingestion-agent")
register_tools(mcp)

mcp_app = mcp.http_app(
    stateless_http=True,
    json_response=True,
)

app = FastAPI(
    title="Salesforce Lakeflow Ingestion Agent MCP Server",
    lifespan=mcp_app.lifespan,
    routes=mcp_app.routes,
)


@app.get("/")
def root() -> dict:
    return {
        "service": "salesforce-lakeflow-ingestion-agent",
        "mcp_endpoint": "/mcp",
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
