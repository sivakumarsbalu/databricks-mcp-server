"""SQL warehouse tools for Databricks MCP Server."""

import asyncio
import json
from typing import Any

from mcp.server import Server
from mcp.types import Tool, TextContent

from databricks.sdk.service.sql import StatementState

from ..config import get_client, databricks, get_security_config
from ..security import SQLQueryValidator


def register_sql_tools(server: Server):
    """Register SQL warehouse tools with the MCP server."""

    @server.list_tools()
    async def list_sql_tools() -> list[Tool]:
        """Return SQL-related tools."""
        return [
            Tool(
                name="databricks_list_warehouses",
                description="List all SQL warehouses in the workspace",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": [],
                },
            ),
            Tool(
                name="databricks_get_warehouse",
                description="Get details about a specific SQL warehouse",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "warehouse_id": {
                            "type": "string",
                            "description": "The ID of the warehouse",
                        },
                    },
                    "required": ["warehouse_id"],
                },
            ),
            Tool(
                name="databricks_start_warehouse",
                description="Start a stopped SQL warehouse",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "warehouse_id": {
                            "type": "string",
                            "description": "The ID of the warehouse to start",
                        },
                    },
                    "required": ["warehouse_id"],
                },
            ),
            Tool(
                name="databricks_stop_warehouse",
                description="Stop a running SQL warehouse",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "warehouse_id": {
                            "type": "string",
                            "description": "The ID of the warehouse to stop",
                        },
                    },
                    "required": ["warehouse_id"],
                },
            ),
            Tool(
                name="databricks_execute_sql",
                description="Execute a SQL query and return results. Use for SELECT queries to fetch data.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "SQL query to execute",
                        },
                        "warehouse_id": {
                            "type": "string",
                            "description": "Warehouse ID. If not provided, uses default warehouse.",
                        },
                        "catalog": {
                            "type": "string",
                            "description": "Catalog to use for the query",
                        },
                        "schema": {
                            "type": "string",
                            "description": "Schema to use for the query",
                        },
                        "max_rows": {
                            "type": "integer",
                            "description": "Maximum rows to return",
                            "default": 1000,
                        },
                    },
                    "required": ["query"],
                },
            ),
            Tool(
                name="databricks_explain_sql",
                description="Get the execution plan for a SQL query",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "SQL query to explain",
                        },
                        "warehouse_id": {
                            "type": "string",
                            "description": "Warehouse ID",
                        },
                    },
                    "required": ["query"],
                },
            ),
            Tool(
                name="databricks_get_query_history",
                description="Get recent query history",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "max_results": {
                            "type": "integer",
                            "description": "Maximum number of queries to return",
                            "default": 25,
                        },
                        "warehouse_id": {
                            "type": "string",
                            "description": "Filter by warehouse ID",
                        },
                    },
                    "required": [],
                },
            ),
        ]

    return {
        "databricks_list_warehouses": list_warehouses,
        "databricks_get_warehouse": get_warehouse,
        "databricks_start_warehouse": start_warehouse,
        "databricks_stop_warehouse": stop_warehouse,
        "databricks_execute_sql": execute_sql,
        "databricks_explain_sql": explain_sql,
        "databricks_get_query_history": get_query_history,
    }


async def list_warehouses(arguments: dict[str, Any]) -> list[TextContent]:
    """List all SQL warehouses."""
    client = get_client()

    warehouses = list(client.warehouses.list())

    result = []
    for wh in warehouses:
        wh_info = {
            "id": wh.id,
            "name": wh.name,
            "state": wh.state.value if wh.state else "UNKNOWN",
            "cluster_size": wh.cluster_size,
            "min_num_clusters": wh.min_num_clusters,
            "max_num_clusters": wh.max_num_clusters,
            "auto_stop_mins": wh.auto_stop_mins,
            "warehouse_type": wh.warehouse_type.value if wh.warehouse_type else None,
        }
        result.append(wh_info)

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def get_warehouse(arguments: dict[str, Any]) -> list[TextContent]:
    """Get details about a specific warehouse."""
    client = get_client()
    warehouse_id = arguments["warehouse_id"]

    wh = client.warehouses.get(id=warehouse_id)

    result = {
        "id": wh.id,
        "name": wh.name,
        "state": wh.state.value if wh.state else "UNKNOWN",
        "cluster_size": wh.cluster_size,
        "min_num_clusters": wh.min_num_clusters,
        "max_num_clusters": wh.max_num_clusters,
        "auto_stop_mins": wh.auto_stop_mins,
        "warehouse_type": wh.warehouse_type.value if wh.warehouse_type else None,
        "num_clusters": wh.num_clusters,
        "num_active_sessions": wh.num_active_sessions,
        "jdbc_url": wh.jdbc_url,
    }

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def start_warehouse(arguments: dict[str, Any]) -> list[TextContent]:
    """Start a SQL warehouse."""
    client = get_client()
    warehouse_id = arguments["warehouse_id"]

    client.warehouses.start(id=warehouse_id)

    return [
        TextContent(
            type="text",
            text=f"SQL warehouse {warehouse_id} is starting. Use databricks_get_warehouse to check status.",
        )
    ]


async def stop_warehouse(arguments: dict[str, Any]) -> list[TextContent]:
    """Stop a SQL warehouse."""
    client = get_client()
    warehouse_id = arguments["warehouse_id"]

    client.warehouses.stop(id=warehouse_id)

    return [TextContent(type="text", text=f"SQL warehouse {warehouse_id} is stopping.")]


async def execute_sql(arguments: dict[str, Any]) -> list[TextContent]:
    """Execute a SQL query and return results."""
    client = get_client()
    security = get_security_config()

    query = arguments["query"]
    warehouse_id = arguments.get("warehouse_id") or databricks.get_default_warehouse_id()
    catalog = arguments.get("catalog")
    schema = arguments.get("schema")
    max_rows = arguments.get("max_rows", 1000)

    # Apply max rows limit from security config
    max_rows = min(max_rows, security.max_sql_result_rows)

    # Validate query in safe mode
    if security.safe_mode:
        validator = SQLQueryValidator()
        is_allowed, reason = validator.validate_for_safe_mode(query)
        if not is_allowed:
            return [
                TextContent(
                    type="text",
                    text=f"Error: {reason}",
                )
            ]

    if not warehouse_id:
        return [
            TextContent(
                type="text",
                text="Error: No warehouse specified and no running warehouse found. Please provide a warehouse_id or start a warehouse.",
            )
        ]

    # Execute statement
    statement = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=query,
        catalog=catalog,
        schema=schema,
        row_limit=max_rows,
        wait_timeout="50s",
    )

    # Wait for completion if still pending (using async sleep to not block event loop)
    max_wait = 60  # seconds
    waited = 0
    while statement.status and statement.status.state in [
        StatementState.PENDING,
        StatementState.RUNNING,
    ]:
        if waited >= max_wait:
            return [
                TextContent(
                    type="text",
                    text=f"Query still running after {max_wait}s. Statement ID: {statement.statement_id}",
                )
            ]
        await asyncio.sleep(2)
        waited += 2
        statement = client.statement_execution.get_statement(
            statement_id=statement.statement_id
        )

    # Check for errors
    if statement.status and statement.status.state == StatementState.FAILED:
        error_msg = "Query failed"
        if statement.status.error:
            error_msg = f"Query failed: {statement.status.error.message}"
        return [TextContent(type="text", text=error_msg)]

    # Format results
    result = {"status": "SUCCESS", "statement_id": statement.statement_id}

    if statement.manifest:
        result["row_count"] = statement.manifest.total_row_count
        result["columns"] = [
            {"name": col.name, "type": col.type_name}
            for col in (statement.manifest.schema.columns or [])
        ]

    if statement.result and statement.result.data_array:
        result["data"] = statement.result.data_array

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def explain_sql(arguments: dict[str, Any]) -> list[TextContent]:
    """Get the execution plan for a SQL query."""
    client = get_client()

    query = arguments["query"]
    warehouse_id = arguments.get("warehouse_id") or databricks.get_default_warehouse_id()

    if not warehouse_id:
        return [
            TextContent(
                type="text",
                text="Error: No warehouse specified and no running warehouse found.",
            )
        ]

    # Execute EXPLAIN query
    explain_query = f"EXPLAIN {query}"

    statement = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=explain_query,
        wait_timeout="30s",
    )

    # Wait for completion (using async sleep to not block event loop)
    max_wait = 30
    waited = 0
    while statement.status and statement.status.state in [
        StatementState.PENDING,
        StatementState.RUNNING,
    ]:
        if waited >= max_wait:
            break
        await asyncio.sleep(1)
        waited += 1
        statement = client.statement_execution.get_statement(
            statement_id=statement.statement_id
        )

    if statement.status and statement.status.state == StatementState.FAILED:
        error_msg = "Explain failed"
        if statement.status.error:
            error_msg = f"Explain failed: {statement.status.error.message}"
        return [TextContent(type="text", text=error_msg)]

    # Extract plan
    if statement.result and statement.result.data_array:
        plan_lines = [row[0] for row in statement.result.data_array if row]
        return [TextContent(type="text", text="\n".join(plan_lines))]

    return [TextContent(type="text", text="No execution plan returned")]


async def get_query_history(arguments: dict[str, Any]) -> list[TextContent]:
    """Get recent query history."""
    client = get_client()

    max_results = arguments.get("max_results", 25)
    warehouse_id = arguments.get("warehouse_id")

    # Build filter
    filter_by = None
    if warehouse_id:
        from databricks.sdk.service.sql import QueryFilter

        filter_by = QueryFilter(warehouse_ids=[warehouse_id])

    queries = client.query_history.list(
        filter_by=filter_by,
        max_results=max_results,
    )

    result = []
    for q in queries:
        query_info = {
            "query_id": q.query_id,
            "status": q.status.value if q.status else "UNKNOWN",
            "query_text": (
                q.query_text[:200] + "..." if q.query_text and len(q.query_text) > 200 else q.query_text
            ),
            "user_name": q.user_name,
            "warehouse_id": q.warehouse_id,
            "execution_end_time_ms": q.execution_end_time_ms,
            "duration_ms": q.duration,
            "rows_produced": q.rows_produced,
        }
        result.append(query_info)

    return [TextContent(type="text", text=json.dumps(result, indent=2))]
