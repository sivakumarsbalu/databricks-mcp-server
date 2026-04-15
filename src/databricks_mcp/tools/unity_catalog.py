"""Unity Catalog tools for Databricks MCP Server."""

import json
from typing import Any

from mcp.server import Server
from mcp.types import Tool, TextContent

from ..config import get_client, databricks


def register_unity_catalog_tools(server: Server):
    """Register Unity Catalog tools with the MCP server."""

    @server.list_tools()
    async def list_uc_tools() -> list[Tool]:
        """Return Unity Catalog-related tools."""
        return [
            Tool(
                name="databricks_list_catalogs",
                description="List all catalogs in the Unity Catalog metastore",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": [],
                },
            ),
            Tool(
                name="databricks_get_catalog",
                description="Get details about a specific catalog",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "catalog_name": {
                            "type": "string",
                            "description": "Name of the catalog",
                        },
                    },
                    "required": ["catalog_name"],
                },
            ),
            Tool(
                name="databricks_list_schemas",
                description="List all schemas in a catalog",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "catalog_name": {
                            "type": "string",
                            "description": "Name of the catalog",
                        },
                    },
                    "required": ["catalog_name"],
                },
            ),
            Tool(
                name="databricks_get_schema",
                description="Get details about a specific schema",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "catalog_name": {
                            "type": "string",
                            "description": "Name of the catalog",
                        },
                        "schema_name": {
                            "type": "string",
                            "description": "Name of the schema",
                        },
                    },
                    "required": ["catalog_name", "schema_name"],
                },
            ),
            Tool(
                name="databricks_list_tables",
                description="List all tables in a schema",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "catalog_name": {
                            "type": "string",
                            "description": "Name of the catalog",
                        },
                        "schema_name": {
                            "type": "string",
                            "description": "Name of the schema",
                        },
                    },
                    "required": ["catalog_name", "schema_name"],
                },
            ),
            Tool(
                name="databricks_describe_table",
                description="Get detailed schema and metadata for a table",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "full_name": {
                            "type": "string",
                            "description": "Full table name (catalog.schema.table)",
                        },
                    },
                    "required": ["full_name"],
                },
            ),
            Tool(
                name="databricks_preview_table",
                description="Preview sample data from a table (first N rows)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "full_name": {
                            "type": "string",
                            "description": "Full table name (catalog.schema.table)",
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Number of rows to preview",
                            "default": 10,
                        },
                        "warehouse_id": {
                            "type": "string",
                            "description": "SQL warehouse to use for query",
                        },
                    },
                    "required": ["full_name"],
                },
            ),
            Tool(
                name="databricks_list_volumes",
                description="List volumes in a schema",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "catalog_name": {
                            "type": "string",
                            "description": "Name of the catalog",
                        },
                        "schema_name": {
                            "type": "string",
                            "description": "Name of the schema",
                        },
                    },
                    "required": ["catalog_name", "schema_name"],
                },
            ),
            Tool(
                name="databricks_get_volume",
                description="Get details about a specific volume",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "full_name": {
                            "type": "string",
                            "description": "Full volume name (catalog.schema.volume)",
                        },
                    },
                    "required": ["full_name"],
                },
            ),
            Tool(
                name="databricks_list_functions",
                description="List functions in a schema",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "catalog_name": {
                            "type": "string",
                            "description": "Name of the catalog",
                        },
                        "schema_name": {
                            "type": "string",
                            "description": "Name of the schema",
                        },
                    },
                    "required": ["catalog_name", "schema_name"],
                },
            ),
            Tool(
                name="databricks_search_tables",
                description="Search for tables by name across catalogs",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Search query (table name pattern)",
                        },
                        "catalog_name": {
                            "type": "string",
                            "description": "Optional: limit search to specific catalog",
                        },
                    },
                    "required": ["query"],
                },
            ),
        ]

    return {
        "databricks_list_catalogs": list_catalogs,
        "databricks_get_catalog": get_catalog,
        "databricks_list_schemas": list_schemas,
        "databricks_get_schema": get_schema,
        "databricks_list_tables": list_tables,
        "databricks_describe_table": describe_table,
        "databricks_preview_table": preview_table,
        "databricks_list_volumes": list_volumes,
        "databricks_get_volume": get_volume,
        "databricks_list_functions": list_functions,
        "databricks_search_tables": search_tables,
    }


async def list_catalogs(arguments: dict[str, Any]) -> list[TextContent]:
    """List all catalogs in the metastore."""
    client = get_client()

    catalogs = list(client.catalogs.list())

    result = []
    for cat in catalogs:
        cat_info = {
            "name": cat.name,
            "comment": cat.comment,
            "owner": cat.owner,
            "created_at": cat.created_at,
            "metastore_id": cat.metastore_id,
        }
        result.append(cat_info)

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def get_catalog(arguments: dict[str, Any]) -> list[TextContent]:
    """Get details about a specific catalog."""
    client = get_client()
    catalog_name = arguments["catalog_name"]

    cat = client.catalogs.get(name=catalog_name)

    result = {
        "name": cat.name,
        "comment": cat.comment,
        "owner": cat.owner,
        "created_at": cat.created_at,
        "updated_at": cat.updated_at,
        "metastore_id": cat.metastore_id,
        "isolation_mode": cat.isolation_mode.value if cat.isolation_mode else None,
    }

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def list_schemas(arguments: dict[str, Any]) -> list[TextContent]:
    """List all schemas in a catalog."""
    client = get_client()
    catalog_name = arguments["catalog_name"]

    schemas = list(client.schemas.list(catalog_name=catalog_name))

    result = []
    for schema in schemas:
        schema_info = {
            "name": schema.name,
            "full_name": schema.full_name,
            "comment": schema.comment,
            "owner": schema.owner,
            "created_at": schema.created_at,
        }
        result.append(schema_info)

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def get_schema(arguments: dict[str, Any]) -> list[TextContent]:
    """Get details about a specific schema."""
    client = get_client()
    catalog_name = arguments["catalog_name"]
    schema_name = arguments["schema_name"]

    schema = client.schemas.get(full_name=f"{catalog_name}.{schema_name}")

    result = {
        "name": schema.name,
        "full_name": schema.full_name,
        "comment": schema.comment,
        "owner": schema.owner,
        "created_at": schema.created_at,
        "updated_at": schema.updated_at,
    }

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def list_tables(arguments: dict[str, Any]) -> list[TextContent]:
    """List all tables in a schema."""
    client = get_client()
    catalog_name = arguments["catalog_name"]
    schema_name = arguments["schema_name"]

    tables = list(
        client.tables.list(catalog_name=catalog_name, schema_name=schema_name)
    )

    result = []
    for table in tables:
        table_info = {
            "name": table.name,
            "full_name": table.full_name,
            "table_type": table.table_type.value if table.table_type else None,
            "data_source_format": (
                table.data_source_format.value if table.data_source_format else None
            ),
            "comment": table.comment,
            "owner": table.owner,
            "created_at": table.created_at,
        }
        result.append(table_info)

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def describe_table(arguments: dict[str, Any]) -> list[TextContent]:
    """Get detailed schema and metadata for a table."""
    client = get_client()
    full_name = arguments["full_name"]

    table = client.tables.get(full_name=full_name)

    result = {
        "name": table.name,
        "full_name": table.full_name,
        "table_type": table.table_type.value if table.table_type else None,
        "data_source_format": (
            table.data_source_format.value if table.data_source_format else None
        ),
        "comment": table.comment,
        "owner": table.owner,
        "created_at": table.created_at,
        "updated_at": table.updated_at,
        "storage_location": table.storage_location,
        "columns": [],
    }

    if table.columns:
        for col in table.columns:
            col_info = {
                "name": col.name,
                "type": col.type_name.value if col.type_name else col.type_text,
                "comment": col.comment,
                "nullable": col.nullable,
                "position": col.position,
            }
            result["columns"].append(col_info)

    if table.properties:
        result["properties"] = table.properties

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def preview_table(arguments: dict[str, Any]) -> list[TextContent]:
    """Preview sample data from a table."""
    client = get_client()

    full_name = arguments["full_name"]
    limit = arguments.get("limit", 10)
    warehouse_id = arguments.get("warehouse_id") or databricks.get_default_warehouse_id()

    if not warehouse_id:
        return [
            TextContent(
                type="text",
                text="Error: No warehouse specified and no running warehouse found. Please provide a warehouse_id or start a warehouse.",
            )
        ]

    # Execute preview query
    query = f"SELECT * FROM {full_name} LIMIT {limit}"

    from databricks.sdk.service.sql import StatementState
    import time

    statement = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=query,
        wait_timeout="30s",
    )

    # Wait for completion
    max_wait = 30
    waited = 0
    while statement.status and statement.status.state in [
        StatementState.PENDING,
        StatementState.RUNNING,
    ]:
        if waited >= max_wait:
            return [TextContent(type="text", text="Query timed out")]
        time.sleep(1)
        waited += 1
        statement = client.statement_execution.get_statement(
            statement_id=statement.statement_id
        )

    if statement.status and statement.status.state == StatementState.FAILED:
        error_msg = "Preview failed"
        if statement.status.error:
            error_msg = f"Preview failed: {statement.status.error.message}"
        return [TextContent(type="text", text=error_msg)]

    result = {"table": full_name, "columns": [], "data": []}

    if statement.manifest and statement.manifest.schema:
        result["columns"] = [
            col.name for col in (statement.manifest.schema.columns or [])
        ]

    if statement.result and statement.result.data_array:
        result["data"] = statement.result.data_array

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def list_volumes(arguments: dict[str, Any]) -> list[TextContent]:
    """List volumes in a schema."""
    client = get_client()
    catalog_name = arguments["catalog_name"]
    schema_name = arguments["schema_name"]

    volumes = list(
        client.volumes.list(catalog_name=catalog_name, schema_name=schema_name)
    )

    result = []
    for vol in volumes:
        vol_info = {
            "name": vol.name,
            "full_name": vol.full_name,
            "volume_type": vol.volume_type.value if vol.volume_type else None,
            "comment": vol.comment,
            "owner": vol.owner,
            "created_at": vol.created_at,
            "storage_location": vol.storage_location,
        }
        result.append(vol_info)

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def get_volume(arguments: dict[str, Any]) -> list[TextContent]:
    """Get details about a specific volume."""
    client = get_client()
    full_name = arguments["full_name"]

    vol = client.volumes.read(name=full_name)

    result = {
        "name": vol.name,
        "full_name": vol.full_name,
        "volume_type": vol.volume_type.value if vol.volume_type else None,
        "comment": vol.comment,
        "owner": vol.owner,
        "created_at": vol.created_at,
        "updated_at": vol.updated_at,
        "storage_location": vol.storage_location,
    }

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def list_functions(arguments: dict[str, Any]) -> list[TextContent]:
    """List functions in a schema."""
    client = get_client()
    catalog_name = arguments["catalog_name"]
    schema_name = arguments["schema_name"]

    functions = list(
        client.functions.list(catalog_name=catalog_name, schema_name=schema_name)
    )

    result = []
    for func in functions:
        func_info = {
            "name": func.name,
            "full_name": func.full_name,
            "comment": func.comment,
            "owner": func.owner,
            "data_type": func.data_type.value if func.data_type else None,
            "external_language": func.external_language,
        }
        result.append(func_info)

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def search_tables(arguments: dict[str, Any]) -> list[TextContent]:
    """Search for tables by name across catalogs."""
    client = get_client()

    query = arguments["query"]
    catalog_name = arguments.get("catalog_name")

    # Use list_summaries for efficient searching
    tables = list(
        client.tables.list_summaries(
            catalog_name=catalog_name or "*",
            schema_name_pattern="*",
            table_name_pattern=f"*{query}*",
        )
    )

    result = []
    for table in tables:
        table_info = {
            "full_name": table.full_name,
            "table_type": table.table_type.value if table.table_type else None,
        }
        result.append(table_info)

    return [TextContent(type="text", text=json.dumps(result, indent=2))]
