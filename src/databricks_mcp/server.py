"""Main MCP server for Databricks integration.

Includes security checks, structured logging, and audit trail.
"""

import asyncio
import time
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from .config import databricks, get_security_config
from .security import (
    get_tool_classification,
    SQLQueryValidator,
    RiskLevel,
    OperationType,
)
from .logging_config import (
    configure_logging,
    set_correlation_id,
    audit_logger,
    logger,
)
from .tools.clusters import register_cluster_tools
from .tools.notebooks import register_notebook_tools
from .tools.sql import register_sql_tools
from .tools.jobs import register_job_tools
from .tools.dbfs import register_dbfs_tools
from .tools.unity_catalog import register_unity_catalog_tools
from .resources.workspace import register_workspace_resources


def create_server() -> Server:
    """Create and configure the MCP server."""
    server = Server("databricks-mcp")

    # Configure structured logging
    configure_logging(level="INFO", json_format=True)

    # Initialize Databricks client
    try:
        databricks.initialize()
        security = get_security_config()

        logger.info(
            f"Connected to Databricks at {databricks.config.host}",
            extra={
                "profile": databricks.config.profile.value,
                "read_only_mode": security.read_only_mode,
                "safe_mode": security.safe_mode,
            }
        )

        if security.read_only_mode:
            logger.info("Server running in READ-ONLY mode")
        if security.safe_mode:
            logger.info("Server running in SAFE mode (destructive operations blocked)")

    except Exception as e:
        logger.error(f"Failed to initialize Databricks client: {e}")
        raise

    # Collect all tool handlers
    tool_handlers: dict[str, Any] = {}

    # Register all tool modules and collect their handlers
    cluster_handlers = register_cluster_tools(server)
    tool_handlers.update(cluster_handlers)

    notebook_handlers = register_notebook_tools(server)
    tool_handlers.update(notebook_handlers)

    sql_handlers = register_sql_tools(server)
    tool_handlers.update(sql_handlers)

    job_handlers = register_job_tools(server)
    tool_handlers.update(job_handlers)

    dbfs_handlers = register_dbfs_tools(server)
    tool_handlers.update(dbfs_handlers)

    uc_handlers = register_unity_catalog_tools(server)
    tool_handlers.update(uc_handlers)

    # Register resources
    register_workspace_resources(server)

    # SQL query validator for safe mode
    sql_validator = SQLQueryValidator()

    def get_tool_description_with_risk(tool: Tool) -> Tool:
        """Enhance tool description with risk level indicator."""
        classification = get_tool_classification(tool.name)
        if classification:
            risk_indicator = ""
            if classification.risk_level == RiskLevel.DESTRUCTIVE:
                risk_indicator = "⚠️ DESTRUCTIVE: "
            elif classification.risk_level == RiskLevel.CRITICAL:
                risk_indicator = "🔴 CRITICAL: "
            elif classification.risk_level == RiskLevel.MODERATE:
                risk_indicator = "⚡ "

            if risk_indicator:
                return Tool(
                    name=tool.name,
                    description=f"{risk_indicator}{tool.description}",
                    inputSchema=tool.inputSchema,
                )
        return tool

    def build_filtered_tools_list() -> list[Tool]:
        """Build the tools list filtered by security configuration."""
        security = get_security_config()
        all_tools = build_all_tools()

        filtered_tools = []
        for tool in all_tools:
            is_allowed, reason = security.is_tool_allowed(tool.name)
            if is_allowed:
                filtered_tools.append(get_tool_description_with_risk(tool))
            else:
                logger.debug(f"Tool {tool.name} filtered out: {reason}")

        return filtered_tools

    def build_all_tools() -> list[Tool]:
        """Build the complete list of all tools."""
        tools = []

        # Cluster tools
        tools.extend([
            Tool(
                name="databricks_list_clusters",
                description="List all clusters in the Databricks workspace with their status and configuration",
                inputSchema={"type": "object", "properties": {}, "required": []},
            ),
            Tool(
                name="databricks_get_cluster",
                description="Get detailed information about a specific cluster",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "cluster_id": {"type": "string", "description": "The ID of the cluster"}
                    },
                    "required": ["cluster_id"],
                },
            ),
            Tool(
                name="databricks_start_cluster",
                description="Start a terminated cluster",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "cluster_id": {"type": "string", "description": "The ID of the cluster to start"}
                    },
                    "required": ["cluster_id"],
                },
            ),
            Tool(
                name="databricks_terminate_cluster",
                description="Terminate (stop) a running cluster",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "cluster_id": {"type": "string", "description": "The ID of the cluster to terminate"}
                    },
                    "required": ["cluster_id"],
                },
            ),
            Tool(
                name="databricks_create_cluster",
                description="Create a new cluster with specified configuration",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "cluster_name": {"type": "string", "description": "Name for the new cluster"},
                        "spark_version": {"type": "string", "description": "Spark version (e.g., '13.3.x-scala2.12')"},
                        "node_type_id": {"type": "string", "description": "Node type ID"},
                        "num_workers": {"type": "integer", "description": "Number of workers", "default": 0},
                        "autotermination_minutes": {"type": "integer", "description": "Auto-termination timeout", "default": 120},
                    },
                    "required": ["cluster_name", "spark_version", "node_type_id"],
                },
            ),
            Tool(
                name="databricks_execute_code",
                description="Execute Python, Scala, or SQL code on a cluster",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "code": {"type": "string", "description": "Code to execute"},
                        "language": {"type": "string", "enum": ["python", "scala", "sql"], "description": "Language", "default": "python"},
                        "cluster_id": {"type": "string", "description": "Cluster ID (optional)"},
                    },
                    "required": ["code"],
                },
            ),
            Tool(
                name="databricks_list_spark_versions",
                description="List available Spark versions",
                inputSchema={"type": "object", "properties": {}, "required": []},
            ),
            Tool(
                name="databricks_list_node_types",
                description="List available node types",
                inputSchema={"type": "object", "properties": {}, "required": []},
            ),
        ])

        # Notebook tools
        tools.extend([
            Tool(
                name="databricks_list_notebooks",
                description="List notebooks and folders in a workspace path",
                inputSchema={
                    "type": "object",
                    "properties": {"path": {"type": "string", "description": "Workspace path", "default": "/"}},
                    "required": [],
                },
            ),
            Tool(
                name="databricks_read_notebook",
                description="Read the content of a notebook",
                inputSchema={
                    "type": "object",
                    "properties": {"path": {"type": "string", "description": "Full path to notebook"}},
                    "required": ["path"],
                },
            ),
            Tool(
                name="databricks_create_notebook",
                description="Create a new notebook",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "Path for notebook"},
                        "language": {"type": "string", "enum": ["PYTHON", "SCALA", "SQL", "R"], "default": "PYTHON"},
                        "content": {"type": "string", "description": "Initial content"},
                        "overwrite": {"type": "boolean", "default": False},
                    },
                    "required": ["path", "content"],
                },
            ),
            Tool(
                name="databricks_update_notebook",
                description="Update an existing notebook",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "Path to notebook"},
                        "content": {"type": "string", "description": "New content"},
                    },
                    "required": ["path", "content"],
                },
            ),
            Tool(
                name="databricks_delete_notebook",
                description="Delete a notebook or folder",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "Path to delete"},
                        "recursive": {"type": "boolean", "default": False},
                    },
                    "required": ["path"],
                },
            ),
            Tool(
                name="databricks_export_notebook",
                description="Export notebook in various formats",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "Path to notebook"},
                        "format": {"type": "string", "enum": ["SOURCE", "HTML", "JUPYTER", "DBC"], "default": "SOURCE"},
                    },
                    "required": ["path"],
                },
            ),
            Tool(
                name="databricks_create_folder",
                description="Create a folder in workspace",
                inputSchema={
                    "type": "object",
                    "properties": {"path": {"type": "string", "description": "Path for folder"}},
                    "required": ["path"],
                },
            ),
            Tool(
                name="databricks_get_notebook_status",
                description="Get metadata about a notebook or folder",
                inputSchema={
                    "type": "object",
                    "properties": {"path": {"type": "string", "description": "Path to check"}},
                    "required": ["path"],
                },
            ),
        ])

        # SQL tools
        tools.extend([
            Tool(
                name="databricks_list_warehouses",
                description="List all SQL warehouses",
                inputSchema={"type": "object", "properties": {}, "required": []},
            ),
            Tool(
                name="databricks_get_warehouse",
                description="Get warehouse details",
                inputSchema={
                    "type": "object",
                    "properties": {"warehouse_id": {"type": "string", "description": "Warehouse ID"}},
                    "required": ["warehouse_id"],
                },
            ),
            Tool(
                name="databricks_start_warehouse",
                description="Start a SQL warehouse",
                inputSchema={
                    "type": "object",
                    "properties": {"warehouse_id": {"type": "string", "description": "Warehouse ID"}},
                    "required": ["warehouse_id"],
                },
            ),
            Tool(
                name="databricks_stop_warehouse",
                description="Stop a SQL warehouse",
                inputSchema={
                    "type": "object",
                    "properties": {"warehouse_id": {"type": "string", "description": "Warehouse ID"}},
                    "required": ["warehouse_id"],
                },
            ),
            Tool(
                name="databricks_execute_sql",
                description="Execute SQL query and return results",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "SQL query"},
                        "warehouse_id": {"type": "string", "description": "Warehouse ID"},
                        "catalog": {"type": "string", "description": "Catalog name"},
                        "schema": {"type": "string", "description": "Schema name"},
                        "max_rows": {"type": "integer", "default": 1000},
                    },
                    "required": ["query"],
                },
            ),
            Tool(
                name="databricks_explain_sql",
                description="Get execution plan for SQL query",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "SQL query"},
                        "warehouse_id": {"type": "string"},
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
                        "max_results": {"type": "integer", "default": 25},
                        "warehouse_id": {"type": "string"},
                    },
                    "required": [],
                },
            ),
        ])

        # Job tools
        tools.extend([
            Tool(
                name="databricks_list_jobs",
                description="List all jobs",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "name_filter": {"type": "string"},
                        "limit": {"type": "integer", "default": 25},
                    },
                    "required": [],
                },
            ),
            Tool(
                name="databricks_get_job",
                description="Get job details",
                inputSchema={
                    "type": "object",
                    "properties": {"job_id": {"type": "integer"}},
                    "required": ["job_id"],
                },
            ),
            Tool(
                name="databricks_create_notebook_job",
                description="Create a job that runs a notebook",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "notebook_path": {"type": "string"},
                        "cluster_id": {"type": "string"},
                        "parameters": {"type": "object"},
                    },
                    "required": ["name", "notebook_path"],
                },
            ),
            Tool(
                name="databricks_run_job",
                description="Trigger a job run",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "integer"},
                        "parameters": {"type": "object"},
                    },
                    "required": ["job_id"],
                },
            ),
            Tool(
                name="databricks_run_notebook_now",
                description="Run notebook immediately as one-time job",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "notebook_path": {"type": "string"},
                        "cluster_id": {"type": "string"},
                        "parameters": {"type": "object"},
                        "run_name": {"type": "string"},
                    },
                    "required": ["notebook_path", "cluster_id"],
                },
            ),
            Tool(
                name="databricks_get_run",
                description="Get run status",
                inputSchema={
                    "type": "object",
                    "properties": {"run_id": {"type": "integer"}},
                    "required": ["run_id"],
                },
            ),
            Tool(
                name="databricks_list_runs",
                description="List recent job runs",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "integer"},
                        "active_only": {"type": "boolean", "default": False},
                        "limit": {"type": "integer", "default": 25},
                    },
                    "required": [],
                },
            ),
            Tool(
                name="databricks_cancel_run",
                description="Cancel a running job",
                inputSchema={
                    "type": "object",
                    "properties": {"run_id": {"type": "integer"}},
                    "required": ["run_id"],
                },
            ),
            Tool(
                name="databricks_get_run_output",
                description="Get run output",
                inputSchema={
                    "type": "object",
                    "properties": {"run_id": {"type": "integer"}},
                    "required": ["run_id"],
                },
            ),
            Tool(
                name="databricks_delete_job",
                description="Delete a job",
                inputSchema={
                    "type": "object",
                    "properties": {"job_id": {"type": "integer"}},
                    "required": ["job_id"],
                },
            ),
        ])

        # DBFS tools
        tools.extend([
            Tool(
                name="databricks_dbfs_list",
                description="List files in DBFS",
                inputSchema={
                    "type": "object",
                    "properties": {"path": {"type": "string", "default": "/"}},
                    "required": [],
                },
            ),
            Tool(
                name="databricks_dbfs_read",
                description="Read file from DBFS",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "offset": {"type": "integer", "default": 0},
                        "length": {"type": "integer", "default": 1048576},
                    },
                    "required": ["path"],
                },
            ),
            Tool(
                name="databricks_dbfs_write",
                description="Write file to DBFS",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "content": {"type": "string"},
                        "overwrite": {"type": "boolean", "default": False},
                    },
                    "required": ["path", "content"],
                },
            ),
            Tool(
                name="databricks_dbfs_delete",
                description="Delete file from DBFS",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "recursive": {"type": "boolean", "default": False},
                    },
                    "required": ["path"],
                },
            ),
            Tool(
                name="databricks_dbfs_mkdirs",
                description="Create DBFS directories",
                inputSchema={
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"],
                },
            ),
            Tool(
                name="databricks_dbfs_get_status",
                description="Get DBFS path status",
                inputSchema={
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"],
                },
            ),
            Tool(
                name="databricks_dbfs_move",
                description="Move/rename DBFS file",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "source_path": {"type": "string"},
                        "destination_path": {"type": "string"},
                    },
                    "required": ["source_path", "destination_path"],
                },
            ),
        ])

        # Unity Catalog tools
        tools.extend([
            Tool(
                name="databricks_list_catalogs",
                description="List all Unity Catalog catalogs",
                inputSchema={"type": "object", "properties": {}, "required": []},
            ),
            Tool(
                name="databricks_get_catalog",
                description="Get catalog details",
                inputSchema={
                    "type": "object",
                    "properties": {"catalog_name": {"type": "string"}},
                    "required": ["catalog_name"],
                },
            ),
            Tool(
                name="databricks_list_schemas",
                description="List schemas in catalog",
                inputSchema={
                    "type": "object",
                    "properties": {"catalog_name": {"type": "string"}},
                    "required": ["catalog_name"],
                },
            ),
            Tool(
                name="databricks_get_schema",
                description="Get schema details",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "catalog_name": {"type": "string"},
                        "schema_name": {"type": "string"},
                    },
                    "required": ["catalog_name", "schema_name"],
                },
            ),
            Tool(
                name="databricks_list_tables",
                description="List tables in schema",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "catalog_name": {"type": "string"},
                        "schema_name": {"type": "string"},
                    },
                    "required": ["catalog_name", "schema_name"],
                },
            ),
            Tool(
                name="databricks_describe_table",
                description="Get table schema and metadata",
                inputSchema={
                    "type": "object",
                    "properties": {"full_name": {"type": "string", "description": "catalog.schema.table"}},
                    "required": ["full_name"],
                },
            ),
            Tool(
                name="databricks_preview_table",
                description="Preview sample data from table",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "full_name": {"type": "string"},
                        "limit": {"type": "integer", "default": 10},
                        "warehouse_id": {"type": "string"},
                    },
                    "required": ["full_name"],
                },
            ),
            Tool(
                name="databricks_list_volumes",
                description="List volumes in schema",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "catalog_name": {"type": "string"},
                        "schema_name": {"type": "string"},
                    },
                    "required": ["catalog_name", "schema_name"],
                },
            ),
            Tool(
                name="databricks_get_volume",
                description="Get volume details",
                inputSchema={
                    "type": "object",
                    "properties": {"full_name": {"type": "string"}},
                    "required": ["full_name"],
                },
            ),
            Tool(
                name="databricks_list_functions",
                description="List functions in schema",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "catalog_name": {"type": "string"},
                        "schema_name": {"type": "string"},
                    },
                    "required": ["catalog_name", "schema_name"],
                },
            ),
            Tool(
                name="databricks_search_tables",
                description="Search tables by name",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "catalog_name": {"type": "string"},
                    },
                    "required": ["query"],
                },
            ),
        ])

        return tools

    @server.list_tools()
    async def list_all_tools() -> list[Tool]:
        """List all available Databricks tools, filtered by security config."""
        return build_filtered_tools_list()

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
        """Handle tool calls with security checks and audit logging."""
        # Generate correlation ID for this request
        correlation_id = set_correlation_id()
        start_time = time.time()

        # Get tool classification
        classification = get_tool_classification(name)
        operation_type = classification.operation_type.value if classification else "unknown"
        risk_level = classification.risk_level.value if classification else "unknown"

        # Check if tool is allowed
        security = get_security_config()
        is_allowed, reason = security.is_tool_allowed(name)

        if not is_allowed:
            logger.warning(
                f"Tool {name} blocked: {reason}",
                extra={"tool": name, "reason": reason}
            )
            audit_logger.log_tool_invocation(
                tool_name=name,
                operation_type=operation_type,
                risk_level=risk_level,
                arguments=arguments,
                result_status="BLOCKED",
                execution_time_ms=(time.time() - start_time) * 1000,
                error_message=reason,
                workspace_host=databricks.config.host,
            )
            return [TextContent(type="text", text=f"Error: {reason}")]

        # Special handling for SQL execution in safe mode
        if name == "databricks_execute_sql" and security.safe_mode:
            query = arguments.get("query", "")
            is_safe, sql_reason = sql_validator.validate_for_safe_mode(query)
            if not is_safe:
                logger.warning(
                    f"SQL query blocked in safe mode: {sql_reason}",
                    extra={"tool": name, "query_preview": query[:100]}
                )
                audit_logger.log_tool_invocation(
                    tool_name=name,
                    operation_type=operation_type,
                    risk_level=risk_level,
                    arguments=arguments,
                    result_status="BLOCKED",
                    execution_time_ms=(time.time() - start_time) * 1000,
                    error_message=sql_reason,
                    workspace_host=databricks.config.host,
                )
                return [TextContent(type="text", text=f"Error: {sql_reason}")]

        # Execute the tool
        if name in tool_handlers:
            try:
                logger.info(
                    f"Executing tool: {name}",
                    extra={
                        "tool": name,
                        "operation_type": operation_type,
                        "risk_level": risk_level,
                    }
                )

                result = await tool_handlers[name](arguments)

                execution_time_ms = (time.time() - start_time) * 1000

                # Log successful execution
                audit_logger.log_tool_invocation(
                    tool_name=name,
                    operation_type=operation_type,
                    risk_level=risk_level,
                    arguments=arguments,
                    result_status="SUCCESS",
                    execution_time_ms=execution_time_ms,
                    workspace_host=databricks.config.host,
                )

                logger.info(
                    f"Tool {name} completed successfully",
                    extra={"tool": name, "execution_time_ms": execution_time_ms}
                )

                return result

            except Exception as e:
                execution_time_ms = (time.time() - start_time) * 1000
                error_message = str(e)

                logger.error(
                    f"Error executing {name}: {e}",
                    extra={"tool": name, "error": error_message}
                )

                audit_logger.log_tool_invocation(
                    tool_name=name,
                    operation_type=operation_type,
                    risk_level=risk_level,
                    arguments=arguments,
                    result_status="ERROR",
                    execution_time_ms=execution_time_ms,
                    error_message=error_message,
                    workspace_host=databricks.config.host,
                )

                return [TextContent(type="text", text=f"Error: {error_message}")]
        else:
            logger.warning(f"Unknown tool called: {name}")
            return [TextContent(type="text", text=f"Unknown tool: {name}")]

    return server


async def run_server():
    """Run the MCP server."""
    server = create_server()

    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


def main():
    """Entry point for the MCP server."""
    asyncio.run(run_server())


if __name__ == "__main__":
    main()
