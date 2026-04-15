"""Cluster management tools for Databricks MCP Server."""

import json
from typing import Any

from mcp.server import Server
from mcp.types import Tool, TextContent

from ..config import get_client, databricks


def register_cluster_tools(server: Server):
    """Register cluster management tools with the MCP server."""

    @server.list_tools()
    async def list_cluster_tools() -> list[Tool]:
        """Return cluster-related tools."""
        return [
            Tool(
                name="databricks_list_clusters",
                description="List all clusters in the Databricks workspace with their status and configuration",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": [],
                },
            ),
            Tool(
                name="databricks_get_cluster",
                description="Get detailed information about a specific cluster",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "cluster_id": {
                            "type": "string",
                            "description": "The ID of the cluster to get details for",
                        },
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
                        "cluster_id": {
                            "type": "string",
                            "description": "The ID of the cluster to start",
                        },
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
                        "cluster_id": {
                            "type": "string",
                            "description": "The ID of the cluster to terminate",
                        },
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
                        "cluster_name": {
                            "type": "string",
                            "description": "Name for the new cluster",
                        },
                        "spark_version": {
                            "type": "string",
                            "description": "Spark version (e.g., '13.3.x-scala2.12'). Use databricks_list_spark_versions to see available versions.",
                        },
                        "node_type_id": {
                            "type": "string",
                            "description": "Node type (e.g., 'Standard_DS3_v2' for Azure, 'i3.xlarge' for AWS)",
                        },
                        "num_workers": {
                            "type": "integer",
                            "description": "Number of worker nodes (0 for single-node cluster)",
                            "default": 0,
                        },
                        "autotermination_minutes": {
                            "type": "integer",
                            "description": "Minutes of inactivity before auto-termination",
                            "default": 120,
                        },
                    },
                    "required": ["cluster_name", "spark_version", "node_type_id"],
                },
            ),
            Tool(
                name="databricks_execute_code",
                description="Execute Python or Scala code on a cluster using the Command Execution API",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "code": {
                            "type": "string",
                            "description": "The code to execute",
                        },
                        "language": {
                            "type": "string",
                            "enum": ["python", "scala", "sql"],
                            "description": "Programming language",
                            "default": "python",
                        },
                        "cluster_id": {
                            "type": "string",
                            "description": "Cluster ID to execute on. If not provided, uses default or first running cluster.",
                        },
                    },
                    "required": ["code"],
                },
            ),
            Tool(
                name="databricks_list_spark_versions",
                description="List available Spark versions for cluster creation",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": [],
                },
            ),
            Tool(
                name="databricks_list_node_types",
                description="List available node types for cluster creation",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "required": [],
                },
            ),
        ]

    return {
        "databricks_list_clusters": list_clusters,
        "databricks_get_cluster": get_cluster,
        "databricks_start_cluster": start_cluster,
        "databricks_terminate_cluster": terminate_cluster,
        "databricks_create_cluster": create_cluster,
        "databricks_execute_code": execute_code,
        "databricks_list_spark_versions": list_spark_versions,
        "databricks_list_node_types": list_node_types,
    }


async def list_clusters(arguments: dict[str, Any]) -> list[TextContent]:
    """List all clusters in the workspace."""
    client = get_client()

    clusters = list(client.clusters.list())

    result = []
    for cluster in clusters:
        cluster_info = {
            "cluster_id": cluster.cluster_id,
            "cluster_name": cluster.cluster_name,
            "state": cluster.state.value if cluster.state else "UNKNOWN",
            "spark_version": cluster.spark_version,
            "node_type_id": cluster.node_type_id,
            "num_workers": cluster.num_workers,
            "creator_user_name": cluster.creator_user_name,
        }
        result.append(cluster_info)

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def get_cluster(arguments: dict[str, Any]) -> list[TextContent]:
    """Get detailed information about a specific cluster."""
    client = get_client()
    cluster_id = arguments["cluster_id"]

    cluster = client.clusters.get(cluster_id=cluster_id)

    cluster_info = {
        "cluster_id": cluster.cluster_id,
        "cluster_name": cluster.cluster_name,
        "state": cluster.state.value if cluster.state else "UNKNOWN",
        "state_message": cluster.state_message,
        "spark_version": cluster.spark_version,
        "node_type_id": cluster.node_type_id,
        "driver_node_type_id": cluster.driver_node_type_id,
        "num_workers": cluster.num_workers,
        "autotermination_minutes": cluster.autotermination_minutes,
        "creator_user_name": cluster.creator_user_name,
        "start_time": cluster.start_time,
        "spark_context_id": cluster.spark_context_id,
        "jdbc_port": cluster.jdbc_port,
        "cluster_memory_mb": cluster.cluster_memory_mb,
        "cluster_cores": cluster.cluster_cores,
    }

    return [TextContent(type="text", text=json.dumps(cluster_info, indent=2))]


async def start_cluster(arguments: dict[str, Any]) -> list[TextContent]:
    """Start a terminated cluster."""
    client = get_client()
    cluster_id = arguments["cluster_id"]

    client.clusters.start(cluster_id=cluster_id)

    return [
        TextContent(
            type="text",
            text=f"Cluster {cluster_id} is starting. Use databricks_get_cluster to check status.",
        )
    ]


async def terminate_cluster(arguments: dict[str, Any]) -> list[TextContent]:
    """Terminate a running cluster."""
    client = get_client()
    cluster_id = arguments["cluster_id"]

    client.clusters.delete(cluster_id=cluster_id)

    return [TextContent(type="text", text=f"Cluster {cluster_id} is being terminated.")]


async def create_cluster(arguments: dict[str, Any]) -> list[TextContent]:
    """Create a new cluster."""
    client = get_client()

    cluster_name = arguments["cluster_name"]
    spark_version = arguments["spark_version"]
    node_type_id = arguments["node_type_id"]
    num_workers = arguments.get("num_workers", 0)
    autotermination_minutes = arguments.get("autotermination_minutes", 120)

    result = client.clusters.create(
        cluster_name=cluster_name,
        spark_version=spark_version,
        node_type_id=node_type_id,
        num_workers=num_workers,
        autotermination_minutes=autotermination_minutes,
    )

    return [
        TextContent(
            type="text",
            text=f"Cluster created successfully!\nCluster ID: {result.cluster_id}\nCluster Name: {cluster_name}",
        )
    ]


async def execute_code(arguments: dict[str, Any]) -> list[TextContent]:
    """Execute code on a cluster."""
    client = get_client()

    code = arguments["code"]
    language = arguments.get("language", "python")
    cluster_id = arguments.get("cluster_id") or databricks.get_default_cluster_id()

    if not cluster_id:
        return [
            TextContent(
                type="text",
                text="Error: No cluster specified and no running cluster found. Please provide a cluster_id or start a cluster.",
            )
        ]

    # Create execution context
    context = client.command_execution.create(
        cluster_id=cluster_id,
        language=language.capitalize(),
    )

    try:
        # Execute the command
        result = client.command_execution.execute(
            cluster_id=cluster_id,
            context_id=context.id,
            language=language.capitalize(),
            command=code,
        )

        # Wait for completion and get results
        execution_result = result.result()

        output = {
            "status": execution_result.status.value if execution_result.status else "UNKNOWN",
            "results": None,
        }

        if execution_result.results:
            if execution_result.results.result_type:
                output["result_type"] = execution_result.results.result_type.value
            if execution_result.results.data:
                output["results"] = execution_result.results.data
            if execution_result.results.cause:
                output["error"] = execution_result.results.cause

        return [TextContent(type="text", text=json.dumps(output, indent=2))]

    finally:
        # Destroy the context
        try:
            client.command_execution.destroy(
                cluster_id=cluster_id, context_id=context.id
            )
        except Exception:
            pass


async def list_spark_versions(arguments: dict[str, Any]) -> list[TextContent]:
    """List available Spark versions."""
    client = get_client()

    versions = client.clusters.spark_versions()

    result = []
    for version in versions.versions or []:
        result.append({"key": version.key, "name": version.name})

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def list_node_types(arguments: dict[str, Any]) -> list[TextContent]:
    """List available node types."""
    client = get_client()

    node_types = client.clusters.list_node_types()

    result = []
    for nt in node_types.node_types or []:
        result.append(
            {
                "node_type_id": nt.node_type_id,
                "memory_mb": nt.memory_mb,
                "num_cores": nt.num_cores,
                "description": nt.description,
                "category": nt.category,
            }
        )

    # Limit to first 50 for readability
    return [TextContent(type="text", text=json.dumps(result[:50], indent=2))]
