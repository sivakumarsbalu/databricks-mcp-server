"""MCP Resources for Databricks workspace information."""

import json
from typing import Any

from mcp.server import Server
from mcp.types import Resource, TextContent

from ..config import get_client, get_config


def register_workspace_resources(server: Server):
    """Register workspace resources with the MCP server."""

    @server.list_resources()
    async def list_resources() -> list[Resource]:
        """Return available workspace resources."""
        return [
            Resource(
                uri="databricks://workspace/info",
                name="Workspace Info",
                description="Current Databricks workspace information and configuration",
                mimeType="application/json",
            ),
            Resource(
                uri="databricks://user/me",
                name="Current User",
                description="Information about the currently authenticated user",
                mimeType="application/json",
            ),
            Resource(
                uri="databricks://clusters/active",
                name="Active Clusters",
                description="List of currently running clusters",
                mimeType="application/json",
            ),
            Resource(
                uri="databricks://warehouses/active",
                name="Active Warehouses",
                description="List of currently running SQL warehouses",
                mimeType="application/json",
            ),
        ]

    @server.read_resource()
    async def read_resource(uri: str) -> str:
        """Read a workspace resource."""
        if uri == "databricks://workspace/info":
            return await get_workspace_info()
        elif uri == "databricks://user/me":
            return await get_current_user()
        elif uri == "databricks://clusters/active":
            return await get_active_clusters()
        elif uri == "databricks://warehouses/active":
            return await get_active_warehouses()
        else:
            raise ValueError(f"Unknown resource URI: {uri}")


async def get_workspace_info() -> str:
    """Get workspace information."""
    client = get_client()
    config = get_config()

    info = {
        "host": config.host,
        "auth_type": config.get_auth_type(),
        "default_cluster_id": config.default_cluster_id,
        "default_warehouse_id": config.default_warehouse_id,
    }

    # Try to get workspace ID from current user
    try:
        me = client.current_user.me()
        if me.user_name:
            info["user"] = me.user_name
    except Exception:
        pass

    return json.dumps(info, indent=2)


async def get_current_user() -> str:
    """Get current user information."""
    client = get_client()

    me = client.current_user.me()

    user_info = {
        "user_name": me.user_name,
        "display_name": me.display_name,
        "id": me.id,
        "active": me.active,
    }

    if me.emails:
        user_info["emails"] = [e.value for e in me.emails]

    if me.groups:
        user_info["groups"] = [g.display for g in me.groups]

    return json.dumps(user_info, indent=2)


async def get_active_clusters() -> str:
    """Get list of running clusters."""
    client = get_client()

    clusters = list(client.clusters.list())

    active = []
    for cluster in clusters:
        if cluster.state and cluster.state.value == "RUNNING":
            active.append(
                {
                    "cluster_id": cluster.cluster_id,
                    "cluster_name": cluster.cluster_name,
                    "spark_version": cluster.spark_version,
                    "node_type_id": cluster.node_type_id,
                    "num_workers": cluster.num_workers,
                }
            )

    return json.dumps(active, indent=2)


async def get_active_warehouses() -> str:
    """Get list of running SQL warehouses."""
    client = get_client()

    warehouses = list(client.warehouses.list())

    active = []
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING":
            active.append(
                {
                    "id": wh.id,
                    "name": wh.name,
                    "cluster_size": wh.cluster_size,
                    "num_clusters": wh.num_clusters,
                }
            )

    return json.dumps(active, indent=2)
