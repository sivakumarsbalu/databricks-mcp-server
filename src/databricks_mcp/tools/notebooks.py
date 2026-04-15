"""Notebook management tools for Databricks MCP Server."""

import base64
import json
from typing import Any

from mcp.server import Server
from mcp.types import Tool, TextContent

from databricks.sdk.service.workspace import (
    ExportFormat,
    ImportFormat,
    Language,
    ObjectType,
)

from ..config import get_client


def register_notebook_tools(server: Server):
    """Register notebook management tools with the MCP server."""

    @server.list_tools()
    async def list_notebook_tools() -> list[Tool]:
        """Return notebook-related tools."""
        return [
            Tool(
                name="databricks_list_notebooks",
                description="List notebooks and folders in a workspace path",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Workspace path to list (e.g., '/Users/user@example.com' or '/Repos')",
                            "default": "/",
                        },
                    },
                    "required": [],
                },
            ),
            Tool(
                name="databricks_read_notebook",
                description="Read the content of a notebook",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Full path to the notebook in the workspace",
                        },
                    },
                    "required": ["path"],
                },
            ),
            Tool(
                name="databricks_create_notebook",
                description="Create a new notebook with initial content",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Full path for the new notebook (including name)",
                        },
                        "language": {
                            "type": "string",
                            "enum": ["PYTHON", "SCALA", "SQL", "R"],
                            "description": "Programming language for the notebook",
                            "default": "PYTHON",
                        },
                        "content": {
                            "type": "string",
                            "description": "Initial content for the notebook (source code)",
                        },
                        "overwrite": {
                            "type": "boolean",
                            "description": "Whether to overwrite if notebook exists",
                            "default": False,
                        },
                    },
                    "required": ["path", "content"],
                },
            ),
            Tool(
                name="databricks_update_notebook",
                description="Update an existing notebook's content",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Full path to the notebook",
                        },
                        "content": {
                            "type": "string",
                            "description": "New content for the notebook",
                        },
                    },
                    "required": ["path", "content"],
                },
            ),
            Tool(
                name="databricks_delete_notebook",
                description="Delete a notebook or folder from the workspace",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Path to the notebook or folder to delete",
                        },
                        "recursive": {
                            "type": "boolean",
                            "description": "If true, delete folder and all contents",
                            "default": False,
                        },
                    },
                    "required": ["path"],
                },
            ),
            Tool(
                name="databricks_export_notebook",
                description="Export a notebook in various formats (SOURCE, HTML, JUPYTER, DBC)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Path to the notebook to export",
                        },
                        "format": {
                            "type": "string",
                            "enum": ["SOURCE", "HTML", "JUPYTER", "DBC"],
                            "description": "Export format",
                            "default": "SOURCE",
                        },
                    },
                    "required": ["path"],
                },
            ),
            Tool(
                name="databricks_create_folder",
                description="Create a folder in the workspace",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Path for the new folder",
                        },
                    },
                    "required": ["path"],
                },
            ),
            Tool(
                name="databricks_get_notebook_status",
                description="Get metadata about a notebook or folder (type, language, path)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Path to the workspace object",
                        },
                    },
                    "required": ["path"],
                },
            ),
        ]

    return {
        "databricks_list_notebooks": list_notebooks,
        "databricks_read_notebook": read_notebook,
        "databricks_create_notebook": create_notebook,
        "databricks_update_notebook": update_notebook,
        "databricks_delete_notebook": delete_notebook,
        "databricks_export_notebook": export_notebook,
        "databricks_create_folder": create_folder,
        "databricks_get_notebook_status": get_notebook_status,
    }


async def list_notebooks(arguments: dict[str, Any]) -> list[TextContent]:
    """List notebooks and folders in a workspace path."""
    client = get_client()
    path = arguments.get("path", "/")

    objects = list(client.workspace.list(path=path))

    result = []
    for obj in objects:
        obj_info = {
            "path": obj.path,
            "object_type": obj.object_type.value if obj.object_type else "UNKNOWN",
        }
        if obj.language:
            obj_info["language"] = obj.language.value
        if obj.modified_at:
            obj_info["modified_at"] = obj.modified_at
        result.append(obj_info)

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def read_notebook(arguments: dict[str, Any]) -> list[TextContent]:
    """Read the content of a notebook."""
    client = get_client()
    path = arguments["path"]

    export_response = client.workspace.export(path=path, format=ExportFormat.SOURCE)

    content = ""
    if export_response.content:
        content = base64.b64decode(export_response.content).decode("utf-8")

    return [TextContent(type="text", text=content)]


async def create_notebook(arguments: dict[str, Any]) -> list[TextContent]:
    """Create a new notebook."""
    client = get_client()

    path = arguments["path"]
    language_str = arguments.get("language", "PYTHON")
    content = arguments["content"]
    overwrite = arguments.get("overwrite", False)

    # Map string to Language enum
    language_map = {
        "PYTHON": Language.PYTHON,
        "SCALA": Language.SCALA,
        "SQL": Language.SQL,
        "R": Language.R,
    }
    language = language_map.get(language_str.upper(), Language.PYTHON)

    # Encode content to base64
    content_b64 = base64.b64encode(content.encode("utf-8")).decode("utf-8")

    client.workspace.import_(
        path=path,
        format=ImportFormat.SOURCE,
        language=language,
        content=content_b64,
        overwrite=overwrite,
    )

    return [TextContent(type="text", text=f"Notebook created successfully at: {path}")]


async def update_notebook(arguments: dict[str, Any]) -> list[TextContent]:
    """Update an existing notebook's content."""
    client = get_client()

    path = arguments["path"]
    content = arguments["content"]

    # First, get the notebook's language
    status = client.workspace.get_status(path=path)

    if status.object_type != ObjectType.NOTEBOOK:
        return [TextContent(type="text", text=f"Error: {path} is not a notebook")]

    # Encode content and import with overwrite
    content_b64 = base64.b64encode(content.encode("utf-8")).decode("utf-8")

    client.workspace.import_(
        path=path,
        format=ImportFormat.SOURCE,
        language=status.language,
        content=content_b64,
        overwrite=True,
    )

    return [TextContent(type="text", text=f"Notebook updated successfully: {path}")]


async def delete_notebook(arguments: dict[str, Any]) -> list[TextContent]:
    """Delete a notebook or folder."""
    client = get_client()

    path = arguments["path"]
    recursive = arguments.get("recursive", False)

    client.workspace.delete(path=path, recursive=recursive)

    return [TextContent(type="text", text=f"Deleted: {path}")]


async def export_notebook(arguments: dict[str, Any]) -> list[TextContent]:
    """Export a notebook in various formats."""
    client = get_client()

    path = arguments["path"]
    format_str = arguments.get("format", "SOURCE")

    # Map string to ExportFormat enum
    format_map = {
        "SOURCE": ExportFormat.SOURCE,
        "HTML": ExportFormat.HTML,
        "JUPYTER": ExportFormat.JUPYTER,
        "DBC": ExportFormat.DBC,
    }
    export_format = format_map.get(format_str.upper(), ExportFormat.SOURCE)

    export_response = client.workspace.export(path=path, format=export_format)

    if export_response.content:
        if export_format in [ExportFormat.SOURCE, ExportFormat.HTML]:
            # Text formats - decode to string
            content = base64.b64decode(export_response.content).decode("utf-8")
            return [TextContent(type="text", text=content)]
        else:
            # Binary formats - return base64
            return [
                TextContent(
                    type="text",
                    text=f"Exported as {format_str}. Content (base64):\n{export_response.content}",
                )
            ]

    return [TextContent(type="text", text="Export completed but no content returned")]


async def create_folder(arguments: dict[str, Any]) -> list[TextContent]:
    """Create a folder in the workspace."""
    client = get_client()
    path = arguments["path"]

    client.workspace.mkdirs(path=path)

    return [TextContent(type="text", text=f"Folder created: {path}")]


async def get_notebook_status(arguments: dict[str, Any]) -> list[TextContent]:
    """Get metadata about a notebook or folder."""
    client = get_client()
    path = arguments["path"]

    status = client.workspace.get_status(path=path)

    result = {
        "path": status.path,
        "object_type": status.object_type.value if status.object_type else "UNKNOWN",
    }

    if status.language:
        result["language"] = status.language.value
    if status.object_id:
        result["object_id"] = status.object_id
    if status.modified_at:
        result["modified_at"] = status.modified_at
    if status.size:
        result["size"] = status.size

    return [TextContent(type="text", text=json.dumps(result, indent=2))]
