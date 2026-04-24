"""DBFS (Databricks File System) tools for Databricks MCP Server."""

import base64
import json
from typing import Any

from mcp.server import Server
from mcp.types import Tool, TextContent

from ..config import get_client
from ..validation import validate_dbfs_path, validate_boolean, validate_integer


def register_dbfs_tools(server: Server):
    """Register DBFS tools with the MCP server."""

    @server.list_tools()
    async def list_dbfs_tools() -> list[Tool]:
        """Return DBFS-related tools."""
        return [
            Tool(
                name="databricks_dbfs_list",
                description="List files and directories in DBFS",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "DBFS path to list (e.g., '/FileStore', '/mnt')",
                            "default": "/",
                        },
                    },
                    "required": [],
                },
            ),
            Tool(
                name="databricks_dbfs_read",
                description="Read content of a file from DBFS (for text files up to 1MB)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "DBFS path to the file",
                        },
                        "offset": {
                            "type": "integer",
                            "description": "Byte offset to start reading from",
                            "default": 0,
                        },
                        "length": {
                            "type": "integer",
                            "description": "Number of bytes to read (max 1MB)",
                            "default": 1048576,
                        },
                    },
                    "required": ["path"],
                },
            ),
            Tool(
                name="databricks_dbfs_write",
                description="Write content to a file in DBFS",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "DBFS path for the file",
                        },
                        "content": {
                            "type": "string",
                            "description": "Content to write (text)",
                        },
                        "overwrite": {
                            "type": "boolean",
                            "description": "Overwrite if file exists",
                            "default": False,
                        },
                    },
                    "required": ["path", "content"],
                },
            ),
            Tool(
                name="databricks_dbfs_delete",
                description="Delete a file or directory from DBFS",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "DBFS path to delete",
                        },
                        "recursive": {
                            "type": "boolean",
                            "description": "Delete recursively if directory",
                            "default": False,
                        },
                    },
                    "required": ["path"],
                },
            ),
            Tool(
                name="databricks_dbfs_mkdirs",
                description="Create directories in DBFS (creates parent dirs as needed)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "DBFS path to create",
                        },
                    },
                    "required": ["path"],
                },
            ),
            Tool(
                name="databricks_dbfs_get_status",
                description="Get metadata about a DBFS path (file size, type)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "DBFS path to check",
                        },
                    },
                    "required": ["path"],
                },
            ),
            Tool(
                name="databricks_dbfs_move",
                description="Move/rename a file or directory in DBFS",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "source_path": {
                            "type": "string",
                            "description": "Source DBFS path",
                        },
                        "destination_path": {
                            "type": "string",
                            "description": "Destination DBFS path",
                        },
                    },
                    "required": ["source_path", "destination_path"],
                },
            ),
        ]

    return {
        "databricks_dbfs_list": dbfs_list,
        "databricks_dbfs_read": dbfs_read,
        "databricks_dbfs_write": dbfs_write,
        "databricks_dbfs_delete": dbfs_delete,
        "databricks_dbfs_mkdirs": dbfs_mkdirs,
        "databricks_dbfs_get_status": dbfs_get_status,
        "databricks_dbfs_move": dbfs_move,
    }


async def dbfs_list(arguments: dict[str, Any]) -> list[TextContent]:
    """List files and directories in DBFS."""
    client = get_client()
    path = arguments.get("path", "/")

    files = list(client.dbfs.list(path=path))

    result = []
    for f in files:
        file_info = {
            "path": f.path,
            "is_dir": f.is_dir,
            "file_size": f.file_size,
            "modification_time": f.modification_time,
        }
        result.append(file_info)

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def dbfs_read(arguments: dict[str, Any]) -> list[TextContent]:
    """Read content of a file from DBFS."""
    client = get_client()

    path = arguments["path"]
    offset = arguments.get("offset", 0)
    length = arguments.get("length", 1048576)  # 1MB max

    # Read the file
    response = client.dbfs.read(path=path, offset=offset, length=length)

    if response.data:
        try:
            # Try to decode as text
            content = base64.b64decode(response.data).decode("utf-8")
            return [TextContent(type="text", text=content)]
        except UnicodeDecodeError:
            # Binary file
            return [
                TextContent(
                    type="text",
                    text=f"Binary file detected. Size: {response.bytes_read} bytes. Base64 content:\n{response.data[:1000]}...",
                )
            ]

    return [TextContent(type="text", text="File is empty or could not be read")]


async def dbfs_write(arguments: dict[str, Any]) -> list[TextContent]:
    """Write content to a file in DBFS."""
    client = get_client()

    path = arguments["path"]
    content = arguments["content"]
    overwrite = arguments.get("overwrite", False)

    # Encode content to base64
    content_b64 = base64.b64encode(content.encode("utf-8")).decode("utf-8")

    # Use put for small files (< 1MB)
    client.dbfs.put(path=path, contents=content_b64, overwrite=overwrite)

    return [TextContent(type="text", text=f"File written successfully: {path}")]


async def dbfs_delete(arguments: dict[str, Any]) -> list[TextContent]:
    """Delete a file or directory from DBFS."""
    client = get_client()

    # Validate path
    path_result = validate_dbfs_path(arguments.get("path"), "path")
    if not path_result.is_valid:
        return [TextContent(type="text", text=f"Error: {path_result.error_message}")]
    path = path_result.sanitized_value

    recursive_result = validate_boolean(arguments.get("recursive"), "recursive", required=False, default=False)
    recursive = recursive_result.sanitized_value

    # Safety check for recursive deletes on high-level paths
    if recursive:
        dangerous_paths = ['/', '/mnt', '/FileStore', '/databricks', '/user']
        if path in dangerous_paths or any(path.rstrip('/') == dp for dp in dangerous_paths):
            return [TextContent(
                type="text",
                text=f"Error: Recursive delete is not allowed on system path '{path}'"
            )]

    client.dbfs.delete(path=path, recursive=recursive)

    return [TextContent(type="text", text=f"Deleted: {path}")]


async def dbfs_mkdirs(arguments: dict[str, Any]) -> list[TextContent]:
    """Create directories in DBFS."""
    client = get_client()
    path = arguments["path"]

    client.dbfs.mkdirs(path=path)

    return [TextContent(type="text", text=f"Directory created: {path}")]


async def dbfs_get_status(arguments: dict[str, Any]) -> list[TextContent]:
    """Get metadata about a DBFS path."""
    client = get_client()
    path = arguments["path"]

    status = client.dbfs.get_status(path=path)

    result = {
        "path": status.path,
        "is_dir": status.is_dir,
        "file_size": status.file_size,
        "modification_time": status.modification_time,
    }

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def dbfs_move(arguments: dict[str, Any]) -> list[TextContent]:
    """Move/rename a file or directory in DBFS."""
    client = get_client()

    source_path = arguments["source_path"]
    destination_path = arguments["destination_path"]

    client.dbfs.move(source_path=source_path, destination_path=destination_path)

    return [
        TextContent(type="text", text=f"Moved: {source_path} -> {destination_path}")
    ]
