"""Jobs and workflows tools for Databricks MCP Server."""

import json
from typing import Any

from mcp.server import Server
from mcp.types import Tool, TextContent

from databricks.sdk.service.jobs import NotebookTask, Task, RunLifeCycleState

from ..config import get_client


def register_job_tools(server: Server):
    """Register job management tools with the MCP server."""

    @server.list_tools()
    async def list_job_tools() -> list[Tool]:
        """Return job-related tools."""
        return [
            Tool(
                name="databricks_list_jobs",
                description="List all jobs in the workspace",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "name_filter": {
                            "type": "string",
                            "description": "Filter jobs by name (contains match)",
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of jobs to return",
                            "default": 25,
                        },
                    },
                    "required": [],
                },
            ),
            Tool(
                name="databricks_get_job",
                description="Get details about a specific job",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "job_id": {
                            "type": "integer",
                            "description": "The ID of the job",
                        },
                    },
                    "required": ["job_id"],
                },
            ),
            Tool(
                name="databricks_create_notebook_job",
                description="Create a job that runs a notebook",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Name for the job",
                        },
                        "notebook_path": {
                            "type": "string",
                            "description": "Path to the notebook to run",
                        },
                        "cluster_id": {
                            "type": "string",
                            "description": "Existing cluster ID to use (optional if new_cluster is provided)",
                        },
                        "parameters": {
                            "type": "object",
                            "description": "Parameters to pass to the notebook (key-value pairs)",
                        },
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
                        "job_id": {
                            "type": "integer",
                            "description": "The ID of the job to run",
                        },
                        "parameters": {
                            "type": "object",
                            "description": "Override parameters for this run",
                        },
                    },
                    "required": ["job_id"],
                },
            ),
            Tool(
                name="databricks_run_notebook_now",
                description="Run a notebook immediately as a one-time job",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "notebook_path": {
                            "type": "string",
                            "description": "Path to the notebook to run",
                        },
                        "cluster_id": {
                            "type": "string",
                            "description": "Cluster ID to run on",
                        },
                        "parameters": {
                            "type": "object",
                            "description": "Parameters to pass to the notebook",
                        },
                        "run_name": {
                            "type": "string",
                            "description": "Name for this run",
                        },
                    },
                    "required": ["notebook_path", "cluster_id"],
                },
            ),
            Tool(
                name="databricks_get_run",
                description="Get the status and details of a job run",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "run_id": {
                            "type": "integer",
                            "description": "The ID of the run",
                        },
                    },
                    "required": ["run_id"],
                },
            ),
            Tool(
                name="databricks_list_runs",
                description="List recent job runs",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "job_id": {
                            "type": "integer",
                            "description": "Filter by job ID",
                        },
                        "active_only": {
                            "type": "boolean",
                            "description": "Only show active runs",
                            "default": False,
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum runs to return",
                            "default": 25,
                        },
                    },
                    "required": [],
                },
            ),
            Tool(
                name="databricks_cancel_run",
                description="Cancel a running job",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "run_id": {
                            "type": "integer",
                            "description": "The ID of the run to cancel",
                        },
                    },
                    "required": ["run_id"],
                },
            ),
            Tool(
                name="databricks_get_run_output",
                description="Get the output of a completed job run",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "run_id": {
                            "type": "integer",
                            "description": "The ID of the run",
                        },
                    },
                    "required": ["run_id"],
                },
            ),
            Tool(
                name="databricks_delete_job",
                description="Delete a job",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "job_id": {
                            "type": "integer",
                            "description": "The ID of the job to delete",
                        },
                    },
                    "required": ["job_id"],
                },
            ),
        ]

    return {
        "databricks_list_jobs": list_jobs,
        "databricks_get_job": get_job,
        "databricks_create_notebook_job": create_notebook_job,
        "databricks_run_job": run_job,
        "databricks_run_notebook_now": run_notebook_now,
        "databricks_get_run": get_run,
        "databricks_list_runs": list_runs,
        "databricks_cancel_run": cancel_run,
        "databricks_get_run_output": get_run_output,
        "databricks_delete_job": delete_job,
    }


async def list_jobs(arguments: dict[str, Any]) -> list[TextContent]:
    """List all jobs in the workspace."""
    client = get_client()

    name_filter = arguments.get("name_filter")
    limit = arguments.get("limit", 25)

    jobs = list(client.jobs.list(name=name_filter, limit=limit))

    result = []
    for job in jobs:
        job_info = {
            "job_id": job.job_id,
            "name": job.settings.name if job.settings else None,
            "creator_user_name": job.creator_user_name,
            "created_time": job.created_time,
        }
        if job.settings and job.settings.schedule:
            job_info["schedule"] = job.settings.schedule.quartz_cron_expression
        result.append(job_info)

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def get_job(arguments: dict[str, Any]) -> list[TextContent]:
    """Get details about a specific job."""
    client = get_client()
    job_id = arguments["job_id"]

    job = client.jobs.get(job_id=job_id)

    result = {
        "job_id": job.job_id,
        "name": job.settings.name if job.settings else None,
        "creator_user_name": job.creator_user_name,
        "created_time": job.created_time,
    }

    if job.settings:
        settings = job.settings
        result["max_concurrent_runs"] = settings.max_concurrent_runs

        if settings.tasks:
            result["tasks"] = []
            for task in settings.tasks:
                task_info = {"task_key": task.task_key}
                if task.notebook_task:
                    task_info["notebook_path"] = task.notebook_task.notebook_path
                if task.existing_cluster_id:
                    task_info["cluster_id"] = task.existing_cluster_id
                result["tasks"].append(task_info)

        if settings.schedule:
            result["schedule"] = {
                "cron": settings.schedule.quartz_cron_expression,
                "timezone": settings.schedule.timezone_id,
            }

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def create_notebook_job(arguments: dict[str, Any]) -> list[TextContent]:
    """Create a job that runs a notebook."""
    client = get_client()

    name = arguments["name"]
    notebook_path = arguments["notebook_path"]
    cluster_id = arguments.get("cluster_id")
    parameters = arguments.get("parameters", {})

    # Build notebook task
    notebook_task = NotebookTask(
        notebook_path=notebook_path,
        base_parameters=parameters if parameters else None,
    )

    # Build task
    task = Task(
        task_key="main_task",
        notebook_task=notebook_task,
        existing_cluster_id=cluster_id,
    )

    # Create job
    job = client.jobs.create(name=name, tasks=[task])

    return [
        TextContent(
            type="text",
            text=f"Job created successfully!\nJob ID: {job.job_id}\nName: {name}",
        )
    ]


async def run_job(arguments: dict[str, Any]) -> list[TextContent]:
    """Trigger a job run."""
    client = get_client()

    job_id = arguments["job_id"]
    parameters = arguments.get("parameters")

    # Convert parameters to notebook params format if provided
    notebook_params = parameters if parameters else None

    run = client.jobs.run_now(job_id=job_id, notebook_params=notebook_params)

    return [
        TextContent(
            type="text",
            text=f"Job run triggered!\nRun ID: {run.run_id}\nUse databricks_get_run to check status.",
        )
    ]


async def run_notebook_now(arguments: dict[str, Any]) -> list[TextContent]:
    """Run a notebook immediately as a one-time job."""
    client = get_client()

    notebook_path = arguments["notebook_path"]
    cluster_id = arguments["cluster_id"]
    parameters = arguments.get("parameters", {})
    run_name = arguments.get("run_name", f"Run: {notebook_path}")

    # Submit run
    run = client.jobs.submit(
        run_name=run_name,
        tasks=[
            Task(
                task_key="notebook_task",
                existing_cluster_id=cluster_id,
                notebook_task=NotebookTask(
                    notebook_path=notebook_path,
                    base_parameters=parameters if parameters else None,
                ),
            )
        ],
    )

    return [
        TextContent(
            type="text",
            text=f"Notebook run submitted!\nRun ID: {run.run_id}\nUse databricks_get_run to check status.",
        )
    ]


async def get_run(arguments: dict[str, Any]) -> list[TextContent]:
    """Get the status of a job run."""
    client = get_client()
    run_id = arguments["run_id"]

    run = client.jobs.get_run(run_id=run_id)

    result = {
        "run_id": run.run_id,
        "job_id": run.job_id,
        "run_name": run.run_name,
        "state": None,
        "result_state": None,
        "start_time": run.start_time,
        "end_time": run.end_time,
        "run_duration": run.run_duration,
        "run_page_url": run.run_page_url,
    }

    if run.state:
        result["state"] = run.state.life_cycle_state.value if run.state.life_cycle_state else None
        result["result_state"] = run.state.result_state.value if run.state.result_state else None
        result["state_message"] = run.state.state_message

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def list_runs(arguments: dict[str, Any]) -> list[TextContent]:
    """List recent job runs."""
    client = get_client()

    job_id = arguments.get("job_id")
    active_only = arguments.get("active_only", False)
    limit = arguments.get("limit", 25)

    runs = list(
        client.jobs.list_runs(
            job_id=job_id,
            active_only=active_only,
            limit=limit,
        )
    )

    result = []
    for run in runs:
        run_info = {
            "run_id": run.run_id,
            "job_id": run.job_id,
            "run_name": run.run_name,
            "state": None,
            "result_state": None,
            "start_time": run.start_time,
        }
        if run.state:
            run_info["state"] = run.state.life_cycle_state.value if run.state.life_cycle_state else None
            run_info["result_state"] = run.state.result_state.value if run.state.result_state else None
        result.append(run_info)

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def cancel_run(arguments: dict[str, Any]) -> list[TextContent]:
    """Cancel a running job."""
    client = get_client()
    run_id = arguments["run_id"]

    client.jobs.cancel_run(run_id=run_id)

    return [TextContent(type="text", text=f"Run {run_id} cancellation requested.")]


async def get_run_output(arguments: dict[str, Any]) -> list[TextContent]:
    """Get the output of a completed job run."""
    client = get_client()
    run_id = arguments["run_id"]

    output = client.jobs.get_run_output(run_id=run_id)

    result = {
        "run_id": run_id,
    }

    if output.notebook_output:
        result["notebook_output"] = {
            "result": output.notebook_output.result,
            "truncated": output.notebook_output.truncated,
        }

    if output.error:
        result["error"] = output.error
    if output.error_trace:
        result["error_trace"] = output.error_trace

    if output.logs:
        result["logs"] = output.logs[:5000]  # Limit log size

    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def delete_job(arguments: dict[str, Any]) -> list[TextContent]:
    """Delete a job."""
    client = get_client()
    job_id = arguments["job_id"]

    client.jobs.delete(job_id=job_id)

    return [TextContent(type="text", text=f"Job {job_id} deleted successfully.")]
