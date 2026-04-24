"""Security module for Databricks MCP Server.

Provides operation classification, SQL query filtering, path validation,
and confirmation workflows for destructive operations.
"""

import re
from enum import Enum
from typing import Optional
from dataclasses import dataclass


class OperationType(Enum):
    """Classification of operation types by risk level."""

    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    EXECUTE = "execute"
    ADMIN = "admin"


class RiskLevel(Enum):
    """Risk levels for operations."""

    SAFE = "safe"
    MODERATE = "moderate"
    DESTRUCTIVE = "destructive"
    CRITICAL = "critical"


@dataclass
class OperationClassification:
    """Classification result for a tool operation."""

    tool_name: str
    operation_type: OperationType
    risk_level: RiskLevel
    requires_confirmation: bool
    description: str


TOOL_CLASSIFICATIONS: dict[str, OperationClassification] = {
    # Read-only tools (SAFE)
    "databricks_list_clusters": OperationClassification(
        "databricks_list_clusters", OperationType.READ, RiskLevel.SAFE, False,
        "List clusters in workspace"
    ),
    "databricks_get_cluster": OperationClassification(
        "databricks_get_cluster", OperationType.READ, RiskLevel.SAFE, False,
        "Get cluster details"
    ),
    "databricks_list_spark_versions": OperationClassification(
        "databricks_list_spark_versions", OperationType.READ, RiskLevel.SAFE, False,
        "List available Spark versions"
    ),
    "databricks_list_node_types": OperationClassification(
        "databricks_list_node_types", OperationType.READ, RiskLevel.SAFE, False,
        "List available node types"
    ),
    "databricks_list_notebooks": OperationClassification(
        "databricks_list_notebooks", OperationType.READ, RiskLevel.SAFE, False,
        "List notebooks in workspace"
    ),
    "databricks_read_notebook": OperationClassification(
        "databricks_read_notebook", OperationType.READ, RiskLevel.SAFE, False,
        "Read notebook content"
    ),
    "databricks_export_notebook": OperationClassification(
        "databricks_export_notebook", OperationType.READ, RiskLevel.SAFE, False,
        "Export notebook"
    ),
    "databricks_get_notebook_status": OperationClassification(
        "databricks_get_notebook_status", OperationType.READ, RiskLevel.SAFE, False,
        "Get notebook metadata"
    ),
    "databricks_list_warehouses": OperationClassification(
        "databricks_list_warehouses", OperationType.READ, RiskLevel.SAFE, False,
        "List SQL warehouses"
    ),
    "databricks_get_warehouse": OperationClassification(
        "databricks_get_warehouse", OperationType.READ, RiskLevel.SAFE, False,
        "Get warehouse details"
    ),
    "databricks_get_query_history": OperationClassification(
        "databricks_get_query_history", OperationType.READ, RiskLevel.SAFE, False,
        "Get query history"
    ),
    "databricks_explain_sql": OperationClassification(
        "databricks_explain_sql", OperationType.READ, RiskLevel.SAFE, False,
        "Explain SQL query plan"
    ),
    "databricks_list_jobs": OperationClassification(
        "databricks_list_jobs", OperationType.READ, RiskLevel.SAFE, False,
        "List jobs"
    ),
    "databricks_get_job": OperationClassification(
        "databricks_get_job", OperationType.READ, RiskLevel.SAFE, False,
        "Get job details"
    ),
    "databricks_get_run": OperationClassification(
        "databricks_get_run", OperationType.READ, RiskLevel.SAFE, False,
        "Get run status"
    ),
    "databricks_list_runs": OperationClassification(
        "databricks_list_runs", OperationType.READ, RiskLevel.SAFE, False,
        "List job runs"
    ),
    "databricks_get_run_output": OperationClassification(
        "databricks_get_run_output", OperationType.READ, RiskLevel.SAFE, False,
        "Get run output"
    ),
    "databricks_dbfs_list": OperationClassification(
        "databricks_dbfs_list", OperationType.READ, RiskLevel.SAFE, False,
        "List DBFS files"
    ),
    "databricks_dbfs_read": OperationClassification(
        "databricks_dbfs_read", OperationType.READ, RiskLevel.SAFE, False,
        "Read DBFS file"
    ),
    "databricks_dbfs_get_status": OperationClassification(
        "databricks_dbfs_get_status", OperationType.READ, RiskLevel.SAFE, False,
        "Get DBFS path status"
    ),
    "databricks_list_catalogs": OperationClassification(
        "databricks_list_catalogs", OperationType.READ, RiskLevel.SAFE, False,
        "List catalogs"
    ),
    "databricks_get_catalog": OperationClassification(
        "databricks_get_catalog", OperationType.READ, RiskLevel.SAFE, False,
        "Get catalog details"
    ),
    "databricks_list_schemas": OperationClassification(
        "databricks_list_schemas", OperationType.READ, RiskLevel.SAFE, False,
        "List schemas"
    ),
    "databricks_get_schema": OperationClassification(
        "databricks_get_schema", OperationType.READ, RiskLevel.SAFE, False,
        "Get schema details"
    ),
    "databricks_list_tables": OperationClassification(
        "databricks_list_tables", OperationType.READ, RiskLevel.SAFE, False,
        "List tables"
    ),
    "databricks_describe_table": OperationClassification(
        "databricks_describe_table", OperationType.READ, RiskLevel.SAFE, False,
        "Describe table schema"
    ),
    "databricks_preview_table": OperationClassification(
        "databricks_preview_table", OperationType.READ, RiskLevel.SAFE, False,
        "Preview table data"
    ),
    "databricks_list_volumes": OperationClassification(
        "databricks_list_volumes", OperationType.READ, RiskLevel.SAFE, False,
        "List volumes"
    ),
    "databricks_get_volume": OperationClassification(
        "databricks_get_volume", OperationType.READ, RiskLevel.SAFE, False,
        "Get volume details"
    ),
    "databricks_list_functions": OperationClassification(
        "databricks_list_functions", OperationType.READ, RiskLevel.SAFE, False,
        "List functions"
    ),
    "databricks_search_tables": OperationClassification(
        "databricks_search_tables", OperationType.READ, RiskLevel.SAFE, False,
        "Search tables"
    ),

    # Write/Create operations (MODERATE)
    "databricks_create_notebook": OperationClassification(
        "databricks_create_notebook", OperationType.WRITE, RiskLevel.MODERATE, False,
        "Create new notebook"
    ),
    "databricks_update_notebook": OperationClassification(
        "databricks_update_notebook", OperationType.WRITE, RiskLevel.MODERATE, True,
        "Update existing notebook content"
    ),
    "databricks_create_folder": OperationClassification(
        "databricks_create_folder", OperationType.WRITE, RiskLevel.SAFE, False,
        "Create workspace folder"
    ),
    "databricks_dbfs_write": OperationClassification(
        "databricks_dbfs_write", OperationType.WRITE, RiskLevel.MODERATE, False,
        "Write file to DBFS"
    ),
    "databricks_dbfs_mkdirs": OperationClassification(
        "databricks_dbfs_mkdirs", OperationType.WRITE, RiskLevel.SAFE, False,
        "Create DBFS directories"
    ),
    "databricks_dbfs_move": OperationClassification(
        "databricks_dbfs_move", OperationType.WRITE, RiskLevel.MODERATE, True,
        "Move/rename DBFS file"
    ),
    "databricks_create_notebook_job": OperationClassification(
        "databricks_create_notebook_job", OperationType.WRITE, RiskLevel.MODERATE, False,
        "Create notebook job"
    ),

    # Start/Stop operations (MODERATE)
    "databricks_start_cluster": OperationClassification(
        "databricks_start_cluster", OperationType.ADMIN, RiskLevel.MODERATE, False,
        "Start cluster (incurs cost)"
    ),
    "databricks_start_warehouse": OperationClassification(
        "databricks_start_warehouse", OperationType.ADMIN, RiskLevel.MODERATE, False,
        "Start SQL warehouse (incurs cost)"
    ),
    "databricks_run_job": OperationClassification(
        "databricks_run_job", OperationType.EXECUTE, RiskLevel.MODERATE, False,
        "Trigger job run"
    ),
    "databricks_run_notebook_now": OperationClassification(
        "databricks_run_notebook_now", OperationType.EXECUTE, RiskLevel.MODERATE, False,
        "Run notebook immediately"
    ),

    # Create with cost implications (MODERATE, requires confirmation)
    "databricks_create_cluster": OperationClassification(
        "databricks_create_cluster", OperationType.ADMIN, RiskLevel.MODERATE, True,
        "Create new cluster (incurs cost)"
    ),

    # Destructive operations (DESTRUCTIVE, requires confirmation)
    "databricks_terminate_cluster": OperationClassification(
        "databricks_terminate_cluster", OperationType.ADMIN, RiskLevel.DESTRUCTIVE, True,
        "Terminate running cluster"
    ),
    "databricks_stop_warehouse": OperationClassification(
        "databricks_stop_warehouse", OperationType.ADMIN, RiskLevel.DESTRUCTIVE, True,
        "Stop SQL warehouse"
    ),
    "databricks_cancel_run": OperationClassification(
        "databricks_cancel_run", OperationType.ADMIN, RiskLevel.MODERATE, True,
        "Cancel running job"
    ),
    "databricks_delete_notebook": OperationClassification(
        "databricks_delete_notebook", OperationType.DELETE, RiskLevel.DESTRUCTIVE, True,
        "Delete notebook or folder"
    ),
    "databricks_dbfs_delete": OperationClassification(
        "databricks_dbfs_delete", OperationType.DELETE, RiskLevel.DESTRUCTIVE, True,
        "Delete DBFS file or directory"
    ),
    "databricks_delete_job": OperationClassification(
        "databricks_delete_job", OperationType.DELETE, RiskLevel.DESTRUCTIVE, True,
        "Delete job"
    ),

    # Code/SQL execution (CRITICAL)
    "databricks_execute_code": OperationClassification(
        "databricks_execute_code", OperationType.EXECUTE, RiskLevel.CRITICAL, True,
        "Execute arbitrary code on cluster"
    ),
    "databricks_execute_sql": OperationClassification(
        "databricks_execute_sql", OperationType.EXECUTE, RiskLevel.CRITICAL, True,
        "Execute SQL query"
    ),
}


class SQLQueryValidator:
    """Validates and classifies SQL queries for safety."""

    DDL_PATTERNS = [
        r'\bCREATE\s+(DATABASE|SCHEMA|TABLE|VIEW|FUNCTION|PROCEDURE)\b',
        r'\bDROP\s+(DATABASE|SCHEMA|TABLE|VIEW|FUNCTION|PROCEDURE)\b',
        r'\bALTER\s+(DATABASE|SCHEMA|TABLE|VIEW)\b',
        r'\bTRUNCATE\s+TABLE\b',
        r'\bRENAME\s+TABLE\b',
    ]

    DML_PATTERNS = [
        r'\bINSERT\s+INTO\b',
        r'\bUPDATE\s+\w+\s+SET\b',
        r'\bDELETE\s+FROM\b',
        r'\bMERGE\s+INTO\b',
        r'\bCOPY\s+INTO\b',
    ]

    DANGEROUS_PATTERNS = [
        r'\bDROP\s+DATABASE\b',
        r'\bDROP\s+SCHEMA\b',
        r'\bDROP\s+TABLE\b',
        r'\bTRUNCATE\b',
        r'\bDELETE\s+FROM\s+\w+\s*;?\s*$',  # DELETE without WHERE
        r'\bUPDATE\s+\w+\s+SET\s+.*(?!WHERE)',  # UPDATE without WHERE (simplified)
    ]

    SAFE_PATTERNS = [
        r'^\s*SELECT\b',
        r'^\s*SHOW\b',
        r'^\s*DESCRIBE\b',
        r'^\s*EXPLAIN\b',
        r'^\s*WITH\b.*\bSELECT\b',
    ]

    def __init__(self):
        self.ddl_re = [re.compile(p, re.IGNORECASE) for p in self.DDL_PATTERNS]
        self.dml_re = [re.compile(p, re.IGNORECASE) for p in self.DML_PATTERNS]
        self.dangerous_re = [re.compile(p, re.IGNORECASE) for p in self.DANGEROUS_PATTERNS]
        self.safe_re = [re.compile(p, re.IGNORECASE) for p in self.SAFE_PATTERNS]

    def classify_query(self, query: str) -> tuple[RiskLevel, str]:
        """Classify a SQL query by risk level.

        Returns:
            Tuple of (RiskLevel, description of why)
        """
        query_stripped = query.strip()

        # Check for dangerous patterns first
        for pattern in self.dangerous_re:
            if pattern.search(query_stripped):
                return RiskLevel.CRITICAL, f"Query contains dangerous operation: {pattern.pattern}"

        # Check for DDL
        for pattern in self.ddl_re:
            if pattern.search(query_stripped):
                return RiskLevel.DESTRUCTIVE, "Query contains DDL statement"

        # Check for DML
        for pattern in self.dml_re:
            if pattern.search(query_stripped):
                return RiskLevel.MODERATE, "Query contains DML statement"

        # Check for safe patterns
        for pattern in self.safe_re:
            if pattern.match(query_stripped):
                return RiskLevel.SAFE, "Read-only query"

        # Unknown pattern - treat as moderate risk
        return RiskLevel.MODERATE, "Query type could not be determined"

    def is_safe_query(self, query: str) -> bool:
        """Check if a query is safe (read-only)."""
        risk_level, _ = self.classify_query(query)
        return risk_level == RiskLevel.SAFE

    def validate_for_safe_mode(self, query: str) -> tuple[bool, str]:
        """Validate a query for safe mode execution.

        Returns:
            Tuple of (is_allowed, reason)
        """
        risk_level, reason = self.classify_query(query)

        if risk_level in (RiskLevel.CRITICAL, RiskLevel.DESTRUCTIVE):
            return False, f"Query blocked in safe mode: {reason}"

        if risk_level == RiskLevel.MODERATE:
            return False, f"Query blocked in safe mode (DML not allowed): {reason}"

        return True, "Query allowed"


class PathValidator:
    """Validates paths for security concerns."""

    BLOCKED_PATH_PATTERNS = [
        r'\.\./',  # Path traversal
        r'\.\.\\',  # Windows path traversal
        r'\x00',  # Null bytes
    ]

    SENSITIVE_PATHS = [
        '/mnt/production',
        '/mnt/prod',
        '/dbfs/production',
        '/dbfs/prod',
        '/Workspace/Production',
        '/Workspace/Prod',
    ]

    def __init__(self, blocked_patterns: Optional[list[str]] = None,
                 sensitive_paths: Optional[list[str]] = None):
        self.blocked_patterns = blocked_patterns or self.BLOCKED_PATH_PATTERNS
        self.sensitive_paths = sensitive_paths or self.SENSITIVE_PATHS
        self.blocked_re = [re.compile(p, re.IGNORECASE) for p in self.blocked_patterns]

    def validate_path(self, path: str) -> tuple[bool, str]:
        """Validate a path for security issues.

        Returns:
            Tuple of (is_valid, reason)
        """
        if not path:
            return False, "Path cannot be empty"

        # Check for blocked patterns
        for pattern in self.blocked_re:
            if pattern.search(path):
                return False, f"Path contains blocked pattern: {pattern.pattern}"

        return True, "Path is valid"

    def is_sensitive_path(self, path: str) -> bool:
        """Check if a path is in a sensitive location."""
        path_lower = path.lower()
        for sensitive in self.sensitive_paths:
            if path_lower.startswith(sensitive.lower()):
                return True
        return False

    def validate_table_name(self, full_name: str) -> tuple[bool, str]:
        """Validate a Unity Catalog table name for SQL injection.

        Returns:
            Tuple of (is_valid, reason)
        """
        if not full_name:
            return False, "Table name cannot be empty"

        # Table names should only contain alphanumeric, underscore, dot
        if not re.match(r'^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$', full_name):
            if not re.match(r'^[a-zA-Z0-9_\.]+$', full_name):
                return False, "Table name contains invalid characters"

        # Check for SQL injection attempts
        dangerous = [';', '--', '/*', '*/', "'", '"', 'DROP', 'DELETE', 'TRUNCATE', 'INSERT', 'UPDATE']
        for pattern in dangerous:
            if pattern.upper() in full_name.upper():
                return False, f"Table name contains potentially dangerous pattern: {pattern}"

        return True, "Table name is valid"


def get_tool_classification(tool_name: str) -> Optional[OperationClassification]:
    """Get the classification for a tool."""
    return TOOL_CLASSIFICATIONS.get(tool_name)


def is_tool_allowed_in_read_only_mode(tool_name: str) -> bool:
    """Check if a tool is allowed in read-only mode."""
    classification = get_tool_classification(tool_name)
    if classification is None:
        return False
    return classification.operation_type == OperationType.READ


def get_tools_by_risk_level(risk_level: RiskLevel) -> list[str]:
    """Get all tools with a specific risk level."""
    return [
        name for name, classification in TOOL_CLASSIFICATIONS.items()
        if classification.risk_level == risk_level
    ]


def get_safe_tools() -> list[str]:
    """Get all safe (read-only) tools."""
    return [
        name for name, classification in TOOL_CLASSIFICATIONS.items()
        if classification.risk_level == RiskLevel.SAFE
    ]


def get_destructive_tools() -> list[str]:
    """Get all destructive tools that require confirmation."""
    return [
        name for name, classification in TOOL_CLASSIFICATIONS.items()
        if classification.risk_level in (RiskLevel.DESTRUCTIVE, RiskLevel.CRITICAL)
    ]
