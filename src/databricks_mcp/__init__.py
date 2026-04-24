"""Databricks MCP Server - Connect Claude Code to Databricks workspaces.

Enhanced with enterprise security features:
- Read-only and safe modes
- Tool allowlisting/blocklisting
- SQL query validation
- Structured JSON logging with audit trail
- Input validation and path sanitization
- Environment profiles (development, staging, production)
"""

__version__ = "0.2.0"

from .config import (
    DatabricksConfig,
    SecurityConfig,
    EnvironmentProfile,
    get_client,
    get_config,
    get_security_config,
)

from .security import (
    OperationType,
    RiskLevel,
    SQLQueryValidator,
    PathValidator,
    get_tool_classification,
    is_tool_allowed_in_read_only_mode,
    get_safe_tools,
    get_destructive_tools,
)

from .logging_config import (
    configure_logging,
    set_correlation_id,
    set_user_id,
    audit_logger,
    logger,
)

__all__ = [
    # Config
    "DatabricksConfig",
    "SecurityConfig",
    "EnvironmentProfile",
    "get_client",
    "get_config",
    "get_security_config",
    # Security
    "OperationType",
    "RiskLevel",
    "SQLQueryValidator",
    "PathValidator",
    "get_tool_classification",
    "is_tool_allowed_in_read_only_mode",
    "get_safe_tools",
    "get_destructive_tools",
    # Logging
    "configure_logging",
    "set_correlation_id",
    "set_user_id",
    "audit_logger",
    "logger",
]
