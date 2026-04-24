"""Configuration and authentication for Databricks MCP Server.

Supports read-only mode, tool allowlisting, environment profiles,
and enhanced security configuration.
"""

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

from .security import get_safe_tools, get_tool_classification, RiskLevel


class EnvironmentProfile(Enum):
    """Environment profiles with different security defaults."""

    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


@dataclass
class SecurityConfig:
    """Security configuration for the MCP server."""

    read_only_mode: bool = False
    safe_mode: bool = False
    require_confirmation_for_destructive: bool = True
    allowed_tools: Optional[list[str]] = None
    blocked_tools: Optional[list[str]] = None
    max_sql_result_rows: int = 10000
    allow_arbitrary_code_execution: bool = False
    allow_arbitrary_sql_execution: bool = False
    sensitive_path_patterns: list[str] = field(default_factory=lambda: [
        '/mnt/production', '/mnt/prod', '/dbfs/production', '/dbfs/prod'
    ])

    @classmethod
    def for_profile(cls, profile: EnvironmentProfile) -> "SecurityConfig":
        """Get security configuration for a specific environment profile."""
        if profile == EnvironmentProfile.PRODUCTION:
            return cls(
                read_only_mode=True,
                safe_mode=True,
                require_confirmation_for_destructive=True,
                allow_arbitrary_code_execution=False,
                allow_arbitrary_sql_execution=False,
                max_sql_result_rows=1000,
            )
        elif profile == EnvironmentProfile.STAGING:
            return cls(
                read_only_mode=False,
                safe_mode=True,
                require_confirmation_for_destructive=True,
                allow_arbitrary_code_execution=False,
                allow_arbitrary_sql_execution=True,
                max_sql_result_rows=5000,
            )
        else:
            return cls(
                read_only_mode=False,
                safe_mode=False,
                require_confirmation_for_destructive=True,
                allow_arbitrary_code_execution=True,
                allow_arbitrary_sql_execution=True,
                max_sql_result_rows=10000,
            )

    def is_tool_allowed(self, tool_name: str) -> tuple[bool, str]:
        """Check if a tool is allowed by the current configuration.

        Returns:
            Tuple of (is_allowed, reason)
        """
        # Check explicit blocklist first
        if self.blocked_tools and tool_name in self.blocked_tools:
            return False, f"Tool '{tool_name}' is explicitly blocked"

        # Check explicit allowlist if set
        if self.allowed_tools is not None:
            if tool_name not in self.allowed_tools:
                return False, f"Tool '{tool_name}' is not in the allowed tools list"

        # Check read-only mode
        if self.read_only_mode:
            safe_tools = get_safe_tools()
            if tool_name not in safe_tools:
                return False, f"Tool '{tool_name}' is not allowed in read-only mode"

        # Check for code execution restrictions
        if not self.allow_arbitrary_code_execution:
            if tool_name == "databricks_execute_code":
                return False, "Arbitrary code execution is disabled"

        # Check for SQL execution restrictions in safe mode
        if not self.allow_arbitrary_sql_execution:
            if tool_name == "databricks_execute_sql":
                return False, "Arbitrary SQL execution is disabled (safe mode)"

        # Check safe mode restrictions
        if self.safe_mode:
            classification = get_tool_classification(tool_name)
            if classification and classification.risk_level in (RiskLevel.DESTRUCTIVE, RiskLevel.CRITICAL):
                return False, f"Tool '{tool_name}' ({classification.risk_level.value}) is blocked in safe mode"

        return True, "Tool is allowed"


@dataclass
class DatabricksConfig:
    """Configuration for Databricks connection."""

    host: str
    token: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    default_cluster_id: Optional[str] = None
    default_warehouse_id: Optional[str] = None
    profile: EnvironmentProfile = EnvironmentProfile.DEVELOPMENT
    security: SecurityConfig = field(default_factory=SecurityConfig)

    @classmethod
    def from_env(cls) -> "DatabricksConfig":
        """Load configuration from environment variables."""
        load_dotenv()

        host = os.getenv("DATABRICKS_HOST")
        if not host:
            raise ValueError(
                "DATABRICKS_HOST environment variable is required. "
                "Set it to your Databricks workspace URL (e.g., https://xxx.cloud.databricks.com)"
            )

        # Determine environment profile
        profile_str = os.getenv("DATABRICKS_MCP_PROFILE", "development").lower()
        try:
            profile = EnvironmentProfile(profile_str)
        except ValueError:
            profile = EnvironmentProfile.DEVELOPMENT

        # Build security config from environment
        security = SecurityConfig.for_profile(profile)

        # Override with explicit environment variables
        if os.getenv("DATABRICKS_MCP_READ_ONLY", "").lower() in ("true", "1", "yes"):
            security.read_only_mode = True

        if os.getenv("DATABRICKS_MCP_SAFE_MODE", "").lower() in ("true", "1", "yes"):
            security.safe_mode = True

        if os.getenv("DATABRICKS_MCP_ALLOW_CODE_EXECUTION", "").lower() in ("true", "1", "yes"):
            security.allow_arbitrary_code_execution = True
        elif os.getenv("DATABRICKS_MCP_ALLOW_CODE_EXECUTION", "").lower() in ("false", "0", "no"):
            security.allow_arbitrary_code_execution = False

        if os.getenv("DATABRICKS_MCP_ALLOW_SQL_EXECUTION", "").lower() in ("true", "1", "yes"):
            security.allow_arbitrary_sql_execution = True
        elif os.getenv("DATABRICKS_MCP_ALLOW_SQL_EXECUTION", "").lower() in ("false", "0", "no"):
            security.allow_arbitrary_sql_execution = False

        # Parse allowed tools list
        allowed_tools_str = os.getenv("DATABRICKS_MCP_ALLOWED_TOOLS", "")
        if allowed_tools_str:
            security.allowed_tools = [t.strip() for t in allowed_tools_str.split(",") if t.strip()]

        # Parse blocked tools list
        blocked_tools_str = os.getenv("DATABRICKS_MCP_BLOCKED_TOOLS", "")
        if blocked_tools_str:
            security.blocked_tools = [t.strip() for t in blocked_tools_str.split(",") if t.strip()]

        # Max SQL result rows
        max_rows = os.getenv("DATABRICKS_MCP_MAX_SQL_ROWS", "")
        if max_rows:
            try:
                security.max_sql_result_rows = int(max_rows)
            except ValueError:
                pass

        return cls(
            host=host,
            token=os.getenv("DATABRICKS_TOKEN"),
            client_id=os.getenv("DATABRICKS_CLIENT_ID"),
            client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
            default_cluster_id=os.getenv("DATABRICKS_CLUSTER_ID"),
            default_warehouse_id=os.getenv("DATABRICKS_WAREHOUSE_ID"),
            profile=profile,
            security=security,
        )

    def get_auth_type(self) -> str:
        """Determine the authentication type being used."""
        if self.token:
            return "pat"
        elif self.client_id and self.client_secret:
            return "oauth"
        else:
            return "default"


class DatabricksClient:
    """Singleton wrapper for Databricks WorkspaceClient."""

    _instance: Optional["DatabricksClient"] = None
    _client: Optional[WorkspaceClient] = None
    _config: Optional[DatabricksConfig] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def initialize(self, config: Optional[DatabricksConfig] = None):
        """Initialize the Databricks client with configuration."""
        if config is None:
            config = DatabricksConfig.from_env()

        self._config = config

        # Build client kwargs based on available credentials
        client_kwargs = {"host": config.host}

        if config.token:
            client_kwargs["token"] = config.token
        elif config.client_id and config.client_secret:
            client_kwargs["client_id"] = config.client_id
            client_kwargs["client_secret"] = config.client_secret

        self._client = WorkspaceClient(**client_kwargs)

    @property
    def client(self) -> WorkspaceClient:
        """Get the WorkspaceClient instance."""
        if self._client is None:
            self.initialize()
        return self._client

    @property
    def config(self) -> DatabricksConfig:
        """Get the configuration."""
        if self._config is None:
            self.initialize()
        return self._config

    @property
    def security(self) -> SecurityConfig:
        """Get the security configuration."""
        return self.config.security

    def is_tool_allowed(self, tool_name: str) -> tuple[bool, str]:
        """Check if a tool is allowed."""
        return self.security.is_tool_allowed(tool_name)

    def is_read_only(self) -> bool:
        """Check if running in read-only mode."""
        return self.security.read_only_mode

    def is_safe_mode(self) -> bool:
        """Check if running in safe mode."""
        return self.security.safe_mode

    def get_default_cluster_id(self) -> Optional[str]:
        """Get the default cluster ID from config or find a running cluster."""
        if self._config and self._config.default_cluster_id:
            return self._config.default_cluster_id

        try:
            clusters = list(self.client.clusters.list())
            for cluster in clusters:
                if cluster.state and cluster.state.value == "RUNNING":
                    return cluster.cluster_id
        except Exception:
            pass

        return None

    def get_default_warehouse_id(self) -> Optional[str]:
        """Get the default warehouse ID from config or find an active warehouse."""
        if self._config and self._config.default_warehouse_id:
            return self._config.default_warehouse_id

        try:
            warehouses = list(self.client.warehouses.list())
            for warehouse in warehouses:
                if warehouse.state and warehouse.state.value == "RUNNING":
                    return warehouse.id
        except Exception:
            pass

        return None


# Global client instance
databricks = DatabricksClient()


def get_client() -> WorkspaceClient:
    """Get the Databricks WorkspaceClient."""
    return databricks.client


def get_config() -> DatabricksConfig:
    """Get the Databricks configuration."""
    return databricks.config


def get_security_config() -> SecurityConfig:
    """Get the security configuration."""
    return databricks.security
