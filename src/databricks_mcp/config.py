"""Configuration and authentication for Databricks MCP Server."""

import os
from dataclasses import dataclass
from typing import Optional

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv


@dataclass
class DatabricksConfig:
    """Configuration for Databricks connection."""

    host: str
    token: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    default_cluster_id: Optional[str] = None
    default_warehouse_id: Optional[str] = None

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

        return cls(
            host=host,
            token=os.getenv("DATABRICKS_TOKEN"),
            client_id=os.getenv("DATABRICKS_CLIENT_ID"),
            client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
            default_cluster_id=os.getenv("DATABRICKS_CLUSTER_ID"),
            default_warehouse_id=os.getenv("DATABRICKS_WAREHOUSE_ID"),
        )

    def get_auth_type(self) -> str:
        """Determine the authentication type being used."""
        if self.token:
            return "pat"
        elif self.client_id and self.client_secret:
            return "oauth"
        else:
            return "default"  # SDK will try to auto-detect


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
        # If neither is set, SDK will try auto-detection (Azure CLI, etc.)

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

    def get_default_cluster_id(self) -> Optional[str]:
        """Get the default cluster ID from config or find a running cluster."""
        if self._config and self._config.default_cluster_id:
            return self._config.default_cluster_id

        # Try to find a running cluster
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

        # Try to find a running warehouse
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
