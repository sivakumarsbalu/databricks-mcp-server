"""Tests for Databricks MCP Server tools."""

import pytest
from unittest.mock import MagicMock, patch


class TestClusterTools:
    """Tests for cluster management tools."""

    @pytest.mark.asyncio
    async def test_list_clusters_returns_formatted_json(self):
        """Test that list_clusters returns properly formatted cluster info."""
        from databricks_mcp.tools.clusters import list_clusters

        mock_cluster = MagicMock()
        mock_cluster.cluster_id = "test-cluster-123"
        mock_cluster.cluster_name = "Test Cluster"
        mock_cluster.state = MagicMock(value="RUNNING")
        mock_cluster.spark_version = "13.3.x-scala2.12"
        mock_cluster.node_type_id = "Standard_DS3_v2"
        mock_cluster.num_workers = 2
        mock_cluster.creator_user_name = "user@example.com"

        with patch("databricks_mcp.tools.clusters.get_client") as mock_client:
            mock_client.return_value.clusters.list.return_value = [mock_cluster]

            result = await list_clusters({})

            assert len(result) == 1
            assert "test-cluster-123" in result[0].text
            assert "RUNNING" in result[0].text


class TestNotebookTools:
    """Tests for notebook management tools."""

    @pytest.mark.asyncio
    async def test_list_notebooks_returns_workspace_objects(self):
        """Test that list_notebooks returns workspace objects."""
        from databricks_mcp.tools.notebooks import list_notebooks

        mock_obj = MagicMock()
        mock_obj.path = "/Users/test/notebook"
        mock_obj.object_type = MagicMock(value="NOTEBOOK")
        mock_obj.language = MagicMock(value="PYTHON")
        mock_obj.modified_at = 1234567890

        with patch("databricks_mcp.tools.notebooks.get_client") as mock_client:
            mock_client.return_value.workspace.list.return_value = [mock_obj]

            result = await list_notebooks({"path": "/Users/test"})

            assert len(result) == 1
            assert "notebook" in result[0].text
            assert "PYTHON" in result[0].text


class TestSQLTools:
    """Tests for SQL warehouse tools."""

    @pytest.mark.asyncio
    async def test_list_warehouses_returns_warehouse_info(self):
        """Test that list_warehouses returns warehouse information."""
        from databricks_mcp.tools.sql import list_warehouses

        mock_wh = MagicMock()
        mock_wh.id = "wh-123"
        mock_wh.name = "Test Warehouse"
        mock_wh.state = MagicMock(value="RUNNING")
        mock_wh.cluster_size = "Small"
        mock_wh.min_num_clusters = 1
        mock_wh.max_num_clusters = 1
        mock_wh.auto_stop_mins = 10
        mock_wh.warehouse_type = MagicMock(value="PRO")

        with patch("databricks_mcp.tools.sql.get_client") as mock_client:
            mock_client.return_value.warehouses.list.return_value = [mock_wh]

            result = await list_warehouses({})

            assert len(result) == 1
            assert "wh-123" in result[0].text
            assert "RUNNING" in result[0].text


class TestUnityCatalogTools:
    """Tests for Unity Catalog tools."""

    @pytest.mark.asyncio
    async def test_list_catalogs_returns_catalog_info(self):
        """Test that list_catalogs returns catalog information."""
        from databricks_mcp.tools.unity_catalog import list_catalogs

        mock_cat = MagicMock()
        mock_cat.name = "main"
        mock_cat.comment = "Main catalog"
        mock_cat.owner = "admin"
        mock_cat.created_at = 1234567890
        mock_cat.metastore_id = "ms-123"

        with patch("databricks_mcp.tools.unity_catalog.get_client") as mock_client:
            mock_client.return_value.catalogs.list.return_value = [mock_cat]

            result = await list_catalogs({})

            assert len(result) == 1
            assert "main" in result[0].text


class TestDBFSTools:
    """Tests for DBFS tools."""

    @pytest.mark.asyncio
    async def test_dbfs_list_returns_file_info(self):
        """Test that dbfs_list returns file information."""
        from databricks_mcp.tools.dbfs import dbfs_list

        mock_file = MagicMock()
        mock_file.path = "/FileStore/data.csv"
        mock_file.is_dir = False
        mock_file.file_size = 1024
        mock_file.modification_time = 1234567890

        with patch("databricks_mcp.tools.dbfs.get_client") as mock_client:
            mock_client.return_value.dbfs.list.return_value = [mock_file]

            result = await dbfs_list({"path": "/FileStore"})

            assert len(result) == 1
            assert "data.csv" in result[0].text


class TestJobTools:
    """Tests for job management tools."""

    @pytest.mark.asyncio
    async def test_list_jobs_returns_job_info(self):
        """Test that list_jobs returns job information."""
        from databricks_mcp.tools.jobs import list_jobs

        mock_job = MagicMock()
        mock_job.job_id = 123
        mock_job.settings = MagicMock()
        mock_job.settings.name = "Test Job"
        mock_job.settings.schedule = None
        mock_job.creator_user_name = "user@example.com"
        mock_job.created_time = 1234567890

        with patch("databricks_mcp.tools.jobs.get_client") as mock_client:
            mock_client.return_value.jobs.list.return_value = [mock_job]

            result = await list_jobs({})

            assert len(result) == 1
            assert "123" in result[0].text
            assert "Test Job" in result[0].text
