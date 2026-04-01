"""
Grafana Service - MCP Tools for querying Grafana/Mimir metrics
Provides read-only access to Grafana Prometheus/Mimir API.
"""

import base64
import logging
from typing import Any, Dict, List, Optional

import httpx
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError

from ..config import get_settings

logger = logging.getLogger(__name__)


class GrafanaClient:
    """Client for interacting with Grafana/Mimir Prometheus API."""

    def __init__(self):
        """Initialize Grafana client with settings."""
        self.settings = get_settings()
        self.base_url = self.settings.grafana_base_url.rstrip('/')
        self.tenant_id = self.settings.grafana_tenant_id
        self.user = self.settings.grafana_user
        self.token = self.settings.grafana_token
        self.timeout = self.settings.grafana_timeout

        if not self.token:
            logger.warning("Grafana token not configured - queries will fail")

    def _get_headers(self) -> Dict[str, str]:
        """Build request headers with authentication."""
        # Build Basic Auth header
        auth_string = f"{self.user}:{self.token}"
        basic_auth = "Basic " + base64.b64encode(auth_string.encode()).decode()

        return {
            "X-Scope-OrgID": self.tenant_id,
            "Authorization": basic_auth,
            "Accept": "application/json"
        }

    async def query_instant(self, promql: str) -> Dict[str, Any]:
        """
        Execute an instant PromQL query.

        Args:
            promql: PromQL query string

        Returns:
            Query result data

        Raises:
            ToolError: If query fails
        """
        url = f"{self.base_url}/api/prom/api/v1/query"
        headers = self._get_headers()
        params = {"query": promql}

        logger.info(f"Executing instant query: {promql}")

        try:
            async with httpx.AsyncClient(verify=False) as client:
                response = await client.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=self.timeout
                )
                response.raise_for_status()
                data = response.json()

                if data.get("status") != "success":
                    raise ToolError(f"Query failed: {data.get('error', 'Unknown error')}")

                return data.get("data", {})

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error querying Grafana: {e}")
            raise ToolError(f"Grafana API error: {e.response.status_code} - {e.response.text}")
        except httpx.RequestError as e:
            logger.error(f"Request error querying Grafana: {e}")
            raise ToolError(f"Failed to connect to Grafana: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error querying Grafana: {e}")
            raise ToolError(f"Query failed: {str(e)}")

    async def query_range(
        self,
        promql: str,
        start: str,
        end: str,
        step: str = "15s"
    ) -> Dict[str, Any]:
        """
        Execute a range PromQL query.

        Args:
            promql: PromQL query string
            start: Start time (RFC3339 or Unix timestamp)
            end: End time (RFC3339 or Unix timestamp)
            step: Query resolution step width (e.g., "15s", "1m", "1h")

        Returns:
            Query result data

        Raises:
            ToolError: If query fails
        """
        url = f"{self.base_url}/api/prom/api/v1/query_range"
        headers = self._get_headers()
        params = {
            "query": promql,
            "start": start,
            "end": end,
            "step": step
        }

        logger.info(f"Executing range query: {promql} from {start} to {end}")

        try:
            async with httpx.AsyncClient(verify=False) as client:
                response = await client.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=self.timeout
                )
                response.raise_for_status()
                data = response.json()

                if data.get("status") != "success":
                    raise ToolError(f"Query failed: {data.get('error', 'Unknown error')}")

                return data.get("data", {})

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error querying Grafana: {e}")
            raise ToolError(f"Grafana API error: {e.response.status_code} - {e.response.text}")
        except httpx.RequestError as e:
            logger.error(f"Request error querying Grafana: {e}")
            raise ToolError(f"Failed to connect to Grafana: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error querying Grafana: {e}")
            raise ToolError(f"Query failed: {str(e)}")

    async def get_label_values(self, label: str) -> List[str]:
        """
        Get all values for a specific label.

        Args:
            label: Label name to query

        Returns:
            List of label values

        Raises:
            ToolError: If query fails
        """
        url = f"{self.base_url}/api/prom/api/v1/label/{label}/values"
        headers = self._get_headers()

        logger.info(f"Fetching label values for: {label}")

        try:
            async with httpx.AsyncClient(verify=False) as client:
                response = await client.get(
                    url,
                    headers=headers,
                    timeout=self.timeout
                )
                response.raise_for_status()
                data = response.json()

                if data.get("status") != "success":
                    raise ToolError(f"Query failed: {data.get('error', 'Unknown error')}")

                return data.get("data", [])

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error querying Grafana: {e}")
            raise ToolError(f"Grafana API error: {e.response.status_code} - {e.response.text}")
        except httpx.RequestError as e:
            logger.error(f"Request error querying Grafana: {e}")
            raise ToolError(f"Failed to connect to Grafana: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error querying Grafana: {e}")
            raise ToolError(f"Query failed: {str(e)}")


def register_grafana_tools(mcp: FastMCP):
    """Register Grafana query tools with the MCP server."""

    client = GrafanaClient()

    @mcp.tool()
    async def query_grafana_metric(
        metric_name: str,
        filters: Optional[str] = None,
        aggregation: Optional[str] = None,
        time_range: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Query Grafana metrics using PromQL (READ-ONLY).

        This tool queries Grafana/Mimir for metrics and returns the current values.
        It does NOT modify any data in Grafana.

        Args:
            metric_name: The metric name to query (e.g., "zbx_system_cpu_utilization")
            filters: Optional label filters in PromQL format (e.g., 'region="southeastasia",env="prod"')
            aggregation: Optional aggregation function (e.g., "avg_over_time", "max_over_time", "min_over_time")
            time_range: Optional time range for aggregation (e.g., "[5m]", "[1h]", "[5d]"). Only used with aggregation.

        Returns:
            Dictionary containing query results with metric labels and values

        Examples:
            - Simple query: query_grafana_metric("zbx_system_cpu_utilization")
            - With filter: query_grafana_metric("zbx_system_cpu_utilization", filters='region="southeastasia"')
            - With aggregation: query_grafana_metric("zbx_system_cpu_utilization", 
                                                     filters='region="southeastasia"',
                                                     aggregation="avg_over_time",
                                                     time_range="[5d]")
        """
        logger.info(f"Querying metric: {metric_name} with filters={filters}, agg={aggregation}")

        # Build PromQL query
        if filters:
            base_query = f"{metric_name}{{{filters}}}"
        else:
            base_query = metric_name

        # Apply aggregation if requested
        if aggregation and time_range:
            promql = f"{aggregation}({base_query}{time_range})"
        elif aggregation:
            promql = f"{aggregation}({base_query})"
        else:
            promql = base_query

        # Execute query
        result = await client.query_instant(promql)

        # Format response
        return {
            "query": promql,
            "result_type": result.get("resultType"),
            "results": result.get("result", [])
        }

    @mcp.tool()
    async def query_grafana_custom_promql(promql: str) -> Dict[str, Any]:
        """
        Execute a custom PromQL query on Grafana (READ-ONLY).

        This allows advanced users to execute any PromQL query directly.
        This is READ-ONLY and does NOT modify data in Grafana.

        Args:
            promql: Full PromQL query string

        Returns:
            Dictionary containing query results

        Example:
            query_grafana_custom_promql('avg_over_time(zbx_system_cpu_utilization{region="southeastasia"}[5d])')
        """
        logger.info(f"Executing custom PromQL: {promql}")

        result = await client.query_instant(promql)

        return {
            "query": promql,
            "result_type": result.get("resultType"),
            "results": result.get("result", [])
        }

    @mcp.tool()
    async def query_grafana_time_range(
        metric_name: str,
        start_time: str,
        end_time: str,
        filters: Optional[str] = None,
        step: str = "1m"
    ) -> Dict[str, Any]:
        """
        Query Grafana metrics over a time range (READ-ONLY).

        This queries historical metric data over a specified time period.
        This is READ-ONLY and does NOT modify data in Grafana.

        Args:
            metric_name: The metric name to query
            start_time: Start time (Unix timestamp or RFC3339 format)
            end_time: End time (Unix timestamp or RFC3339 format)
            filters: Optional label filters in PromQL format
            step: Query resolution step (e.g., "15s", "1m", "5m", "1h")

        Returns:
            Dictionary containing time series data

        Example:
            query_grafana_time_range("zbx_system_cpu_utilization", 
                                     "2026-02-18T00:00:00Z", 
                                     "2026-02-18T23:59:59Z",
                                     filters='region="southeastasia"',
                                     step="5m")
        """
        logger.info(f"Querying time range for {metric_name}: {start_time} to {end_time}")

        # Build PromQL query
        if filters:
            promql = f"{metric_name}{{{filters}}}"
        else:
            promql = metric_name

        # Execute range query
        result = await client.query_range(promql, start_time, end_time, step)

        return {
            "query": promql,
            "start": start_time,
            "end": end_time,
            "step": step,
            "result_type": result.get("resultType"),
            "results": result.get("result", [])
        }

    @mcp.tool()
    async def list_grafana_label_values(label_name: str) -> List[str]:
        """
        List all possible values for a Grafana label (READ-ONLY).

        Useful for discovering available filters like regions, environments, services, etc.
        This is READ-ONLY and does NOT modify data in Grafana.

        Args:
            label_name: Name of the label to query (e.g., "region", "env", "service")

        Returns:
            List of available values for the label

        Example:
            list_grafana_label_values("region")  # Returns ["southeastasia", "westeurope", ...]
        """
        logger.info(f"Listing values for label: {label_name}")

        values = await client.get_label_values(label_name)

        return values

    @mcp.tool()
    async def query_server_metrics_bulk(
        servers: str,
        time_range: str = "7d",
        return_as_file: bool = True,
    ) -> Dict[str, Any]:
        """
        Query CPU utilisation, disk IO read and disk IO write for a list of servers (READ-ONLY).

        Makes three Grafana queries (one per metric) using avg_over_time over the specified
        time range, then consolidates the results into a clean list keyed by server name.
        When Grafana returns multiple series for the same instance only the first one is kept.

        Metrics queried:
            - zbx_system_cpu_utilization
            - zbx_system_disk_io_time_read_seconds_avg
            - zbx_system_disk_io_time_write_seconds_avg

        Args:
            servers: Semicolon-delimited list of server instance names (e.g. "baupins0002;baupins0004")
            time_range: PromQL time range window (default "7d", e.g. "1h", "24h", "7d")
            return_as_file: When True (default), returns the result as a base64-encoded CSV file.
                            The SDK automatically intercepts this, saves it to session context, and
                            strips the base64 from the agent's context window. The agent can then
                            call list_available_files() and upload_dataframe() to load it as a
                            SQLite table. Set to False to get raw JSON rows instead.

        Returns:
            When return_as_file=True: dict with keys name, content_base64, size, mime_type,
            row_count — SDK will auto-save as a file.
            When return_as_file=False: list of dicts with keys: server, cpu_util,
            io_read, io_write. Values are numeric strings or null if not found.

        Example:
            query_server_metrics_bulk("baupins0002;baupins0004")
        """
        import asyncio
        import csv
        import io

        server_list = [s.strip() for s in servers.split(";") if s.strip()]
        if not server_list:
            if return_as_file:
                return {
                    "name": "grafana_server_metrics.csv",
                    "content_base64": "",
                    "size": 0,
                    "mime_type": "text/csv",
                    "row_count": 0,
                    "message": "No servers provided.",
                }
            return []

        logger.info(f"Bulk server metrics query for {len(server_list)} servers over [{time_range}]")

        # Build instance regex filter — append .* to each name so partial suffixes are matched
        instance_regex = "|".join(f"{s}.*" for s in server_list)
        instance_filter = f'instance=~"{instance_regex}"'

        metrics = {
            "cpu_util": "zbx_system_cpu_utilization",
            "io_read": "zbx_system_disk_io_time_read_seconds_avg",
            "io_write": "zbx_system_disk_io_time_write_seconds_avg",
        }

        # Helper: map each returned instance back to the original server name (prefix match).
        # Grafana may return e.g. "pjpkoks0016_kok_prod_cnb" when we queried for "pjpkoks0016".
        # Only the first series that matches a given server is kept.
        def _extract_first_value_per_instance(result_list: List[Dict]) -> Dict[str, str]:
            mapping: Dict[str, str] = {}
            for series in result_list:
                instance = series.get("metric", {}).get("instance")
                if not instance:
                    continue
                # Find the first server in our list whose name is a prefix of this instance
                for server in server_list:
                    if instance.lower().startswith(server.lower()) and server not in mapping:
                        value_pair = series.get("value", [])
                        if len(value_pair) >= 2:
                            mapping[server] = value_pair[1]
                        break
            return mapping

        # Fire all three queries concurrently
        metric_data: Dict[str, Dict[str, str]] = {}

        async def _fetch(key: str, metric: str):
            promql = f'avg_over_time({metric}{{{instance_filter}}}[{time_range}])'
            logger.info(f"Querying {key}: {promql}")
            result = await client.query_instant(promql)
            metric_data[key] = _extract_first_value_per_instance(result.get("result", []))

        await asyncio.gather(*[_fetch(k, m) for k, m in metrics.items()])

        # Consolidate into one record per server
        rows: List[Dict[str, Any]] = []
        for server in server_list:
            rows.append({
                "server": server,
                "cpu_util": metric_data.get("cpu_util", {}).get(server),
                "io_read": metric_data.get("io_read", {}).get(server),
                "io_write": metric_data.get("io_write", {}).get(server),
            })

        if not return_as_file:
            return rows

        # Encode as CSV so the SDK intercepts content_base64, saves the
        # file to session context, and returns only metadata to the LLM.
        if not rows:
            return {
                "name": "grafana_server_metrics.csv",
                "content_base64": "",
                "size": 0,
                "mime_type": "text/csv",
                "row_count": 0,
                "message": "No rows returned for the given server filter.",
            }

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()), lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)
        csv_bytes = buf.getvalue().encode("utf-8")

        logger.info(
            f"query_server_metrics_bulk: returning {len(rows)} rows as CSV file "
            f"({len(csv_bytes):,} bytes) — SDK will auto-save to session context"
        )

        return {
            "name": "grafana_server_metrics.csv",
            "content_base64": base64.b64encode(csv_bytes).decode("ascii"),
            "size": len(csv_bytes),
            "mime_type": "text/csv",
            "row_count": len(rows),
        }

    @mcp.tool()
    async def query_server_synthetic_utilisation(
        servers: str,
        return_as_file: bool = True,
    ) -> Dict[str, Any]:
        """
        Query synthetic utilisation score for a list of servers via the considered_dead:bool metric (READ-ONLY).

        Queries the `considered_dead:bool` metric using an anchored regex per server so that
        only the intended instances are matched (e.g. "byffm4" matches "byffm4_ffm_prod_cnb"
        but NOT "byffm40002").  When Grafana returns multiple series for the same server only
        the first match is kept.

        Args:
            servers: Semicolon-delimited list of server instance names (e.g. "byffm4;bydar4")
            return_as_file: When True (default), returns the result as a base64-encoded CSV file.
                            The SDK automatically intercepts this, saves it to session context, and
                            strips the base64 from the agent's context window. The agent can then
                            call list_available_files() and upload_dataframe() to load it as a
                            SQLite table. Set to False to get raw JSON rows instead.

        Returns:
            When return_as_file=True: dict with keys name, content_base64, size, mime_type,
            row_count — SDK will auto-save as a file.
            When return_as_file=False: list of dicts with keys: server, util_score.
            util_score is a numeric string or null if the server was not found.

        Example:
            query_server_synthetic_utilisation("byffm4;bydar4")
        """
        import csv
        import io

        server_list = [s.strip() for s in servers.split(";") if s.strip()]
        if not server_list:
            if return_as_file:
                return {
                    "name": "grafana_synthetic_utilisation.csv",
                    "content_base64": "",
                    "size": 0,
                    "mime_type": "text/csv",
                    "row_count": 0,
                    "message": "No servers provided.",
                }
            return []

        logger.info(f"Synthetic utilisation query for {len(server_list)} servers")

        # Build anchored regex: each server matches itself plus any suffix separated by _ or .
        instance_regex = "|".join(
            f"^{s}([_.][a-zA-Z0-9]*)*$" for s in server_list
        )
        promql = f'considered_dead:bool{{instance=~"{instance_regex}"}}'
        logger.info(f"Querying: {promql}")

        result = await client.query_instant(promql)
        result_list = result.get("result", [])

        # Map returned instances back to the original server name (prefix match, first wins)
        mapping: Dict[str, str] = {}
        for series in result_list:
            instance = series.get("metric", {}).get("instance")
            if not instance:
                continue
            for server in server_list:
                if instance.lower().startswith(server.lower()) and server not in mapping:
                    value_pair = series.get("value", [])
                    if len(value_pair) >= 2:
                        mapping[server] = value_pair[1]
                    break

        rows: List[Dict[str, Any]] = [
            {"server": server, "util_score": mapping.get(server)}
            for server in server_list
        ]

        if not return_as_file:
            return rows

        if not rows:
            return {
                "name": "grafana_synthetic_utilisation.csv",
                "content_base64": "",
                "size": 0,
                "mime_type": "text/csv",
                "row_count": 0,
                "message": "No rows returned for the given server filter.",
            }

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()), lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)
        csv_bytes = buf.getvalue().encode("utf-8")

        logger.info(
            f"query_server_synthetic_utilisation: returning {len(rows)} rows as CSV file "
            f"({len(csv_bytes):,} bytes) — SDK will auto-save to session context"
        )

        return {
            "name": "grafana_synthetic_utilisation.csv",
            "content_base64": base64.b64encode(csv_bytes).decode("ascii"),
            "size": len(csv_bytes),
            "mime_type": "text/csv",
            "row_count": len(rows),
        }

    logger.info("Grafana tools registered (READ-ONLY access)")
