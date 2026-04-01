"""
Databricks Service - ELSA UC3 Data Connection
Provides MCP tools to query CMDB / ServiceNow data from Elsa Databricks.
"""

import logging
from contextlib import contextmanager
from typing import Any, Optional, Union

from databricks import sql as databricks_sql
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError

from ..config import get_settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

@contextmanager
def _get_connection():
    """Open a short-lived Databricks SQL connection and close it when done."""
    settings = get_settings()

    kwargs: dict[str, Any] = {
        "server_hostname": settings.databricks_server_hostname,
        "http_path": settings.databricks_http_path,
        "access_token": settings.databricks_access_token,
        "catalog": settings.databricks_catalog,
        "schema": settings.databricks_schema,
    }

    if settings.databricks_use_proxy and settings.databricks_proxy_host:
        kwargs["_use_proxy"] = True
        kwargs["_proxy_host"] = settings.databricks_proxy_host
        kwargs["_proxy_port"] = settings.databricks_proxy_port

    conn = databricks_sql.connect(**kwargs)
    try:
        yield conn
    finally:
        conn.close()


def _rows_to_dicts(cursor) -> list[dict[str, Any]]:
    """Convert cursor rows to a list of column-name-keyed dicts."""
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _build_ci_filter(column: str, ci_item: Optional[str]) -> str:
    """
    Build a SQL WHERE fragment for one or more CI items.

    Accepts a single name or a semicolon-separated list, e.g.
    ``"SERVER-A;SERVER-B;SERVER-C"``.  Each token is matched as either:
      - an exact (case-insensitive) match:  column = 'token'
      - a prefix followed by underscore:    column LIKE 'token\_%' ESCAPE '\'
      - a prefix followed by a dot:         column LIKE 'token.%'

    This means ``SERVER`` matches ``SERVER``, ``SERVER_PROD``, and
    ``SERVER.something`` but NOT ``SERVER01`` or ``SERVERX``.
    Returns an empty string when ci_item is None or blank.
    """
    if not ci_item:
        return ""
    tokens = [t.strip() for t in ci_item.split(";") if t.strip()]
    if not tokens:
        return ""
    conditions = " or ".join(
        f"(lower({column}) = lower('{token}')"
        f" or lower({column}) like lower('{token}\\_%') escape '\\\\'"
        f" or lower({column}) like lower('{token}.%'))"
        for token in tokens
    )
    return f"and ({conditions})"


# ---------------------------------------------------------------------------
# Tool registration
# ---------------------------------------------------------------------------

def register_databricks_tools(mcp: FastMCP):
    """Register all Databricks / ELSA tools with the MCP server."""

    # ------------------------------------------------------------------
    # 1. Change requests for CI items
    # ------------------------------------------------------------------
    @mcp.tool()
    async def get_change_requests(
        ci_item: Optional[str] = None,
        since_date: str = "2025-01-01",
        exclude_standard: bool = True,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """
        Retrieve ServiceNow change requests (CHG) linked to server CI items.

        Joins snow_task_ci_view with snow_change_request_view and
        snow_cmdb_ci_server_view to return only server-related changes.

        Args:
            ci_item:          Optional CI item name filter. Supports a semicolon-separated
                              list for multiple CIs, e.g. ``"SERVER-A;SERVER-B;SERVER-C"``.
                              Each token is matched with a case-insensitive LIKE (partial match).
            since_date:       Only return records created on or after this date (YYYY-MM-DD).
            exclude_standard: When True (default), filters out changes with DV_TYPE = 'Standard'.
            limit:            Maximum number of rows to return (default 500).

        Returns:
            List of dicts with keys: dv_ci_item, dv_task, start_date, end_date,
            dv_category, dv_type.
        """
        logger.info(f"get_change_requests called: ci_item={ci_item}, since={since_date}, exclude_standard={exclude_standard}")

        ci_filter = _build_ci_filter("t.DV_CI_ITEM", ci_item)
        standard_filter = "and DV_TYPE != 'Standard'" if exclude_standard else ""

        query = f"""
            select lower(t.DV_CI_ITEM)  as dv_ci_item,
                   t.DV_TASK            as dv_task,
                   c.START_DATE         as start_date,
                   c.END_DATE           as end_date,
                   c.DV_CATEGORY        as dv_category,
                   c.DV_TYPE            as dv_type
            from snow_task_ci_view t
            left join efdataonelh_prd.generaldiscovery_servicenow_r.snow_change_request_view c
              on t.TASK = c.SYS_ID
            left join snow_cmdb_ci_server_view s
              on t.CI_ITEM = s.SYS_ID
            where t.DV_TASK like 'CHG%'
              and t.SYS_CREATED_ON >= '{since_date}'
              and s.SYS_ID is not NULL
              {standard_filter}
              {ci_filter}
            order by dv_ci_item ASC, start_date DESC
            limit {limit}
        """

        try:
            with _get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    return _rows_to_dicts(cursor)
        except Exception as exc:
            logger.error(f"get_change_requests failed: {exc}")
            raise ToolError(f"Databricks query failed: {exc}") from exc

    # ------------------------------------------------------------------
    # 2. Incidents for CI items
    # ------------------------------------------------------------------
    @mcp.tool()
    async def get_incidents(
        ci_item: Optional[str] = None,
        since_date: str = "2025-01-01",
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """
        Retrieve ServiceNow incidents (INC) linked to server CI items.

        Joins snow_task_ci_view with snow_incident_view and
        snow_cmdb_ci_server_view, excluding Event Monitoring contacts.

        Args:
            ci_item:    Optional CI item name filter. Supports a semicolon-separated
                        list for multiple CIs, e.g. ``"SERVER-A;SERVER-B;SERVER-C"``.
                        Each token is matched with a case-insensitive LIKE (partial match).
            since_date: Only return records created on or after this date (YYYY-MM-DD).
            limit:      Maximum number of rows to return (default 500).

        Returns:
            List of dicts with keys: dv_ci_item, dv_task, opened_at,
            dv_incident_state, dv_close_code, dv_contact_type.
        """
        logger.info(f"get_incidents called: ci_item={ci_item}, since={since_date}")

        ci_filter = _build_ci_filter("t.DV_CI_ITEM", ci_item)

        query = f"""
            select lower(t.DV_CI_ITEM)  as dv_ci_item,
                   t.DV_TASK            as dv_task,
                   i.OPENED_AT          as opened_at,
                   i.DV_INCIDENT_STATE  as dv_incident_state,
                   i.DV_CLOSE_CODE      as dv_close_code,
                   i.DV_CONTACT_TYPE    as dv_contact_type
            from snow_task_ci_view t
            left join efdataonelh_prd.generaldiscovery_servicenow_r.snow_incident_view i
              on t.TASK = i.SYS_ID
            left join snow_cmdb_ci_server_view s
              on t.CI_ITEM = s.SYS_ID
            where t.DV_TASK like 'INC%'
              and t.SYS_CREATED_ON >= '{since_date}'
              and s.SYS_ID is not NULL
              and DV_CONTACT_TYPE != 'Event Monitoring'
              {ci_filter}
            order by dv_ci_item ASC, opened_at DESC
            limit {limit}
        """

        try:
            with _get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    return _rows_to_dicts(cursor)
        except Exception as exc:
            logger.error(f"get_incidents failed: {exc}")
            raise ToolError(f"Databricks query failed: {exc}") from exc

    # ------------------------------------------------------------------
    # 3. Application / Server inventory
    # ------------------------------------------------------------------
    @mcp.tool()
    async def get_app_server_inventory(
        app_name: Optional[str] = None,
        server_name: Optional[str] = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """
        Retrieve the EA CMDB app-to-server inventory from ELSA.

        Combines business applications (via BeatID) and patterns linked to
        server records. Deduplicates via UNION.

        Args:
            app_name:    Optional app name filter (LIKE match).
            server_name: Optional server name filter (LIKE match).
            limit:       Maximum rows to return (default 1000).

        Returns:
            List of dicts with keys: app_name, app_sys_class_name, app_beatid,
            internal_lifecycle, dv_business_criticality, appsvc_name,
            server_name, server_sys_class_name, server_dv_install_status,
            server_dv_used_for.
        """
        logger.info(f"get_app_server_inventory: app={app_name}, server={server_name}")

        app_filter = f"and lower(i.APP_NAME) like lower('%{app_name}%')" if app_name else ""
        srv_filter = f"and lower(i.SERVER_NAME) like lower('%{server_name}%')" if server_name else ""

        query = f"""
            select distinct
                   i.APP_NAME                    as app_name,
                   i.APP_SYS_CLASS_NAME          as app_sys_class_name,
                   b.NUMBER                      as app_beatid,
                   b.INTERNAL_LIFECYCLE          as internal_lifecycle,
                   b.DV_BUSINESS_CRITICALITY     as dv_business_criticality,
                   i.APPSVC_NAME                 as appsvc_name,
                   i.SERVER_NAME                 as server_name,
                   i.SERVER_SYS_CLASS_NAME       as server_sys_class_name,
                   i.SERVER_DV_INSTALL_STATUS    as server_dv_install_status,
                   s.DV_U_USED_FOR               as server_dv_used_for
            from snow_ea_cmdb_inventory_view_001 i
            left join snow_x_inpgh_upmx_business_application_view b on i.APP_SYS_ID = b.SYS_ID
            left join snow_cmdb_ci_server_view s on i.SERVER_SYS_ID = s.SYS_ID
            where b.SYS_ID is not NULL and s.SYS_ID is not NULL
              {app_filter} {srv_filter}

            union

            select distinct
                   i.APP_NAME,
                   i.APP_SYS_CLASS_NAME,
                   p.NUMBER                      as app_beatid,
                   p.INTERNAL_LIFECYCLE,
                   p.DV_BUSINESS_CRITICALITY,
                   i.APPSVC_NAME,
                   i.SERVER_NAME,
                   i.SERVER_SYS_CLASS_NAME,
                   i.SERVER_DV_INSTALL_STATUS,
                   s.DV_U_USED_FOR               as server_dv_used_for
            from snow_ea_cmdb_inventory_view_001 i
            left join snow_x_inpgh_upmx_pattern_view p on i.APP_SYS_ID = p.SYS_ID
            left join snow_cmdb_ci_server_view s on i.SERVER_SYS_ID = s.SYS_ID
            where p.SYS_ID is not NULL and s.SYS_ID is not NULL
              {app_filter} {srv_filter}

            limit {limit}
        """

        try:
            with _get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    return _rows_to_dicts(cursor)
        except Exception as exc:
            logger.error(f"get_app_server_inventory failed: {exc}")
            raise ToolError(f"Databricks query failed: {exc}") from exc

    # ------------------------------------------------------------------
    # 4. Active servers
    # ------------------------------------------------------------------
    @mcp.tool()
    async def get_active_servers(
        used_for: Optional[str] = None,
        name_filter: Optional[str] = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """
        Retrieve all active servers from snow_cmdb_ci_server_view.

        Args:
            used_for:    Optional filter on DV_U_USED_FOR field (LIKE match).
            name_filter: Optional filter on server NAME (LIKE match).
            limit:       Maximum rows to return (default 1000).

        Returns:
            List of dicts with keys: name, dv_install_status,
            dv_u_used_for, short_description.
        """
        logger.info(f"get_active_servers: used_for={used_for}, name={name_filter}")

        used_for_filter = f"and lower(DV_U_USED_FOR) like lower('%{used_for}%')" if used_for else ""
        name_f = f"and lower(NAME) like lower('%{name_filter}%')" if name_filter else ""

        query = f"""
            select NAME             as name,
                   DV_INSTALL_STATUS as dv_install_status,
                   DV_U_USED_FOR    as dv_u_used_for,
                   SHORT_DESCRIPTION as short_description
            from snow_cmdb_ci_server_view
            where U_ACTIVE = true
              {used_for_filter}
              {name_f}
            limit {limit}
        """

        try:
            with _get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    return _rows_to_dicts(cursor)
        except Exception as exc:
            logger.error(f"get_active_servers failed: {exc}")
            raise ToolError(f"Databricks query failed: {exc}") from exc

    # ------------------------------------------------------------------
    # 5. Server decommission summary (app inventory + CHG/INC counts)
    # ------------------------------------------------------------------
    @mcp.tool()
    async def get_server_decommission_summary(
        server_name: Optional[str] = None,
        since_date: str = "2025-01-01",
        limit: int = 1000,
        return_as_file: bool = True,
    ) -> Union[list[dict[str, Any]], dict[str, Any]]:
        """
        Retrieve a decommission-readiness summary per server: linked business
        applications/patterns, lifecycle and criticality metadata, whether the
        server is shared or dedicated, and the number of change requests (CHG)
        and incidents (INC) since a given date.

        Uses FULL JOINs so that active servers with no BEAT app association are
        also returned (they are often the best decommission candidates).
        Orphaned apps/patterns and inactive servers are excluded via subquery filters.

        Args:
            server_name: Optional server name filter. Supports a
                         semicolon-separated list for multiple servers, e.g.
                         ``"SERVER-A;SERVER-B;SERVER-C"``. Each token is
                         matched with a case-insensitive LIKE (partial match).
            since_date:      Count CHG/INC records created on or after this date
                             (YYYY-MM-DD, default ``"2025-01-01"``).
            limit:           Maximum rows to return (default 1000).
            return_as_file:  When True, encodes the result as a base64 CSV file
                             payload instead of returning raw rows (default False).

        Returns:
            List of dicts with keys: app_beatid, app_name, app_sys_class_name,
            internal_lifecycle, app_u_active, app_dv_alias, dv_business_criticality,
            server_name, server_u_number, server_sys_class_name,
            server_dv_install_status, server_u_active, server_dv_used_for,
            server_short_description, app_count, scope, chg_tasks, inc_tasks.
        """
        logger.info(
            f"get_server_decommission_summary: server={server_name}, since={since_date}"
        )

        server_filter = _build_ci_filter("s.NAME", server_name)

        query = f"""
            select distinct
                   b.NUMBER                      AS app_beatid,
                   b.NAME                        AS app_name,
                   b.SYS_CLASS_NAME              AS app_sys_class_name,
                   b.INTERNAL_LIFECYCLE          AS internal_lifecycle,
                   b.U_ACTIVE                    AS app_u_active,
                   b.DV_ALIAS                    AS app_dv_alias,
                   b.DV_BUSINESS_CRITICALITY     AS dv_business_criticality,
                   lower(s.NAME)                 AS server_name,
                   s.U_NUMBER                    AS server_u_number,
                   s.SYS_CLASS_NAME              AS server_sys_class_name,
                   s.DV_INSTALL_STATUS           AS server_dv_install_status,
                   s.U_ACTIVE                    AS server_u_active,
                   s.DV_U_USED_FOR               AS server_dv_used_for,
                   s.SHORT_DESCRIPTION           AS server_short_description,
                   s.DV_U_HOSTING_ZONE AS server_dv_u_hosting_zone,
                   s.DV_LOCATION AS server_dv_location,
                   CASE WHEN i2.APP_COUNT IS NULL THEN 0 ELSE i2.APP_COUNT END AS app_count,
                   CASE WHEN i2.APP_COUNT > 1 THEN 'Shared' ELSE 'Dedicated' END AS scope,
                   CASE WHEN t.CHG_TASKS IS NULL THEN 0 ELSE t.CHG_TASKS END AS chg_tasks,
                   CASE WHEN t.INC_TASKS IS NULL THEN 0 ELSE t.INC_TASKS END AS inc_tasks

            from (
                select distinct APP_SYS_ID, APP_NAME, SERVER_SYS_ID, SERVER_NAME
                from efdataonelh_prd.generaldiscovery_servicenow_r.snow_ea_cmdb_inventory_view_001
                where APP_SYS_ID is not NULL and SERVER_SYS_ID is not NULL
                  and lower(APP_NAME) not like '%(orphaned)%'
            ) as i

            full join (
                select distinct
                    SYS_ID, NUMBER, NAME, SYS_CLASS_NAME, INTERNAL_LIFECYCLE, U_ACTIVE,
                    DV_BUSINESS_CRITICALITY, DV_ALIAS, SHORT_DESCRIPTION
                from efdataonelh_prd.generaldiscovery_servicenow_r.snow_x_inpgh_upmx_business_application_view
                where lower(NAME) not like '%(orphaned)%'
                union
                select distinct
                    SYS_ID, NUMBER, NAME, SYS_CLASS_NAME, INTERNAL_LIFECYCLE, U_ACTIVE,
                    DV_BUSINESS_CRITICALITY, DV_ALIAS, SHORT_DESCRIPTION
                from efdataonelh_prd.generaldiscovery_servicenow_r.snow_x_inpgh_upmx_pattern_view
                where lower(NAME) not like '%(orphaned)%'
            ) b on i.APP_SYS_ID = b.SYS_ID

            full join (
                select distinct
                    SYS_ID, U_NUMBER, NAME, U_ACTIVE, SYS_CLASS_NAME, DV_INSTALL_STATUS,
                    DV_U_USED_FOR, SHORT_DESCRIPTION, DV_U_HOSTING_ZONE, DV_LOCATION
                from efdataonelh_prd.generaldiscovery_servicenow_r.snow_cmdb_ci_server_view
                where U_ACTIVE = true
            ) s on i.SERVER_SYS_ID = s.SYS_ID

            left join (
                select SERVER_SYS_ID, count(distinct APP_SYS_ID) AS APP_COUNT
                from efdataonelh_prd.generaldiscovery_servicenow_r.snow_ea_cmdb_inventory_view_001
                where APP_SYS_ID is not NULL and SERVER_SYS_ID is not NULL
                  and lower(APP_NAME) not like '%(orphaned)%'
                group by SERVER_SYS_ID
            ) i2 on s.SYS_ID = i2.SERVER_SYS_ID

            left join (
                select lower(s2.NAME) AS SERVER_NAME,
                       count(distinct t1.DV_TASK) AS CHG_TASKS,
                       count(distinct t2.TASK)    AS INC_TASKS
                from efdataonelh_prd.generaldiscovery_servicenow_r.snow_cmdb_ci_server_view s2
                left join efdataonelh_prd.generaldiscovery_servicenow_r.snow_task_ci_view t1
                  on s2.SYS_ID = t1.CI_ITEM
                  and t1.DV_TASK like 'CHG%'
                  and t1.SYS_CREATED_ON >= '{since_date}'
                left join efdataonelh_prd.generaldiscovery_servicenow_r.snow_change_request_view c
                  on t1.TASK = c.SYS_ID
                  and c.DV_TYPE not like 'Standard'
                left join efdataonelh_prd.generaldiscovery_servicenow_r.snow_task_ci_view t2
                  on s2.SYS_ID = t2.CI_ITEM
                  and t2.DV_TASK like 'INC%'
                  and t2.SYS_CREATED_ON >= '{since_date}'
                left join efdataonelh_prd.generaldiscovery_servicenow_r.snow_incident_view i3
                  on t2.TASK = i3.SYS_ID
                  and i3.DV_CONTACT_TYPE not like 'Event Monitoring'
                where s2.U_ACTIVE = true
                  and c.SYS_ID is not NULL
                  and i3.SYS_ID is not NULL
                  and t1.SYS_ID is not NULL
                  and t2.SYS_ID is not NULL
                group by s2.NAME
            ) t on lower(s.NAME) = lower(t.SERVER_NAME)

            where s.SYS_ID is not NULL
              {server_filter}

            limit {limit}
        """

        try:
            with _get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    rows = _rows_to_dicts(cursor)

            # Deduplicate by server_name, keeping only the first occurrence
            seen_servers: set[str] = set()
            unique_rows = []
            for row in rows:
                srv = row.get("server_name")
                if srv not in seen_servers:
                    seen_servers.add(srv)
                    unique_rows.append(row)
            rows = unique_rows

            if not return_as_file:
                return rows

            # Encode as CSV so the SDK intercepts content_base64, saves the
            # file to session context, and returns only metadata to the LLM.
            import base64
            import csv
            import io

            if not rows:
                return {
                    "name": "elsa_server_data.csv",
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
                f"get_server_decommission_summary: returning {len(rows)} rows as CSV file "
                f"({len(csv_bytes):,} bytes) — SDK will auto-save to session context"
            )

            return {
                "name": "elsa_server_data.csv",
                "content_base64": base64.b64encode(csv_bytes).decode("ascii"),
                "size": len(csv_bytes),
                "mime_type": "text/csv",
                "row_count": len(rows),
            }

        except Exception as exc:
            logger.error(f"get_server_decommission_summary failed: {exc}")
            raise ToolError(f"Databricks query failed: {exc}") from exc

    # ------------------------------------------------------------------
    # 6. BEAT master data
    # ------------------------------------------------------------------
    @mcp.tool()
    async def get_beat_master_data(
        number: Optional[str] = None,
        name: Optional[str] = None,
        lifecycle: Optional[str] = None,
        limit: int = 1000,
        return_as_file: bool = True,
    ) -> dict[str, Any]:
        """
        Retrieve BEAT master data (active business applications and patterns) from ServiceNow CMDB.

        Combines data from both snow_x_inpgh_upmx_business_application_view and
        snow_x_inpgh_upmx_pattern_view, deduplicating to return only the latest
        version (max OPTIME) for each NUMBER.

        Args:
            number:         Optional filter on NUMBER field. Supports a 
                            semicolon-separated list for multiple values, e.g.
                            ``"BEAT001;BEAT002;BEAT003"``. Each token is
                            matched with a case-insensitive LIKE (partial match).
            name:           Optional filter on NAME field. Supports a 
                            semicolon-separated list for multiple values, e.g.
                            ``"App A;App B;App C"``. Each token is
                            matched with a case-insensitive LIKE (partial match).
            lifecycle:      Optional filter on INTERNAL_LIFECYCLE field. Supports a 
                            semicolon-separated list for multiple values, e.g.
                            ``"Active;Retired"``. Each token is
                            matched with a case-insensitive LIKE (partial match).
            limit:          Maximum rows to return (default 1000).
            return_as_file: When True (default), returns the result as a
                            base64-encoded CSV file. The SDK automatically
                            intercepts this, saves it to session context, and
                            strips the base64 from the agent's context window.
                            The agent can then call list_available_files() and
                            upload_dataframe() to load it as a SQLite table.
                            Set to False to get raw JSON rows instead.

        Returns:
            When return_as_file=True: dict with keys name, content_base64,
            size, mime_type, row_count — SDK will auto-save as a file.
            When return_as_file=False: list of dicts with keys: number, name, 
            internal_lifecycle, dv_business_criticality, dv_alias, short_description, 
            sys_created_on, sys_updated_on, dv_business_owner, dv_u_system_owner, 
            dv_steward, u_decomissioning_confirmed, u_abandoned, u_retired_start_date.
        """
        logger.info(f"get_beat_master_data: number={number}, name={name}, lifecycle={lifecycle}")

        number_filter = _build_ci_filter("NUMBER", number)
        name_filter = _build_ci_filter("NAME", name)
        lifecycle_filter = _build_ci_filter("INTERNAL_LIFECYCLE", lifecycle)

        query = f"""
             select NUMBER, NAME, INTERNAL_LIFECYCLE, DV_BUSINESS_CRITICALITY, DV_ALIAS, SHORT_DESCRIPTION,
                SYS_CREATED_ON, SYS_UPDATED_ON, DV_BUSINESS_OWNER, DV_U_SYSTEM_OWNER, DV_STEWARD,
                U_DECOMISSIONING_CONFIRMED, U_ABANDONED, U_RETIRED_START_DATE  
            from (
                select OPTIME || NUMBER uniqueID, NUMBER, NAME, INTERNAL_LIFECYCLE, DV_BUSINESS_CRITICALITY, DV_ALIAS, SHORT_DESCRIPTION,
                    SYS_CREATED_ON, SYS_UPDATED_ON, DV_BUSINESS_OWNER, DV_U_SYSTEM_OWNER, DV_STEWARD,
                    U_DECOMISSIONING_CONFIRMED, U_ABANDONED, U_RETIRED_START_DATE
                from `efdataonelh_prd`.`generaldiscovery_servicenow_r`.`snow_x_inpgh_upmx_business_application_view`
                union
                select  OPTIME || NUMBER  uniqueID, NUMBER, NAME, INTERNAL_LIFECYCLE, DV_BUSINESS_CRITICALITY, DV_ALIAS, SHORT_DESCRIPTION,
                    SYS_CREATED_ON, SYS_UPDATED_ON, DV_BUSINESS_OWNER, DV_U_SYSTEM_OWNER, DV_STEWARD,
                    U_DECOMISSIONING_CONFIRMED, U_ABANDONED, U_RETIRED_START_DATE
                from `efdataonelh_prd`.`generaldiscovery_servicenow_r`.`snow_x_inpgh_upmx_pattern_view`
            )
            where uniqueID IN (
                select max(OPTIME) || NUMBER
                from (
                    select OPTIME, NUMBER
                    from `efdataonelh_prd`.`generaldiscovery_servicenow_r`.`snow_x_inpgh_upmx_business_application_view`
                    union
                    select  OPTIME, NUMBER
                    from `efdataonelh_prd`.`generaldiscovery_servicenow_r`.`snow_x_inpgh_upmx_pattern_view`
                )
                group by NUMBER
            )
            {number_filter}
            {name_filter}
            {lifecycle_filter}
            limit {limit}
        """

        try:
            with _get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    rows = _rows_to_dicts(cursor)

            if not return_as_file:
                return rows

            # Encode as CSV so the SDK intercepts content_base64, saves the
            # file to session context, and returns only metadata to the LLM.
            import base64
            import csv
            import io

            if not rows:
                return {
                    "name": "beat_master_data.csv",
                    "content_base64": "",
                    "size": 0,
                    "mime_type": "text/csv",
                    "row_count": 0,
                    "message": "No rows returned for the given filters.",
                }

            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()), lineterminator='\n')
            writer.writeheader()
            writer.writerows(rows)
            csv_bytes = buf.getvalue().encode("utf-8")

            logger.info(
                f"get_beat_master_data: returning {len(rows)} rows as CSV file "
                f"({len(csv_bytes):,} bytes) — SDK will auto-save to session context"
            )

            return {
                "name": "beat_master_data.csv",
                "content_base64": base64.b64encode(csv_bytes).decode("ascii"),
                "size": len(csv_bytes),
                "mime_type": "text/csv",
                "row_count": len(rows),
            }

        except Exception as exc:
            logger.error(f"get_beat_master_data failed: {exc}")
            raise ToolError(f"Databricks query failed: {exc}") from exc

    # ------------------------------------------------------------------
    # 6b. BEAT master data (raw JSON, no file encoding)
    # ------------------------------------------------------------------
    @mcp.tool()
    async def get_beat_master_data_test(
        number: Optional[str] = None,
        name: Optional[str] = None,
        lifecycle: Optional[str] = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """
        Retrieve BEAT master data (active business applications and patterns) from ServiceNow CMDB.

        Identical to get_beat_master_data but returns raw JSON rows instead of a
        base64-encoded CSV file.

        Args:
            number:    Optional filter on NUMBER field. Supports a
                       semicolon-separated list e.g. ``"BEAT001;BEAT002"``.
                       Each token is matched case-insensitively.
            name:      Optional filter on NAME field. Supports a
                       semicolon-separated list e.g. ``"App A;App B"``.
            lifecycle: Optional filter on INTERNAL_LIFECYCLE field. Supports a
                       semicolon-separated list e.g. ``"Active;Retired"``.
            limit:     Maximum rows to return (default 1000).

        Returns:
            List of dicts with keys: number, name, internal_lifecycle,
            dv_business_criticality, dv_alias, short_description,
            sys_created_on, sys_updated_on, dv_business_owner,
            dv_u_system_owner, dv_steward, u_decomissioning_confirmed,
            u_abandoned, u_retired_start_date.
        """
        logger.info(f"get_beat_master_data_test: number={number}, name={name}, lifecycle={lifecycle}")

        number_filter = _build_ci_filter("NUMBER", number)
        name_filter = _build_ci_filter("NAME", name)
        lifecycle_filter = _build_ci_filter("INTERNAL_LIFECYCLE", lifecycle)

        query = f"""
            select NUMBER, NAME, INTERNAL_LIFECYCLE, DV_BUSINESS_CRITICALITY, DV_ALIAS, SHORT_DESCRIPTION,
                SYS_CREATED_ON, SYS_UPDATED_ON, DV_BUSINESS_OWNER, DV_U_SYSTEM_OWNER, DV_STEWARD,
                U_DECOMISSIONING_CONFIRMED, U_ABANDONED, U_RETIRED_START_DATE  
            from (
                select OPTIME || NUMBER uniqueID, NUMBER, NAME, INTERNAL_LIFECYCLE, DV_BUSINESS_CRITICALITY, DV_ALIAS, SHORT_DESCRIPTION,
                    SYS_CREATED_ON, SYS_UPDATED_ON, DV_BUSINESS_OWNER, DV_U_SYSTEM_OWNER, DV_STEWARD,
                    U_DECOMISSIONING_CONFIRMED, U_ABANDONED, U_RETIRED_START_DATE
                from `efdataonelh_prd`.`generaldiscovery_servicenow_r`.`snow_x_inpgh_upmx_business_application_view`
                union
                select  OPTIME || NUMBER  uniqueID, NUMBER, NAME, INTERNAL_LIFECYCLE, DV_BUSINESS_CRITICALITY, DV_ALIAS, SHORT_DESCRIPTION,
                    SYS_CREATED_ON, SYS_UPDATED_ON, DV_BUSINESS_OWNER, DV_U_SYSTEM_OWNER, DV_STEWARD,
                    U_DECOMISSIONING_CONFIRMED, U_ABANDONED, U_RETIRED_START_DATE
                from `efdataonelh_prd`.`generaldiscovery_servicenow_r`.`snow_x_inpgh_upmx_pattern_view`
            )
            where uniqueID IN (
                select max(OPTIME) || NUMBER
                from (
                    select OPTIME, NUMBER
                    from `efdataonelh_prd`.`generaldiscovery_servicenow_r`.`snow_x_inpgh_upmx_business_application_view`
                    union
                    select  OPTIME, NUMBER
                    from `efdataonelh_prd`.`generaldiscovery_servicenow_r`.`snow_x_inpgh_upmx_pattern_view`
                )
                group by NUMBER
            )
            
            {number_filter}
            {name_filter}
            {lifecycle_filter}
            limit {limit}
        """

        try:
            with _get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    return _rows_to_dicts(cursor)
        except Exception as exc:
            logger.error(f"get_beat_master_data_test failed: {exc}")
            raise ToolError(f"Databricks query failed: {exc}") from exc

    # ------------------------------------------------------------------
    # 7. Servers by BEAT ID - testing tool to get server names linked to BEATID(needed while we don't have a2a set in place to get server assessment froms server agent manually)
    # ------------------------------------------------------------------
    @mcp.tool()
    async def get_servers_by_beat_id(
        beat_id: Optional[str] = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """
        TESTING TOOL - returns json format
        Retrieve servers linked to specific BEAT IDs (business applications/patterns).

        Returns only the server names associated with the given BEAT ID(s).

        Args:
            beat_id: Optional BEAT ID filter. Supports a semicolon-separated
                     list for multiple BEAT IDs, e.g. ``"BEAT001;BEAT002;BEAT003"``.
                     Each token is matched with a case-insensitive LIKE (partial match).
            limit:   Maximum rows to return (default 1000).

        Returns:
            List of dicts with key: server_name.
        """
        logger.info(f"get_servers_by_beat_id: beat_id={beat_id}")

        beat_filter = _build_ci_filter("b.NUMBER", beat_id)

        query = f"""
            select distinct
                   lower(i.SERVER_NAME) AS server_name

            from snow_ea_cmdb_inventory_view_001 i

            left join snow_cmdb_ci_server_view s
              on i.SERVER_SYS_ID = s.SYS_ID

            left join (
                select distinct SYS_ID, NUMBER
                from snow_x_inpgh_upmx_business_application_view
                union
                select distinct SYS_ID, NUMBER
                from snow_x_inpgh_upmx_pattern_view
            ) b ON i.APP_SYS_ID = b.SYS_ID

            where b.SYS_ID is not NULL
              and s.SYS_ID is not NULL
              {beat_filter}

            limit {limit}
        """

        try:
            with _get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    return _rows_to_dicts(cursor)
        except Exception as exc:
            logger.error(f"get_servers_by_beat_id failed: {exc}")
            raise ToolError(f"Databricks query failed: {exc}") from exc

    # ------------------------------------------------------------------
    # 8. Apps underlying servers
    # ------------------------------------------------------------------
    @mcp.tool()
    async def get_apps_underlying_servers(
        beat_id: Optional[str] = None,
        limit: int = 1000,
        return_as_file: bool = True,
    ) -> dict[str, Any]:
        """
        Retrieve server names for BEAT IDs (business applications/patterns).

        Returns only the distinct server names associated with the given BEAT ID(s).

        Args:
            beat_id:        Optional BEAT ID filter. Supports a semicolon-separated
                            list for multiple BEAT IDs, e.g. ``"BEAT001;BEAT002;BEAT003"``.
                            Each token is matched with a case-insensitive LIKE (partial match).
            limit:          Maximum rows to return (default 1000).
            return_as_file: When True (default), returns the result as a
                            base64-encoded CSV file. The SDK automatically
                            intercepts this, saves it to session context, and
                            strips the base64 from the agent's context window.
                            The agent can then call list_available_files() and
                            upload_dataframe() to load it as a SQLite table.
                            Set to False to get raw JSON rows instead.

        Returns:
            When return_as_file=True: dict with keys name, content_base64,
            size, mime_type, row_count — SDK will auto-save as a file.
            When return_as_file=False: list of dicts with key: server_name.
        """
        logger.info(f"get_apps_underlying_servers: beat_id={beat_id}")

        beat_filter = _build_ci_filter("b.NUMBER", beat_id)

        query = f"""
            select distinct
                   lower(i.SERVER_NAME) AS server_name

            from snow_ea_cmdb_inventory_view_001 i

            left join snow_cmdb_ci_server_view s
              on i.SERVER_SYS_ID = s.SYS_ID

            left join (
                select distinct SYS_ID, NUMBER
                from snow_x_inpgh_upmx_business_application_view
                union
                select distinct SYS_ID, NUMBER
                from snow_x_inpgh_upmx_pattern_view
            ) b ON i.APP_SYS_ID = b.SYS_ID

            where b.SYS_ID is not NULL
              and s.SYS_ID is not NULL
              {beat_filter}

            limit {limit}
        """

        try:
            with _get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    rows = _rows_to_dicts(cursor)

            if not return_as_file:
                return rows

            # Encode as CSV so the SDK intercepts content_base64, saves the
            # file to session context, and returns only metadata to the LLM.
            import base64
            import csv
            import io

            if not rows:
                return {
                    "name": "Server Input.csv",
                    "content_base64": "",
                    "size": 0,
                    "mime_type": "text/csv",
                    "row_count": 0,
                    "message": "No rows returned for the given BEAT ID filter.",
                }

            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()), lineterminator='\n')
            writer.writeheader()
            writer.writerows(rows)
            csv_bytes = buf.getvalue().encode("utf-8")

            logger.info(
                f"get_apps_underlying_servers: returning {len(rows)} rows as CSV file "
                f"({len(csv_bytes):,} bytes) — SDK will auto-save to session context"
            )

            return {
                "name": "Server Input.csv",
                "content_base64": base64.b64encode(csv_bytes).decode("ascii"),
                "size": len(csv_bytes),
                "mime_type": "text/csv",
                "row_count": len(rows),
            }

        except Exception as exc:
            logger.error(f"get_apps_underlying_servers failed: {exc}")
            raise ToolError(f"Databricks query failed: {exc}") from exc

    # ------------------------------------------------------------------
    # 9. Generic SQL execution (power-user escape hatch)
    # ------------------------------------------------------------------
    @mcp.tool()
    async def execute_sql(
        query: str,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """
        Execute an arbitrary read-only SQL SELECT query against Elsa Databricks.

        A LIMIT clause is automatically appended if not already present to
        prevent accidental full-table scans.

        Args:
            query: A SQL SELECT statement to execute.
            limit: Safety row limit appended when the query has no LIMIT
                   clause (default 200).

        Returns:
            List of dicts representing the result rows.
        """
        if not query.strip().upper().startswith("SELECT"):
            raise ToolError("Only SELECT statements are allowed.")

        safe_query = query.rstrip().rstrip(";")
        if "limit" not in safe_query.lower():
            safe_query = f"{safe_query}\nLIMIT {limit}"

        logger.info(f"execute_sql: {safe_query[:200]}")

        try:
            with _get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(safe_query)
                    return _rows_to_dicts(cursor)
        except Exception as exc:
            logger.error(f"execute_sql failed: {exc}")
            raise ToolError(f"Databricks query failed: {exc}") from exc

    logger.info("Databricks (ELSA) tools registered: get_change_requests, get_incidents, get_app_server_inventory, get_active_servers, get_server_decommission_summary, get_beat_master_data, get_beat_master_data_test, get_servers_by_beat_id, get_apps_underlying_servers, execute_sql")
