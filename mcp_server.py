# mcp_snowflake_local.py
from fastmcp import FastMCP
import snowflake.connector

mcp = FastMCP("TerraDataLocal")

@mcp.tool()
def get_snowflake_valuation(parcel_id: str):
    """Standardized MCP tool to get data from Snowflake."""
    # (Your snowflake connection code here)
    return f"Valuation for {parcel_id} is $1.2M (Fetched via Snowflake)"