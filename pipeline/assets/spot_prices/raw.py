from dagster import (
    AssetExecutionContext,
    RetryPolicy,
    asset,
)

from ...resources import ApiResource
from .common import entsoe_automation_condition, price_partitions


@asset(
    partitions_def=price_partitions,
    automation_condition=entsoe_automation_condition,
    retry_policy=RetryPolicy(max_retries=50, delay=300),
)
def raw_entsoe_xml(context: AssetExecutionContext, entsoe: ApiResource):
    """Fetches raw XML for the specific partitioned date in UTC."""
    # Get the time window for the partition (already in UTC)
    start_dt, end_dt = context.partition_time_window

    start_str = start_dt.strftime("%Y%m%d%H%M")
    end_str = end_dt.strftime("%Y%m%d%H%M")

    context.log.info(f"Fetching prices for {context.partition_key} ({start_dt} to {end_dt})")
    xml_data = entsoe.fetch_day_ahead_prices("10YFI-1--------U", start_str, end_str)
    return xml_data
