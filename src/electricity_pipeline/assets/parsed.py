import pandas as pd
from bs4 import BeautifulSoup
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    MetadataValue,
    Output,
    asset,
    asset_check,
)

from ..resources import ApiResource
from .common import PriceConfig, daily_partitions, entsoe_automation_condition


def expected_interval_count(start_utc, end_utc, freq_minutes: int = 15) -> int:
    """Returns the number of expected intervals for time window."""
    return len(
        pd.date_range(start=start_utc, end=end_utc, freq=f"{freq_minutes}min", inclusive="left")
    )


def apply_partition_filter(context: AssetExecutionContext | None, df: pd.DataFrame) -> pd.DataFrame:
    """Filter the DataFrame against the partition time window."""
    if not context or not context.has_partition_key:
        return df

    start_utc, end_utc = context.partition_time_window
    return df[(df["timestamp"] >= start_utc) & (df["timestamp"] < end_utc)]


@asset(
    partitions_def=daily_partitions,
    automation_condition=entsoe_automation_condition,
)
def parsed_electricity_prices(
    context: AssetExecutionContext, entsoe: ApiResource, config: PriceConfig
) -> Output[pd.DataFrame]:
    """Fetches XML from ENTSO-E and parses it into a DataFrame"""

    start_dt, end_dt = context.partition_time_window
    start_str = start_dt.strftime("%Y%m%d%H%M")
    end_str = end_dt.strftime("%Y%m%d%H%M")

    context.log.info(f"Fetching prices for {context.partition_key} ({start_dt} to {end_dt})")
    xml_data = entsoe.fetch_day_ahead_prices("10YFI-1--------U", start_str, end_str)

    soup = BeautifulSoup(xml_data, "lxml-xml")

    prices = []
    for time_series in soup.find_all("TimeSeries"):
        period = time_series.find("Period")
        if not period or period.resolution.text != "PT15M":
            continue

        start = pd.to_datetime(period.timeInterval.start.text)
        end = pd.to_datetime(period.timeInterval.end.text)

        points_map = {
            int(p.position.text): float(p.find("price.amount").text)
            for p in period.find_all("Point")
            if p.position and p.find("price.amount")
        }

        if not points_map:
            continue

        timestamps = pd.date_range(start=start, end=end, freq="15min", inclusive="left")
        last_price = None
        for pos, ts in enumerate(timestamps, start=1):
            if pos in points_map:
                last_price = points_map[pos]
            if last_price is not None:
                prices.append({"timestamp": ts, "price_eur_mwh": last_price})

    if not prices:
        raise ValueError("No price data found in XML — will retry including the API call")

    df = pd.DataFrame(prices).drop_duplicates(subset=["timestamp"])
    df = apply_partition_filter(context, df)

    start_utc, end_utc = context.partition_time_window
    expected_rows = expected_interval_count(*context.partition_time_window)
    if len(df) < expected_rows:
        raise ValueError(f"Insufficient data: parsed {len(df)} rows, expected {expected_rows}")

    return Output(
        value=df,
        metadata={"num_rows": len(df), "preview": MetadataValue.md(df.head(5).to_markdown())},
    )


@asset_check(asset=parsed_electricity_prices)
def check_full_day_data(context: AssetExecutionContext, parsed_electricity_prices: pd.DataFrame):
    """Checks if the partition contains the expected number of rows (96 for 15m)."""
    start_utc, end_utc = context.partition_time_window
    expected_rows = expected_interval_count(*context.partition_time_window)
    actual_rows = len(parsed_electricity_prices)

    return AssetCheckResult(
        passed=bool(actual_rows >= expected_rows),
        metadata={"expected_rows": expected_rows, "actual_rows": actual_rows},
    )
