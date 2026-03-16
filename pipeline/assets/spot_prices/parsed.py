from datetime import timedelta

import pandas as pd
from bs4 import BeautifulSoup
from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    AutomationCondition,
    MetadataValue,
    Output,
    asset,
    asset_check,
)

from .common import PriceConfig, price_partitions


def apply_partition_filter(context: AssetExecutionContext | None, df: pd.DataFrame) -> pd.DataFrame:
    """Filter the DataFrame against the partition time window."""
    if not context or not context.has_partition_key:
        return df

    start_utc, end_utc = context.partition_time_window
    return df[(df["timestamp"] >= start_utc) & (df["timestamp"] < end_utc)]


@asset(partitions_def=price_partitions, automation_condition=AutomationCondition.eager())
def parsed_electricity_prices(
    context: AssetExecutionContext, raw_entsoe_xml: str, config: PriceConfig
) -> Output[pd.DataFrame]:
    """Parses ENTSO-E API XML response into a DataFrame, accepting only 15-minute resolution."""
    soup = BeautifulSoup(raw_entsoe_xml, "lxml-xml")

    prices = []
    for time_series in soup.find_all("TimeSeries"):
        period = time_series.find("Period")
        if not period or period.resolution.text != "PT15M":
            continue

        start = pd.to_datetime(period.timeInterval.start.text)
        end = pd.to_datetime(period.timeInterval.end.text)
        step = timedelta(minutes=15)

        points_map = {
            int(p.position.text): float(p.find("price.amount").text)
            for p in period.find_all("Point")
            if p.position and p.find("price.amount")
        }

        if not points_map:
            continue

        num_intervals = int((end - start).total_seconds() / step.total_seconds())
        last_price = None
        for pos in range(1, num_intervals + 1):
            if pos in points_map:
                last_price = points_map[pos]
            if last_price is not None:
                prices.append({"timestamp": start + (pos - 1) * step, "price_eur_mwh": last_price})

    if not prices:
        raise ValueError("No price data found in XML")

    df = pd.DataFrame(prices).drop_duplicates(subset=["timestamp"])
    df = apply_partition_filter(context, df)

    return Output(
        value=df,
        metadata={"num_rows": len(df), "preview": MetadataValue.md(df.head(5).to_markdown())},
    )


def validate_full_day_data(
    partition_time_window: tuple[pd.Timestamp, pd.Timestamp], df: pd.DataFrame
) -> AssetCheckResult:
    """Logic to check if the partition contains the expected number of rows (96 for 15m)."""
    start_utc, end_utc = partition_time_window
    expected_rows = int((end_utc - start_utc).total_seconds() / (15 * 60))
    actual_rows = len(df)

    return AssetCheckResult(
        passed=bool(actual_rows >= expected_rows),
        metadata={
            "expected_rows": expected_rows,
            "actual_rows": actual_rows,
        },
    )


@asset_check(asset=parsed_electricity_prices)
def check_full_day_data(context: AssetExecutionContext, parsed_electricity_prices: pd.DataFrame):
    """Checks if the partition contains the expected number of rows (96 for 15m)."""
    return validate_full_day_data(context.partition_time_window, parsed_electricity_prices)
