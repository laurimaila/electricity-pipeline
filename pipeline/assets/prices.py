import os
import xml.etree.ElementTree as ET
from datetime import timedelta, timezone

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    Config,
    DailyPartitionsDefinition,
    MetadataValue,
    Output,
    RetryPolicy,
    asset,
)
from sqlalchemy import text

from ..resources import ApiResource, PostgresResource


class PriceConfig(Config):
    vat_percentage: float = float(os.getenv("VAT_PERCENTAGE", 25.5))


# 24h partitions starting from beginning of november 2025, UTC+2
prices_partitions = DailyPartitionsDefinition(
    start_date="2025-10-01", timezone="Europe/Helsinki", end_offset=2
)

entsoe_automation_condition = AutomationCondition.missing() & AutomationCondition.on_cron(
    "*/15 * * * *"
)


@asset(
    partitions_def=prices_partitions,
    automation_condition=entsoe_automation_condition,
    retry_policy=RetryPolicy(max_retries=5, delay=900),
)
def raw_entsoe_xml(context: AssetExecutionContext, entsoe: ApiResource):
    """Fetches raw XML for the specific partitioned date in Finnish time blocks."""
    # Get the time window for the partition
    start_dt, end_dt = context.partition_time_window

    # Convert to UTC for ENTSO-E API call
    start_utc = start_dt.astimezone(timezone.utc)
    end_utc = end_dt.astimezone(timezone.utc)

    start_str = start_utc.strftime("%Y%m%d%H%M")
    end_str = end_utc.strftime("%Y%m%d%H%M")

    context.log.info(f"Fetching prices for {context.partition_key} ({start_dt} to {end_dt})")
    xml_data = entsoe.fetch_day_ahead_prices("10YFI-1--------U", start_str, end_str)
    return xml_data


@asset(partitions_def=prices_partitions, automation_condition=AutomationCondition.eager())
def parsed_electricity_prices(raw_entsoe_xml: str, config: PriceConfig) -> Output[pd.DataFrame]:
    """Parses ENTSO-E XML response into a DataFrame, accepting only 15-minute resolution."""
    root = ET.fromstring(raw_entsoe_xml)
    namespace = ""
    if "}" in root.tag:
        namespace = root.tag.split("}")[0] + "}"

    prices = []
    for time_series in root.findall(f".//{namespace}TimeSeries"):
        period = time_series.find(f"{namespace}Period")
        if period is None:
            continue

        start_elem = period.find(f"{namespace}timeInterval/{namespace}start")
        end_elem = period.find(f"{namespace}timeInterval/{namespace}end")
        if (
            start_elem is None
            or start_elem.text is None
            or end_elem is None
            or end_elem.text is None
        ):
            continue

        start_time = pd.to_datetime(start_elem.text)
        end_time = pd.to_datetime(end_elem.text)

        res_elem = period.find(f"{namespace}resolution")
        if res_elem is None or res_elem.text is None:
            continue
        resolution = res_elem.text

        # Only accept 15-minute resolution as requested
        if resolution == "PT15M":
            step = timedelta(minutes=15)
        else:
            continue

        # Parse existing points into a map
        points_map = {}
        for point in period.findall(f"{namespace}Point"):
            pos_elem = point.find(f"{namespace}position")
            price_elem = point.find(f"{namespace}price.amount")
            if (
                pos_elem is not None
                and pos_elem.text is not None
                and price_elem is not None
                and price_elem.text is not None
            ):
                points_map[int(pos_elem.text)] = float(price_elem.text)

        if not points_map:
            continue

        # Calculate number of expected points
        # Using .total_seconds() to avoid type mismatch issues in division
        num_intervals = int((end_time - start_time).total_seconds() / step.total_seconds())

        last_price = None
        for pos in range(1, num_intervals + 1):
            if pos in points_map:
                last_price = points_map[pos]

            if last_price is not None:
                ts = start_time + (pos - 1) * step
                prices.append({"timestamp": ts, "price_eur_mwh": last_price})

    if not prices:
        raise ValueError("No price data found in XML")

    df = pd.DataFrame(prices).drop_duplicates(subset=["timestamp"])
    return Output(
        value=df,
        metadata={"num_rows": len(df), "preview": MetadataValue.md(df.head(5).to_markdown())},
    )


@asset(
    partitions_def=prices_partitions,
    automation_condition=AutomationCondition.eager(),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
)
def db_electricity_prices(
    context: AssetExecutionContext,
    parsed_electricity_prices: pd.DataFrame,
    postgres: PostgresResource,
    config: PriceConfig,
):
    """Saves the electricity prices to the PostgreSQL database and maintains a calculated view."""
    engine = postgres.get_engine()
    vat_multiplier = 1 + (config.vat_percentage / 100.0)

    with engine.begin() as conn:
        # Create the base table if it doesn't exist
        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS electricity_prices (
                timestamp TIMESTAMP PRIMARY KEY,
                price_eur_mwh NUMERIC NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        # CREATE OR REPLACE VIEW ensures the logic stays updated if config changes
        conn.execute(
            text(f"""
            CREATE OR REPLACE VIEW v_electricity_prices AS
            SELECT
                timestamp,
                price_eur_mwh,
                (price_eur_mwh / 10.0) as price_cent_kwh,
                (price_eur_mwh / 10.0) * {vat_multiplier} as price_vat_cent_kwh,
                created_at
            FROM electricity_prices;
        """)
        )

        # Prepare records for database insertion
        records = parsed_electricity_prices.to_dict("records")

        if records:
            conn.execute(
                text("""
                INSERT INTO electricity_prices (timestamp, price_eur_mwh)
                VALUES (:timestamp, :price_eur_mwh)
                ON CONFLICT (timestamp) DO UPDATE SET
                    price_eur_mwh = EXCLUDED.price_eur_mwh
                """),
                records,
            )

    # Latest timestamp for Dagster metadata
    latest_ts = parsed_electricity_prices["timestamp"].max()

    return Output(
        value=None,
        metadata={
            "num_rows": len(parsed_electricity_prices),
            "latest_timestamp": MetadataValue.text(latest_ts.strftime("%Y-%m-%d %H:%M:%S UTC")),
        },
    )
