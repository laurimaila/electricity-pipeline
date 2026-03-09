import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from dagster import asset, Output, MetadataValue, AssetExecutionContext, DailyPartitionsDefinition, AutomationCondition, Config, RetryPolicy
from ..resources import ApiResource, PostgresResource
from sqlalchemy import text

class PriceConfig(Config):
    vat_percentage: float = 25.5 # Can be overridden with DAGSTER_CONFIG_price_config

# Partitions starting from beginning of 2026
prices_partitions = DailyPartitionsDefinition(start_date="2026-01-01", end_offset=1)

# Condition for polling
entsoe_automation_condition = AutomationCondition.missing() & AutomationCondition.on_cron("*/15 12,13,14 * * *")

@asset(
    partitions_def=prices_partitions,
    automation_condition=entsoe_automation_condition,
    retry_policy=RetryPolicy(max_retries=3, delay=300)
)
def raw_entsoe_xml(context: AssetExecutionContext, entsoe: ApiResource):
    """Fetches raw XML for the specific partitioned date."""
    partition_date_str = context.partition_key
    partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d")

    if entsoe.api_key == "mock" or not entsoe.api_key:
        start_iso = partition_date.strftime("%Y-%m-%dT00:00Z")
        end_iso = (partition_date + timedelta(days=1)).strftime("%Y-%m-%dT00:00Z")
        return f"""<?xml version="1.0" encoding="UTF-8"?>
        <Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0">
            <TimeSeries>
                <Period>
                    <timeInterval><start>{start_iso}</start><end>{end_iso}</end></timeInterval>
                    <resolution>PT15M</resolution>
                    <Point><position>1</position><price.amount>50.5</price.amount></Point>
                    <Point><position>2</position><price.amount>48.2</price.amount></Point>
                    <Point><position>3</position><price.amount>45.0</price.amount></Point>
                    <Point><position>4</position><price.amount>44.5</price.amount></Point>
                </Period>
            </TimeSeries>
        </Publication_MarketDocument>
        """

    start_str = partition_date.strftime("%Y%m%d0000")
    end_str = (partition_date + timedelta(days=1)).strftime("%Y%m%d0000")

    context.log.info(f"Fetching prices for {partition_date_str}")
    xml_data = entsoe.fetch_day_ahead_prices("10YFI-1--------U", start_str, end_str)
    return xml_data

@asset(
    partitions_def=prices_partitions,
    automation_condition=AutomationCondition.eager()
)
def parsed_electricity_prices(raw_entsoe_xml: str, config: PriceConfig) -> Output[pd.DataFrame]:
    """Parses ENTSO-E XML response into a DataFrame."""
    root = ET.fromstring(raw_entsoe_xml)
    namespace = ""
    if '}' in root.tag:
        namespace = root.tag.split('}')[0] + "}"

    prices = []
    for time_series in root.findall(f'.//{namespace}TimeSeries'):
        period = time_series.find(f'{namespace}Period')
        if period is None:
            continue

        start_elem = period.find(f"{namespace}timeInterval/{namespace}start")
        if start_elem is None or start_elem.text is None:
            continue

        start_time = pd.to_datetime(start_elem.text)
        res_elem = period.find(f"{namespace}resolution")
        if res_elem is None or res_elem.text is None:
            continue
        resolution = res_elem.text

        for point in period.findall(f'{namespace}Point'):
            position_elem = point.find(f'{namespace}position')
            price_elem = point.find(f'{namespace}price.amount')

            if position_elem is None or position_elem.text is None or price_elem is None or price_elem.text is None:
                continue

            position = int(position_elem.text)
            price_mwh = float(price_elem.text)

            ts = start_time + (timedelta(hours=position-1) if resolution == 'PT60M' else timedelta(minutes=15*(position-1)))

            prices.append({
                'timestamp': ts,
                'price_eur_mwh': price_mwh
            })

    if not prices:
        raise ValueError("No price data found in XML")

    df = pd.DataFrame(prices).drop_duplicates(subset=['timestamp'])
    return Output(value=df, metadata={"num_rows": len(df), "preview": MetadataValue.md(df.head(5).to_markdown())})

@asset(
    partitions_def=prices_partitions,
    automation_condition=AutomationCondition.eager()
)
def db_electricity_prices(context: AssetExecutionContext, parsed_electricity_prices: pd.DataFrame, postgres: PostgresResource, config: PriceConfig):
    """Saves the electricity prices to the PostgreSQL database and maintains a calculated view."""
    engine = postgres.get_engine()
    vat_multiplier = 1 + (config.vat_percentage / 100.0)

    with engine.begin() as conn:
        # Create the base table if it doesn't exist
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS electricity_prices (
                timestamp TIMESTAMP PRIMARY KEY,
                price_eur_mwh NUMERIC NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # CREATE OR REPLACE VIEW ensures the logic stays updated if config changes
        conn.execute(text(f"""
            CREATE OR REPLACE VIEW v_electricity_prices AS
            SELECT
                timestamp,
                price_eur_mwh,
                (price_eur_mwh / 10.0) as price_cent_kwh,
                (price_eur_mwh / 10.0) * {vat_multiplier} as price_vat_cent_kwh,
                created_at
            FROM electricity_prices;
        """))

        # Upsert the raw data
        for _, row in parsed_electricity_prices.iterrows():
            conn.execute(text("""
                INSERT INTO electricity_prices (timestamp, price_eur_mwh)
                VALUES (:timestamp, :price_mwh)
                ON CONFLICT (timestamp) DO UPDATE SET
                    price_eur_mwh = EXCLUDED.price_eur_mwh
            """), {
                "timestamp": row['timestamp'],
                "price_mwh": row['price_eur_mwh']
            })

    # Latest timestamp for Dagster metadata
    latest_ts = parsed_electricity_prices['timestamp'].max()

    return Output(
        value=None,
        metadata={
            "num_rows": len(parsed_electricity_prices),
            "latest_timestamp": MetadataValue.text(latest_ts.strftime("%Y-%m-%d %H:%M:%S UTC"))
        }
    )
