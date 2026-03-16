import pandas as pd
from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    MetadataValue,
    Output,
    RetryPolicy,
    asset,
)
from sqlalchemy import text

from ...resources import PostgresResource
from .common import PriceConfig, price_partitions


@asset(
    partitions_def=price_partitions,
    automation_condition=AutomationCondition.eager(),
    retry_policy=RetryPolicy(max_retries=5, delay=300),
)
def db_electricity_prices(
    context: AssetExecutionContext,
    parsed_electricity_prices: pd.DataFrame,
    postgres: PostgresResource,
    config: PriceConfig,
):
    """Saves price data to Postgres database and maintains a calculated view."""
    engine = postgres.get_engine(pool_size=1, max_overflow=0)
    vat_multiplier = 1 + (config.vat_percentage / 100.0)

    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                CREATE TABLE IF NOT EXISTS electricity_prices (
                    timestamp TIMESTAMP PRIMARY KEY,
                    price_eur_mwh NUMERIC NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            )

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
    finally:
        engine.dispose()

    # Latest timestamp for Dagster metadata
    latest_ts = parsed_electricity_prices["timestamp"].max()

    return Output(
        value=None,
        metadata={
            "num_rows": len(parsed_electricity_prices),
            "latest_timestamp": MetadataValue.text(latest_ts.strftime("%Y-%m-%d %H:%M:%S UTC")),
        },
    )
