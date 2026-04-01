import pandas as pd
from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    MetadataValue,
    Output,
    asset,
)
from sqlalchemy import text

from ..resources import PostgresResource
from .common import daily_partitions


@asset(
    partitions_def=daily_partitions,
    automation_condition=AutomationCondition.eager(),
)
def db_electricity_prices(
    context: AssetExecutionContext,
    parsed_electricity_prices: pd.DataFrame,
    postgres: PostgresResource,
):
    """Saves price data to Postgres database."""
    engine = postgres.get_engine(pool_size=1, max_overflow=0)

    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                CREATE TABLE IF NOT EXISTS electricity_prices (
                timestamp TIMESTAMPTZ PRIMARY KEY,
                price_eur_mwh NUMERIC NOT NULL,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
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
