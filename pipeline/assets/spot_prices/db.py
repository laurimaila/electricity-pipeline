import pandas as pd
from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    Backoff,
    MetadataValue,
    Output,
    RetryPolicy,
    asset,
    job,
    op,
)
from sqlalchemy import text

from ...resources import PostgresResource
from .common import PriceConfig, price_partitions


@op
def setup_db_op(postgres: PostgresResource, config: PriceConfig):
    """Sets up the Postgres schema and views."""
    engine = postgres.get_engine(pool_size=1, max_overflow=0)
    vat_multiplier = 1 + (config.vat_percentage / 100.0)

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

            conn.execute(
                text(f"""
                CREATE OR REPLACE VIEW v_electricity_prices AS
                SELECT
                    timestamp,
                    (price_eur_mwh / 10.0) as price_cent_kwh,
                    (price_eur_mwh / 10.0) * {vat_multiplier} as price_vat_cent_kwh,
                    created_at
                FROM electricity_prices;
            """)
            )

            conn.execute(
                text(f"""
                CREATE OR REPLACE VIEW v_daily_electricity_prices AS
                SELECT
                    date_trunc('day', timestamp AT TIME ZONE 'Europe/Helsinki')
                        AT TIME ZONE 'Europe/Helsinki' AS bucket_day,
                    AVG(price_eur_mwh / 10.0) AS avg_price_cent_kwh,
                    AVG((price_eur_mwh / 10.0) * {vat_multiplier}) AS avg_price_vat_cent_kwh
                FROM electricity_prices
                GROUP BY bucket_day;
            """)
            )

            conn.execute(
                text(f"""
                CREATE OR REPLACE VIEW v_monthly_electricity_prices AS
                SELECT
                    date_trunc('month', timestamp AT TIME ZONE 'Europe/Helsinki')
                        AT TIME ZONE 'Europe/Helsinki' AS bucket_month,
                    AVG(price_eur_mwh / 10.0) AS avg_price_cent_kwh,
                    AVG((price_eur_mwh / 10.0) * {vat_multiplier}) AS avg_price_vat_cent_kwh
                FROM electricity_prices
                GROUP BY bucket_month;
            """)
            )

    finally:
        engine.dispose()


@job
def db_setup_job():
    """Job to set up the database schema once."""
    setup_db_op()


@asset(
    partitions_def=price_partitions,
    automation_condition=AutomationCondition.eager(),
    retry_policy=RetryPolicy(
        max_retries=5,
        delay=10,
        backoff=Backoff.EXPONENTIAL,
    ),
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
