from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from dagster import build_asset_context

from pipeline.assets.spot_prices import (
    apply_partition_filter,
    parsed_electricity_prices,
)

API_RES_MOCK_PATH = Path(__file__).parent / "fixtures" / "api_res_mock.xml"


@pytest.fixture
def sample_xml() -> str:
    return API_RES_MOCK_PATH.read_text()


@pytest.fixture
def sample_df(sample_xml) -> pd.DataFrame:
    mock_entsoe = MagicMock()
    mock_entsoe.fetch_day_ahead_prices.return_value = sample_xml

    ctx = build_asset_context(
        resources={"entsoe": mock_entsoe},
        partition_key="2026-03-14",
    )
    result = parsed_electricity_prices(context=ctx, config=None)
    return result.value


def test_parses_one_time_series(sample_df):
    """One TimeSeries in the XML should be parsed, for 96 price rows."""
    assert len(sample_df) == 96


def test_correct_columns(sample_df):
    assert set(sample_df.columns) == {"timestamp", "price_eur_mwh"}


def test_no_duplicate_timestamps(sample_df):
    assert not sample_df["timestamp"].duplicated().any()


def test_partition_filter(sample_df):
    """Only rows within the partition window should be returned."""
    ctx = MagicMock()
    ctx.has_partition_key = True
    ctx.partition_time_window = (
        pd.Timestamp("2026-03-13T23:00:00Z"),
        pd.Timestamp("2026-03-14T23:00:00Z"),
    )

    result = apply_partition_filter(ctx, sample_df)
    assert len(result) == 96
    assert result["timestamp"].min() == pd.Timestamp("2026-03-13T23:00:00Z")
    assert result["timestamp"].max() == pd.Timestamp("2026-03-14T22:45:00Z")
