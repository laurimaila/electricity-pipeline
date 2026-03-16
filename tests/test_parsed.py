from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from pipeline.assets.spot_prices import (
    apply_partition_filter,
    parsed_electricity_prices,
    validate_full_day_data,
)

API_RES_MOCK_PATH = Path(__file__).parent / "fixtures" / "api_res_mock.xml"


@pytest.fixture
def sample_xml() -> str:
    return API_RES_MOCK_PATH.read_text()


@pytest.fixture
def sample_df(sample_xml) -> pd.DataFrame:
    """Parsed df from the mock XML, no context."""

    result = parsed_electricity_prices(context=None, raw_entsoe_xml=sample_xml, config=None)
    return result.value


def test_parses_two_time_series(sample_df):
    """Both TimeSeries in the XML should be parsed, 96 rows each = 192 total."""
    assert len(sample_df) == 192


def test_correct_columns(sample_df):
    assert set(sample_df.columns) == {"timestamp", "price_eur_mwh"}


def test_check_full_day_data_passes(sample_df):
    partition_window = (
        pd.Timestamp("2026-03-13T23:00:00Z"),
        pd.Timestamp("2026-03-14T23:00:00Z"),
    )
    df = sample_df[
        (sample_df["timestamp"] >= partition_window[0])
        & (sample_df["timestamp"] < partition_window[1])
    ]
    res = validate_full_day_data(partition_window, df)
    assert res.passed is True
    assert res.metadata["actual_rows"].value == 96


def test_check_full_day_data_fails_on_incomplete(sample_df):
    partition_window = (
        pd.Timestamp("2026-03-13T23:00:00Z"),
        pd.Timestamp("2026-03-14T23:00:00Z"),
    )
    df = sample_df.iloc[:90]
    res = validate_full_day_data(partition_window, df)
    assert res.passed is False
    assert res.metadata["actual_rows"].value == 90


def test_no_duplicate_timestamps(sample_df):
    assert not sample_df["timestamp"].duplicated().any()


def test_forward_fill(sample_df):
    """Positions 2-4 are missing in TimeSeries 1 — they should be forward-filled from pos 1."""
    first_four = sample_df[sample_df["timestamp"] < pd.Timestamp("2026-03-13T23:01:00Z")]
    assert (first_four["price_eur_mwh"] == 4.99).all()


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
