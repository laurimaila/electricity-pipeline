from pathlib import Path
from typing import cast

import pandas as pd
import pytest
from dagster import Output

from pipeline.assets.prices import PriceConfig, parsed_electricity_prices


@pytest.fixture
def mock_xml() -> str:
    """Fixture to load the mock XML data from a file."""
    path = Path(__file__).parent / "mock_prices.xml"
    return path.read_text()


def test_parsed_electricity_prices(mock_xml: str):
    """Test parsing the mock XML data using the mock_xml fixture."""
    config = PriceConfig()
    output = cast(Output, parsed_electricity_prices(mock_xml, config))
    df = output.value

    assert isinstance(df, pd.DataFrame)
    # 2 days * 96 points/day = 192 points
    assert len(df) == 192

    # First point (2026-03-13 23:00)
    assert df.iloc[0]["price_eur_mwh"] == 4.99

    # Check gap filling (position 2, 3, 4 should take position 1's price)
    assert df.iloc[1]["price_eur_mwh"] == 4.99
    assert df.iloc[2]["price_eur_mwh"] == 4.99
    assert df.iloc[3]["price_eur_mwh"] == 4.99

    # Position 5 (2026-03-14 00:00)
    assert df.iloc[4]["price_eur_mwh"] == 4.84

    # Check if first timestamp is parsed correctly
    assert df.iloc[0]["timestamp"].strftime("%Y-%m-%dT%H:%M:%SZ") == "2026-03-13T23:00:00Z"
