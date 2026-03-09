import pandas as pd
from pipeline.assets.prices import parsed_electricity_prices
from dagster import Output

def test_parsed_electricity_prices():
    # Arrange
    mock_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0">
        <TimeSeries>
            <Period>
                <timeInterval>
                    <start>2024-03-08T23:00Z</start>
                    <end>2024-03-09T23:00Z</end>
                </timeInterval>
                <resolution>PT60M</resolution>
                <Point><position>1</position><price.amount>50.5</price.amount></Point>
                <Point><position>2</position><price.amount>48.2</price.amount></Point>
                <Point><position>3</position><price.amount>45.0</price.amount></Point>
            </Period>
        </TimeSeries>
    </Publication_MarketDocument>
    """

    # Act
    output = parsed_electricity_prices(mock_xml)
    assert isinstance(output, Output)
    df = output.value

    # Assert
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert df.iloc[0]['price_eur_mwh'] == 50.5
    assert df.iloc[1]['price_eur_mwh'] == 48.2
    assert df.iloc[2]['price_eur_mwh'] == 45.0
    # Check if first timestamp is parsed correctly (2024-03-08 23:00:00 UTC)
    assert df.iloc[0]['timestamp'].strftime("%Y-%m-%dT%H:%M:%SZ") == "2024-03-08T23:00:00Z"
