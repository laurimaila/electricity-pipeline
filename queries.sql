-- Test queries for SQLTools
-- @block Query prices between two timestamps
SELECT
    timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Helsinki' as timestamp_fi,
    price_eur_mwh,
    ROUND(price_cent_kwh_vat, 2) as price_cent_kwh_vat,
    created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Helsinki' as created_at_fi
FROM v_electricity_prices
WHERE timestamp >= '2026-03-09 00:00:00'
  AND timestamp <  '2026-03-10 00:00:00'
ORDER BY timestamp ASC;

-- @block Query future prices
SELECT
    timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Helsinki' as timestamp_fi,
    price_eur_mwh,
    ROUND(price_cent_kwh_vat, 2) as price_cent_kwh_vat,
    created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Helsinki' as created_at_fi
FROM v_electricity_prices
WHERE timestamp >= CURRENT_DATE
ORDER BY timestamp ASC;

-- @block Query 10 cheapest future intervals
SELECT
    timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Helsinki' as timestamp_fi,
    ROUND(price_cent_kwh_vat, 2) as price_cent_kwh_vat
FROM v_electricity_prices
WHERE timestamp >= CURRENT_DATE
ORDER BY price_cent_kwh_vat ASC
LIMIT 10;
