-- Test queries for SQLTools
-- @block Query prices between two timestamps
SELECT timestamp,
    price_eur_mwh,
    ROUND(price_vat_cent_kwh, 2) as price_vat_cent_kwh,
    created_at
FROM v_electricity_prices
WHERE timestamp >= '2026-03-09 00:00:00'
    AND timestamp < '2026-03-10 00:00:00'
ORDER BY timestamp ASC;
-- @block Query future prices
SELECT timestamp,
    price_eur_mwh,
    ROUND(price_vat_cent_kwh, 2) as price_vat_cent_kwh,
    created_at
FROM v_electricity_prices
WHERE timestamp >= CURRENT_DATE
ORDER BY timestamp ASC;
-- @block Query 10 cheapest future intervals
SELECT timestamp,
    ROUND(price_vat_cent_kwh, 2) as price_vat_cent_kwh
FROM v_electricity_prices
WHERE timestamp >= CURRENT_DATE
ORDER BY price_vat_cent_kwh ASC
LIMIT 10;
