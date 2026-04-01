SELECT date_trunc(
        'month',
        timestamp AT TIME ZONE 'Europe/Helsinki'
    ) AT TIME ZONE 'Europe/Helsinki' AS bucket_month,
    AVG(price_cent_kwh) AS avg_price_cent_kwh,
    AVG(price_vat_cent_kwh) AS avg_price_vat_cent_kwh
FROM {{ ref('v_electricity_prices') }}
GROUP BY 1
