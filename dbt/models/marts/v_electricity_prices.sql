SELECT timestamp,
    (price_eur_mwh / 10.0) as price_cent_kwh,
    (price_eur_mwh / 10.0) * {{ var('vat_multiplier') }} as price_vat_cent_kwh,
    created_at
FROM {{ source('public', 'electricity_prices') }}
