{{ config(
    materialized = 'view',
    schema       = 'silver'
) }}

WITH base AS (
    -- Must read from bronze_listings NOT bronze_reviews
    SELECT * FROM {{ ref('bronze_listings') }}
    WHERE listing_id IS NOT NULL
),

currency_mapped AS (
    SELECT
        *,
        CASE city
            WHEN 'New York'       THEN 'USD'
            WHEN 'Paris'          THEN 'EUR'
            WHEN 'Rome'           THEN 'EUR'
            WHEN 'Sydney'         THEN 'AUD'
            WHEN 'Bangkok'        THEN 'THB'
            WHEN 'Cape Town'      THEN 'ZAR'
            WHEN 'Mexico City'    THEN 'MXN'
            WHEN 'Istanbul'       THEN 'TRY'
            WHEN 'Hong Kong'      THEN 'HKD'
            WHEN 'Rio de Janeiro' THEN 'BRL'
            ELSE 'USD'
        END AS currency,

        CASE city
            WHEN 'New York'       THEN price_raw * 1.00
            WHEN 'Paris'          THEN price_raw * 1.08
            WHEN 'Rome'           THEN price_raw * 1.08
            WHEN 'Sydney'         THEN price_raw * 0.65
            WHEN 'Bangkok'        THEN price_raw * 0.028
            WHEN 'Cape Town'      THEN price_raw * 0.054
            WHEN 'Mexico City'    THEN price_raw * 0.058
            WHEN 'Istanbul'       THEN price_raw * 0.031
            WHEN 'Hong Kong'      THEN price_raw * 0.128
            WHEN 'Rio de Janeiro' THEN price_raw * 0.20
            ELSE price_raw
        END AS price_usd

    FROM base
),

cleaned AS (
    SELECT
        listing_id,
        listing_name,
        host_id,
        host_since_date,
        host_location,
        host_listings_count,
        host_is_superhost,
        host_identity_verified,
        host_has_profile_pic,
        instant_bookable,
        COALESCE(host_response_time, 'Unknown')             AS host_response_time,
        COALESCE(host_response_rate_pct, 0)                 AS host_response_rate_pct,
        COALESCE(host_acceptance_rate_pct, 0)               AS host_acceptance_rate_pct,
        city,
        COALESCE(neighbourhood, 'Unknown')                  AS neighbourhood,
        COALESCE(district, 'Unknown')                       AS district,
        latitude,
        longitude,
        property_type,
        room_type,
        COALESCE(accommodates, 1)                           AS accommodates,
        COALESCE(bedrooms, 1)                               AS bedrooms,
        amenities,
        currency,
        price_raw,
        ROUND(price_usd, 2)                                 AS price_usd,
        minimum_nights,
        maximum_nights,
        COALESCE(review_scores_rating, 0)                   AS review_scores_rating,
        COALESCE(review_scores_accuracy, 0)                 AS review_scores_accuracy,
        COALESCE(review_scores_cleanliness, 0)              AS review_scores_cleanliness,
        COALESCE(review_scores_checkin, 0)                  AS review_scores_checkin,
        COALESCE(review_scores_communication, 0)            AS review_scores_communication,
        COALESCE(review_scores_location, 0)                 AS review_scores_location,
        COALESCE(review_scores_value, 0)                    AS review_scores_value,
        DATEDIFF('year', host_since_date, CURRENT_DATE())   AS host_years_active,
        CASE
            WHEN minimum_nights <= 1  THEN 'Flexible'
            WHEN minimum_nights <= 7  THEN 'Weekly'
            WHEN minimum_nights <= 30 THEN 'Monthly'
            ELSE 'Long Stay'
        END                                                 AS stay_type,
        CASE
            WHEN price_usd < 50   THEN 'Budget'
            WHEN price_usd < 150  THEN 'Mid-range'
            WHEN price_usd < 300  THEN 'Premium'
            ELSE 'Luxury'
        END                                                 AS price_category,
        CASE
            WHEN host_is_superhost = TRUE THEN 'Superhost'
            ELSE 'Regular Host'
        END                                                 AS host_type,
        loaded_at

    FROM currency_mapped
    WHERE price_raw > 0
    AND price_raw < 10000
    AND accommodates > 0
    AND city IN (
      'New York', 'Paris', 'Rome', 'Sydney',
      'Bangkok', 'Cape Town', 'Mexico City',
      'Istanbul', 'Hong Kong', 'Rio de Janeiro'
  )
)

SELECT * FROM cleaned