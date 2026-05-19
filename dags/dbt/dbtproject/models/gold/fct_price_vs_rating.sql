{{ config(
    materialized = 'table',
    schema       = 'gold'
) }}

WITH listings AS (
    SELECT * FROM {{ ref('silver_listings') }}
)

SELECT
    city,
    room_type,
    property_type,
    price_category,

    -- Volume
    COUNT(DISTINCT listing_id)                                  AS total_listings,
    COUNT(DISTINCT host_id)                                     AS total_hosts,

    -- Price metrics
    ROUND(AVG(price_usd), 2)                                   AS avg_price_usd,
    ROUND(MEDIAN(price_usd), 2)                                AS median_price_usd,
    ROUND(MIN(price_usd), 2)                                   AS min_price_usd,
    ROUND(MAX(price_usd), 2)                                   AS max_price_usd,

    -- Rating metrics (exclude zeros)
    ROUND(AVG(NULLIF(review_scores_rating, 0)), 2)             AS avg_rating,
    ROUND(AVG(NULLIF(review_scores_cleanliness, 0)), 2)        AS avg_cleanliness,
    ROUND(AVG(NULLIF(review_scores_value, 0)), 2)              AS avg_value_score,
    ROUND(AVG(NULLIF(review_scores_location, 0)), 2)           AS avg_location_score,

    -- Accommodation metrics
    ROUND(AVG(accommodates), 1)                                AS avg_accommodates,
    ROUND(AVG(bedrooms), 1)                                    AS avg_bedrooms,
    ROUND(AVG(minimum_nights), 1)                              AS avg_minimum_nights,

    -- Host quality
    ROUND(
        COUNT(CASE WHEN host_is_superhost = TRUE THEN 1 END)
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                                          AS superhost_pct,

    ROUND(AVG(host_response_rate_pct), 2)                      AS avg_response_rate,

    -- Instant bookable rate
    ROUND(
        COUNT(CASE WHEN instant_bookable = TRUE THEN 1 END)
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                                          AS instant_bookable_pct,

    CURRENT_TIMESTAMP()                                        AS updated_at

FROM listings
WHERE price_usd > 0
  AND review_scores_rating > 0
GROUP BY
    city,
    room_type,
    property_type,
    price_category

ORDER BY
    city,
    price_category,
    room_type