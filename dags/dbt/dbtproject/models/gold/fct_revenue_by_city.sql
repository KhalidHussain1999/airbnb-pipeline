{{ config(
    materialized = 'table',
    schema       = 'gold'
) }}

WITH listings AS (
    SELECT * FROM {{ ref('silver_listings') }}
),

city_metrics AS (
    SELECT
        city,
        currency,

        -- Volume metrics
        COUNT(DISTINCT listing_id)                              AS total_listings,
        COUNT(DISTINCT host_id)                                 AS total_hosts,

        -- Superhost metrics
        COUNT(DISTINCT CASE
            WHEN host_is_superhost = TRUE
            THEN host_id END)                                   AS superhost_count,

        ROUND(
            COUNT(DISTINCT CASE WHEN host_is_superhost = TRUE THEN host_id END)
            / NULLIF(COUNT(DISTINCT host_id), 0) * 100, 2
        )                                                       AS superhost_pct,

        -- Price metrics in USD
        ROUND(AVG(price_usd), 2)                               AS avg_price_usd,
        ROUND(MIN(price_usd), 2)                               AS min_price_usd,
        ROUND(MAX(price_usd), 2)                               AS max_price_usd,
        ROUND(MEDIAN(price_usd), 2)                            AS median_price_usd,

        -- Rating metrics
        ROUND(AVG(NULLIF(review_scores_rating, 0)), 2)         AS avg_rating,
        ROUND(AVG(NULLIF(review_scores_cleanliness, 0)), 2)    AS avg_cleanliness_score,
        ROUND(AVG(NULLIF(review_scores_location, 0)), 2)       AS avg_location_score,

        -- Stay metrics
        ROUND(AVG(minimum_nights), 1)                          AS avg_minimum_nights,

        -- Instant bookable
        COUNT(DISTINCT CASE
            WHEN instant_bookable = TRUE
            THEN listing_id END)                               AS instant_bookable_count,

        ROUND(
            COUNT(DISTINCT CASE WHEN instant_bookable = TRUE THEN listing_id END)
            / NULLIF(COUNT(DISTINCT listing_id), 0) * 100, 2
        )                                                      AS instant_bookable_pct,

        -- Price category breakdown
        COUNT(CASE WHEN price_category = 'Budget'    THEN 1 END) AS budget_listings,
        COUNT(CASE WHEN price_category = 'Mid-range' THEN 1 END) AS midrange_listings,
        COUNT(CASE WHEN price_category = 'Premium'   THEN 1 END) AS premium_listings,
        COUNT(CASE WHEN price_category = 'Luxury'    THEN 1 END) AS luxury_listings,

        CURRENT_TIMESTAMP()                                    AS updated_at

    FROM listings
    GROUP BY city, currency
)

SELECT * FROM city_metrics
ORDER BY total_listings DESC