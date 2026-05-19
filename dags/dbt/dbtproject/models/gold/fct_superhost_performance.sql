{{ config(
    materialized = 'table',
    schema       = 'gold'
) }}

WITH listings AS (
    SELECT * FROM {{ ref('silver_listings') }}
)

SELECT
    city,
    host_type,
    host_is_superhost,

    -- Volume
    COUNT(DISTINCT host_id)                                     AS total_hosts,
    COUNT(DISTINCT listing_id)                                  AS total_listings,

    -- Pricing
    ROUND(AVG(price_usd), 2)                                   AS avg_price_usd,
    ROUND(MEDIAN(price_usd), 2)                                AS median_price_usd,
    ROUND(MIN(price_usd), 2)                                   AS min_price_usd,
    ROUND(MAX(price_usd), 2)                                   AS max_price_usd,

    -- Ratings (exclude zeros — listings with no reviews)
    ROUND(AVG(NULLIF(review_scores_rating, 0)), 2)             AS avg_rating,
    ROUND(AVG(NULLIF(review_scores_cleanliness, 0)), 2)        AS avg_cleanliness,
    ROUND(AVG(NULLIF(review_scores_value, 0)), 2)              AS avg_value_score,
    ROUND(AVG(NULLIF(review_scores_location, 0)), 2)           AS avg_location_score,
    ROUND(AVG(NULLIF(review_scores_communication, 0)), 2)      AS avg_communication_score,

    -- Host responsiveness
    ROUND(AVG(host_response_rate_pct), 2)                      AS avg_response_rate_pct,
    ROUND(AVG(host_acceptance_rate_pct), 2)                    AS avg_acceptance_rate_pct,

    -- Experience
    ROUND(AVG(host_years_active), 1)                           AS avg_host_years_active,
    ROUND(AVG(host_listings_count), 1)                         AS avg_listings_per_host,

    -- Booking flexibility
    ROUND(AVG(minimum_nights), 1)                              AS avg_minimum_nights,

    -- Instant bookable rate
    ROUND(
        COUNT(CASE WHEN instant_bookable = TRUE THEN 1 END)
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                                          AS instant_bookable_pct,

    -- Price category breakdown
    COUNT(CASE WHEN price_category = 'Budget'    THEN 1 END)   AS budget_count,
    COUNT(CASE WHEN price_category = 'Mid-range' THEN 1 END)   AS midrange_count,
    COUNT(CASE WHEN price_category = 'Premium'   THEN 1 END)   AS premium_count,
    COUNT(CASE WHEN price_category = 'Luxury'    THEN 1 END)   AS luxury_count,

    CURRENT_TIMESTAMP()                                        AS updated_at

FROM listings
GROUP BY city, host_type, host_is_superhost
ORDER BY city, host_is_superhost DESC