{{ config(
    materialized = 'table',
    schema       = 'gold'
) }}

WITH reviews AS (
    SELECT * FROM {{ ref('silver_reviews') }}
),

listings AS (
    SELECT
        listing_id,
        city,
        room_type,
        property_type,
        price_category,
        host_type
    FROM {{ ref('silver_listings') }}
),

joined AS (
    SELECT
        r.review_id,
        r.listing_id,
        r.reviewer_id,
        r.review_date,
        r.month_name,
        r.month_number,
        r.year_number,
        r.quarter_number,
        r.season,
        r.day_of_week,
        l.city,
        l.room_type,
        l.property_type,
        l.price_category,
        l.host_type
    FROM reviews r
    INNER JOIN listings l
        ON r.listing_id = l.listing_id
)

SELECT
    city,
    year_number,
    month_number,
    month_name,
    quarter_number,
    season,
    room_type,
    price_category,

    -- Volume metrics
    COUNT(review_id)                        AS total_reviews,
    COUNT(DISTINCT listing_id)              AS active_listings,
    COUNT(DISTINCT reviewer_id)             AS unique_reviewers,

    -- Average reviews per listing that month
    ROUND(
        COUNT(review_id)
        / NULLIF(COUNT(DISTINCT listing_id), 0)
    , 2)                                    AS avg_reviews_per_listing,

    CURRENT_TIMESTAMP()                     AS updated_at

FROM joined
GROUP BY
    city,
    year_number,
    month_number,
    month_name,
    quarter_number,
    season,
    room_type,
    price_category

ORDER BY
    city,
    year_number,
    month_number