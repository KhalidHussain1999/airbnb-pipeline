{{ config(
    materialized = 'view',
    schema       = 'silver'
) }}

WITH base AS (
    SELECT * FROM {{ ref('bronze_reviews') }}
    WHERE review_date IS NOT NULL
)

SELECT
    review_id,
    listing_id,
    reviewer_id,
    review_date,

    -- Break date into parts for seasonal analysis
    DATE_TRUNC('month', review_date)        AS review_month,
    DATE_TRUNC('year',  review_date)        AS review_year,
    DAYOFWEEK(review_date)                  AS day_of_week,
    MONTHNAME(review_date)                  AS month_name,
    MONTH(review_date)                      AS month_number,
    YEAR(review_date)                       AS year_number,
    QUARTER(review_date)                    AS quarter_number,

    -- Season based on month
    CASE
        WHEN MONTH(review_date) IN (12, 1, 2)  THEN 'Winter'
        WHEN MONTH(review_date) IN (3, 4, 5)   THEN 'Spring'
        WHEN MONTH(review_date) IN (6, 7, 8)   THEN 'Summer'
        ELSE 'Autumn'
    END                                     AS season,

    loaded_at

FROM base